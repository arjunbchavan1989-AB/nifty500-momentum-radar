#!/usr/bin/env python3
"""
Nifty 500 Momentum Radar - daily data pipeline.

Universe: Nifty 500 constituents.
Filters: close > EMA50 AND close > EMA200 AND 30-day avg traded value (close x volume) >= 3 crore INR.
Signals: latest-day volume >= 2x 50-day avg; abs(day change) >= 4%.
Catalysts: Google News RSS (last 7 days) classified by keywords.

Output: data/momentum.json
"""
import argparse
import csv
import json
import re
import sys
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import yfinance as yf

ROOT = Path(__file__).resolve().parent

# --- Config -----------------------------------------------------------------
AVG_TURNOVER_30_MIN = 30_000_000  # 3 crore INR average daily traded value (close x volume)
VOL_SPIKE_RATIO = 2.0           # vs 50-day avg volume
MOVE_PCT_MIN = 4.0              # abs day change %
NEWS_WINDOW_DAYS = 7
CHUNK = 80

NSE_CSV = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"
MIRROR_CSV = "https://raw.githubusercontent.com/varunsrik/nifty_dashboard/main/data/nifty_500_constituents.csv"

CATALYST_RULES = {
    "orderWin": [
        "order win", "wins order", "win order", "bagged", "bags order", "receives order",
        "received order", "receives work order", "secures order", "secured order", "secure order",
        "work order", "purchase order", "order worth", "contract worth", "contract from",
        "letter of award", "loa", "new order", "order book", "empanel", "empanelled",
        "contract win", "wins contract", "bags contract",
    ],
    "launch": [
        "launch", "unveil", "roll out", "rolls out", "rolled out", "introduce", "introducing",
        "debut", "new product", "new service", "commences commercial", "inaugurat",
        "launches", "launched",
    ],
    "appointment": [
        "appoint", "appointed", "joins as", "takes charge", "takes over as", "new ceo",
        "new cfo", "new md", "new chairman", "new managing director", "re-appoint",
        "resign", "resigns", "steps down", "step down", "cease to be", "key managerial",
    ],
    "results": [
        "q1", "q2", "q3", "q4", "results", "net profit", "pat rises", "pat falls", "pat ",
        "revenue", "earnings", "topline", "bottomline", "profit rises", "profit falls",
        "profit jumps", "net loss", "quarterly result",
    ],
    "ma": [
        "acquisition", "acquire", "merger", "demerger", "stake sale", "stake buy",
        "acquires stake", "divest", "divestment", "hive off", "buyout", "takeover",
    ],
    "expansion": [
        "expansion", "new plant", "capacity expansion", "capex", "greenfield", "brownfield",
        "new facility", "new unit", "new line", "ramp up", "capacity addition",
    ],
    "partnership": [
        "partnership", "mou", "tie-up", "tie up", "collaboration", "joint venture",
        "signs agreement", "pact", "strategic alliance",
    ],
    "rating": [
        "upgrade", "downgrade", "target price", "brokerage", "initiates coverage",
        "buy rating", "outperform", "underperform", "raises target", "cuts target",
    ],
    "fundraise": [
        "qip", "rights issue", "fundraise", "fund raising", "raises rs", "ipo", "fpo",
        "preferential issue", "private placement",
    ],
}

# --- Helpers ----------------------------------------------------------------

def log(msg):
    print(msg, flush=True)


def fetch_constituents():
    """Load bundled snapshot, try to refresh from NSE archives / mirror."""
    base = {}
    p = ROOT / "constituents.csv"
    if p.exists():
        with open(p, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("Symbol"):
                    base[row["Symbol"]] = {
                        "industry": row.get("Industry", "").strip(),
                        "sector": row.get("Sector", "").strip(),
                    }
    symbols = list(base.keys())

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    fetched_from = "bundled"
    for name, url in [("nse", NSE_CSV), ("mirror", MIRROR_CSV)]:
        try:
            r = requests.get(url, headers=headers, timeout=20)
            r.raise_for_status()
            rows = list(csv.reader(r.text.splitlines()))
            if len(rows) < 400:
                continue
            hdr = rows[0]
            sym_i = hdr.index("Symbol")
            ind_i = hdr.index("Industry") if "Industry" in hdr else None
            fresh = []
            for row in rows[1:]:
                if len(row) <= sym_i or not row[sym_i].strip():
                    continue
                sym = row[sym_i].strip()
                meta = base.get(sym) or {"industry": "", "sector": ""}
                if ind_i is not None and len(row) > ind_i and row[ind_i].strip():
                    meta["industry"] = row[ind_i].strip()
                    if not meta.get("sector"):
                        meta["sector"] = row[ind_i].strip()
                fresh.append((sym, meta))
            if len(fresh) < 400:
                continue
            symbols = [s for s, _ in fresh]
            base = dict(fresh)
            fetched_from = name
            # persist refreshed list back to the snapshot
            with open(p, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["Symbol", "Industry", "Sector"])
                for sym, meta in fresh:
                    w.writerow([sym, meta.get("industry", ""), meta.get("sector", "")])
            break
        except Exception as e:
            log(f"  constituents source '{name}' failed: {e}")

    log(f"constituents: {len(symbols)} symbols (source: {fetched_from})")
    return symbols, base


def fetch_prices(symbols):
    """Download ~1y daily OHLCV for all symbols. Returns {symbol: df}."""
    out = {}
    tickers = [s + ".NS" for s in symbols]
    for i in range(0, len(tickers), CHUNK):
        chunk = tickers[i:i + CHUNK]
        try:
            df = yf.download(chunk, period="1y", interval="1d", group_by="ticker",
                             auto_adjust=True, progress=False, threads=True)
        except Exception as e:
            log(f"  download chunk {i // CHUNK} failed: {e}")
            continue
        for j, t in enumerate(chunk):
            sym = symbols[i + j]
            try:
                sub = df[t].dropna(how="all")
                if len(sub) and sub["Close"].notna().any():
                    out[sym] = sub
            except KeyError:
                pass
    log(f"prices: {len(out)} of {len(symbols)} symbols")
    return out


def compute_metrics(sym, df, meta):
    d = df[["Close", "Volume"]].dropna()
    closes, vols = d["Close"], d["Volume"]
    if len(closes) < 60 or len(vols) < 30:
        return None
    last = float(closes.iloc[-1])
    prev = float(closes.iloc[-2]) if len(closes) > 1 else last
    ema50 = float(closes.ewm(span=50, adjust=False).mean().iloc[-1])
    ema200 = float(closes.ewm(span=200, adjust=False).mean().iloc[-1]) if len(closes) >= 200 else None
    avg_vol30 = float(vols.iloc[-30:].mean())
    avg_vol50 = float(vols.iloc[-50:].mean()) if len(vols) >= 50 else avg_vol30
    avg_turnover30 = float((d["Close"] * d["Volume"]).iloc[-30:].mean())
    last_vol = float(vols.iloc[-1])
    day_chg = (last / prev - 1) * 100 if prev else 0.0
    hi52 = float(closes.max())
    rec = {
        "symbol": sym,
        "sector": meta.get("sector") or "Unknown",
        "industry": meta.get("industry") or "",
        "close": round(last, 2),
        "dayChangePct": round(day_chg, 2),
        "ema50": round(ema50, 2),
        "ema200": round(ema200, 2) if ema200 else None,
        "above50": last > ema50,
        "above200": (last > ema200) if ema200 else False,
        "avgVol30": int(avg_vol30),
        "avgVol50": int(avg_vol50),
        "avgTurnover30": int(avg_turnover30),
        "lastVol": int(last_vol),
        "volRatio": round(last_vol / avg_vol50, 2) if avg_vol50 else 0.0,
        "volSpike": last_vol >= VOL_SPIKE_RATIO * avg_vol50 if avg_vol50 else False,
        "move4": abs(day_chg) >= MOVE_PCT_MIN,
        "pctFrom52wHigh": round((last / hi52 - 1) * 100, 1),
        "asOf": str(closes.index[-1].date()),
        "news": [],
    }
    rec["qualified"] = bool(rec["above50"] and rec["above200"] and avg_turnover30 >= AVG_TURNOVER_30_MIN)
    return rec


# --- News -------------------------------------------------------------------

def classify(title):
    t = title.lower()
    tags = []
    for cat, phrases in CATALYST_RULES.items():
        for ph in phrases:
            if ph in t:
                tags.append(cat)
                break
    return tags


def fetch_news(sym):
    """Google News RSS, last 7 days, for one symbol."""
    url = "https://news.google.com/rss/search"
    params = {"q": f'"{sym}" when:{NEWS_WINDOW_DAYS}d', "hl": "en-IN", "gl": "IN", "ceid": "IN:en"}
    try:
        r = requests.get(url, params=params, timeout=15,
                         headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
        r.raise_for_status()
    except Exception:
        return []
    items = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=NEWS_WINDOW_DAYS)
    try:
        root = ET.fromstring(r.content)
    except ET.ParseError:
        return []
    for it in root.findall(".//item"):
        title = (it.findtext("title") or "").strip()
        link = (it.findtext("link") or "").strip()
        pub = it.findtext("pubDate") or ""
        src = it.find("source")
        source = src.text.strip() if src is not None and src.text else ""
        try:
            dt = datetime.strptime(pub, "%a, %d %b %Y %H:%M:%S %Z")
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if dt < cutoff:
                continue
            pub_iso = dt.strftime("%Y-%m-%dT%H:%M")
        except ValueError:
            pub_iso = ""
        if not title:
            continue
        # strip trailing " - Source" that Google adds
        if source and title.endswith(" - " + source):
            title = title[: -len(source) - 3]
        items.append({
            "title": title,
            "link": link,
            "source": source,
            "date": pub_iso,
            "tags": classify(title),
        })
    return items[:12]


def fetch_all_news(records, workers=6):
    # News for all trend-qualified stocks (above both EMAs) so coverage holds
    # at any volume threshold the dashboard user selects.
    qualified = [r for r in records if r["above50"] and r["above200"]]
    log(f"news: fetching for {len(qualified)} trend-qualified stocks...")

    def work(r):
        time.sleep(0.2)  # gentle pacing
        return r["symbol"], fetch_news(r["symbol"])

    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for sym, news in ex.map(work, qualified):
            rec = next(r for r in qualified if r["symbol"] == sym)
            rec["news"] = news
            done += 1
            if done % 50 == 0:
                log(f"  news: {done}/{len(qualified)}")
    n_with = sum(1 for r in qualified if r["news"])
    log(f"news: {n_with} stocks have recent items")


# --- Main -------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(ROOT / "data" / "momentum.json"))
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--skip-news", action="store_true")
    args = ap.parse_args()

    log("== Nifty 500 Momentum Radar pipeline ==")
    symbols, meta = fetch_constituents()
    prices = fetch_prices(symbols)

    records = []
    failed = []
    for sym in symbols:
        df = prices.get(sym)
        if df is None:
            failed.append(sym)
            continue
        rec = compute_metrics(sym, df, meta.get(sym, {}))
        if rec:
            records.append(rec)
        else:
            failed.append(sym)
    log(f"metrics: {len(records)} stocks computed, {len(failed)} failed")

    if not args.skip_news:
        fetch_all_news(records, workers=args.workers)

    qualified = [r for r in records if r["qualified"]]
    trendq = [r for r in records if r["above50"] and r["above200"]]
    stats = {
        "universe": len(symbols),
        "computed": len(records),
        "failed": failed,
        "qualified": len(qualified),
        "trendQualified": len(trendq),
        "volSpikes": sum(1 for r in records if r["volSpike"]),
        "move4": sum(1 for r in records if r["move4"]),
        "withNews": sum(1 for r in trendq if r["news"]),
        "catalysts": {
            cat: sum(1 for r in trendq for n in r["news"] if cat in n["tags"])
            for cat in CATALYST_RULES
        },
    }

    as_of = max((r["asOf"] for r in records), default="")
    doc = {
        "generatedAt": datetime.now(timezone.utc).astimezone(
            timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%dT%H:%M:%S+05:30"),
        "asOf": as_of,
        "config": {
            "avgTurnover30Min": AVG_TURNOVER_30_MIN,
            "volSpikeRatio": VOL_SPIKE_RATIO,
            "movePctMin": MOVE_PCT_MIN,
            "newsWindowDays": NEWS_WINDOW_DAYS,
        },
        "stats": stats,
        "stocks": records,
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, separators=(",", ":"))
    log(f"wrote {out} ({out.stat().st_size / 1024:.0f} KB)")
    log(f"stats: {json.dumps(stats, default=str)[:400]}")

    # quality gate signal for CI (volume threshold is strict in corrections;
    # qualified can legitimately be 0, so gate on coverage + signal richness)
    if len(records) < 300 or (stats["volSpikes"] + stats["move4"]) < 5:
        log("DATA QUALITY GATE FAILED")
        sys.exit(2)


if __name__ == "__main__":
    main()
