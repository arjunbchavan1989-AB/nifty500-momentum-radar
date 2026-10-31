# Nifty 500 Momentum Radar

Self-updating dashboard of Nifty 500 stocks in uptrends, with volume spikes, big moves, and news catalysts. Built for evening (8-9 PM IST) market preparation.

**Live site**: https://arjunbchavan1989-AB.github.io/nifty500-momentum-radar/

## What it shows

For every Nifty 500 stock (492 currently with price data on Yahoo Finance):

- **Trend**: close vs 50-day and 200-day EMA
- **Liquidity**: 30-day and 50-day average volume (shares), latest-day volume vs 50-day average
- **Signals (latest session)**: volume ≥ 2x the 50-day average; absolute move ≥ 4%
- **Catalysts**: headlines from the last 7 days (Google News, India edition), auto-tagged by keyword — order win, product/service launch, key-person appointment, results, M&A, expansion, partnership, rating, fundraise — with source links
- **Context**: % from 52-week high, sector/industry

**Qualified** view = above both the 50 and 200 EMA + 30-day average daily traded value (close × volume) ≥ ₹3 crore — the default. Share-count and ₹10 cr turnover thresholds are also available in the filter bar.

## Auto-update

GitHub Actions refreshes `data/momentum.json` and redeploys GitHub Pages daily:
- nominal **19:00 IST** (13:30 UTC) run, plus a **14:00 IST** hedge run
- GitHub's scheduler can delay cron by hours (observed 3-6h), so exact timing is not guaranteed; the two schedules together aim to have data ready before the 8-9 PM IST prep window
- a data-quality gate (fails if <300 stocks computed or too few signals) prevents bad refreshes from being committed or deployed

## Data sources

- **Constituents**: Nifty 500 list — NSE archives when reachable, else a community mirror, else the bundled `constituents.csv` snapshot (also auto-refreshed and committed by the pipeline when a live source works)
- **Prices/volumes**: Yahoo Finance daily OHLCV (split/dividend-adjusted), ~1 year
- **News**: Google News RSS (India edition), last 7 days, keyword-classified — heuristic, verify on the linked article before acting

## Run locally

```bash
pip install -r requirements.txt
python3 pipeline.py --workers 6
python3 -m http.server 8000   # open http://localhost:8000
```

Pipeline options: `--workers N` (news fetch concurrency), `--skip-news`, `--out PATH`.

## Notes

- 8 constituents currently return no Yahoo data (delisted/renamed symbols like TATAMOTORS after the demerger) and fail gracefully
- Volumes are in shares; the dashboard shows them in crore
- This is a research tool, not investment advice
