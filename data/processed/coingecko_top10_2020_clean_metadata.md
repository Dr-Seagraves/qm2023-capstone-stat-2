# CoinGecko Top 10 (2020+) Cleaning Metadata

## Scope
- Input file: /workspaces/qm2023-capstone-stat-2/data/raw/coingecko_ranking.csv
- Output file: /workspaces/qm2023-capstone-stat-2/data/processed/coingecko_top10_2020_clean.csv
- Rule: keep first 10 unique coins by file order (symbols selected: ['btc', 'eth', 'usdt', 'xrp', 'bnb', 'usdc', 'sol', 'trx', 'doge', 'figr_heloc'])
- Rule: omit all rows before 2020-01-01

## Result Summary
- Rows output: 20244
- Distinct coins: 10 (bnb, btc, doge, eth, figr_heloc, sol, trx, usdc, usdt, xrp)
- Date range: 2020-01-01 to 2026-02-18
- Dropped rows (before 2020): 12404
- Dropped rows (invalid datetime): 0

## Missing-Value Decisions
- Numeric columns: price, market_cap, total_volume
- Order: forward-fill within coin -> backward-fill within coin -> coin median -> global median

## Missing Counts
- price: before=0, after=0, ffill=0, bfill=0, coin_median=0, global_median=0
- market_cap: before=0, after=0, ffill=0, bfill=0, coin_median=0, global_median=0
- total_volume: before=0, after=0, ffill=0, bfill=0, coin_median=0, global_median=0
