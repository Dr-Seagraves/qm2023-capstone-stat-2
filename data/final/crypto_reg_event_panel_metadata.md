# Crypto Regulatory Event Panel Metadata

## Inputs
- Returns/volatility input: /workspaces/qm2023-capstone-stat-2/data/final/coingecko_top10_2020_returns_volatility.csv
- SEC events input: /workspaces/qm2023-capstone-stat-2/data/processed/sec_press_litigation_crypto_only.csv

## Variables
- Outcome: `outcome_realized_vol_30d` (from rolling_vol_30d)
- Driver: `driver_sec_event_indicator` (1 if date is an SEC event_date else 0)
- Controls: `control_market_cap`, `control_total_volume`, `control_btc_corr_30d`
- Group variable: `token_group` in {defi, centralized_exchange, stablecoin}

## Grouping Rule
- Symbol list mapping for stablecoins and centralized-exchange tokens is hard-coded in script.
- All remaining tokens default to `defi`.

## Panel Summary
- Rows: 20244
- Tokens: 10 (bnb, btc, doge, eth, figr_heloc, sol, trx, usdc, usdt, xrp)
- Groups present: centralized_exchange, defi, stablecoin
- Date range: 2020-01-01 to 2026-02-18
- Unique SEC action dates in input: 4
- Panel rows flagged with SEC indicator = 1: 40

## Missing-Value Decisions
- Outcome/control fields are carried from upstream processed files.
- No additional imputation is performed in this panel step.
