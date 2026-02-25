# Data Dictionary

## Dataset Overview

This data dictionary documents the cleaned CoinGecko panel and macro control files currently available in:

- data/processed/coingecko_ranking_cleaned.csv
- data/processed/vix_cleaned.csv
- data/processed/ffeffective_rate_cleaned.csv
- data/processed/epu_index_cleaned.csv
- data/processed/crypto_reg_event_panel.csv
- data/final/Top 10 Panel/crypto_reg_event_panel_with_macro.csv

Current dataset coverage:

- Observations (rows): 74,490
- Entities (cryptocurrencies): 50 (coin_symbol)
- Time periods (daily dates): 2,192
- Date range: 2020-02-19 to 2026-02-18
- Panel structure: Daily crypto-level panel (plus daily macro controls)

## Variable Definitions

| Variable | Description | Type | Source | Units |
|---|---|---|---|---|
| coin_rank | CoinGecko market-cap rank assigned to the cryptocurrency (lower = larger market cap). | Integer | CoinGecko markets ranking metadata merged from per-coin files | Rank (1 = highest) |
| coin_id | CoinGecko coin identifier string. | String | CoinGecko metadata | ID string (text) |
| coin_name | Full cryptocurrency name. | String | CoinGecko metadata | Text |
| coin_symbol | Ticker symbol used to identify each cryptocurrency (e.g., btc, eth). | String | CoinGecko metadata / filename mapping | Symbol text |
| source_file | Original raw file used for that coin’s time series. | String | Local raw CoinGecko export files | Filename |
| snapped_at | Observation date for the market data point. | Date (YYYY-MM-DD) | Raw CoinGecko time-series file | Calendar date |
| price | Daily cryptocurrency price in U.S. dollars. | Float | CoinGecko historical market data | USD |
| market_cap | Daily market capitalization in U.S. dollars. | Float | CoinGecko historical market data | USD |
| total_volume | Daily traded volume in U.S. dollars. | Float | CoinGecko historical market data | USD/day |

## Macro Control Variable Definitions

### data/processed/vix_cleaned.csv

| Variable | Description | Type | Source | Units |
|---|---|---|---|---|
| observation_date | Observation date for VIX close value. | Date (YYYY-MM-DD) | FRED VIX CSV (data/raw/VIX.csv) | Calendar date |
| vix | CBOE Volatility Index close value. | Float | FRED VIX series (VIXCLS) | Index points |

### data/processed/ffeffective_rate_cleaned.csv

| Variable | Description | Type | Source | Units |
|---|---|---|---|---|
| observation_date | Observation date for effective Fed Funds rate. | Date (YYYY-MM-DD) | FRED DFF CSV (data/raw/FFEffective Rate.csv) | Calendar date |
| ffeffective_rate | Effective Federal Funds Rate. | Float | FRED DFF series | Percent |

### data/processed/epu_index_cleaned.csv

| Variable | Description | Type | Source | Units |
|---|---|---|---|---|
| observation_date | Observation date for the EPU index. | Date (YYYY-MM-DD) | FRED USEPUINDXD CSV (data/raw/USEPUINDXD.csv) | Calendar date |
| epu_index | Economic Policy Uncertainty Index for the United States. | Float | FRED USEPUINDXD series | Index points |

### data/final/Top 10 Panel/crypto_reg_event_panel_with_macro.csv

| Variable | Description | Type | Source | Units |
|---|---|---|---|---|
| date | Daily date key for merged macro controls. | Date (YYYY-MM-DD) | Derived from processed macro files | Calendar date |
| vix | VIX close value. | Float | data/processed/vix_cleaned.csv | Index points |
| ffeffective_rate | Effective Federal Funds Rate. | Float | data/processed/ffeffective_rate_cleaned.csv | Percent |
| epu_index | Economic Policy Uncertainty Index for the United States. | Float | data/processed/epu_index_cleaned.csv | Index points |
| outcome_vol_blank_reason | Short flag explaining blank outcome_realized_vol_30d values (`no_prior_ret_yet` or `-`). | String | Derived from outcome_realized_vol_30d missingness in event panel | Categorical text |
| btc_corr_blank_reason | Short flag explaining blank control_btc_corr_30d values (`no_paired_ret_yet` or `-`). | String | Derived from control_btc_corr_30d missingness in event panel | Categorical text |

## Cleaning Decisions Summary

The cleaned dataset is produced by code/clean_coingecko_data.py and reflects these decisions:

1. Missing-value filtering on market variables: rows with missing price, market_cap, or total_volume are dropped.
2. Date parsing and normalization: snapped_at is converted to datetime, including removal of a trailing  UTC tag when present.
3. Sample window restriction: only observations on or after 2020-02-19 are retained (intended 6-year capstone window).
4. Output target: cleaned data are written to data/processed/coingecko_ranking_cleaned.csv.

Upstream merging (code/merge_raw_by_coingecko_rank.py) additionally indicates:

- Coin-level raw files are merged into a single panel and sorted by coin_rank, coin_symbol, and snapped_at.
- Ranking/name/symbol metadata are attached from CoinGecko API (or website fallback).
- In the current cleaned file, coin_id is blank for all rows, which is consistent with metadata fallback when IDs are unavailable (we had to download all cryptocurrencies' data one by one, otherwise we would have had to pay to get CoinGecko APIs).

Macro control cleaning (code/clean_macro_series.py) additionally indicates:

- Keep only rows with valid date + numeric value.
- Treat blank strings and . as missing.
- Drop duplicate dates (keep last) and sort by date ascending.
- Outputs: data/processed/vix_cleaned.csv (1,569 rows), data/processed/ffeffective_rate_cleaned.csv (2,241 rows), and data/processed/epu_index_cleaned.csv (2,246 rows).

Macro merge step (data/final/Top 10 Panel/crypto_reg_event_panel_with_macro.csv) additionally indicates:

- Daily left merge of VIX, effective Fed Funds, and EPU onto the crypto event panel by date.

## Variable Interpretation (Event Panel)

- outcome_realized_vol_30d:
	- Meaning: 30-day realized volatility from daily returns.
	- Why blanks: no prior return observations yet.

- driver_sec_event_indicator:
	- Meaning: binary SEC-event flag (1 = SEC crypto-related event date, 0 = non-event date).

- control_btc_corr_30d:
	- Meaning: rolling 30-day correlation between each coin's returns and BTC returns.
	- Why many blanks: no paired return observations yet, or not enough paired observations.

## Team Sign-off

- Luke Birdseye
- Ben Brown
- Katie Koonts
- James Gawey
- Dani Gamboa
