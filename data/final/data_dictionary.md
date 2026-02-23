# Data Dictionary

## Dataset Overview

This data dictionary documents the cleaned CoinGecko ranking panel currently available in:

- `data/processed/coingecko_ranking_cleaned.csv`

Current dataset coverage:

- **Observations (rows):** 74,490
- **Entities (cryptocurrencies):** 50 (`coin_symbol`)
- **Time periods (daily dates):** 2,192
- **Date range:** 2020-02-19 to 2026-02-18
- **Panel structure:** Daily crypto-level panel

## Variable Definitions

| Variable | Description | Type | Source | Units |
|---|---|---|---|---|
| `coin_rank` | CoinGecko market-cap rank assigned to the cryptocurrency (lower = larger market cap). | Integer | CoinGecko markets ranking metadata merged from per-coin files | Rank (1 = highest) |
| `coin_id` | CoinGecko coin identifier string. | String | CoinGecko metadata | ID string (text) |
| `coin_name` | Full cryptocurrency name. | String | CoinGecko metadata | Text |
| `coin_symbol` | Ticker symbol used to identify each cryptocurrency (e.g., `btc`, `eth`). | String | CoinGecko metadata / filename mapping | Symbol text |
| `source_file` | Original raw file used for that coin’s time series. | String | Local raw CoinGecko export files | Filename |
| `snapped_at` | Observation date for the market data point. | Date (`YYYY-MM-DD`) | Raw CoinGecko time-series file | Calendar date |
| `price` | Daily cryptocurrency price in U.S. dollars. | Float | CoinGecko historical market data | USD |
| `market_cap` | Daily market capitalization in U.S. dollars. | Float | CoinGecko historical market data | USD |
| `total_volume` | Daily traded volume in U.S. dollars. | Float | CoinGecko historical market data | USD/day |

## Cleaning Decisions Summary

The cleaned dataset is produced by `code/clean_coingecko_data.py` and reflects these decisions:

1. **Missing-value filtering on market variables:** rows with missing `price`, `market_cap`, or `total_volume` are dropped.
2. **Date parsing and normalization:** `snapped_at` is converted to datetime, including removal of a trailing ` UTC` tag when present.
3. **Sample window restriction:** only observations on or after **2020-02-19** are retained (intended 6-year capstone window).
4. **Output target:** cleaned data are written to `data/processed/coingecko_ranking_cleaned.csv`.

Upstream merging (`code/merge_raw_by_coingecko_rank.py`) additionally indicates:

- Coin-level raw files are merged into a single panel and sorted by `coin_rank`, `coin_symbol`, and `snapped_at`.
- Ranking/name/symbol metadata are attached from CoinGecko API (or website fallback).
- In the current cleaned file, `coin_id` is blank for all rows, which is consistent with fallback metadata behavior when IDs are unavailable (we had to download all cryptocurrencies' data one by one, we had to pay to get ConGecko APIs).

## Team Sign-off

- Luke Birdseye
- Ben Brown
- Katie Koonts
- James Gawey
- Dani Gamboa
