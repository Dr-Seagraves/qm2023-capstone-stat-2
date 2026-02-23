# M1 Data Quality Report

## 1) Data Sources

### Primary Source

- **Dataset name:** CoinGecko Ranking Panel (crypto market data)
- **Source:** CoinGecko historical exports merged in project pipeline (`data/raw/coingecko_ranking.csv`)
- **Coverage (current M1 data):**
  - **Entities:** 50 cryptocurrencies (`coin_symbol`)
  - **Frequency:** Daily
  - **Date range (raw):** 2013-04-28 to 2026-02-18
  - **Date range (cleaned M1 sample):** 2020-02-19 to 2026-02-18
- **Initial row count (raw):** 99,447
- **Key variables:** `coin_symbol`, `snapped_at`, `price`, `market_cap`, `total_volume`, `coin_rank`, `coin_name`, `coin_id`, `source_file`

### Supplementary Sources

Planned supplementary series (from project scope in README) are not yet present in the `data/` folder at M1:

- **Economic Policy Uncertainty (EPU) Index** — planned monthly uncertainty index
- **FRED macro/financial series** — planned variables include Fed Funds Rate and VIX

Current M1 status for supplementary data:

- **Source files available:** none in repository `data/raw/` yet
- **Variables merged into final M1 panel:** none yet
- **Supplementary date range in merged panel:** not applicable (no supplementary merge completed)

---

## 2) Data Cleaning Decisions

Raw rows: **99,447**  
Cleaned rows: **74,490**  
Rows removed: **24,957** (25.10% of raw)

### Cleaning log

| Variable / Check | Missing (% / count) | Decision | Justification |
|---|---:|---|---|
| `price` | 0.00% / 0 (raw) | Keep | No missingness observed. |
| `market_cap` | 0.004% / 4 (raw) | Drop rows with missing `market_cap` | Market cap is core to ranking/scale analysis; script drops rows missing core market variables. |
| `total_volume` | 0.00% / 0 (raw) | Keep | No missingness observed. |
| `coin_id` | 100.00% / 99,447 (raw), 100.00% / 74,490 (cleaned) | Keep variable, flag as quality issue | Upstream metadata fallback left IDs blank; retain for schema compatibility and document limitation. |
| `snapped_at` datetime parsing | 0 bad dates in raw and cleaned | Correct type to datetime/date | Ensures valid time indexing and filtering; script strips trailing ` UTC` when present. |
| Date window filter | Not a missingness rule | Keep only dates >= 2020-02-19 | Aligns sample to capstone analysis period (2020–2026). |
| Duplicates on (`coin_symbol`,`snapped_at`) | 0 duplicate rows (raw and cleaned) | Keep all (no dedupe needed) | Entity-time key uniqueness already satisfied. |
| Outliers in `price`, `market_cap`, `total_volume` | Extreme tails present (about 2% outside [P1, P99] by construction) | Keep in M1; flag for robustness in M3 | Crypto market values are inherently heavy-tailed; dropping/winsorizing now could remove meaningful shock information. |
| Size/volume filters | Not applied in M1 | No cap/drop rule applied | Avoid arbitrary exclusion at baseline; will test alternative filters in robustness stage. |

### Notes on row loss

Most row reduction from raw to cleaned is attributable to the **date restriction** (keeping 2020-02-19 onward). A very small amount is due to dropping rows with missing `market_cap`.

---

## 3) Merge Strategy

### Current merge logic implemented in M1

1. **Upstream construction merge (completed):** per-coin historical files are combined and enriched with CoinGecko rank/name metadata by coin symbol (`code/merge_raw_by_coingecko_rank.py`).
2. **Cleaning stage (completed):** missing-value and date-window filters applied (`code/clean_coingecko_data.py`).
3. **Supplementary merge (not yet completed):** EPU/FRED not yet ingested into `data/`.

### Join details

- **Implemented key alignment:** (`coin_symbol` from filename/metadata) + daily `snapped_at` within each coin file.
- **Practical join behavior in upstream script:** left-preserving behavior for time-series rows (rows retained even if metadata like `coin_id` is unavailable; fallback values are used).
- **Duplicate key verification:** 0 duplicates for (`coin_symbol`,`snapped_at`) in both raw and cleaned outputs.

### Before/after counts and reasonableness

- **After upstream merge output (raw panel):** 99,447 rows
- **After M1 cleaning output (processed panel):** 74,490 rows
- **Reasonableness checks:**
  - 50 entities and 2,192 dates in cleaned panel imply a balanced maximum of 109,600 rows; observed 74,490 confirms an **unbalanced** panel, which is expected for crypto listing/history variation.
  - Key uniqueness check passed (0 duplicate entity-date rows).

---

## 4) Final Dataset Summary

### Structure

- **Entity variable:** `coin_symbol`
- **Time variable:** `snapped_at` (daily)
- **Panel type:** Unbalanced panel
- **Final dimensions:** 74,490 observations, 50 entities, 2,192 daily periods (2020-02-19 to 2026-02-18)

### Sample statistics (cleaned M1 panel)

| Variable | Mean | Std Dev | Min | Max | Missing (%) |
|---|---:|---:|---:|---:|---:|
| `coin_rank` | 22.819 | 14.322 | 1.000 | 50.000 | 0.000 |
| `price` | 1,684.496 | 9,838.644 | 0.000 | 124,773.508 | 0.000 |
| `market_cap` | 48,461,928,804.178 | 198,474,369,028.451 | 0.000 | 2,486,073,086,655.287 | 0.000 |
| `total_volume` | 4,080,497,710.112 | 14,669,831,869.966 | 0.000 | 926,767,674,515.136 | 0.000 |
| `coin_id` | N/A | N/A | N/A | N/A | 100.000 |

### Data quality flags

- **DQ-01:** `coin_id` is fully missing (100%); analyses should key on `coin_symbol` + `snapped_at`.
- **DQ-02:** Heavy right tails in market variables (expected in crypto); outlier sensitivity checks needed in M3.
- **DQ-03:** Panel is unbalanced relative to max possible 50 × 2,192.

---

## 5) Reproducibility Checklist

| Item | Status | Evidence |
|---|---|---|
| Scripted pipeline runs | ✅ | `code/merge_raw_by_coingecko_rank.py`, `code/clean_coingecko_data.py` |
| Relative path management used | ✅ | `code/config_paths.py` centralizes project paths |
| Output location defined | ✅ | Processed output: `data/processed/coingecko_ranking_cleaned.csv` |
| No manual editing of dataset files | ✅ | Workflow is script-based; output produced by Python scripts |
| Metadata documented | ✅ | `data/final/data_dictionary.md` created |
| AI Audit completed | ⏳ | Add team’s required AI-audit artifact/statement for sign-off |

---

## 6) Ethical Considerations

- **What data are we losing?**
  - Restricting to 2020-02-19 onward excludes earlier crypto regimes (e.g., pre-2020 cycles), reducing long-horizon comparability.
  - Unbalanced coverage means some smaller/newer/short-lived assets contribute fewer observations.
- **Who might we exclude?**
  - Assets with shorter listing histories or unstable reporting are implicitly underrepresented.
  - Analyses may therefore emphasize larger, persistent coins relative to fringe assets.
- **Transparency commitments**
  - Keep all cleaning rules explicitly documented (this report + data dictionary).
  - Report sensitivity to outlier handling and potential size/volume filters in M3 robustness checks.
  - Clearly disclose `coin_id` completeness limitation and use of `coin_symbol` as operational key.

---

## Team Sign-off

- Luke Birdseye
- Ben Brown
- Katie Koonts
- James Gawey
- Dani Gamboa
