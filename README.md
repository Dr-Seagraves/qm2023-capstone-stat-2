[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/gp9US0IQ)
[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-2e0aaae1b6195c2367325f4f02e2d04e9abb55f0b24a779b69b11b9e10269abc.svg)](https://classroom.github.com/online_ide?assignment_repo_id=22639577&assignment_repo_type=AssignmentRepo)
# QM 2023 Capstone Project

**Team Name:** Stat 2

**Members:**
- Luke
- Ben
- Katie
- James
- Dani

__Cryptocurrency Volatility, Policy Uncertainty & Macro Shocks (2020–2026)__

**Research Topic:** Economic policy uncertainty & regulatory changes as cryptocurrency return volatility drivers.

**Research Question:** How have economic policy uncertainty and regulatory shocks (e.g., SEC actions, interest rate changes) affected cryptocurrency return volatility from 2020–2026?

**Datasets:**
- CoinGecko: daily price, trading volume, market capitalization for top 50 cryptocurrencies (2020–2026).
- Economic Policy Uncertainty Index: monthly uncertainty scores.
- FRED: Fed Funds Rate, VIX (market fear gauge).

Semester-long capstone for Statistics II: Data Analytics.

## Project Structure

- **code/** — Python scripts and notebooks. Use `config_paths.py` for paths.
- **data/raw/** — Original data (read-only)
- **data/processed/** — Intermediate cleaning outputs
- **data/final/** — M1 output: analysis-ready panel
- **results/figures/** — Visualizations
- **results/tables/** — Regression tables, summary stats
- **results/reports/** — Milestone memos
- **tests/** — Autograding test suite

Run `python code/config_paths.py` to verify paths.

## Safe Top-10 Monthly Merge

To merge only top-10 entities by month/date without changing non-top-10 rows, use:

`/bin/python3 code/merge_top10_monthly_safe.py --base data/raw/reit_master_template.csv --top data/final/coingecko_top10_2020_returns_volatility.csv --base-entity-col reit_id --base-date-col date --top-entity-col coin_symbol --top-date-col date --out data/final/reit_with_top10_monthly_merge.csv`

This writes a new file and preserves all base rows; only rows whose entity appears in the top dataset are enriched.

For SEC crypto litigation events merged by month only:

`/bin/python3 code/merge_top10_monthly_safe.py --base data/raw/coingecko_ranking.csv --top data/processed/sec_press_litigation_crypto_only.csv --base-date-col snapped_at --top-date-col event_month --month-only --disable-top-filter --prefix sec_ --out data/final/coingecko_with_sec_monthly.csv`

This also writes a new file and keeps all base rows, adding SEC monthly columns with the `sec_` prefix.

## Assignment-Style Panel Command (grader-safe)

If you want to follow the required REIT pipeline exactly, use the panel builder with SEC as supplementary data:

`/bin/python3 code/build_reit_panel.py --source data/raw/reit_master_template.csv --entity-col reit_id --date-col date --supplementary data/processed/sec_press_litigation_crypto_only.csv --output-csv data/final/reit_panel_monthly_with_sec.csv --output-meta-json data/final/reit_panel_monthly_with_sec_metadata.json --output-meta-md data/final/reit_panel_monthly_with_sec_metadata.md`

This keeps your raw files unchanged and produces a monthly REIT panel plus metadata outputs in `data/final`.

Alternate (with within-REIT numeric forward/backward fill):

`/bin/python3 code/build_reit_panel.py --source data/raw/reit_master_template.csv --entity-col reit_id --date-col date --supplementary data/processed/sec_press_litigation_crypto_only.csv --fill-forward-numeric --output-csv data/final/reit_panel_monthly_with_sec_filled.csv --output-meta-json data/final/reit_panel_monthly_with_sec_filled_metadata.json --output-meta-md data/final/reit_panel_monthly_with_sec_filled_metadata.md`

## Crypto Regulatory Event Panel (Key Variables)

To build the crypto panel with required event-study variables (outcome, driver, controls, groups):

`/bin/python3 code/build_crypto_reg_event_panel.py`

Outputs:
- `data/final/crypto_reg_event_panel.csv`
- `data/final/crypto_reg_event_panel_metadata.md`
