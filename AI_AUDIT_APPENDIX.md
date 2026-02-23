# AI Audit Appendix (M1 Capstone Work)

## AI Tools Used
- GitHub Copilot

## Per Task

- **Task:** Merge 50 cryptocurrency spreadsheets into one raw file.
- **Prompt:** "help me merge all `*-usd-max.csv` files (top 50 coins) into one raw dataset, attach rank metadata, and save one merged CSV in the raw data folder"
- **AI Output:** Produced/used the merge workflow in `code/merge_raw_by_coingecko_rank.py`, which:
  - Fetches CoinGecko rank/name/symbol metadata.
  - Builds a unified panel with `coin_rank`, `coin_name`, `coin_symbol`, `source_file`, and market variables.
  - Sorts by rank/symbol/date and writes the merged raw output.
- **Verification:**
  - Confirmed merged file generation in `data/raw/`.
  - Verified input file count was 50 and rows were appended into a single table.
  - Checked file consistency and logic.
- **Critique:**
  - Correct: merge logic and output structure were appropriate for M1.
  - Limitation: `coin_id` can be blank under fallback metadata retrieval (we clarified that by explaining that getting APIs meant to pay for them, so we fixed that by importing the data one by one, manually).

- **Task:** Cleaning merged data (last 6 years + quality checks).
- **Prompt:** "clean the merged CoinGecko panel by dropping missing core market fields, converting date types, keeping only the last 6 years, and saving to processed data"
- **AI Output:** Produced/used `code/clean_coingecko_data.py`, which:
  - Drops rows missing `price`, `market_cap`, or `total_volume`.
  - Parses `snapped_at` to datetime (removing trailing ` UTC` if present).
  - Filters to dates >= `2020-02-19`.
  - Writes cleaned output to `data/processed/coingecko_ranking_cleaned.csv`.
- **Verification:**
  - Compared raw vs processed row counts and confirmed expected reduction.
  - Checked cleaned panel date range aligns with 2020-02-19 to 2026-02-18.
- **Critique:**
  - Correct: cleaning decisions are accurate and reproducible.
  - Caveat: filtering to 6 years improves focus but removes pre-2020 history; noted in ethical considerations.

- **Task:** Create `data/final/data_dictionary.md` with required sections (dataset overview, variable definitions, cleaning summary).
- **Prompt:** "based on the processed data we have so far, help me create a Data Dictionary... should contain dataset overview, variable definitions table, cleaning decisions summary"
- **AI Output:** Drafted and created `data/final/data_dictionary.md` using observed dataset metadata from `data/processed/coingecko_ranking_cleaned.csv` (rows, entities, time periods, date range, variable table, and cleaning notes tied to project scripts).
- **Verification:**
  - Confirmed file exists at the required location.
  - Checked that all required sections were present.
  - Cross-checked counts and date range against the processed CSV and script logic.
- **Critique:**
  - Correct: structure and required sections were complete.
  - Limitation identified: `coin_id` is 100% missing in the current processed data; retained and flagged transparently (we clarified that by explaining that getting APIs meant to pay for them, so we fixed that by importing the data one by one, manually).

- **Task:** Create `data/M1_data_quality_report.md` with all required sections for M1.
- **Prompt:** "i need help creating a Data Quality Report... Required sections: Data Sources, Data Cleaning Decisions, Merge Strategy, Final Dataset Summary, Reproducibility Checklist, Ethical Considerations"
- **AI Output:** Drafted and created `data/M1_data_quality_report.md` including:
  - Primary source coverage and row counts (raw vs cleaned).
  - Missingness/duplicates/type checks and cleaning decisions.
  - Merge strategy description and before/after row-count checks.
  - Final panel summary and sample statistics table.
- **Verification:**
  - Recomputed row counts and QA metrics from raw/processed files.
  - Confirmed consistency with `code/clean_coingecko_data.py`, `code/merge_raw_by_coingecko_rank.py`, and `code/config_paths.py`.
  - Confirmed report includes all required sections and team sign-off names.
- **Critique:**
  - Correct: report is complete for currently available data and files.
  - Team responsibility: update this report in later milestones when supplementary merges are added.

## Summary

- **Total AI use in this assignment stage:** 4 major uses (merge pipeline support, cleaning pipeline support, data dictionary, data quality report).
- **Primary use cases:**
  1. Data engineering support (multi-file merge into one raw panel).
  2. Data cleaning pipeline support (missingness/date filter/type handling).
  3. Structured technical writing from project requirements.
  4. Data documentation using dataset metrics.
- **Verification method used by team:**
  - Script runs against repository requirements.
  - Manual checklist against assignment requirements.
  - Checks between reported data and CSV/script outputs.

## Responsibility Statement

All outputs are our team’s responsibility.

## Team Sign-off

- Luke Birdseye
- Ben Brown
- Katie Koonts
- James Gawey
- Dani Gamboa
