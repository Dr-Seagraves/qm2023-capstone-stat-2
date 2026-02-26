# AI Audit Appendix (M1 Capstone Work)

## AI Tools Used
- GitHub Copilot

## Per Task

- **Task:** Merge 50 cryptocurrency spreadsheets into one raw file.
- **Prompt:** "help me merge all files (top 50 coins) into one raw dataset, attach rank metadata, and save one merged CSV in the raw data folder"
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

- **Task:** Clean VIX and Effective Federal Funds Rate raw files.
- **Prompt:** "with the vix and the ffedective rate files clean them (blank files, parse dates, etc.) and create two cleaned files in the processed data folder"
- **AI Output:** Produced/used `code/clean_macro_series.py`, which:
  - Reads `data/raw/VIX.csv` and `data/raw/FFEffective Rate.csv`.
  - Keeps only valid date + numeric rows.
  - Treats blank strings and `.` as missing.
  - Removes duplicate dates & sorts by date ascending.
  - Generates outputs as `data/processed/vix_cleaned.csv` and `data/processed/ffeffective_rate_cleaned.csv`.
- **Verification:**
  - Confirmed both cleaned files exist in `data/processed/`.
  - Checked both files have 0 nulls in required fields and 0 duplicate dates.
- **Critique:**
  - Correct: it created reproducible cleaned macro controls from raw source files.

- **Task:** Merge cleaned VIX + Effective Federal Funds Rate into one final macro spreadsheet.
- **Prompt:** "grab the two files added here for context & merge them into a single spreadsheet with the required columns & locate it in the final data folder"
- **AI Output:** Merged `data/processed/vix_cleaned.csv` and `data/processed/ffeffective_rate_cleaned.csv` by date into `data/final/macro_controls_merged.csv` with columns:
  - `date`
  - `vix`
  - `ffeffective_rate`
  Later updated VIX missing market-closed dates to the label `closed` (market closed) in this final macro file.
- **Verification:**
  - Confirmed merged file creation in `data/final/`.
  - Confirmed required columns and date ordering.
  - Confirmed VIX blanks show `closed` labeling.
- **Critique:**
  - Correct: produced a single macro control spreadsheet suitable for downstream merge.
  - Caveat: `closed` is a label and should be considered later when modeling.

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

- **Task:** Create `M1_data_quality_report.md` with all required sections for M1.
- **Prompt:** "i need help creating a Data Quality Report... Required sections: Data Sources, Data Cleaning Decisions, Merge Strategy, Final Dataset Summary, Reproducibility Checklist, Ethical Considerations"
- **AI Output:** Drafted and created `M1_data_quality_report.md` including:
  - Primary source coverage and row counts (raw vs cleaned).
  - Missingness/duplicates/type checks and cleaning decisions.
  - Merge strategy description and before/after row-count checks.
  - Final panel summary and sample statistics table.
- **Verification:**
  - Recomputed row counts and QA metrics from raw/processed files.
  - Confirmed consistency with `code/clean_coingecko_data.py`, `code/merge_raw_by_coingecko_rank.py`, and `code/config_paths.py`.
  - Confirmed report includes all required sections and team sign-off names.
- **Critique:**
  - Correct: the report is complete for currently available data and files.

- **Task:** Create final CoinGecko dataset without `coin_id` when column is fully blank.
- **Prompt:** "grab the coingecko ranking cleaned in the processed data, and delete the coin id column if it's blank in every row and put that finalcoingeckodata in the final data folder"
- **AI Output:** Verified that `coin_id` was blank in all rows of `data/processed/coingecko_ranking_cleaned.csv`, removed that column, and created `data/final/finalcoingeckodata.csv`.
- **Verification:**
  - Check `coin_id` column is blank for every row in processed data.
  - Confirmed the output file creation at `data/final/finalcoingeckodata.csv`.
  - Checked that the header no longer includes `coin_id` and that row count is preserved (74,490 rows).
- **Critique:**
  - Correct: we simplified the final data file.

- **Task:** Clean Economic Policy Uncertainty (EPU) Index data and integrate into final panel with macro controls.
- **Prompt:** "do you see the economic policy uncertainty index data" → "i've got the economic uncertainty index on the website from fred how do i upload it to here" → "its there its under raw data and its called USEPUINDXD" → "ok good can you clean that data" → "yes do all that please" → "i just renamed it to the USEPUINDXD.csv" → "i just uploaded the new version of the USEPUINDXD.csv, i originally uploaded the wrong one. so can you go and clean the new one that i just uploaded"
- **AI Output:** 
  - Updated `code/clean_macro_series.py` to detect and clean EPU raw files `USEPUINDXD.csv`).
  - Applied same cleaning logic as VIX/Fed: drop missing values, parse dates, drop duplicates, sort by date.
  - Generated `data/processed/epu_index_cleaned.csv` (2020-01-01 to 2026-02-18).
  - Updated merge logic (`code/merge_final_with_macro_controls.py`, exposed via `code/merge_final_panel.py`) to include EPU in the left merge onto crypto panel.
  - Updated documentation:
    - `data/final/data_dictionary.md` to include EPU variable definition and cleaning details.
    - `M1_data_quality_report.md` to reflect EPU in supplementary sources, cleaning log, and merge strategy.
    - `data/final/crypto_analysis_panel_metadata.md` to list EPU as a macro control.
- **Verification:**
  - Confirmed raw file `data/raw/USEPUINDXD.csv` exists.
  - Ran cleaning script and confirmed output: 2,246 cleaned rows spanning 2020-01-01 to 2026-02-18.
  - Ran merge script and confirmed everything worked.
  - Verified all documentation files were updated with correct row counts and date ranges.
- **Critique:**
  - Correct: EPU data was successfully cleaned and integrated into the analysis pipeline.
  - Improvement: The second upload corrected the date range—new file starts from 2020-01-01.

- **Task:** Add and use the official `M1 Assignment Description.pdf` to align repository deliverables with required locations and naming.
- **Prompt:** "reorganize files to match assignment format"
- **AI Output:**
  - Reviewed `M1 Assignment Description.pdf` requirements.
  - Mapped repository structure against required M1 structure.
  - Confirmed required deliverables/locations used for M1 submission (`README.md`, `M1_data_quality_report.md`, `data/final/data_dictionary.md`, `AI_AUDIT_APPENDIX.md`).
- **Verification:**
  - Checked assignment tree and required filenames against actual repository paths.
  - Confirmed deliverables exist at required root/final locations.
- **Critique:**
  - Correct: this ensured project organization follows assignment instructions rather than the prior layout.

- **Task:** Adapt existing files/code/data to M1-required format without deleting core data.
- **Prompt:** "could you re organize everything so it matches the location and everything as it is instructed in m1 assignment description?"
- **AI Output:**
  - Reorganized outputs into M1-required locations.
  - Added M1 entrypoint script names:
    - `code/fetch_coingecko_data.py`
    - `code/fetch_sec_data.py`
    - `code/fetch_macro_data.py`
    - `code/fetch_crypto_event_data.py`
    - `code/merge_final_panel.py`
- **Verification:**
  - Ran full pipeline (`python code/run_all.py`) and confirmed successful completion.
  - Confirmed final outputs generated in expected M1 locations.
- **Critique:**
  - Correct: improved clarity and assignment alignment while keeping prior work intact.

- **Task:** Reorder final crypto analysis panel for clarity by date then coin ranking.
- **Prompt:** "in the crypto analysis panel could you please reorder the spreadsheet by 1. date & by 2. coin ranking for clarity?"
- **AI Output:**
  - Updated `code/merge_final_with_macro_controls.py` to sort final merged panel by `date` then numeric `coin_rank` before saving.
  - Regenerated `data/final/crypto_analysis_panel.csv` with deterministic ordering.
- **Verification:**
  - Ran `python code/merge_final_panel.py`.
  - Checked top rows and validated sorted order.
- **Critique:**
  - Correct: improves readability and consistency of the final panel without changing values.

## Summary

- **Total AI use in this assignment stage:** 11 major uses (merge pipeline support, cleaning pipeline support, VIX/Fed macro cleaning, VIX/Fed macro merge, data dictionary, data quality report, final dataset export without blank `coin_id`, EPU data cleaning and integration, M1 assignment alignment, repository format adapted to M1 structure, final panel standardization).
- **Primary use cases:**
  1. Data engineering support (multi-file merge into one raw panel).
  2. Data cleaning pipeline support (missingness/date filter/type handling).
  3. Macro control data integration (VIX, Fed Funds, EPU).
  4. Repository structure and deliverable compliance with assignment specification.
  5. Structured technical writing from project requirements.
  6. Data documentation using dataset metrics.
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
