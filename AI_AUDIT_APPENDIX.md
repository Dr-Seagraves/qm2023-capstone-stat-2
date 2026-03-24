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

## M2 Addendum (EDA Dashboard Stage)

- **Task (Plot 1: Correlation Heatmap):** Build the required heatmap using the final panel and specified variable set.
- **Prompt:** "For Plot 1, use `data/final/crypto_analysis_panel.csv` and build a heatmap with `outcome_realized_vol_30d`, `driver_sec_event_indicator`, `control_market_cap`, `control_total_volume`, `control_btc_corr_30d`, `vix`, `ffeffective_rate`, and `epu_index` (log-transform market cap and volume before correlation)."
- **AI Output:** Added Plot 1 code in `capstone_eda.ipynb` that computes the correlation matrix on the requested fields and exports `results/figures/M2_plot1_correlation_heatmap.png`.
- **Verification:** Confirmed matrix values populate and figure exports at 300 DPI.
- **Critique:** Correct for multicollinearity screening; correlation is descriptive only.

- **Task (Plot 2: Outcome Time Series):** Build the required outcome-over-time visualization from the final panel.
- **Prompt:** "For Plot 2, use `outcome_realized_vol_30d` from `crypto_analysis_panel.csv`, aggregate by date, and plot average volatility over time."
- **AI Output:** Added Plot 2 code in `capstone_eda.ipynb` that groups by `date`, computes `outcome_mean`, and exports `results/figures/M2_plot2_outcome_time_series.png`.
- **Verification:** Confirmed date aggregation and successful export.
- **Critique:** Correct for trend/volatility-clustering EDA; does not isolate causal drivers.

- **Task (Plot 3: Dual-Axis Outcome vs Driver):** Build the required dual-axis plot using the SEC-event driver.
- **Prompt:** "For Plot 3, use `outcome_realized_vol_30d` and `driver_sec_event_indicator` from `crypto_analysis_panel.csv`; show daily event-share bars plus 30-day smoothed driver and include event-timing context."
- **AI Output:** Implemented dual-axis panel with outcome line, event-day bars, 30-day driver smoothing, and cumulative event-days panel; exported `results/figures/M2_plot3_dual_axis_outcome_driver.png`.
- **Verification:** Confirmed plot updates after SEC backfill and that labels now reflect sparse events across 2020-2026.
- **Critique:** Correctly communicates sparse-event timing; dual-axis charts can still be visually sensitive to scaling choices.

- **Task (Plot 4: Lagged Effect Analysis):** Build lag-correlation test for the SEC-event driver using entity-safe shifts.
- **Prompt:** "For Plot 4, use `driver_sec_event_indicator` as the driver and test lags 0, 1, 2, 3, 6, 12 with `groupby('coin_symbol').shift(lag)` against `outcome_realized_vol_30d`."
- **AI Output:** Added lag-correlation bar chart in `capstone_eda.ipynb`; exported `results/figures/M2_plot4_lagged_effects.png`.
- **Verification:** Confirmed lag results update after SEC-source correction and no cross-entity leakage in lag construction.
- **Critique:** Correct method for preliminary lag selection; correlations are small and not equivalent to model significance testing.

- **Task (Plot 5: Group Box Plots):** Build distribution comparison by token group from the final panel.
- **Prompt:** "For Plot 5, use `token_group` and `outcome_realized_vol_30d` from `crypto_analysis_panel.csv` and produce box plots by group."
- **AI Output:** Implemented group box plots and exported `results/figures/M2_plot5_group_boxplot.png`.
- **Verification:** Confirmed all three groups are present and plotted with consistent labels.
- **Critique:** Correct for heteroskedasticity/outlier diagnostics; outliers are expected in crypto volatility tails.

- **Task (Plot 6: Group Sensitivity):** Compute and visualize group-specific sensitivity to the SEC-event driver.
- **Prompt:** "For Plot 6, compute group-level correlation of `outcome_realized_vol_30d` with `driver_sec_event_indicator` by `token_group` and plot horizontal bars."
- **AI Output:** Added group sensitivity computation and bar chart; exported `results/figures/M2_plot6_group_sensitivity.png`.
- **Verification:** Confirmed stablecoin/centralized_exchange/defi group correlations are produced from the corrected panel.
- **Critique:** Correct for heterogeneity screening; group-level correlation can be noisy with sparse event days.

- **Task (Plot 7: Control Scatter Plots):** Build required outcome-vs-control scatter relationships.
- **Prompt:** "For Plot 7, use `outcome_realized_vol_30d` vs `control_total_volume` (log scale) and vs `epu_index` with trend lines."
- **AI Output:** Implemented two-panel scatter/trendline figure and exported `results/figures/M2_plot7_control_scatterplots.png`.
- **Verification:** Confirmed controls and transformations match requested fields.
- **Critique:** Correct for bivariate pattern inspection; omitted-variable bias still applies.

- **Task (Plot 8: Time Series Decomposition):** Build trend/seasonal/residual decomposition from aggregated outcome series.
- **Prompt:** "For Plot 8, aggregate `outcome_realized_vol_30d` by date, interpolate daily frequency, and decompose into observed/trend/seasonal/residual components."
- **AI Output:** Added decomposition-style four-panel plot and exported `results/figures/M2_plot8_time_series_decomposition.png`.
- **Verification:** Confirmed all four components render and export successfully.
- **Critique:** Compatible with environment constraints and useful for EDA; decomposition assumptions should be stated before formal modeling.

- **Task (M2 Narrative Summary):** Polish and refine our M2 summary using accurate terminology, while keeping it aligned to our ideas and the same dataset used in plots.
- **Prompt:** "I already wrote my M2 summary. Using the same `crypto_analysis_panel.csv` fields used in Plots 1-8, help me polish my wording with best-practice terminology and tighten the findings, M3 hypotheses, and data-quality mitigations."
- **AI Output:** Updated `M2_EDA_summary.md` and `results/reports/M2_EDA_summary.md` with refreshed metrics after SEC event backfill.
- **Verification:** Recomputed correlations, lag values, missingness, quantiles, and event-date distribution from final panel before writing.
- **Critique:** Empirically aligned with current data pipeline; conclusions remain exploratory pending M3 estimation.

## Responsibility Statement

All outputs are our team’s responsibility.

## Team Sign-off

- Luke Birdseye
- Ben Brown
- Katie Koonts
- James Gawey
- Dani Gamboa
