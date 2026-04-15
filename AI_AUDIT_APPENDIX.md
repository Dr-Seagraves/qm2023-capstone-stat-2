# AI Audit Appendix (Capstone Work)

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

## M2 (EDA Dashboard Stage)

- **Task (Plot 1: Correlation Heatmap):** Create heatmap using the final panel and specific variables.
- **Prompt:** "for plot 1, use `data/final/crypto_analysis_panel.csv` and create a heatmap with `outcome_realized_vol_30d`, `driver_sec_event_indicator`, `control_market_cap`, `control_total_volume`, `control_btc_corr_30d`, `vix`, `ffeffective_rate`, and `epu_index`"
- **AI Output:** It added Plot 1 code in `capstone_eda.ipynb`, computing all correlations, applying readable axis names (for example, `outcome_realized_vol_30d` -> "30D Realized Volatility"), and exporting `results/figures/M2_plot1_correlation_heatmap.png`.
- **Verification:** Confirmed all the values were located where needed and the figure exported at 300 DPI.
- **Critique:** Correlation is basically descriptive.

- **Task (Plot 2: Outcome Time Series):** Create outcome-over-time graph from the final panel.
- **Prompt:** "for plot 2, use `outcome_realized_vol_30d` from `crypto_analysis_panel.csv` and then plot average volatility over time"
- **AI Output:** It added Plot 2 code in `capstone_eda.ipynb`, grouping by `date`, computing `outcome_mean`, and exporting `results/figures/M2_plot2_outcome_time_series.png`.
- **Verification:** Confirmed the date aggregation and that the export was successful.
- **Critique:** Does not isolate causal drivers.

- **Task (Plot 3: Dual-Axis Outcome vs Driver):** Create dual-axis plot using the SEC-event driver.
- **Prompt:** "for plot 3, use `outcome_realized_vol_30d` and `driver_sec_event_indicator` from `crypto_analysis_panel.csv` showing daily events and include timing context"
- **AI Output:** It created a dual-axis panel with outcome line, event-day bars, 30-day driver. It also created an event-days panel and exported `results/figures/M2_plot3_dual_axis_outcome_driver.png`.
- **Verification:** Confirmed that the plot updated after adding some SEC data and that labels now read clearly (for example, "Average 30-day realized volatility" and "SEC event share (0 to 1)").
- **Critique:** Dual-axis charts can be more difficult to read, hence understand.

- **Task (Plot 4: Lagged Effect Analysis):** Create lag-correlation test for the SEC-event driver.
- **Prompt:** "for plot 4, use `driver_sec_event_indicator` as the driver and test lags 0, 1, 2, 3, 6, 12 with `groupby('coin_symbol').shift(lag)` against `outcome_realized_vol_30d`"
- **AI Output:** It added lag-correlation bar chart in `capstone_eda.ipynb` and exported `results/figures/M2_plot4_lagged_effects.png`.
- **Verification:** Confirmed there is no leakage in lag construction. Updated y-axis label to "Correlation with realized volatility" (simplified from "Pearson correlation" terminology).
- **Critique:** Correlations are small and not really significant.

- **Task (Plot 5: Group Box Plots):** Create distribution comparison by token group from the final panel.
- **Prompt:** "for plot 5, use `token_group` and `outcome_realized_vol_30d` from `crypto_analysis_panel.csv` and produce box plots"
- **AI Output:** It created group box plots and exported `results/figures/M2_plot5_group_boxplot.png`.
- **Verification:** Confirmed all three groups are present and plotted with clear labels (x-axis: "Token Group", y-axis: "30-day realized volatility").
- **Critique:** Large outliers, eventhough they are expected in crypto volatility.

- **Task (Plot 6: Group Sensitivity):** Compute group-specific sensitivity to SEC-event drivers.
- **Prompt:** "for plot 6, compute group correlations of `outcome_realized_vol_30d` with `driver_sec_event_indicator` by `token_group` & plot horizontal bars"
- **AI Output:** It computed group sensitivity and created the bar chart. Then, it exported `results/figures/M2_plot6_group_sensitivity.png`.
- **Verification:** Confirmed stablecoin/centralized_exchange/defi group correlations stem from the correct panel. Updated x-axis label to "Correlation: realized volatility vs SEC event indicator" for readability.
- **Critique:** Group-level correlations can be kind of trivial if there are lots of sparse event days.

- **Task (Plot 7: Control Scatter Plots):** Create outcome v. control scatter relationships.
- **Prompt:** "for plot 7, use `outcome_realized_vol_30d` v. `control_total_volume` and v. `epu_index` with trend lines"
- **AI Output:** It created two panels with scatter/trendline figures and exported `results/figures/M2_plot7_control_scatterplots.png`.
- **Verification:** Confirmed everything matches the requested fields/data. Updated plot titles and x-axis labels to use "(Scaled)" notation instead of logarithmic formulas for accessibility.
- **Critique:** Omitted-variable bias may still apply.

- **Task (Plot 8: Time Series Decomposition):** Create trend/seasonal/residual decomposition from aggregated outcome series.
- **Prompt:** "for Plot 8, aggregate `outcome_realized_vol_30d` by date, add daily frequency, and decompose into observed/trend/seasonal/residual"
- **AI Output:** It created four decomposition plots and exported `results/figures/M2_plot8_time_series_decomposition.png`.
- **Verification:** Confirmed all four components exported successfully.
- **Critique:** Decomposition should be analyzed before modeling.

- **Task (Plot 9: SEC Event-Window Volatility Profile):** Create inference-oriented event-window comparison.
- **Prompt:** "for plot 9, use SEC event dates and create an event-window volatility profile"
- **AI Output:** It added Plot 9 code in `capstone_eda.ipynb`, aligned SEC event dates to panel dates, computed mean `outcome_realized_vol_30d` by relative event day, and exported `results/figures/M2_plot9_event_window_volatility_profile.png`.
- **Verification:** Confirmed the table was built correctly.
- **Critique:** Event-window averages should be followed by controlled M3 estimation.

- **Task (Plot 10: VIX Regime Group Comparison):** Compare volatility across low/mid/high VIX regimes by token group.
- **Prompt:** "for plot 10, split `vix` into low/mid/high regimes and compare `outcome_realized_vol_30d` by `token_group` in bar chart"
- **AI Output:** It added Plot 10 code in `capstone_eda.ipynb`, formed VIX groups, added mean volatility by regime and `token_group`, and exported `results/figures/M2_plot10_vix_regime_group_comparison.png`.
- **Verification:** Confirmed all token groups were included and high-VIX means were highest for `defi` and `centralized_exchange`, as expected.
- **Critique:** Regime comparisons are somehow non-causal.

- **Task (Plot 11: Macro Lead-Lag Profile):** Compare lead-lag correlation structure of volatility with VIX and EPU.
- **Prompt:** "for plot 11, compute lead-lag correlations between daily average `outcome_realized_vol_30d` and `vix` and `epu_index`"
- **AI Output:** It added Plot 11 code in `capstone_eda.ipynb`, calculated lagged correlations for `vix` and `epu_index` against daily mean volatility, and exported `results/figures/M2_plot11_macro_lead_lag_profile.png`.
- **Verification:** Confirmed the VIX peaks around lag 9 and EPU correlations stay smaller. Simplified axis labels to "Lag (days)" and "Correlation" for clarity (removed mathematical formula notation).
- **Critique:** Lead-lag correlations should be double-checked with fixed-effects models.

- **Task (Plot 12: Coin-Level Macro Sensitivity Betas):** Compare standardized VIX and EPU sensitivities coin by coin.
- **Prompt:** "for plot 12, estimate coin-by-coin standardized sensitivities of `outcome_realized_vol_30d` to `vix` and `epu_index` using OLS"
- **AI Output:** It added Plot 12 code in `capstone_eda.ipynb`, ran per-coin standardized regressions for `vix` and `epu_index`, and exported `results/figures/M2_plot12_coin_macro_sensitivity_betas.png`.
- **Verification:** Confirmed all cryptos were plotted and that VIX sensitivity exceeded EPU. Updated y-axis label to "Sensitivity score" for accessibility (simplified from "Standardized beta coefficient").
- **Critique:** Linear assumptions may hide nonlinear responses.

- **Task (Plot 13: Event vs Non-Event Volatility by Coin):** Compare mean volatility on SEC event days v. non-event days for each coin.
- **Prompt:** "for plot 13, compare event-day v non-event-day mean `outcome_realized_vol_30d` for each crypto, in dumbbell plot"
- **AI Output:** It added Plot 13 code in `capstone_eda.ipynb`, computed coin-level event and non-event mean volatility, built dumbbell comparison figure, and exported `results/figures/M2_plot13_event_vs_nonevent_coin_dumbbell.png`.
- **Verification:** Confirmed event and non-event means were mapped correctly.
- **Critique:** Event v. non-event comparisons should be tested with additional controls in M3.

- **Task (M2 Narrative Summary):** Polish M2 summary using accurate terminology while keeping it aligned to group ideas and findings.
- **Prompt:** "we wrote our M2 summary. now help polish the wording to reinforce findings, hypotheses, and data-quality mitigations"
- **AI Output:** It updated `M2_EDA_summary.md` and `results/reports/M2_EDA_summary.md` with more accurate terminology, keeping our already written ideas.
- **Verification:** Double-checked that findings in summary files are aligned with plot outputs and metrics.
- **Critique:** None.

## M3 (Econometric Models Stage)

- **Task (Prompt 1: Model A specification design):** Select the exact Model A setup for this crypto panel.
- **Prompt:** "Help me choose the exact Model A specification for this crypto panel, including outcome, driver lag, controls, entity FE, and time FE."
- **AI Output:** Implemented a production Model A setup in `capstone_models.py` with:
  - Outcome: `outcome_realized_vol_30d`.
  - Driver construction: `driver_sec_event_indicator` with lag variants 0, 1, 2, 3.
  - FE structure: `entity_effects=True` and `time_effects=True` for baseline models.
  - Controls included in estimation matrix: `log_market_cap`, `log_total_volume`, `control_btc_corr_30d`.
  - Interaction strategy for identification under two-way FE when the driver is date-common.
- **Verification:** Confirmed the implemented variables and model structure in `capstone_models.py` and verified model output generation.
- **Critique:** Correct for this dataset structure; direct time-common effects are absorbed under time FE, so interaction-based identification is required.

- **Task (Prompt 2: End-to-end Model A script section):** Create and run a complete Model A section.
- **Prompt:** "Write a clean Model A section for my script that runs end to end and saves outputs."
- **AI Output:** Added full Model A pipeline in `capstone_models.py` and ensured it saves outputs to required M3 folders:
  - `results/tables/M3_modelA_coefficients_long.csv`
  - `results/tables/M3_modelA_regression_table.csv`
  - `results/tables/M3_regression_table.csv`
  - `results/tables/M3_modelA_breusch_pagan.csv`
  - `results/tables/M3_modelA_vif.csv`
  - `results/tables/M3_modelA_robustness_lags.csv`
  - `results/tables/M3_modelA_robustness_outlier_trim.csv`
  - `results/tables/M3_modelA_robustness_group_subsamples.csv`
  - `results/figures/M3_residuals_vs_fitted.png`
  - `results/figures/M3_qq_plot.png`
- **Verification:** Re-ran script and confirmed successful completion plus regeneration of all listed artifacts.
- **Critique:** Correct and reproducible for Model A requirements.

- **Task (Prompt 3: absorbed variable diagnosis):** Explain and fix absorption issue under two-way FE.
- **Prompt:** "Explain why my main driver gets absorbed in two way fixed effects and show how to fix identification."
- **AI Output:** Addressed absorption by creating group interaction terms for the driver (`driver_sec_event_indicator_lagX_x_grp_*`) so differential effects remain identified under date FE.
- **Verification:** Confirmed model estimation no longer fails due to missing absorbed driver coefficient in baseline table, and interaction coefficients appear in `M3_modelA_coefficients_long.csv`.
- **Critique:** Correct econometric handling for date-common policy/event drivers in panel FE context.

- **Task (Prompt 4: clustered v. unadjusted SE comparison):** Add robust-SE comparison.
- **Prompt:** "Add clustered standard errors and compare them against unadjusted standard errors in one table."
- **AI Output:** Added both model versions:
  - `FE_standard_SE` (unadjusted)
  - `FE_clustered_SE` (clustered by entity)
  And exported side-by-side comparisons in `results/tables/M3_modelA_regression_table.csv`.
- **Verification:** Confirmed table includes both model columns and coefficient/SE formats with significance stars.
- **Critique:** Correctly satisfies robust-standard-error comparison requirement.

- **Task (Prompt 5: required diagnostics):** Add BP, VIF, and residual diagnostics.
- **Prompt:** "Add Breusch Pagan, VIF, residuals vs fitted, and Q Q diagnostics for Model A."
- **AI Output:** Implemented diagnostics in `capstone_models.py` and exported:
  - `results/tables/M3_modelA_breusch_pagan.csv`
  - `results/tables/M3_modelA_vif.csv`
  - `results/figures/M3_residuals_vs_fitted.png`
  - `results/figures/M3_qq_plot.png`
- **Verification:** Confirmed all files exist and contain expected diagnostic outputs/plots.
- **Critique:** Correct; diagnostics were computed from fitted model residual structures and documented.

- **Task (Prompt 8: publication-style regression table):** Build a deliverable-ready Model A table.
- **Prompt:** "Build a publication style regression table from my FE results with stars and standard errors."
- **AI Output:** Added formatted export for key FE models with stars and parenthetical SE values in:
  - `results/tables/M3_modelA_regression_table.csv`
  - `results/tables/M3_regression_table.csv`
- **Verification:** Checked table layout and entries match required readability standards for M3 submission.
- **Critique:** Correct; table is concise and submission-ready for Model A section.

- **Task (Prompt 9: interpretation memo drafting from results):** Create Model A interpretation memo using actual estimates.
- **Prompt:** "Generate a Model A interpretation memo from my actual coefficients and p values."
- **AI Output:** Drafted `M3_interpretation.md` using computed outputs from Model A files, including:
  - headline effect statements,
  - diagnostics interpretation,
  - robustness summary,
  - caveats.
- **Verification:** Cross-checked numbers and significance references against `results/tables/M3_modelA_coefficients_long.csv` and other diagnostic/robustness exports.
- **Critique:** Correct and evidence-based; tied directly to generated model artifacts.

- **Task (Prompt 11: completeness check against rubric):** Validate whether Model A deliverables are complete.
- **Prompt:** "Check whether my current Model A deliverables satisfy the milestone rubric and list any missing items."
- **AI Output:** Completed checklist review against Model A requirements in `README(3).md`, confirming model estimation, diagnostics, robustness checks, table exports, figures, and memo file.
- **Verification:** Re-ran `capstone_models.py`, confirmed all required Model A artifacts regenerate, and identified one caveat in subgroup robustness (`centralized_exchange` row with divide-by-zero error in `M3_modelA_robustness_group_subsamples.csv`).
- **Critique:** Correct assessment; Model A is substantively complete with one subgroup-spec caveat flagged transparently.

- **Task (Model B Option 3: Random Forest vs OLS):** Use Machine Learning comparison for M3 option 3.
- **Prompt:** "for model B, create the code for option 3"
- **AI Output:** It created the code in `capstone_models.py` for Model B Option 3, creating the following outputs: `results/tables/M3_modelB_option3_metrics.csv`, `results/tables/M3_modelB_option3_feature_importance.csv`, `results/tables/M3_modelB_option3_ols_coefficients.csv`, `results/tables/M3_modelB_option3_predictions.csv` `results/figures/M3_modelB_option3_actual_vs_predicted.png`
- **Verification:** Confirmed the script location, name, and labels match M3 deliverable expectations in the README file. Confirmed the code is the one needed if we follow Option 3 in Model B.
- **Critique:** None.

- **Task (Update M3 Interpretation Memo):** Add Model B summary section to complete the memo.
- **Prompt:** "Can you help me complete part 6, the interpretation memo. These are the required parts. Required sections: Model A headline: '[1 unit/pp] increase in [DRIVER] → [magnitude] change in [OUTCOME]' with p-value. Interpret in economic units. Economic interpretation: 2-3 causal channels (e.g., leverage, discount rate, demand). Model B summary: DiD/ARIMA/ML results; key takeaway. Diagnostics: Heteroskedasticity (Breusch-Pagan), VIF, residual plots—implications and fixes. Robustness: Clustered SEs, alternative lags, outlier exclusions. Caveats: Omitted variables, parallel trends (DiD), external validity."
- **AI Output:** Updated M3_interpretation.md by adding the Model B Summary section, including results from the ML comparison (OLS vs. Random Forest metrics, feature importance, OLS coefficients, and key takeaway). Also updated the title from "M3 Interpretation Memo (Model A Only)" to "M3 Interpretation Memo" and removed the scope note to reflect coverage of both models.
- **Verification:** Confirmed the memo now includes all required sections as per README(3).md, with the Model B summary providing key results and takeaway from the ML comparison.
- **Critique:** Correct; completes the memo for full M3 compliance without altering existing content.

## Responsibility Statement

All outputs are our team’s responsibility.

## Team Sign-off

- Luke Birdseye
- Ben Brown
- Katie Koonts
- James Gawey
- Dani Gamboa
