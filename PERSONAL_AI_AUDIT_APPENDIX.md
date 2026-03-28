# Personal AI Audit Appendix (M2)

## Student
- James Gawey

## AI Tools Used
- GitHub Copilot

## Scope
This document summarizes my individual use of AI assistance for Milestone 2 (EDA dashboard stage), including prompts, outputs, verification actions, and critiques.

## Personal AI-Assisted Tasks

- **Task:** Build and verify M2 visual outputs in the notebook workflow.
- **Prompt (representative):** "Create the required M2 plots from `data/final/crypto_analysis_panel.csv` and save each figure to `results/figures` with descriptive labels."
- **AI Output:** Assisted with plot logic patterns, code refinement, and export paths for the full M2 figure set.
- **Verification:** Checked that each output file exists in `results/figures`, reviewed labels/titles, and cross-checked plotted fields against requested variables.
- **Critique:** AI improved speed and consistency, but all interpretation still required manual review to avoid overclaiming from descriptive patterns.

- **Task:** Improve M2 written summary language and alignment with observed metrics.
- **Prompt (representative):** "Polish our M2 summary wording while preserving findings and model implications."
- **AI Output:** Helped tighten language in the summary narrative and align findings with EDA outputs.
- **Verification:** Compared text in `M2_EDA_summary.md` and `results/reports/M2_EDA_summary.md` against observed plot metrics and variable definitions.
- **Critique:** Wording quality improved, but accuracy depended on manual metric checks.

- **Task:** Generate table deliverables for M2 reporting support.
- **Prompt (representative):** "Create tables for descriptive statistics, correlations, missingness, and event vs non-event volatility."
- **AI Output:** Added `code/build_m2_tables.py` and generated:
  - `results/tables/M2_table1_descriptive_stats.csv`
  - `results/tables/M2_table2_correlation_matrix.csv`
  - `results/tables/M2_table3_missingness_report.csv`
  - `results/tables/M2_table4_event_vs_nonevent_volatility.csv`
- **Verification:** Executed script and confirmed table files are generated with expected schemas and values.
- **Critique:** Automates reproducible reporting, but table interpretation and final narrative choices remain my responsibility.

## Quality and Responsibility Notes

- I treated AI suggestions as draft support, not as final truth.
- I verified file outputs, variable mappings, and summary claims before accepting changes.
- I accepted responsibility for final content decisions and submission quality.

## Personal Sign-Off
- James Gawey
