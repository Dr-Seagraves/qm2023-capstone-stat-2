# Reproducibility Check (Milestone 4)

Date run: 2026-04-22  
Branch used: `personal-capstone-work`  
Environment: Codespaces (Linux), Python 3.12.3 via `/bin/python3`

## Commands Executed

1. Smoke tests

```bash
/bin/python3 -m unittest -v tests/test_project_smoke.py
```

Result: PASS (3 tests, 0 failures)

- `test_required_docs_exist`: ok
- `test_required_m3_outputs_exist`: ok
- `test_group_subsample_table_contains_all_groups`: ok

2. End-to-end pipeline

```bash
/bin/python3 code/run_all.py
```

Result: PASS

Scripts completed in order:

- `code/config_paths.py`
- `code/fetch_coingecko_data.py`
- `code/fetch_sec_data.py`
- `code/fetch_macro_data.py`
- `code/fetch_crypto_event_data.py`
- `code/merge_final_panel.py`

Terminal output confirmed successful completion with:

`Pipeline completed successfully.`

## Key Outputs Re-Confirmed

- Final panel: `data/final/crypto_analysis_panel.csv`
- Metadata: `data/final/crypto_analysis_panel_metadata.md`
- M3 core tables:
  - `results/tables/M3_modelA_regression_table.csv`
  - `results/tables/M3_modelA_breusch_pagan.csv`
  - `results/tables/M3_modelA_vif.csv`
  - `results/tables/M3_modelA_robustness_lags.csv`
  - `results/tables/M3_modelA_robustness_outlier_trim.csv`
  - `results/tables/M3_modelA_robustness_group_subsamples.csv`
  - `results/tables/M3_modelB_option3_metrics.csv`
- M3 figures:
  - `results/figures/M3_residuals_vs_fitted.png`
  - `results/figures/M3_qq_plot.png`
  - `results/figures/M3_modelB_option3_actual_vs_predicted.png`

## Notes

- Reproducibility checks passed in this environment without manual debugging.
- If additional memo edits change reported values, rerun the same two commands above before final submission.
