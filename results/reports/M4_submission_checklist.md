# Milestone 4 Submission Checklist

Use this as a final pass before turning in the project.

## 1) Reproducibility and Technical Rigor (10 pts)

- [x] Run the documented pipeline successfully (`/bin/python3 code/run_all.py`).
- [x] Run smoke tests (`/bin/python3 -m unittest -v tests/test_project_smoke.py`).
- [x] Confirm required M3 diagnostics exist:
  - `M3_modelA_breusch_pagan.csv`
  - `M3_modelA_vif.csv`
  - `M3_residuals_vs_fitted.png`
  - `M3_qq_plot.png`
- [x] Confirm robustness files exist:
  - `M3_modelA_robustness_lags.csv`
  - `M3_modelA_robustness_outlier_trim.csv`
  - `M3_modelA_robustness_group_subsamples.csv`
- [ ] Re-run M3 model scripts after any late data/code edits.
- [ ] Verify every number in final memo matches current outputs.

## 2) Structure and Clarity (10 pts)

- [x] Draft includes all required sections:
  - Executive Summary
  - Methodology
  - Results
  - Conclusions and Recommendations
  - References
  - AI Audit Appendix
- [ ] Keep final memo length in target range (5-7 pages equivalent).
- [ ] Remove heavy technical jargon for committee audience.
- [ ] Final grammar and typo pass.

## 3) Results and Interpretation (12 pts)

- [x] Include FE results table content (Model A).
- [x] Include Model B comparison (OLS vs Random Forest).
- [x] Include at least one key substantive figure from `results/figures/`.
- [x] Include at least one diagnostic figure (`M3_residuals_vs_fitted.png` or `M3_qq_plot.png`).
- [x] Interpret coefficient magnitude and sign in plain language.
- [x] Discuss non-significant findings explicitly (for example, DeFi interaction at baseline).
- [x] Discuss robustness evidence (lags, outlier trim, subgroup checks).

## 4) Recommendations and Caveats (8 pts)

- [x] Include specific portfolio recommendations by token group.
- [x] Include scenario analysis (policy tightening vs easing).
- [x] Include uncertainty and caveats (identification limits, collinearity, external validity).
- [ ] Add explicit position sizes if your instructor expects percentages.

## 5) Final Packaging

- [ ] Ensure file names are final and consistent.
- [ ] Confirm all required milestone files are present at repo root.
- [ ] Commit from personal branch and push.
- [ ] Open PR if course workflow requires review.
