# M3 Interpretation Memo

## Model A Headline

In the two way fixed effects specification (entity FE + date FE, clustered by coin), SEC event exposure is estimated through interaction terms with token groups because the raw event indicator is common across all tokens on each date and is absorbed by time fixed effects.

- For stablecoins relative to the baseline group, a 1 unit increase in `driver_sec_event_indicator` is associated with a +0.007431 increase in `outcome_realized_vol_30d` (p < 0.001).
- For DeFi relative to the baseline group, the same increase is associated with +0.002691 (p = 0.453), not statistically significant.

Main table source: `results/tables/M3_modelA_coefficients_long.csv`.

## Economic Interpretation

1. Differential sensitivity by token class appears stronger for stablecoins than for DeFi in this sample period, conditional on coin and date fixed effects.
2. The positive sign on stablecoin interaction can reflect re pricing and liquidity fragmentation channels around regulatory headlines, where "stability" narratives are challenged and short-horizon variance rises.
3. In contrast, DeFi exposure appears noisier and less precisely estimated after conditioning on within-coin and common-time shocks.

## Model B Summary

Model B: Machine Learning Comparison (Random Forest vs. OLS)

Results: OLS achieved higher predictive accuracy on the test set (R² = 0.163, RMSE = 0.036) compared to Random Forest (R² = 0.109, RMSE = 0.038). Feature importance from Random Forest highlights token group (stablecoin: 34%, DeFi: 1%) and macroeconomic variables (Fed Funds rate: 18%, BTC correlation: 16%) as key predictors, while the SEC event indicator has negligible importance (0.001%). OLS coefficients show the SEC event indicator is insignificant (coeff = 0.0003, p = 0.88).

Key Takeaway: Despite Random Forest's ability to capture nonlinearities, OLS provides better out-of-sample predictions and interpretability. The SEC event indicator has minimal predictive power for realized volatility, suggesting regulatory events do not strongly drive short-term volatility beyond what is captured by token characteristics and macro controls.

## Diagnostics (Required)

### Heteroskedasticity (Breusch-Pagan)

- LM p-value: 9.59e-132
- F-test p-value: 6.96e-134

Interpretation: strong evidence of heteroskedasticity, so clustered standard errors are appropriate and used for primary inference.

Source: `results/tables/M3_modelA_breusch_pagan.csv`.

### Multicollinearity (VIF)

- `log_market_cap`: 355.73
- `log_total_volume`: 327.25
- `control_btc_corr_30d`: 4.57

Interpretation: severe collinearity between size/liquidity controls; coefficient-level interpretation for those controls should be cautious.

Source: `results/tables/M3_modelA_vif.csv`.

### Residual Diagnostics

- Residuals vs fitted plot saved: `results/figures/M3_residuals_vs_fitted.png`
- Q-Q plot saved: `results/figures/M3_qq_plot.png`

Interpretation: residual diagnostics are provided to assess variance pattern and tail behavior; inference is based on clustered SE to address non-constant variance.

## Robustness Checks (Required)

### 1. Clustered vs Standard SE

Both versions are estimated and reported in the regression table (`Model_1_FE_Standard` vs `Model_2_FE_Clustered`) in:

- `results/tables/M3_modelA_regression_table.csv`
- `results/tables/M3_regression_table.csv`

### 2. Alternative Lag Structures (0-3)

For the DeFi interaction term:

- Lag 0: 0.002691 (p = 0.453)
- Lag 1: 0.002604 (p = 0.536)
- Lag 2: 0.002335 (p = 0.579)
- Lag 3: 0.000088 (p = 0.982)

Interpretation: the DeFi differential effect is not robustly significant across short lags.

Source: `results/tables/M3_modelA_robustness_lags.csv`.

### 3. Outlier Exclusion (Top 1% Outcome Trim)

For the DeFi interaction term:

- Baseline clustered: 0.002691 (p = 0.453)
- Trimmed sample: 0.004706 (p = 0.061)

Interpretation: effect magnitude increases and approaches conventional significance after trimming extreme-volatility tails, indicating sensitivity to outliers.

Source: `results/tables/M3_modelA_robustness_outlier_trim.csv`.

### 4. Group Subsamples

Subsample models were estimated with entity FE only so a time-common SEC event driver remains identifiable within each group.

- Centralized exchange: -0.006963 (p = 0.138, pooled OLS fallback due subgroup FE singularity)
- DeFi: -0.006489 (p = 0.081)
- Stablecoin: -0.000415 (p < 0.001)

Source: `results/tables/M3_modelA_robustness_group_subsamples.csv`.

## Caveats

1. The SEC event indicator is common by date, so in two-way FE it is identified only through interactions with cross-sectional group structure, not as a standalone level effect.
2. High VIF among `log_market_cap` and `log_total_volume` suggests unstable individual control coefficients.
3. One subgroup (centralized_exchange) required a pooled OLS fallback in robustness estimation because FE estimation became singular in that slice.
4. External validity is limited to this token universe and sample window.
5. Results are conditional associations under FE assumptions; omitted time-varying confounders at the coin level may still bias estimates.
