# M2 EDA Summary

## Key Findings

- The strongest control correlation with volatility is `control_btc_corr_30d` ($r=0.253$), while `ffeffective_rate` is the strongest negative macro correlate ($r=-0.257$), indicating that market regime and macro tightening both matter for crypto risk.
- After SEC source backfill, the panel now includes 16 in-sample SEC event dates across 2020-2026 (3 in 2020, 1 in 2021, 3 in 2022, 4 in 2023, 2 in 2024, 1 in 2025, 2 in 2026), improving historical coverage for M3.
- SEC-event indicator correlation with volatility is small and negative at lag 0 ($r=-0.008$), decays toward zero by longer lags, and is near zero by lag 12 ($r=0.0005$), suggesting weak average unconditional effects.
- Lag testing across 0, 1, 2, 3, 6, and 12 days shows the strongest absolute SEC-event relationship at lag 0 ($r=-0.008$), making contemporaneous or short-lag specifications the primary M3 candidates.
- Group heterogeneity remains present: `stablecoin` ($r=-0.026$), `centralized_exchange` ($r=-0.022$), and `defi` ($r=-0.010$) show different driver sensitivities, motivating group-interaction specifications.
- Volatility is right-skewed with elevated tail risk (median $=0.0298$, 90th percentile $=0.0648$, 99th percentile $=0.1362$), consistent with crisis clustering and non-Gaussian residual risk.

## Hypotheses For M3

### Hypothesis 1: Regulatory Driver Effect
- Claim: SEC-event intensity is associated with lower short-horizon realized volatility in this sample.
- Model specification: Regress `outcome_realized_vol_30d` on `driver_sec_event_indicator` (baseline lag = 0, with 1-3 day lag robustness checks), with date and coin controls.
- Expected sign: Negative coefficient on lagged driver.
- Mechanism: Event periods may coincide with de-risking, liquidity withdrawal, or temporary stabilization in high-volatility tokens.

### Hypothesis 2: Macro Stress and Funding Conditions
- Claim: Higher market stress (`vix`) increases crypto volatility, while tighter rate regimes (`ffeffective_rate`) are associated with lower realized volatility in the panel average.
- Model specification: Include `vix` and `ffeffective_rate` jointly with the regulatory driver and fixed effects.
- Expected sign: Positive on `vix`, negative on `ffeffective_rate`.
- Mechanism: Broad risk sentiment raises volatility, while sustained policy tightening may compress speculative turnover and realized variation.

### Hypothesis 3: Group Heterogeneity In Regulatory Sensitivity
- Claim: Regulatory sensitivity differs by token group (`stablecoin`, `centralized_exchange`, `defi`).
- Model specification: Add interaction terms `driver_sec_event_indicator × token_group` (plus short-lag robustness terms).
- Expected sign: More negative sensitivity for `stablecoin` and `centralized_exchange` than `defi` in this sample.
- Mechanism: Group-level differences in business model, liquidity structure, and compliance exposure may transmit regulatory shocks differently.

## Data Quality Flags And M3 Mitigations

- Missingness is concentrated in rolling-window measures: `outcome_realized_vol_30d` missing share is 1.51% and `control_btc_corr_30d` is 1.37%, mostly from initial lookback periods.
- Planned mitigation: Use listwise deletion for baseline models and run robustness checks with balanced-sample windows.
- Volatility shows heavy tails and outlier episodes (99th percentile far above median), implying non-constant variance.
- Planned mitigation: Use heteroskedasticity-robust standard errors and test winsorized robustness.
- Several controls are materially correlated with each other and with the outcome (for example, market-cap/volume transforms vs. rates), creating multicollinearity risk.
- Planned mitigation: Check VIF, avoid redundant controls, and report sensitivity to alternative control sets.
- Relationship magnitudes for SEC-event effects are small in unconditional correlations.
- Planned mitigation: prioritize conditional models with lags, interactions, and fixed effects rather than relying on bivariate interpretation.
