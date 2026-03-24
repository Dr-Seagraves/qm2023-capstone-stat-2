# M2 EDA Summary

## Key Findings

- Volatility comoves with both market and macro channels: `control_btc_corr_30d` has the strongest positive association with volatility ($r=0.253$), while `ffeffective_rate` is the strongest negative macro correlate ($r=-0.257$), supporting inclusion of both in M3.
- SEC-event effects are weak and short-lived in unconditional diagnostics: the strongest SEC-event correlation appears at lag 0 ($r=-0.008$), with event-window means showing no volatility spike (pre: $0.0336$, event: $0.0308$, post: $0.0324$).
- Cross-sectional heterogeneity is meaningful, but macro stress is the dominant channel: high-VIX regimes raise volatility most for `defi` and `centralized_exchange` (high-low gaps about $0.0129$ and $0.0128$), and VIX dominates EPU in lead-lag/sensitivity checks (best VIX lag $=9$, $r=0.308$; EPU peak $|r|=0.040$).
- Distribution and data-quality diagnostics support feasible modeling with robust inference: volatility is right-skewed with heavy tails (median $=0.0298$, 99th percentile $=0.1362$), while missingness in core variables remains low (`outcome_realized_vol_30d`: 1.51%, `control_btc_corr_30d`: 1.37%) and mostly due to initial lookback windows.

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
