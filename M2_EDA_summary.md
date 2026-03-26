# M2 EDA Summary

## Key Findings

- Volatility moves according to both market regime and macroeconomic trends: `control_btc_corr_30d` has the strongest positive correlation with volatility ($r=0.253$), while `ffeffective_rate` is the strongest negative macro correlation ($r=-0.257$), which makes them both good candidates to be included in M3.
- SEC-event effects are weak: the strongest SEC-event correlation appears at lag 0 ($r=-0.008$), with event-window means showing no volatility spike (pre: $0.0336$, event: $0.0308$, post: $0.0324$).
- Cross-sectional heterogeneity is significant, even though macro stress is the main channel: high-VIX market regimes raise volatility specially for `defi` and `centralized_exchange` (high-low gaps around $0.0129$ and $0.0128$), and VIX's relationship is stronger tha EPU's in lead-lag/sensitivity checks (best VIX lag $=9$, $r=0.308$; EPU peak $|r|=0.040$).
- Distribution and data-quality diagnostics support feasible modeling with robust inference: volatility is right-skewed with heavy tails (median $=0.0298$, 99th percentile $=0.1362$), while missingness in core variables remains low (`outcome_realized_vol_30d`: 1.51%, `control_btc_corr_30d`: 1.37%) and mostly due to initial lookback issues.

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

- Missingness is mainly found because of initial lookback windows: `outcome_realized_vol_30d` missing share is 1.51% and `control_btc_corr_30d` is 1.37%.
- M3 Mitigation: Use list-based elimination for baseline models and run robustness checks using balanced sample windows.
- Volatility shows heavy tails and outliers (99th percentile far above median), proving non-constant variance.
- M3 Mitigation: Use heteroskedasticity-robust standard errors and test winsorized robustness.
- Some controls are correlated with each other as well as with the outcome (for instance, market-cap/volume transforms v. rates), creating multicollinearity risk.
- M3 Mitigation: Check VIF, avoid redundant controls, and report sensitivity to other control sets.
- Relationship magnitudes for SEC-event effects are small in unconditional correlations.
- M3 Mitigation: prioritize conditional models with lags, interactions, and fixed effects rather than relying too much on bivariate interpretation.
