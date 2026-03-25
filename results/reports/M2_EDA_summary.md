# M2 EDA Summary

## Key Findings

- Volatility varies along with both market regime and macro trends: `control_btc_corr_30d` has the strongest positive correlation with volatility ($r=0.253$), whereas `ffeffective_rate` is the strongest negative macro correlate ($r=-0.257$), which makes them both good candidates to be included in M3.
- SEC-event effects are weak and short: the strongest SEC-event correlation is at lag 0 ($r=-0.008$), with event-window means showing no volatility spike (pre: $0.0336$, event: $0.0308$, post: $0.0324$).
- Cross-sectional heterogeneity is significant, but macro stress is the main cause of change: high-VIX regimes raise volatility, specially for `defi` and `centralized_exchange` (high-low gaps around $0.0129$ and $0.0128$), and VIX seems better than EPU for sensitivity checks (best VIX lag $=9$, $r=0.308$; EPU peak $|r|=0.040$).
- Volatility is right-skewed with heavy tails (median $=0.0298$, 99th percentile $=0.1362$), and missingness in core variables stays low (`outcome_realized_vol_30d`: 1.51%, `control_btc_corr_30d`: 1.37%), mostly due to initial lookback windows.

## Hypotheses For M3

### Hypothesis 1: Regulatory Driver Effect
- Claim: SEC-event intensity is related to lower short-term volatility in this sample.
- Model specification: Do a regression of `outcome_realized_vol_30d` on `driver_sec_event_indicator` (baseline lag = 0, with 1-3 day lag robustness checks), including date and cryptocurrency controls.
- Expected sign: Negative coefficient.
- Mechanism: Event periods may happen simultaneously with de-risking, liquidity withdrawal, or temporary stabilization in high-volatility cryptos.

### Hypothesis 2: Macro Stress and Funding Conditions
- Claim: Higher market stress (`vix`) increases crypto volatility, while tighter rate regimes (`ffeffective_rate`) are associated with lower crypto volatility.
- Model specification: Include `vix` and `ffeffective_rate` together with the regulatory driver and fixed effects.
- Expected sign: Positive on `vix`, negative on `ffeffective_rate`.
- Mechanism: General risk sentiment raises volatility, whereas policy tightening may reduce speculative operations.

### Hypothesis 3: Group Heterogeneity In Regulatory Sensitivity
- Claim: Regulatory sensitivity differs by token group (`stablecoin`, `centralized_exchange`, `defi`).
- Model specification: Add interaction terms `driver_sec_event_indicator × token_group` plus short-lag robustness terms.
- Expected sign: More negative sensitivity for `stablecoin` and `centralized_exchange` than for `defi`.
- Mechanism: Token type differences, liquidity, and policy exposure may transmit regulatory shocks in many different ways.

## Data Quality Flags And M3 Mitigations

- Missingness is mainly found because of initial lookback windows: `outcome_realized_vol_30d` missing share is 1.51% and `control_btc_corr_30d` is 1.37%.
- Planned mitigation: Use listwise deletion for baseline models and run robustness checks with balanced-sample windows.
- Volatility shows heavy tails and outlier episodes (99th percentile far above median), implying non-constant variance.
- Planned mitigation: Use heteroskedasticity-robust standard errors and test winsorized robustness.
- Several controls are materially correlated with each other and with the outcome (for example, market-cap/volume transforms vs. rates), creating multicollinearity risk.
- Planned mitigation: Check VIF, avoid redundant controls, and report sensitivity to alternative control sets.
- Relationship magnitudes for SEC-event effects are small in unconditional correlations.
- Planned mitigation: prioritize conditional models with lags, interactions, and fixed effects rather than relying on bivariate interpretation.
