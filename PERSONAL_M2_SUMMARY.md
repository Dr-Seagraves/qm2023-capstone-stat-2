# Personal M2 EDA Summary (James Gawey)

## Key Findings

- The strongest macro/control associations in the panel were `control_btc_corr_30d` (positive) and `ffeffective_rate` (negative), which supports including both in M3 model specifications.
- SEC-event relationships were weak in unconditional EDA comparisons, so event effects should be evaluated in conditional models rather than in raw bivariate interpretation.
- Volatility behavior varied across token groups under higher stress regimes, suggesting interaction terms are appropriate in M3.
- Outcome distributions showed right skew and heavy tails, supporting robust standard errors and outlier-sensitive robustness checks.

## Personal M3 Hypotheses

### Hypothesis 1: SEC Driver Effect
- **Claim:** SEC-event intensity is associated with short-horizon volatility changes, but effect size is likely small after controls.
- **Model direction:** Include lagged `driver_sec_event_indicator` terms with date and coin fixed effects.
- **Expected sign:** Mild negative or near-zero baseline effect after controls.

### Hypothesis 2: Macro Stress Channel
- **Claim:** Macro stress has stronger explanatory power for volatility than raw event-day indicators.
- **Model direction:** Include `vix`, `ffeffective_rate`, and `epu_index` jointly in fixed-effects regressions.
- **Expected sign:** Positive for `vix`; negative for `ffeffective_rate`; weaker, data-dependent sign for `epu_index`.

### Hypothesis 3: Heterogeneous Effects by Token Group
- **Claim:** Regulatory and macro sensitivity differs by `token_group`.
- **Model direction:** Add interaction terms between the driver and token-group indicators.
- **Expected sign:** Stronger sensitivity in `defi` and `centralized_exchange` than in `stablecoin`.

## Data Quality Flags and Planned Mitigations

- **Missingness from rolling-window construction:** Use consistent sample windows and report effective sample sizes.
- **Heavy tails/outliers in volatility:** Use heteroskedasticity-robust inference and winsorized sensitivity checks.
- **Potential multicollinearity across controls:** Monitor VIF and compare reduced vs full control sets.
- **Small unconditional SEC correlations:** Prioritize fixed-effects specifications with lag structures and interactions.

## Personal Reflection

M2 successfully clarified variable behavior and helped narrow credible M3 specifications. The most important takeaway is that macro conditions appear to explain more volatility variation than headline event timing in simple EDA, so M3 should focus on conditional structure and heterogeneity.
