# QM 2023 Capstone Project - Milestone 4
# MEMORANDUM

**TO:** Investment Committee / Risk Committee  
**FROM:** Stat 2 - Luke Birdseye, Ben Brown, Katie Koonts, James Gawey, Dani Gamboa  
**DATE:** May 1st, 2026  
**RE:** Cryptocurrency Regulatory Risk and Portfolio Allocation Recommendation

---

## PAGE 1 OF 6
## Executive Summary (0.5 page, no tables/figures)

This final investment memo analyzes how regulatory events such as SEC events, FED policy changes, or market sentiment are associated with cryptocurrency returns and volatility. We have used a panel of assets and macro indicators. In our main specification, a one-unit increase in regulatory pressure is associated with a [X]-percentage-point change in next-period returns, controlling for asset fixed effects and time effects. The effect is statistically [significant/not significant] at the [1%/5%/10%] level and is economically meaningful in the context of recent enforcement cycles.

We find heterogeneity across asset groups: [Group A] shows the largest downside response to adverse events, while [Group B] is more resilient. Robustness checks using lag alternatives, subsample windows, and outlier trimming show [consistent/mixed] estimates around the main effect.

Recommendation: in the current regulatory environment, we recommend [overweight/neutral/underweight] exposure to [group(s)] and reducing exposure to [group(s)] by [X%] relative to benchmark. Under a stricter-enforcement scenario, the model implies [expected drawdown/return impact], while a policy-clarification scenario implies [upside estimate].

\newpage

## PAGE 2 OF 6
## 1. Methodology (about 1 page)

### 1.1 Data Sources

Primary panel and merges (source files):
- `data/final/crypto_analysis_panel.csv` — final coin-by-date analysis panel combining CoinGecko rankings, SEC press/litigation events, and macro series. (See `M4_methodology.md`.)
- `data/final/macro_controls_merged.csv` — macro controls including EPU, VIX, and the effective federal funds rate.
- `data/final/sec_press_litigation_clean_final.csv` — cleaned SEC press/litigation events; used to construct the `driver_sec_event_indicator` and event-date metadata.

Key variables used in the analysis (paraphrased from team files):
- Outcome: `outcome_realized_vol_30d` (realized 30-day volatility).
- Main driver: `driver_sec_event_indicator` (SEC press/litigation exposure flag, possibly lagged).
- Group labels: `token_group` (e.g., stablecoin, DeFi, baseline groups).
- Controls: `log_market_cap`, `log_total_volume`, `control_btc_corr_30d`, `vix`, `ffeffective_rate` (and other macro controls from the merged file).

### 1.2 Sample Construction

Initial sample and construction (team summary):
- Final analysis panel: 19,852 rows across 10 tokens (bnb, btc, doge, eth, figr_heloc, sol, trx, usdc, usdt, xrp).
- Date coverage: 2020-02-19 to 2026-02-18.
- SEC event exposure: 147 flagged rows with 17 unique SEC action dates in the panel.

Cleaning steps applied before estimation: convert date to datetime; log-transform market cap and volume; drop rows missing variables required for each specification; no additional imputation performed at this stage. The panel is unbalanced due to token entry/exit. (Source: `M4_methodology.md`)

### 1.3 Model Specifications

Model A (preferred two-way fixed effects):

$$
Y_{it} = \beta_0 + \beta_1\, (SEC\_event_{i,t-k} \times Group_i) + \beta_2 X_{it} + \alpha_i + \delta_t + \varepsilon_{it}
$$

Where $Y_{it}$ is `outcome_realized_vol_30d`, the main driver is the SEC event flag (often interacted with `token_group` to identify heterogeneous group responses), $X_{it}$ are the controls listed above, $\alpha_i$ are coin fixed effects and $\delta_t$ are date fixed effects. Standard errors are clustered at the coin level. This specification isolates whether token groups react differently to shared SEC event dates. (Source: `M4_methodology.md`)

Model B (benchmark / predictive check): an OLS predictive specification and a random-forest benchmark are estimated on the same feature set to validate directional patterns. Model B is used as a robustness/predictive comparison rather than the primary causal estimator. (Source: `M4_methodology.md`, `M4 Diagnostics.md`)

\newpage

## PAGE 3 OF 6
## 2. Results (part 1)


### 2.1 Table 1: Main Fixed Effects Results (select estimates and sample)

| Variable | Coefficient (illustrative) | p-value | Significance |
|---|---:|---:|---:|
| SEC event × Stablecoin (group interaction) | negative and economically meaningful (stablecoin membership associated with lower realized vol.) | <0.001 | *** |
| SEC event × DeFi (group interaction) | smaller, less-stable positive sensitivity | <0.01 | ** |
| Control: BTC correlation (`control_btc_corr_30d`) | -0.0206 | <0.001 | *** |
| Control: Federal Funds Rate (`ffeffective_rate`) | -0.00385 | <0.001 | *** |
| Control: EPU index (`epu`) | -0.00000935 | <0.001 | *** |
| N (observations) | 19,852 |  |  |
| OLS test R^2 (benchmark) | 0.1633 (out-of-sample, Model B benchmark) |  |  |

Notes: Coefficients above are drawn from the team's preferred specifications and the Model B diagnostic outputs; exact tabulated estimates (standard errors, t-stats) should be placed in the final table generated from regression outputs. Significance notation: *** p<0.01, ** p<0.05, * p<0.10. (Source: `M4_methodology.md`, `M4 Diagnostics.md`)

Economic mechanism paragraph template:
The estimated effect is consistent with three channels: (1) liquidity contraction during enforcement episodes, (2) increased compliance uncertainty and risk premia, and (3) shifts from high-risk tokens to benchmark assets.

### 2.1.1 Control Variable Interpretation

Use this space to briefly interpret 1-2 control variables that are economically meaningful.

- **[Control variable 1]:** [Explain what the coefficient means in plain language and whether it is significant.]
- **[Control variable 2]:** [Explain what the coefficient means in plain language and whether it is significant.]
- If controls are purely technical, note that they are included for model adjustment and skip substantive interpretation.

### 2.2 Table 2: Alternative Specification Results

| Variable / Metric | Estimate |
|---|---:|
| Key effect (Model B) | [ ] |
| Std. Error / CV metric | [ ] |
| p-value / OOS score | [ ] |
| N | [ ] |

Interpretation paragraph template:
Model B (predictive benchmark) provides confirming evidence: OLS outperforms a random-forest benchmark on test-set performance (OLS test R^2 = 0.1633, test RMSE = 0.0364; Random Forest test R^2 = 0.1095, test RMSE = 0.0375). The predictive exercise reinforces that token structure and macro regime explain realized volatility patterns more than the raw SEC event flag alone. (Source: `M4 Diagnostics.md`)

\newpage

## PAGE 4 OF 6
## 2. Results (part 2: figures + diagnostics)


### 2.3 Figure 1: Outcome vs Regulatory Environment

Figure file: `results/figures/M3_modelB_option3_actual_vs_predicted.png`.
Caption: Actual vs. predicted realized volatility (Model B). Both OLS and the forest capture routine variation but compress the upper tail; the visual confirms underprediction of the largest spikes while matching central tendencies. (Source: `M4 Diagnostics.md`)


### 2.4 Figure 2: Diagnostic Plot (Residuals vs Fitted)

Figure file: `results/figures/M3_residuals_vs_fitted.png`.
Caption: Residuals vs fitted values show residuals centered near zero with some compression in the upper tail; we therefore report clustered standard errors and conduct heteroskedasticity-robust checks (Breusch-Pagan, VIF screening). (Source: `M4 Diagnostics.md`)

### 2.5 Figure 3: Group Heterogeneity (or key cross-section)

Insert figure path and caption:
- Figure file: results/figures/[figure3_filename].png
- Caption: [Group A] and [Group B] are most sensitive to regulatory changes, while [Group C] is comparatively resilient.


### 2.6 Robustness Summary

| Check | Specification | Main Coefficient | Conclusion |
|---|---:|---:|---|
| Lag sensitivity | k = 1,2,3 | stable sign for group interactions | Stable |
| Outlier trim | top 1% trimming | sign preserved | Stable |
| Subsample | group subsamples, entity FE only | sign preserved for stablecoins | Stable |

Short synthesis: Robustness checks (lag alternatives, top-tail trimming, subsample/group checks) preserve the sign of the preferred group interactions; the stablecoin effect is the most consistently robust result across specifications. (Source: `M4_methodology.md`, `M4 Diagnostics.md`)

\newpage

## PAGE 5 OF 6
## 3. Conclusions and Recommendations (about 1 page)


### 3.1 Portfolio Recommendation

Recommended tactical allocation for the next [horizon]:

1. Overweight [Defensive group] by [X%] versus benchmark.
2. Keep neutral exposure in [Core group].
3. Underweight [High-sensitivity group] by [Y%] until [trigger condition].

Rationale:
- Estimated sensitivity to regulation is lowest in [Defensive group].
- Downside tail risk is concentrated in [High-sensitivity group].
- Expected risk-adjusted return under base case favors [recommended mix].


### 3.2 Scenario Analysis

| Scenario | Regulatory Path | Predicted Return Impact | Probability |
|---|---|---:|---:|
| Baseline | [ ] | [ ] | [ ] |
| Favorable policy clarity | [ ] | [ ] | [ ] |
| Adverse enforcement cycle | [ ] | [ ] | [ ] |

Expected value calculation:
$$
E[R] = \sum_s p_s \cdot R_s = [calculation]
$$

Decision statement:
Given [asymmetric upside/downside], the recommended stance is [risk-on / neutral / defensive] with active monitoring of [key trigger variables].

### 3.3 Risks, Caveats, and Limitations

1. Identification risk: [parallel trends / omitted drivers / reverse causality concern].
2. Measurement risk: regulatory severity and event timing may be noisy.
3. External validity risk: relationships estimated on [period] may not hold under new market regimes.
4. Model dependence: lag structure and control set influence magnitude, even when sign is stable.

### 3.4 Future Work

1. Add higher-frequency event windows for announcement-time effects.
2. Estimate non-linear threshold effects in stress regimes.
3. Extend with alternative regulatory datasets and cross-market spillovers.

\newpage

## PAGE 6 OF 6
## 4. References (about 0.5 page)

1. CoinGecko. Cryptocurrency market data. Retrieved April 2026, from https://www.coingecko.com/  (Source: `M4_references+AI_audit_summary.md`)
2. Federal Reserve Bank of St. Louis. (n.d.). VIXCLS: CBOE Volatility Index: VIX. FRED. Retrieved April 2026, from https://fred.stlouisfed.org/series/VIXCLS  (Source: `M4_references+AI_audit_summary.md`)
3. Federal Reserve Bank of St. Louis. (n.d.). DFF: Effective Federal Funds Rate. FRED. Retrieved April 2026, from https://fred.stlouisfed.org/series/DFF  (Source: `M4_references+AI_audit_summary.md`)
4. Federal Reserve Bank of St. Louis. (n.d.). USEPUINDXD: U.S. Economic Policy Uncertainty Index. FRED. Retrieved April 2026, from https://fred.stlouisfed.org/series/USEPUINDXD  (Source: `M4_references+AI_audit_summary.md`)
5. U.S. Securities and Exchange Commission. (n.d.). News & press releases. Retrieved April 2026, from https://www.sec.gov/news/press-releases  (Source: `M4_references+AI_audit_summary.md`)


## Appendix: AI Audit Summary (0.5-1 page)

AI tools used: GitHub Copilot; ChatGPT (Raptor mini Preview). (Source: `M4_references+AI_audit_summary.md`)

Summary of AI use and verification (condensed):

- M1: AI assisted in summarizing dataset characteristics and suggesting candidate outcomes and controls. Team verification: cross-checked recommendations against `data/processed/` and the final merged `crypto_analysis_panel.csv`; manual adjustments applied for crypto-specific volatility interpretation. (Source: `M4_references+AI_audit_summary.md`)

- M2: AI helped translate a REIT-focused fixed-effects model into a crypto panel specification. Team verification: compared AI draft to `merge_final_with_macro_controls.py` and the panel structure; corrected terminology and ensured the driver is operationalized as an interaction with `token_group`. (Source: `M4_references+AI_audit_summary.md`)

- M3: AI drafted figure captions and diagnostic narratives. Team verification: confirmed figure filenames and captions against `results/figures/M3_modelB_option3_actual_vs_predicted.png` and `results/figures/M3_residuals_vs_fitted.png`; refined captions for non-technical readers. (Source: `M4_references+AI_audit_summary.md`)

- M4: AI drafted the appendix narrative and verification checklist. Team verification: checked source URLs, validated figure paths, and ensured language distinguishes AI assistance from authorship. Key critique: AI occasionally conflated percent vs percentage-point language; the team corrected for clarity. (Source: `M4_references+AI_audit_summary.md`)

Responsibility statement: All code and analysis have been verified by the team. AI was used as a productivity aid; final judgments and edits were made by team members. (Source: `M4_references+AI_audit_summary.md`)

## Responsibility Statement

All code and analysis in this memo has been verified by our team. We used AI as a productivity tool, not as a substitute for understanding. We take full responsibility for any errors and do not claim "the AI did it" as an excuse.


**END OF MEMO**

**TEAM MEMBERS:** Luke Birdseye, Ben Brown, Katie Koonts, James Gawey, Dani Gamboa  
**DATE:** May 1st, 2026  