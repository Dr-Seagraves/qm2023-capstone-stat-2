# QM 2023 Capstone Project - Milestone 4
# MEMORANDUM

**TO:** Investment Committee / Risk Committee  
**FROM:** Luke Birdseye, Ben Brown, Katie Koonts, James Gawey, Dani Gamboa  
**DATE:** May 1st, 2026  
**RE:** Cryptocurrency Regulatory Risk and Portfolio Allocation Recommendation

---

## PAGE 1 OF 6
## Executive Summary (0.5 page, no tables/figures)

This memo analyzes how regulatory events are associated with cryptocurrency returns and volatility using a panel of assets and macro controls. In our main specification, a one-unit increase in regulatory pressure is associated with a [X]-percentage-point change in next-period returns, controlling for asset fixed effects and time effects. The effect is statistically [significant/not significant] at the [1%/5%/10%] level and is economically meaningful in the context of recent enforcement cycles.

We find heterogeneity across asset groups: [Group A] shows the largest downside response to adverse events, while [Group B] is more resilient. Robustness checks using lag alternatives, subsample windows, and outlier trimming show [consistent/mixed] estimates around the main effect.

Recommendation: in the current regulatory environment, we recommend [overweight/neutral/underweight] exposure to [group(s)] and reducing exposure to [group(s)] by [X%] relative to benchmark. Under a stricter-enforcement scenario, the model implies [expected drawdown/return impact], while a policy-clarification scenario implies [upside estimate].

\newpage

## PAGE 2 OF 6
## 1. Methodology (about 1 page)

### 1.1 Data Sources

Primary panel source:
- File: data/final/crypto_analysis_panel.csv
- Coverage: [N_assets] assets, [frequency], [start_date] to [end_date]
- Key variables: [outcome], [event/regulatory variable], [controls], [asset_id], [date]

Supplementary controls:
- File: data/final/macro_controls_merged.csv
- Variables: [EPU], [VIX], [FF Effective Rate], [other controls used]

Regulatory event source:
- File: data/final/sec_press_litigation_clean_final.csv
- Variables: [event indicator], [event type], [event severity/flags]

### 1.2 Sample Construction

Initial sample:
- [N_assets] x [N_periods] = [N_initial] observations

Cleaning and merge steps:
- Removed duplicate asset-date rows: [N_removed]
- Removed missing dependent-variable rows: [N_removed]
- Winsorized/extreme-filter rule: [describe exact threshold]
- Final merged panel: [N_final] observations
- Panel type: [balanced/unbalanced], with note: [entry/exit explanation]

### 1.3 Model Specifications

Model A (main fixed effects):

$$
Y_{it} = \beta_0 + \beta_1 RegEvent_{i,t-k} + \beta_2 X_{it} + \alpha_i + \delta_t + \varepsilon_{it}
$$

Definitions:
- $Y_{it}$: [return / volatility outcome] for asset $i$ at time $t$
- $RegEvent_{i,t-k}$: lagged regulatory event variable
- $X_{it}$: controls ([list controls used in your code])
- $\alpha_i$: asset fixed effects
- $\delta_t$: time fixed effects
- Standard errors clustered at the asset level

Model B (alternative specification: DiD / ML / ARIMA):

$$
Y_{it} = \theta_0 + \theta_1 Treated_i + \theta_2 Post_t + \theta_3 (Treated_i \times Post_t) + \theta_4 X_{it} + u_{it}
$$

Use this section to document your actual alternative model and identification rationale.

\newpage

## PAGE 3 OF 6
## 2. Results (part 1)

### 2.1 Table 1: Main Fixed Effects Results

| Variable | Coefficient | Std. Error | t-stat | p-value | Significance |
|---|---:|---:|---:|---:|---|
| Regulatory variable (lag k) | [ ] | [ ] | [ ] | [ ] | [ ] |
| Control 1 | [ ] | [ ] | [ ] | [ ] | [ ] |
| Control 2 | [ ] | [ ] | [ ] | [ ] | [ ] |
| Control 3 | [ ] | [ ] | [ ] | [ ] | [ ] |
| Asset Fixed Effects | Yes |  |  |  |  |
| Time Fixed Effects | Yes |  |  |  |  |
| N (observations) | [ ] |  |  |  |  |
| R^2 (within) | [ ] |  |  |  |  |

Notes: Clustered standard errors at asset level. Significance: *** p<0.01, ** p<0.05, * p<0.10.

Interpretation paragraph template:
The coefficient on the lagged regulatory variable ([beta1]=[ ], p=[ ]) implies that a one-unit increase in regulatory pressure changes returns by [ ] percentage points, holding asset and time effects constant. During [event window], this corresponds to an aggregate effect of approximately [ ] percentage points.

Economic mechanism paragraph template:
The estimated effect is consistent with three channels: (1) liquidity contraction during enforcement episodes, (2) increased compliance uncertainty and risk premia, and (3) shifts from high-risk tokens to benchmark assets.

### 2.2 Table 2: Alternative Specification Results

| Variable / Metric | Estimate |
|---|---:|
| Key effect (Model B) | [ ] |
| Std. Error / CV metric | [ ] |
| p-value / OOS score | [ ] |
| N | [ ] |

Interpretation paragraph template:
Model B provides [confirming/contrasting] evidence. The main relationship remains [direction], with magnitude [similar/larger/smaller] than Model A by [ ] units.

\newpage

## PAGE 4 OF 6
## 2. Results (part 2: figures + diagnostics)

### 2.3 Figure 1: Outcome vs Regulatory Environment

Insert figure path and caption:
- Figure file: results/figures/[figure1_filename].png
- Caption: [Outcome] moves [inversely/positively] with regulatory pressure, especially in [period]. This visual pattern aligns with the estimated coefficient from Model A.

### 2.4 Figure 2: Group Heterogeneity (or key cross-section)

Insert figure path and caption:
- Figure file: results/figures/[figure2_filename].png
- Caption: [Group A] and [Group B] are most sensitive to regulatory changes, while [Group C] is comparatively resilient.

### 2.5 Figure 3: Diagnostic Plot (Residuals vs Fitted)

Insert figure path and caption:
- Figure file: results/figures/[figure3_filename].png
- Caption: Residuals are centered around zero with [no clear / some] structure; robust clustered errors are used to address heteroskedasticity concerns.

### 2.6 Robustness Summary

| Check | Specification | Main Coefficient | Conclusion |
|---|---|---:|---|
| Lag sensitivity | k = 1,2,3 | [ ] | [Stable/Not stable] |
| Outlier trim | [rule] | [ ] | [Stable/Not stable] |
| Subsample | [window/group] | [ ] | [Stable/Not stable] |

Short synthesis:
Across robustness checks, the sign of the key coefficient is [consistent/mixed]. The preferred estimate remains [ ] because it balances identification quality and predictive stability.

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

Use APA-style entries and replace placeholders below.

1. CoinGecko. (Year). Dataset/API documentation. Retrieved from [URL].
2. U.S. Securities and Exchange Commission. (Year). Press releases and litigation notices. Retrieved from [URL].
3. Federal Reserve Economic Data (FRED). (Year). [Series names used]. Retrieved from [URL].
4. [Academic source]. (Year). [Title]. [Journal/Working paper details].

## Appendix: AI Audit Summary (0.5-1 page)

This appendix is required.

AI tools used:
- [Tool/model]
- [Tool/model]

Verification example for M1:
- Prompt: [paste actual prompt]
- Output used: [summary]
- Verification: [tests/checks performed]
- Critique: [what AI got wrong and what your team changed]

Verification example for M2:
- Prompt: [paste actual prompt]
- Output used: [summary]
- Verification: [checks performed]
- Critique: [limitations corrected]

Verification example for M3/M4:
- Prompt: [paste actual prompt]
- Output used: [summary]
- Verification: [cross-check with model outputs/tables]
- Critique: [final edits and judgment]


**END OF MEMO**

**TEAM MEMBERS:** Luke Birdseye, Ben Brown, Katie Koonts, James Gawey, Dani Gamboa  
**DATE:** May 1st, 2026  