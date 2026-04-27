# MEMORANDUM

**TO:** Investment Committee  
**FROM:** QM 2023 Capstone Team (Luke Birdseye, Ben Brown, Katie Koonts, James Gawey, Dani Gamboa)  
**DATE:** April 22, 2026  
**RE:** Cryptocurrency Regulatory Risk and Volatility: Portfolio Risk Management Guidance (2020-2026)

## Executive Summary

We analyze a daily crypto panel covering 10 top-market-cap tokens from February 2020 through February 2026 (19,852 observations) to evaluate how regulatory events and macro conditions are associated with short-horizon realized volatility. Our main two-way fixed effects model is designed to isolate within-token variation over time while controlling for shared market-day shocks.

The central statistical finding is that stablecoins show a significant positive volatility differential on SEC event days relative to the baseline token group in the clustered-standard-error specification (coefficient = 0.007431, p < 0.001), while the DeFi differential is small and not statistically significant at baseline (coefficient = 0.002691, p = 0.453). In practical terms, event periods appear to increase near-term variance most clearly for stablecoins in this sample, consistent with liquidity fragmentation and repricing around compliance-sensitive narratives.

For forecasting, a benchmark OLS model outperforms Random Forest out of sample (OLS: R2 = 0.163, RMSE = 0.0364; Random Forest: R2 = 0.109, RMSE = 0.0375), indicating the relationship is reasonably captured by linear structure once controls and group structure are included. Given current policy uncertainty, we recommend maintaining core exposure in large, compliance-mature assets while capping tactical exposure to event-sensitive subgroups and using event-window risk overlays.

## 1. Methodology

### 1.1 Data Sources

- Primary market panel: CoinGecko-based daily token data processed into top-10 token panel with returns and realized volatility.
- Regulatory events: SEC press and litigation releases processed into event indicators.
- Macro controls: VIX, Effective Federal Funds Rate, and Economic Policy Uncertainty Index.

Final merged panel:

- Entities: 10 tokens
- Frequency: Daily
- Window: 2020-02-19 to 2026-02-18
- Observations: 19,852

### 1.2 Model Design

Model A uses two-way fixed effects (token FE + date FE) with clustered standard errors at the token level. Because the raw SEC event indicator is common across all tokens on each day, level effects are absorbed by date fixed effects; identification comes through interaction terms with token-group indicators.

Core specification includes:

- SEC event interaction terms by token group (DeFi and stablecoin differentials)
- Controls for log market cap, log trading volume, and BTC correlation (`control_btc_corr_30d`)
- Robustness checks: lag structure (0-3), outlier trim (top 1% volatility removed), and group subsamples

Model B compares out-of-sample prediction performance between OLS and Random Forest.

## 2. Results

### 2.1 Main Econometric Results (Model A)

From the clustered FE model:

- Stablecoin interaction: 0.007431 (SE 0.001509, p < 0.001)
- DeFi interaction: 0.002691 (SE 0.003584, p = 0.453)

Interpretation: stablecoins appear significantly more event-sensitive than the baseline group in short-horizon volatility, while DeFi effects are imprecisely estimated in the baseline specification.

### 2.2 Diagnostics

- Breusch-Pagan test rejects homoskedasticity strongly (LM p-value 9.59e-132; F p-value 6.96e-134), supporting robust/clustered inference.
- Multicollinearity is severe between size/liquidity controls:
  - VIF(log_market_cap) = 355.73
  - VIF(log_total_volume) = 327.25

Implication: coefficient-level interpretation for collinear controls should be cautious; emphasis should remain on core event-group signals and model-level fit/robustness.

### 2.3 Robustness

- Lag tests for DeFi interaction are consistently non-significant:
  - Lag 0: 0.002691 (p = 0.453)
  - Lag 1: 0.002604 (p = 0.536)
  - Lag 2: 0.002335 (p = 0.579)
  - Lag 3: 0.000088 (p = 0.982)
- Outlier-trim check increases DeFi estimate and approaches significance:
  - Baseline clustered: 0.002691 (p = 0.453)
  - Top-1% trimmed: 0.004706 (p = 0.061)

Interpretation: DeFi effect estimates are sensitive to tail events, while stablecoin event sensitivity remains strong.

### 2.4 Predictive Comparison (Model B)

Test-set performance:

- OLS: R2 = 0.1633, RMSE = 0.03639
- Random Forest: R2 = 0.1095, RMSE = 0.03754

Conclusion: OLS is preferable for this workflow because it provides better out-of-sample performance and clearer interpretability.

## 3. Conclusions and Recommendations

### 3.1 Portfolio Guidance

Given the observed event sensitivity pattern, we recommend the following near-term positioning framework:

- Maintain core allocation in large-cap benchmark assets (for example, BTC/ETH) as a portfolio anchor.
- Keep stablecoin-linked tactical positions capped around major regulatory announcement windows.
- Use event-window volatility overlays (temporary hedges or reduced leverage) rather than broad de-risking.

### 3.2 Scenario Analysis

- Higher enforcement-intensity scenario: expect short-horizon variance pressure in event-sensitive groups, especially stablecoins.
- Policy-clarity scenario (clearer legislative/regulatory framework): expect partial normalization of event-driven volatility spikes and narrower cross-group dispersion.

### 3.3 Caveats

- Two-way FE identifies differential group effects, not a standalone level effect of SEC events.
- High multicollinearity among some controls reduces confidence in individual control coefficients.
- One subgroup robustness slice required fallback estimation due FE singularity.
- External validity is limited to the selected token universe and sample period.

## 4. References

- CoinGecko API documentation and token market data.
- SEC press release and litigation release archives.
- FRED series: VIX (`VIXCLS`), Effective Federal Funds Rate (`DFF`), Economic Policy Uncertainty (`USEPUINDXD`).
- Course materials and milestone instructions for panel-data econometrics.

## 5. AI Audit Appendix (Submission Note)

AI usage documentation is provided in `AI_AUDIT_APPENDIX.md`. Team review validated model specifications, variable interpretations, and narrative framing before inclusion in this memo draft.
