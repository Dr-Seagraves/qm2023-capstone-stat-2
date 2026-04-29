# Milestone 4 Methodology

## Part 2: Methodology

This memo uses the final crypto analysis panel in `data/final/crypto_analysis_panel.csv` and focuses on the token groups that are most relevant to the investment recommendation question: stablecoins and DeFi tokens. The panel is organized at the coin-by-date level and combines SEC event exposure with market outcomes, crypto controls, and macro controls.

### Data and Sample Construction

The starting point is the merged panel created from CoinGecko rankings, SEC press/litigation events, and macro series. The key variables used in the analysis are:

- Outcome: `outcome_realized_vol_30d`
- Main driver: `driver_sec_event_indicator`
- Group labels: `token_group`
- Controls: `log_market_cap`, `log_total_volume`, `control_btc_corr_30d`, `vix`, and `ffeffective_rate`

The sample is cleaned before estimation by converting the date variable to datetime, transforming market cap and volume with logs, and dropping rows with missing values in the variables required for each specification.

### Panel Summary

The final analysis panel contains 19,852 rows across 10 tokens: bnb, btc, doge, eth, figr_heloc, sol, trx, usdc, usdt, and xrp. The date range runs from 2020-02-19 to 2026-02-18. The SEC event indicator flags 147 rows and the panel contains 17 unique SEC action dates. No additional imputation is performed in this panel step.

### Identification Strategy

The core method is a two way fixed effects panel regression. Coin fixed effects absorb time invariant differences across tokens, and date fixed effects absorb market wide shocks that affect all tokens on the same day. Because the SEC event indicator is common across all coins on a given date, the driver is identified through interactions with token group indicators rather than as a standalone level effect.

The estimated structure is:

`outcome_it = beta * (SEC_event x group) + controls + coin_FE + date_FE + error_it`

This design asks whether stablecoins or DeFi tokens react differently to SEC event exposure than the omitted baseline group, holding constant common market shocks and time invariant coin characteristics.

### Model Comparison

To complement the fixed effects results, the project also estimates a predictive benchmark using ordinary least squares and random forest on the same feature set. That comparison is not used as the causal identification strategy; instead, it serves as a check on whether the group patterns found in the fixed effects model also show up in a predictive setting. The saved outputs show that OLS performs better than random forest on the test split, so the linear specification remains the primary decision rule.

### Diagnostic and Robustness Checks

The methodology includes several checks to make the recommendation defensible:

- Clustered standard errors at the coin level to account for within-coin serial correlation and heteroskedasticity.
- Breusch-Pagan testing for heteroskedasticity.
- VIF screening for multicollinearity among controls.
- Alternative lag specifications for the SEC event driver.
- Top 1% outcome trimming to test sensitivity to extreme volatility periods.
- Group subsample estimation with entity fixed effects only, so the common SEC event driver remains identifiable within each token group.

### Recommendation Rule

The investment recommendation is based on the group with the strongest and most robust estimated sensitivity to SEC event exposure, combined with whether the effect survives the robustness checks above. In this sample, stablecoins are the clearest high sensitivity group, while DeFi shows a weaker and less stable response. The methodology therefore prioritizes token groups that exhibit persistent event sensitivity, economically meaningful effect sizes, and consistent behavior across model checks.

### Deliverable Logic

This methodology is designed to support a short investment memo: identify the panel, estimate group specific event sensitivity, validate the result with diagnostics and robustness tests, and then translate the statistical pattern into a practical group recommendation.