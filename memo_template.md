## Investment Memo Template (Dataset-Agnostic) 

## **QM 2023 Capstone Project - Milestone 4** 

## **MEMORANDUM** 

**TO:** [Your Decision-Maker Audience - Choose One:] 

- **Investment Committee** (REITs, stocks, crypto): Portfolio allocation decisions 

- **Policy Committee** (economic indicators, labor data): Policy recommendations 

- **Risk Committee** (banking, insurance data): Risk management guidance **Executive Team** (firm-level data): Strategic business decisions 

- **Research Community** (academic framing): Contributions to literature 

**FROM:** [Team Name: List all team member names] **DATE:** [Submission Date] **RE:** [Your Research Question + Recommendation Context] 

**REIT Example:** _REIT Return Sensitivity to Interest Rate Policy – Investment Strategy Recommendation_ **Crypto** 

**Example:** _Cryptocurrency Regulatory Risk Assessment – Portfolio Risk Management_ **Housing Example:** _Housing Market Response to Monetary Policy – Policy Briefing for Fed Analysts_ 

## Executive Summary 

[2-3 paragraphs, approximately 0.5 page. No tables or figures. Use business language appropriate for your audience.] 

## Structure Template 

## **[Paragraph 1: State the key finding in plain language]** 

- What did you analyze? ([N] entities, [time period]) 

- What did you find? (e.g., "A [X unit] increase in [DRIVER] changes [OUTCOME] by [Y units]") What's the causal mechanism? (List 2-3 economic channels) 

## **[Paragraph 2: Group-level insights OR robustness evidence]** 

- **If your dataset has groups** (sectors, regions, asset types): Which groups are most/least sensitive? **If no natural grouping** : What robustness checks confirm the main finding? Magnitude of differential effects or evidence of stability 

## **[Paragraph 3: Recommendation for decision-maker]** 

Given current environment, what should decision-makers do? 

- Specific, actionable advice (not vague "be cautious") 

- Forward-looking guidance (scenario analysis) 

Example: REIT Investment Committee 

1 / 18 

Using a panel of 532 US equity REITs over 2015-2023, we find that a 1 percentage point increase in the Federal Funds Rate reduces REIT returns by 2.5 percentage points (p < 0.01), controlling for REIT and time fixed effects. This effect occurs with a 2-month lag and is driven by leverage costs and discount rate increases. Sector analysis reveals that Retail and Office REITs are highly sensitive (correlations: -0.45, -0.42), while Industrial REITs are resilient (-0.15). 

Given the current elevated rate environment (5.0-5.25%), we recommend a **15% overweight** in Industrial REITs and a **neutral-to-underweight** stance in Retail and Office REITs. If the Federal Reserve cuts rates by 100 basis points over the next 12 months, our model predicts a 2.5% recovery in aggregate REIT returns. 

## Example: Cryptocurrency Risk Committee 

Analyzing 150 cryptocurrencies over 2020-2024, we find that negative regulatory announcements (SEC enforcement actions, exchange restrictions) reduce token returns by 8.3 percentage points within one week (p < 0.001), controlling for token and time fixed effects. This effect is driven by liquidity channel disruptions (reduced exchange listings) and institutional investor exit. DeFi tokens exhibit higher sensitivity (-12.1%) than centralized exchange tokens (-5.4%), reflecting differential regulatory risk exposure. 

Given ongoing regulatory uncertainty (SEC classification debates, stablecoin regulation), we recommend **reducing exposure to DeFi tokens by 20%** and **increasing allocation to** 

**Bitcoin/Ethereum** (lower regulatory risk due to established compliance frameworks). If Congress passes comprehensive crypto legislation (30% probability over 12 months), our model predicts a 1520% recovery in high-risk DeFi assets. 

## Using This Template with AI Assistance 

**This template uses REIT examples because that's the course default dataset.** If using alternative data (crypto, housing, macro, firm-level), follow this translation workflow: 

## Step 1: Identify Conceptual Equivalent 

## **AI Prompt Template:** 

```
I'm working on a Milestone 4 Investment Memo for a panel data course. The REIT
example uses
```

```
'[REIT_VARIABLE]' which measures [CONCEPT]. My dataset is [DATASET_NAME] at
[ENTITY-TIME LEVEL].
```

```
What variable in my dataset measures the same concept? List 2-3 candidates and
explain the
economic mechanism.
```

## **Verification Checklist:** 

**==> picture [10 x 9] intentionally omitted <==**

**==> picture [10 x 10] intentionally omitted <==**

I understand why each suggested variable measures this concept 

I checked variable distribution ( `df[X].describe()` ) to ensure units make sense 

2 / 18 

**==> picture [10 x 10] intentionally omitted <==**

**==> picture [10 x 10] intentionally omitted <==**

- I found 1+ academic paper using this variable in my domain (cite in References) 

- I can explain this to a classmate unfamiliar with my dataset 

## Step 2: Translate Economic Interpretation 

## **AI Prompt Template:** 

```
The REIT memo says: '[REIT_INTERPRETATION]'. Translate this to my dataset
([DATASET_NAME])
```

```
using variable [MY_VAR]. Ensure units are correct and the economic mechanism is
domain-appropriate.
```

## **Verification Checklist:** 

**==> picture [10 x 10] intentionally omitted <==**

**==> picture [10 x 9] intentionally omitted <==**

**==> picture [10 x 10] intentionally omitted <==**

**==> picture [10 x 9] intentionally omitted <==**

- My interpretation uses domain-appropriate terminology (not REIT jargon) 

- Units are explicit and match my data (percentage points, basis points, dollars, etc.) 

- I can explain the causal mechanism without looking at AI output 

- I checked the magnitude against domain literature (e.g., crypto returns are more volatile than REITs) 

## Step 3: Document in AI Audit 

   - Record all translation prompts and AI responses 

   - Explain what you chose vs. what AI suggested 

   - **Critique:** What did AI miss? What domain knowledge did you add? 

- **Common Pitfall:** Blind find-replace of variable names without understanding mechanisms. Example: 

   - ❌ Bad: "A 1% increase in Bitcoin price reduces Ethereum returns..." (nonsensical) 

   - ✅ Good: "A 1 percentage point increase in regulatory severity reduces DeFi token returns..." (causal mechanism) 

## Common Translations Reference 

|**REIT Concept**|**Crypto Equivalent**|**Housing Equivalent**|**Macro Equivalent**|
|---|---|---|---|
|**Grouping Variable**||||
|Sector (Retail, Office,<br>Industrial)|Token Type (DeFi, CEX,<br>NFT)|Region (Urban,<br>Suburban, Rural)|Industry<br>(Manufacturing,<br>Services)|
|**Outcome Variable**||||
|Monthly Return|Token Return|Rent Growth|Employment Growth|
|(`ret`)|(`return_pct`)|(`rent_growth_yoy`)|(`emp_growth`)|
|**Policy/Treatment**||||
|**Variable**||||



3 / 18 

|**REIT Concept**|**Crypto Equivalent**|**Housing Equivalent**|**Macro Equivalent**|
|---|---|---|---|
|Federal Funds Rate|Regulatory Sentiment|Mortgage Rate|Fiscal Shock|
|(`FEDFUNDS`)|(`reg_severity`)|(`MORTGAGE30US`)|(`govt_spending_chg`)|
|**Control Variables**||||
|Market Cap (`mcap`)|Trading Volume<br>(`volume_usd`)|Median Home Price<br>(`price_median`)|GDP (`gdp_real`)|
|Momentum Factor|Price Momentum|Price Appreciation|Output Gap|
|(`MOM`)|(`price_mom_6m`)|(`price_mom_12m`)|(`output_gap`)|
|**Mechanism**||||
|**Channels**||||
|Leverage costs|Liquidity disruption|Affordability shock|Credit tightening|
||||Labor market|
|Discount rate|Institutional exit|Supply response|adjustment|
|Demand effects<br>(rental)|Network effects|Migration patterns|Consumption<br>smoothing|



## 1. Methodology 

[Approximately 1 page. Explain data sources, sample construction, and model specifications **in plain language** .] 

## 1.1 Data Sources 

## **[Your Primary Dataset]** 

- **Source:** [Database name, provider, or instructor-provided file] 

- **Coverage:** [N] unique [entities] ([entity type]: REITs, tokens, counties, firms), [frequency] observations from [start date] to [end date] 

- **Variables:** [Outcome variable], [entity identifiers], [grouping variable if applicable], [time-varying characteristics] 

## **REIT Example:** 

Source: CRSP/Ziman Real Estate Database (instructor-provided) 

- Coverage: 532 unique US equity REITs, monthly observations from January 2015 to December 2023 Variables: Total returns ( `ret` ), market capitalization ( `mcap` ), sector classification, share price 

## **Crypto Example:** 

Source: CoinGecko API + Messari Crypto Data 

- Coverage: 150 cryptocurrencies (tokens with >$50M market cap), daily observations from January 2020 to December 2024 

- Variables: Daily returns ( `return_pct` ), market cap ( `mcap_usd` ), token type (DeFi/CEX/NFT), trading volume 

4 / 18 

## **[Supplementary Data: Control Variables or Policy Indicators]** 

- **Source:** [Database name, API, or research paper] 

- **Coverage/Frequency:** [Match primary dataset frequency or explain lag structure] **Variables:** [List 3-5 key variables with brief descriptions] 

## **REIT Example:** 

Source: Federal Reserve Economic Data (FRED) via API 

- Series: 

- `FEDFUNDS` : Effective Federal Funds Rate (%) 

- `MORTGAGE30US` : 30-Year Fixed Mortgage Rate (%) 

- `CPIAUCSL` : Consumer Price Index (All Urban Consumers) 

- `UNRATE` : Unemployment Rate (%) 

## **Crypto Example:** 

Source: Regulatory Events Database (Custom-built from SEC press releases) 

- Variables: 

   - `reg_severity` : Regulatory announcement severity (scale 1-10, hand-coded) 

   - `enforcement_count` : Monthly SEC enforcement actions against crypto firms `exchange_delisting` : Binary indicator for exchange restrictions 

**[Optional: Factor Premiums or Risk Measures]** _Only include if you constructed domain-specific factors (e.g., REIT factors, crypto liquidity metrics)._ 

## **REIT Example:** 

- Source: Seagraves et al. (2025) research data 

- Factors: SIZE, VALUE, MOM (Momentum), QLTY (Quality), LOWVOL (Low Volatility), REV (Reversal) Description: Monthly factor returns based on tercile portfolio sorts 

## 1.2 Sample Construction 

## **Panel Structure:** 

- **Initial sample:** [entities] × [Y] [time periods] = [Z] total observations 

- **Entity type:** [REITs, tokens, counties, firms, countries] **Time unit:** [Monthly, daily, quarterly, annual] 

## **Data Cleaning Steps:** 

Removed duplicate [entity-time] pairs ([N] rows) 

- Dropped observations with missing [outcome variable] ([N] rows) 

- **[Dataset-specific cleaning]:** [Describe domain-appropriate filters] 

   - _REIT example:_ Winsorized extreme returns (>200% or <-100%) at 99th/1st percentiles; excluded small-cap REITs (market cap < $10M) 

5 / 18 

- _Crypto example:_ Excluded stablecoins (low volatility by design); dropped tokens with <30 days trading history; capped returns at ±100% (flash crash protection) 

- _Housing example:_ Excluded counties with <50 transactions/month (noisy estimates); imputed missing rent data using regional medians 

**Final Sample:** [Z] [entity-time] observations **Panel Type:** [Balanced / Unbalanced] ([explain why, e.g., entities enter/exit market]) 

## 1.3 Model Specifications 

## **Model A: Fixed Effects Regression (Primary Specification)** 

We estimate the causal effect of [DRIVER] on [OUTCOME] using a two-way fixed effects panel regression: 

```
[OUTCOME]_it = β₀ + β₁·[DRIVER]_lag_it + β₂·[CONTROL_1]_it + β₃·[CONTROL_2]_it +
α_i + δ_t + ε_it
```

## **Where:** 

- **[OUTCOME]_it:** [Your dependent variable description] for entity i in time t 

- **[DRIVER]_lag_it:** [Your key treatment/policy variable] with [X-period] lag (based on M2 EDA lag analysis) 

- **[CONTROL_1], [CONTROL_2]:** [Control variables that vary across entities and time] 

- **α_i:** Entity fixed effect (controls for time-invariant entity characteristics) 

- **δ_t:** Time fixed effect (controls for aggregate time shocks affecting all entities) 

- **ε_it:** Error term (clustered standard errors at entity level) 

**Rationale:** Fixed Effects controls for unobserved entity-specific factors that might confound the [DRIVER] effect. By comparing each entity to itself over time, we isolate the causal effect of [DRIVER] changes. 

## **REIT Example:** 

```
ret_it = β₀ + β₁·FEDFUNDS_lag2_it + β₂·MOM_it + β₃·QLTY_it + β₄·SIZE_it + α_i +
δ_t + ε_it
```

- **ret_it:** Monthly return for REIT i in month t 

- **FEDFUNDS_lag2_it:** Federal Funds Rate with 2-month lag 

- **α_i:** Controls for time-invariant REIT characteristics (management quality, property portfolio) 

- **Rationale:** Comparing each REIT to itself over time eliminates bias from unobserved REIT-specific factors 

## **Crypto Example:** 

6 / 18 

```
return_it = β₀ + β₁·reg_severity_lag1_it + β₂·volume_it + β₃·btc_return_it + α_i +
δ_t + ε_it
```

- **return_it:** Daily return for token i on day t 

- **reg_severity_lag1_it:** Regulatory severity index with 1-day lag 

- **volume_it:** Log trading volume (liquidity control) 

- **btc_return_it:** Bitcoin return (market factor) 

- **α_i:** Controls for time-invariant token characteristics (blockchain, use case) 

- **Rationale:** Comparing each token to itself over time eliminates bias from technology differences 

## **Model B: [Difference-in-Differences / ARIMA / ML Comparison]** 

[Describe your chosen Model B following the same structure as Model A. Include specification, variables, and rationale.] 

## **When to use each Model B option:** 

- **Difference-in-Differences (DiD):** If you have a clear treatment/control group and a policy shock (e.g., Fed rate hike, regulatory change) 

- **ARIMA:** If you have a single time series or want to forecast future values 

- **ML Comparison (Random Forest, XGBoost):** If you want to compare linear model to non-linear alternatives 

## **DiD Example (REIT):** 

```
ret_it = β₀ + β₁·Sensitive_i + β₂·Post2022_t + β₃·(Sensitive_i × Post2022_t) +
controls + ε_it
```

- **Sensitive_i = 1** if REIT is in Retail or Office sector (rate-sensitive), 0 if Industrial (resilient) 

- **Post2022_t = 1** if month ≥ January 2022 (rate hike period), 0 otherwise 

- **β₃:** DiD estimator (differential effect on sensitive sectors post-shock) 

- **Rationale:** Exploits natural experiment of Fed rate hike to identify causal effects. Assumes parallel trends (tested with placebo test). 

## **DiD Example (Crypto):** 

```
return_it = β₀ + β₁·DeFi_i + β₂·PostSEC_t + β₃·(DeFi_i × PostSEC_t) + controls +
ε_it
```

- **DeFi_i = 1** if token is DeFi protocol, 0 if centralized exchange token 

- **PostSEC_t = 1** if date ≥ June 2023 (SEC DeFi crackdown), 0 otherwise 

- **β₃:** DiD estimator (differential effect on DeFi tokens post-regulation) 

7 / 18 

**Rationale:** Compares DeFi tokens to CEX tokens (control group) before/after regulatory shock 

## 2. Results 

[Approximately 1.5-2 pages. Present regression tables, figures, and interpretation.] 

## 2.1 Fixed Effects Model (Main Specification) 

**Table 1: Fixed Effects Regression Results** 

|**Variable**|**Coefficient**|**Std. Error**|**t-statistic**|**p-value**|**Significance**|
|---|---|---|---|---|---|
|[DRIVER] (lag X)|[β₁]|[SE]|[t]|[p]|[**_/_**_/_]|
|[CONTROL_1]|[β₂]|[SE]|[t]|[p]||
|[CONTROL_2]|[β₃]|[SE]|[t]|[p]||
|**Entity Fixed Effects**|Yes|||||
|**Time Fixed Effects**|Yes|||||
|**N (observations)**|[N]|||||
|**R² (within)**|[R²]|||||



_Notes:_ Clustered standard errors at [entity] level. *** p<0.01, ** p<0.05, * p<0.10. 

## **REIT Example:** 

|**Variable**|**Coefficient**|**Std. Error**|**t-statistic**|**p-value**|**Significance**|
|---|---|---|---|---|---|
|FEDFUNDS (lag 2)|-0.025|0.010|-2.50|0.012|**|
|MOM|0.042|0.013|3.23|0.001|***|
|QLTY|0.031|0.015|2.07|0.039|**|
|SIZE|-0.008|0.011|-0.73|0.465||
|**REIT Fixed Effects**|Yes|||||
|**Time Fixed Effects**|Yes|||||
|**N (observations)**|54,320|||||
|**R² (within)**|0.38|||||



## Interpretation Framework 

## **[Paragraph 1: Main coefficient interpretation with units]** 

The coefficient on [DRIVER] ([β₁], p = [p-value]) indicates that a **[1 unit] increase in [DRIVER] changes [OUTCOME] by [β₁ units]** , holding constant entity and time fixed effects. 

8 / 18 

## **Example interpretation template:** 

**==> picture [10 x 10] intentionally omitted <==**

   - percentage points" 

- "A one-standard-deviation increase in [DRIVER] is associated with a [Y]% change in [OUTCOME]" 

## **[Paragraph 2: Economic significance with context]** 

This effect is: 

- **Economically significant:** [Provide real-world context. Example: "During the 2022-2023 rate hike cycle 

- (0% → 5.25%), the model predicts a cumulative [X]% decline in [OUTCOME]"] 

- **Statistically robust:** Survives multiple robustness checks ([list 2-3 checks from M3]) 

## **[Paragraph 3: Transmission mechanisms]** 

## **Consistent with economic theory:** 

- **Channel 1:** [Explain first causal mechanism] 

- **Channel 2:** [Explain second causal mechanism] 

- **[Optional] Channel 3:** [Explain third mechanism if applicable] 

## **Use domain-appropriate mechanisms:** 

- **REIT mechanisms:** Leverage channel (debt financing costs), discount rate channel (DCF valuation), demand channel (rental market effects) 

- **Crypto mechanisms:** Liquidity channel (exchange restrictions), institutional channel (fund flows), network effects (adoption) 

- **Housing mechanisms:** Affordability channel (purchasing power), supply response (construction), regional variation (elasticity differences) 

## **REIT Example Interpretation:** 

The coefficient on FEDFUNDS_lag2 (-0.025, p = 0.012) indicates that a **1 percentage point increase in the Federal Funds Rate reduces REIT returns by 2.5 percentage points** , holding constant REIT and time fixed effects. This effect is: 

- **Economically significant:** During the 2022-2023 rate hike cycle (0% → 5.25%), the model predicts a cumulative 13.1% decline in REIT returns (2.5% × 5.25 pp). 

- **Statistically robust:** Survives multiple robustness checks (alternative lags, exclusion of COVID crash, sector subsamples). 

## **Consistent with economic theory:** 

- **Leverage channel:** REITs are highly leveraged (~60% debt-to-assets). Rising rates increase financing costs, reducing profitability. 

- **Discount rate channel:** REITs are valued based on discounted future cash flows. Higher rates increase the discount factor, lowering present value. 

## **[Paragraph 4: Control variable interpretation]** 

9 / 18 

Briefly interpret 1-2 control variables if they are economically meaningful. Skip if controls are purely technical. 

## **REIT Example:** 

- **Momentum (MOM):** 1 SD increase in momentum premium → 4.2% higher REIT returns. Confirms trend-following behavior. 

- **Quality (QLTY):** 1 SD increase in quality score → 3.1% higher returns. REITs with stable cash flows outperform. 

- **Size (SIZE):** No significant effect after controlling for factors and fixed effects. 

## 2.2 [Model B Results] 

## **Table 2: [DiD / ARIMA / ML] Results** 

[Insert your Table 2 here, following the same format as Table 1] 

## **Interpretation:** 

[Interpret Model B results following the same structure: coefficient estimates, economic significance, statistical robustness, consistency with theory] 

## 2.3 Group Heterogeneity Analysis (If Applicable) 

## **When to include this section:** 

- Your dataset has meaningful groups (sectors, asset types, regions, size quartiles) 

- You expect differential sensitivity across groups (tested in M2 EDA) 

## **If no natural grouping exists:** 

- Replace this section with **robustness checks** (split sample by time period, alternative variable definitions) 

Or skip and add discussion in Limitations section 

## **Table 3: Sector-Specific Sensitivity (Optional)** 

|**Group**||**N**|**Correlation with [DRIVER]**|**Mean [OUTCOME]**|**Interpretation**|
|---|---|---|---|---|---|
|[Group|1]|[N]|[ρ]|[μ]|[Most/Least sensitive]|
|[Group|2]|[N]|[ρ]|[μ]||
|[Group|3]|[N]|[ρ]|[μ]||



## **REIT Example:** 

|||**Correlation with**|**Mean**||
|---|---|---|---|---|
|**Sector**|**N**|**FEDFUNDS**|**Return**|**Interpretation**|



10 / 18 

|||**Correlation with**|**Mean**||
|---|---|---|---|---|
|**Sector**|**N**|**FEDFUNDS**|**Return**|**Interpretation**|
|Retail|8,420|-0.45|-0.012|Highly sensitive (consumer<br>spending)|
|Office|6,890|-0.42|-0.015|Highly sensitive (financing costs)|
|Industrial|5,120|-0.15|0.008|Resilient (e-commerce demand)|



## **Crypto Example:** 

|**Token**<br>**Type**|**N**|**Correlation with Reg**<br>**Severity**|**Mean**<br>**Return**|**Interpretation**|
|---|---|---|---|---|
|DeFi|2,450|-0.58|-0.025|Highly sensitive (regulatory risk)|
|CEX|1,820|-0.32|-0.010|Moderate sensitivity|
|NFT|980|-0.21|-0.035|Low sensitivity (different risk|
|||||factors)|



## 2.4 Visual Evidence 

## **Figure 1: [OUTCOME] vs. [DRIVER] (Dual-Axis Plot)** 

**==> picture [13 x 13] intentionally omitted <==**

Dual-Axis Plot 

**Caption Template:** _[OUTCOME] ([color], left axis) exhibits [positive/inverse/no] co-movement with [DRIVER] ([color], right axis). The [time period] [event description] coincides with [outcome behavior], consistent with our regression estimates._ 

## **REIT Example:** 

**==> picture [13 x 13] intentionally omitted <==**

Dual-Axis Plot 

_Caption:_ Aggregate REIT returns (blue, left axis) exhibit inverse co-movement with the Federal Funds Rate (red, right axis). The 2022-2023 rate hike cycle coincides with declining REIT performance, consistent with our regression estimates. 

## **Crypto Example:** 

**==> picture [13 x 13] intentionally omitted <==**

Dual-Axis Plot 

_Caption:_ Cryptocurrency returns (blue, left axis) exhibit sharp declines during regulatory crackdowns (red, right axis). The June 2023 SEC enforcement wave coincides with -15% average token returns, consistent with our DiD estimates. 

## **Figure 2: [Group] Sensitivity to [DRIVER] (If Applicable)** 

11 / 18 

**==> picture [13 x 12] intentionally omitted <==**

Sensitivity Bar Chart 

**Caption Template:** _[Group A] and [Group B] are highly sensitive to [DRIVER] (correlations: [ρ₁], [ρ₂]), while [Group C] is resilient ([ρ₃]). This heterogeneity informs our [audience-specific] recommendations._ 

## **REIT Example:** 

**==> picture [13 x 12] intentionally omitted <==**

Sector Sensitivity Bar Chart 

_Caption:_ Retail and Office REITs are highly sensitive to interest rate changes (correlations: -0.45, -0.42), while Industrial REITs are resilient (-0.15). This heterogeneity informs our sector-specific investment recommendations. 

## **Figure 3: Residuals vs. Fitted Values (Diagnostic Plot)** 

**==> picture [13 x 13] intentionally omitted <==**

## Residuals Plot 

**Caption (Generic):** _Residuals scatter randomly around zero with no clear pattern, suggesting homoskedasticity (after applying clustered standard errors). Q-Q plot (not shown) indicates approximate normality with [fat/thin] tails, which is [typical/atypical] for [data type]._ 

## 3. Conclusions & Recommendations 

[Approximately 1 page. Translate findings into actionable advice for your decision-maker audience.] 

## 3.1 [Audience-Specific Recommendations] 

## **Choose the recommendation framing appropriate for your audience:** 

## **Option 1: Investment Committee (REITs, Stocks, Crypto, Assets)** 

Based on our empirical analysis, we provide the following **portfolio recommendations** for the current [market condition]: 

## **Recommended [Sector/Asset/Group] Allocation (Tactical):** 

## **1. Overweight [Group A] ([X]% allocation above benchmark)** 

- **Rationale:** [Group A] exhibits [low/high] sensitivity to [DRIVER] (correlation = [ρ]). [Domain-specific fundamentals explanation]. 

- **Expected performance:** [Outperformance/hedging] relative to other groups in [current environment]. 

## **2. Neutral-to-Underweight [Group B]** 

**Rationale:** [Group B] is highly sensitive to [DRIVER] (correlation = [ρ]). Expected to 

- [underperform/outperform] until [condition changes]. 

- **Caveat:** Selective exposure to high-quality [subset] may offer value. 

12 / 18 

## **REIT Investment Committee Example:** 

Based on our empirical analysis, we provide the following **portfolio recommendations** for the current interest rate environment (Federal Funds Rate: 5.0-5.25%): 

## **Recommended Sector Allocation (Tactical):** 

## **1. Overweight Industrial REITs (15% allocation above benchmark)** 

- **Rationale:** Industrial REITs exhibit low interest rate sensitivity (correlation = -0.15) and are driven by structural demand from e-commerce and logistics. Sector fundamentals (supply constraints, long-term leases) are inelastic to monetary policy. 

- **Expected performance:** Outperformance relative to other REIT sectors in elevated rate environment. 

## **2. Neutral-to-Underweight Retail and Office REITs** 

- **Rationale:** These sectors are highly rate-sensitive (correlations: -0.45, -0.42) due to consumer spending sensitivity and high leverage. Expected to underperform until rates stabilize or decline. 

- **Caveat:** Selective exposure to high-quality Retail REITs (e.g., prime shopping centers with anchor tenants) may offer value, but broad sector exposure is not recommended. 

## **Crypto Portfolio Example:** 

Based on our empirical analysis, we provide the following **portfolio recommendations** for the current regulatory environment (ongoing SEC enforcement): 

## **Recommended Token Type Allocation (Tactical):** 

## **1. Overweight Bitcoin/Ethereum (30% allocation above benchmark)** 

- **Rationale:** BTC and ETH have established regulatory frameworks (CFTC commodity classification, ETF approvals). Low sensitivity to SEC enforcement actions. 

**Expected performance:** Defensive positioning in uncertain regulatory environment. 

## **2. Reduce DeFi Token Exposure (-20% relative to benchmark)** 

- **Rationale:** DeFi tokens are highly sensitive to regulatory crackdowns (correlation = -0.58). Ongoing SEC scrutiny of DeFi protocols creates tail risk. 

**Caveat:** If Congress passes comprehensive crypto legislation (30% probability over 12 months), DeFi could recover 15-20%. 

## **Option 2: Policy Committee (Economic Indicators, Labor Data)** 

Based on our empirical analysis, we provide the following **policy recommendations** for [government agency / central bank]: 

## **Policy Implication 1:** 

**Finding:** [Summary of main effect] 

**Recommendation:** [Policy action to address finding] 

13 / 18 

**Rationale:** [Economic mechanism + distributional effects] 

## **Policy Implication 2:** 

**Finding:** [Group heterogeneity result] 

- **Recommendation:** [Targeted intervention for vulnerable groups] 

- **Rationale:** [Equity considerations] 

## **Housing Policy Example:** 

Based on our empirical analysis, we provide the following **policy recommendations** for the Federal Reserve: 

## **Policy Implication 1: Interest Rate Sensitivity Varies by Region** 

- **Finding:** A 1 percentage point increase in mortgage rates reduces home sales by 12% in supplyconstrained markets (San Francisco, New York) but only 4% in supply-elastic markets (Houston, Dallas). **Recommendation:** Consider regional heterogeneity when setting monetary policy. Rate hikes have disproportionate effects on coastal housing markets. 

- **Rationale:** Supply elasticity determines price vs. quantity adjustment. Inelastic markets experience larger price declines and affordability shocks. 

## **Policy Implication 2: First-Time Buyers Disproportionately Affected** 

- **Finding:** Rate increases reduce first-time buyer share by 8 percentage points, while repeat buyers (less leverage-dependent) are unaffected. 

- **Recommendation:** Pair rate hikes with first-time buyer assistance programs (down payment grants, FHA loan expansion) to mitigate distributional effects. 

## **Option 3: Risk Committee (Banking, Insurance, Internal Risk)** 

Based on our empirical analysis, we provide the following **risk management recommendations** : 

## **Risk Exposure 1:** 

- **Finding:** [Quantify risk exposure to DRIVER] 

- **Recommendation:** [Hedging strategy or exposure limit] 

- **Implementation:** [Specific actions] 

## **Risk Exposure 2:** 

**Finding:** [Tail risk or non-linear effect] 

**Recommendation:** [Stress testing or scenario planning] 

## 3.2 [Scenario Analysis] 

We model [2-4] scenarios for the next [time period] based on [event/policy expectations]: 

**Scenario [DRIVER] Change Predicted [OUTCOME] Impact Probability** 

14 / 18 

|**Scenario**|**[DRIVER] Change**|**Predicted [OUTCOME] Impact**|**Probability**|
|---|---|---|---|
|**Baseline:**[Description]|[X]|[Impact]|[%]|
|**Optimistic:**[Description]|[Y]|[Impact]|[%]|
|**Pessimistic:**[Description]|[Z]|[Impact]|[%]|



**Expected value:** Weighted average [outcome] = [calculation] 

**Recommendation:** Given [asymmetric upside/downside], a [stance description] is warranted. 

## **REIT Example:** 

|**Scenario**|**Rate**|**Predicted REIT Return**|**Probability (Futures**|
|---|---|---|---|
||**Change**|**Impact**|**Markets)**|
|**Baseline:**Rates hold at 5.0-||||
|5.25%|0 bp|Flat returns (0% to +2%)|40%|
|**Dovish:**Fed cuts 100 bp to 4.0-<br>4.25%|-100 bp|+2.5% recovery (with 2-<br>month lag)|35%|
|**Hawkish:**Fed raises 50 bp to<br>5.5-5.75%|+50 bp|-1.25% decline|25%|



**Expected value:** Weighted average return = (0.40 × 1%) + (0.35 × 2.5%) + (0.25 × -1.25%) = **+0.96%** 

**Recommendation:** Given asymmetric upside (dovish scenario) and limited downside (rate hikes unlikely beyond 50 bp), a **neutral-to-slight-overweight** stance on aggregate REIT exposure is warranted. 

## 3.3 Risk Assessment 

## **Model Risks:** 

1. **Assumption of stable elasticity:** Our model assumes the historical relationship ([β] = [value]) will hold in the future. If structural changes have altered this relationship, predictions may be biased. 

2. **Omitted variable bias:** [List 2-3 potentially confounding factors]. If these correlate with [DRIVER], our coefficient estimates may be confounded. 

3. **External validity:** Results are based on [time period] data. May not generalize to [different context]. 

## **[Domain-Specific] Risks:** 

1. **[Risk 1]:** [Describe risk and potential impact] 

2. **[Risk 2]:** [Describe risk and potential impact] 

3. **[Risk 3]:** [Describe risk and potential impact] 

## **REIT Example:** 

**Market Risks:** 

15 / 18 

1. **Rate volatility:** If the Fed reverses course (multiple rate cuts followed by hikes), REIT returns will exhibit increased volatility, making forecasts less reliable. 

2. **Sector-specific shocks:** Retail REITs face structural headwinds (e-commerce disruption) independent of rates. Our sector recommendations assume no exogenous shocks. 

3. **Liquidity risk:** In a market downturn, REIT liquidity may decline, amplifying volatility and reducing the effectiveness of tactical rebalancing. 

## 3.4 Caveats and Limitations 

## **Model Limitations:** 

1. **Fixed Effects assumption:** We assume [entity] characteristics are time-invariant. If [entities] change structure during sample period, our FE estimates may be biased. 

2. **[Model B] assumption:** [State key assumption for Model B, e.g., parallel trends for DiD, stationarity for ARIMA]. [Evidence for/against assumption]. 

3. **Lag specification:** We chose a [X-period] lag based on M2 EDA. Alternative lag structures yield similar but not identical results. The true lag may vary by [group/context]. 

4. **Measurement error:** [Describe any measurement challenges in your data]. Alternative definitions could yield different results. 

## **REIT Example:** 

## **Caveats and Limitations:** 

1. **Fixed Effects assumption:** We assume REIT characteristics (management quality, property portfolio) are time-invariant. If REITs restructure portfolios during the sample period (e.g., exit retail, enter industrial), our FE estimates may be biased. 

2. **Parallel trends (DiD):** Our DiD analysis assumes sensitive and resilient sectors would have followed parallel trends absent the rate shock. Pre-trend divergence in 2019-2020 suggests this assumption may be violated, potentially overstating the causal effect. 

3. **Lag specification:** We chose a 2-month lag based on M2 EDA. Alternative lag structures (1-month, 3- month) yield similar but not identical results. The true lag may vary by sector or REIT size. 

4. **Measurement error:** REIT factor premiums are based on tercile sorts, which may not fully capture factor exposure heterogeneity. Alternative factor construction methods (e.g., value-weighted, decile sorts) could yield different results. 

## 3.5 Future Research Directions 

To refine this analysis, future work could: 

- **[Research direction 1]:** [Explain what additional data or methods would improve the analysis] 

- **[Research direction 2]:** [Explain how to test key assumptions] 

- **[Research direction 3]:** [Explain how to extend external validity] 

## **REIT Example:** 

To refine this analysis, future work could: 

16 / 18 

- **Incorporate investor sentiment data** (VIX, REIT fund flows) to control for market psychology 

- **Analyze heterogeneous treatment effects** by REIT size, leverage, or geographic concentration **Extend the sample period** to include earlier rate cycles (1990s, 2000s) to test external validity 

- **Model non-linearities** (e.g., threshold effects where rate increases above 5% have disproportionate impacts) 

## 4. References 

[List all data sources, academic papers (if cited), and external references in APA or similar format] 

## **Required:** 

1. **[Your primary data source]** (Year). _[Dataset name and description]._ Retrieved from [URL or "Provided by instructor"] 

2. **[Supplementary data source]** (Year). _[Dataset name]._ Retrieved from [URL] 

**Optional (if cited):** 3. **[Academic paper from Orbis Lit-Anchor]:** Author, A., & Author, B. (Year). Title. _Journal Name, Volume_ (Issue), pages. 

## **REIT Example:** 

1. **Federal Reserve Economic Data (FRED).** (2024). _Effective Federal Funds Rate, 30-Year Mortgage Rate, CPI, Unemployment Rate._ Retrieved from https://fred.stlouisfed.org 

2. **CRSP/Ziman Real Estate Database.** (2024). _REIT Master Panel: Monthly returns and characteristics for US equity REITs._ Provided by instructor. 

3. **Letdin, M., et al.** (2026). _REIT Factors._ Working paper. 

4. **Smith, J., & Johnson, L.** (2020). Interest rate sensitivity of real estate investment trusts. _Journal of Financial Economics, 125_ (3), 450-472. 

## Appendix: AI Audit Summary 

[0.5-1 page. Summarize AI use across all milestones. **MANDATORY - Missing = 0/50 points** ] 

## AI Tools Used 

- [List all AI tools: ChatGPT, Claude, GitHub Copilot, etc.] 

- [Specify model versions if known: GPT-4, Claude Sonnet, etc.] 

## Key Verification Examples 

## **M1 Example:** 

**Prompt:** "[Your actual prompt to AI]" 

- **Output:** [Brief summary or code snippet AI provided] 

- **Verification:** [How you tested/validated the output] 

- **Critique:** [What you changed or what AI got wrong] 

**M2 Example:** 

17 / 18 

- **Prompt:** "[Your actual prompt to AI]" 

- **Output:** [Brief summary or code snippet AI provided] 

- **Verification:** [How you tested/validated the output] 

- **Critique:** [What you changed or what AI got wrong] 

## **M3 Example:** 

- **Prompt:** "[Your actual prompt to AI]" 

- **Output:** [Brief summary or code snippet AI provided] 

- **Verification:** [How you tested/validated the output] 

- **Critique:** [What you changed or what AI got wrong] 

## **M4 Example:** 

- **Prompt:** "[Your actual prompt to AI for memo writing/editing]" 

- **Output:** [Brief summary of AI response] 

- **Verification:** [How you fact-checked interpretation/recommendations] 

- **Critique:** [What domain knowledge did you add that AI missed?] 

## **REIT Example (M3):** 

- **Prompt (ChatGPT):** "Interpret a Fixed Effects coefficient of -0.025 for FEDFUNDS." 

- **Output:** "A 1% increase in FEDFUNDS reduces returns by 2.5%." 

- **Critique:** Phrasing is ambiguous ("1%" vs. "1 pp"). Corrected to: "A 1 percentage point increase in FEDFUNDS reduces returns by 2.5 percentage points." 

## Responsibility Statement 

All code and analysis in this memo has been verified by our team. We used AI as a **productivity tool** , not as a substitute for understanding. We take full responsibility for any errors and do not claim "the AI did it" as an excuse. 

## **END OF MEMO** 

## _Team Members:_ 

- [Name 1] 

- [Name 2] 

- [Name 3] 

- [Name 4] (if applicable) 

## _Submission Date:_ [Date] 

_Course:_ QM 2023: Statistics II / Data Analytics, Spring 2026, University of Tulsa 

18 / 18 

