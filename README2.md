# Converted from README(2).pdf

## Page 1

Milestone 2: EDA Dashboard QM 2023 Capstone Project Due: Friday, Week 10 (March 27, 2026) by 11:59 PM Points: 50 (25% of capstone grade) Format: Team
submission via shared GitHub repository (main branch) Overview Milestone 2 transitions from data engineering (M1) to exploratory data analysis. Your goal is to uncover
patterns, correlations, and relationships in your data that will guide your econometric speciﬁcations in M3.
Real-world context: Before running regressions, analysts must understand their data. What variables correlate?
At what lags? Are there subgroups with different sensitivities? EDA is where hypotheses are formed, not tested.
Success criterion: Your visualizations should be publication-ready (titles, labels, legends, captions) and every
plot should tell a story that informs your M3 models.
Note: Guidance and short snippets are provided for each required plot. Adapt patterns to your
dataset and variable names — do not copy full solutions.
Dataset Adaptation Note Alternative Dataset Teams: The required plots below use REIT examples, but the concepts apply to all panel
datasets: - Correlation heatmap →  Works for any dataset (outcome + drivers + controls) - Time series →  Plot
your outcome variable over time - Dual-axis →  Overlay outcome + key driver (e.g., returns + rates, prices +
sentiment) - Lagged effects →  Test which lag structure is optimal for your driver variable - Group analysis
(Plots 5-6) →  CONDITIONAL: If your dataset has groups (sectors, regions, asset types), complete these plots.
If not, replace with alternatives (see “Dataset Without Groups” section below). - Scatter plots →  Bivariate
relationships (outcome vs. control variables) - Decomposition →  Trend + seasonal + residual components
Translation Guidance: See Dataset-Translation-Examples.md for worked examples translating REIT plots
to Crypto, Housing, and Macro datasets.
Open Data Catalog Teams: If your dataset comes from the Open Data catalog, the same plot types
apply. Replace “REIT returns” with your outcome variable, “FEDFUNDS” with your driver
variable, and “sectors” with whatever grouping variable exists in your data (if any). If you don’t
have natural groups, see the “Alternatives for Datasets Without Groups” section below.
Learning Objectives By completing M2, you will:
1. Create publication-quality visualizations (titles, labels, legends, captions)
2. Identify correlation patterns between outcome and driver variables
3. Determine optimal lag structures for time-series relationships
4. Detect group heterogeneity (if applicable to your dataset)
2/25/26, 12:18 AM README ﬁle:///tmp/M2-EDA-Dashboard_README.html 1/8

## Page 2

5. Formulate testable hypotheses for M3 econometric models
6. Diagnose data quality issues (outliers, missing values, heteroskedasticity)
7. Communicate visual insights through economic interpretation
Deliverables
1. Jupyter Notebook: capstone_eda.ipynb
Requirements: - Runs from top to bottom without errors (Restart Kernel →  Run All) - Minimum 8 required
visualizations (see below) - Every plot has: - Descriptive title - Axis labels with units - Legend (if multiple
series) - Caption explaining insight + economic interpretation - Saved to results/figures/ as PNG ﬁles (300
DPI). Use FIGURES_DIR from conﬁg_paths.
Structure: Section 1: Imports + data loading (from conﬁg_paths, load M1 output) →  Section 2: Summary
statistics →  Section 3: Correlation analysis →  Section 4: Time series →  Section 5: Lagged effect analysis →
Section 6: Group analysis (if applicable) →  Section 7: Factor/control relationships →  Section 8: Time series
decomposition
2. Required Visualizations (8 minimum)
Plot 1: Correlation Heatmap (REQUIRED) Purpose: Identify which variables are strongly correlated with your outcome variable.
REIT Example Variables: ret, fedfunds, mortgage30us, cpiaucsl, unrate, mom, qlty, size
Generic Pattern: Include your [OUTCOME], [DRIVER], and [CONTROL] variables.
Interpretation Guidance: - What is the strongest correlation with your outcome? (Will likely be your main
driver for M3) - Are any control variables highly correlated with each other? (Multicollinearity warning for M3)
- Do correlations match your theoretical expectations?
Guidance: Use sns.heatmap() on your correlation matrix. Include outcome, driver, and controls. Set
sns.set_style("whitegrid") and save to FIGURES_DIR. Key snippet: corr_matrix =
data[vars_to_plot].corr().
Caption Template: > “The heatmap reveals a [strong/moderate/weak] negative correlation (r = X.XX) between
[OUTCOME] and [DRIVER]. [Explain why this makes economic sense]. [CONTROL1] shows [pattern],
suggesting [implication for M3].” Plot 2: Time Series of Outcome Variable (REQUIRED) Purpose: Visualize trends, volatility, and outlier periods.
Generic Pattern: Plot [OUTCOME] on Y-axis, [TIME] on X-axis.
REIT Example: Average REIT returns over time (2015-2024).
Crypto Example: Bitcoin returns over time (2020-2024).
2/25/26, 12:18 AM README ﬁle:///tmp/M2-EDA-Dashboard_README.html 2/8

## Page 3

Housing Example: Median home price growth rate over time (2010-2024).
Interpretation Guidance: - Identify crisis periods (e.g., March 2020 COVID crash, 2022 rate hikes) - Note
volatility clustering (periods of high vs. low variance) - Detect secular trends (persistent upward/downward
movement) Plot 3: Dual-Axis Plot (Outcome vs. Driver) (REQUIRED) Purpose: Visualize co-movement between outcome and key driver variable.
Generic Pattern: Left Y-axis = [OUTCOME], Right Y-axis = [DRIVER], shared X-axis = [TIME].
REIT Example: REIT returns (left) vs. Federal Funds Rate (right).
Crypto Example: Token returns (left) vs. Regulatory Severity Index (right).
Housing Example: Home price growth (left) vs. Mortgage rates (right).
Interpretation Guidance: - Do the two series move together (positive correlation) or opposite (negative)? - Is
there a visible lag (driver moves ﬁrst, outcome follows)? - Are there periods where the relationship breaks down?
Plot 4: Lagged Effect Analysis (REQUIRED) Purpose: Determine at what lag the driver variable most strongly correlates with the outcome.
Method: Test lags 0, 1, 2, 3, 6, 12 (adjust for your frequency: months vs. days). Use
data.groupby('[entity_id]')['[driver]'].shift(lag) to create lagged driver—critical: group by entity
to avoid cross-entity leakage. Plot bar chart of correlation vs. lag. Identify optimal lag for M3.
Interpretation Guidance: - What is the optimal lag? (This becomes your lag speciﬁcation in M3) - Does the
relationship strengthen or weaken with longer lags? - What is the economic mechanism for this lag? (E.g.,
“REITs reﬁnance debt after 2 months”) Plot 5: Group Box Plots (CONDITIONAL - See Alternatives Below) When to Include: Your dataset has meaningful groups (sectors, regions, asset types, size quartiles).
Purpose: Compare outcome distributions across groups.
REIT Example: Box plots of returns by REIT sector (Retail, Ofﬁce, Industrial, Residential).
Crypto Example: Box plots of returns by token type (DeFi, CEX, NFT, Stablecoin).
Housing Example: Box plots of price growth by region (Urban, Suburban, Rural).
Guidance: Use data.boxplot(column='[outcome]', by='[group_variable]') or seaborn boxplot. Remove
default suptitle; add proper axis labels and units.
Interpretation Guidance: - Which group has the highest/lowest median outcome? - Are there outliers speciﬁc to
certain groups? - Does variance differ across groups? (Heteroskedasticity warning for M3)
2/25/26, 12:18 AM README ﬁle:///tmp/M2-EDA-Dashboard_README.html 3/8

## Page 4

Plot 6: Group Sensitivity Analysis (CONDITIONAL - See Alternatives Below)
When to Include: Your dataset has groups AND you expect differential sensitivity to the driver.
Purpose: Segment groups based on sensitivity to the driver variable.
REIT Example: Sector-level correlation with FEDFUNDS; color-code sensitive (r < -0.3) vs. resilient (r ≥ -0.3).
Crypto Example: Token type correlation with regulatory sentiment; identify most affected types.
Housing Example: Regional correlation with mortgage rates; identify rate-sensitive markets.
Guidance: Compute group_sensitivity = data.groupby('[group_variable]').apply(lambda x:
x['[outcome]'].corr(x['[driver]'])). Plot horizontal bars; color-code by threshold (e.g., red if r < -0.3).
Flag sensitive groups for M3 interaction terms.
Interpretation Guidance: - Which groups are most sensitive to the driver? - What economic mechanisms
explain these differences? (E.g., “Retail REITs have higher leverage”) - Should M3 include group × driver
interaction terms?
Alternatives for Datasets Without Groups If your dataset does not have natural grouping variables (e.g., single-entity time series, macro indicators),
replace Plots 5-6 with ONE of these alternatives:
Alternative A: Time Period Subsample Analysis Purpose: Compare relationship strength across different time periods (bull vs. bear, pre/post crisis). Split data by
date; compute correlation per period; bar chart.
Alternative B: Rolling Correlation Analysis Purpose: Show how the relationship changes over time. Use data['[outcome]'].rolling(window=6).corr(data['[driver]']) (adjust window for your frequency). Line
plot over time.
Alternative C: Size Quartile Analysis Purpose: Segment by size (market cap, volume, GDP). Use pd.qcut(data['[size_variable]'], q=4); box
plot outcome by quartile.
Plot 7: Factor/Control Variable Scatter Plots (REQUIRED) Purpose: Visualize bivariate relationships between outcome and control variables.
Generic Pattern: Scatter plot of [OUTCOME] vs. [CONTROL] with regression line.
REIT Example: Returns vs. Momentum (MOM), Returns vs. Quality (QLTY).
Crypto Example: Returns vs. Trading Volume, Returns vs. Market Cap.
2/25/26, 12:18 AM README ﬁle:///tmp/M2-EDA-Dashboard_README.html 4/8

## Page 5

Housing Example: Price Growth vs. Employment Growth, Price Growth vs. New Construction.
Guidance: Use sns.regplot() for scatter + regression line. Add units to axis labels; save to FIGURES_DIR.
Plot 8: Time Series Decomposition (REQUIRED) Purpose: Separate trend, seasonal, and residual components.
Guidance: Use statsmodels.tsa.seasonal.seasonal_decompose. Aggregate to time series ﬁrst if panel data
(groupby('[time]')['[outcome]'].mean()). Period: 12 for monthly, 365 for daily, 4 for quarterly. Plot
observed, trend, seasonal, residual in 4-panel ﬁgure.
Interpretation Guidance: - Is there a clear upward/downward trend? - Are seasonal patterns strong? (If yes,
include seasonal dummies in M3) - Are residuals white noise or do they show structure? (Structure →  additional
controls needed)
3. Summary Markdown: M2_EDA_summary.md
Required sections: - Key Findings (3-5 bullets): Correlations, optimal lag, group sensitivity, outliers, control
patterns—each with economic mechanism. - Hypotheses for M3 (3+): Hypothesis 1 (driver effect): claim,
model spec, expected sign, mechanism. Hypothesis 2 (control premiums). Hypothesis 3 (group heterogeneity, if
applicable). See Dataset-Translation-Examples.md for REIT/Crypto/Housing examples. - Data Quality
Flags: Outlier periods, missing values, heteroskedasticity, multicollinearity—and planned M3 mitigations.
4. AI Audit Appendix: AI_AUDIT_APPENDIX.md
Same requirements as M1. Document all AI use (ChatGPT, Copilot, etc.) with speciﬁc prompts, outputs,
veriﬁcation, and critique.
Technical Requirements
1. Jupyter Notebook Best Practices
Markdown Cells: - Use headings (#, ##, ###) to structure sections - Write narrative explanations before each
visualization - Include captions after each plot explaining the insight
Code Cells: - One logical step per cell (easier to debug) - Use comments to explain complex code - Print output
(summary statistics, row counts) to verify logic Visualization Standards: - Title: Every plot must have a descriptive title - Axis labels: Include units (e.g.,
“Return (%)”, “Month (YYYY-MM)”) - Legends: Clearly label all lines or groups - Color choices: Use
colorblind-friendly palettes (e.g., seaborn.color_palette("colorblind")) - Font size: Increase if needed for
readability (plt.rcParams['font.size'] = 12) Grading Rubric (50 points) See rubric.md for detailed breakdown. Summary:
2/25/26, 12:18 AM README ﬁle:///tmp/M2-EDA-Dashboard_README.html 5/8

## Page 6

Component Points Criteria Data Loading & Summary 10 Notebook runs without errors; data dimensions veriﬁed; summary stats presented Visualization Quality 20 All 8 required plots present;
publication-ready (titles, labels, legends); appropriate plot types Analysis & Interpretation 15 Captions explain insights; economic intuition provided; patterns connected to theory Hypothesis Formulation 5 Clear hypotheses for M3; testable and grounded in EDA ﬁndings Total: 50 points Common Pitfalls and How to Avoid Them Pitfall 1: Plots Without Titles or Labels Problem: Axes say ret and fedfunds with no explanation. Solution: Every plot needs: - Title explaining what
is shown - X-axis label with units - Y-axis label with units - Legend if multiple series
Pitfall 2: No Economic Interpretation Problem: Caption says “Returns and rates are negatively correlated” (just restates the visual). Solution: Explain
why economically. “Rising rates increase ﬁnancing costs for leveraged [entities], reducing proﬁtability and
depressing returns.” Pitfall 3: Ignoring Outliers Problem: March 2020 COVID crash dominates all visualizations, obscuring patterns. Solution: Either: -
Winsorize extreme values for visualization (document this) - Create separate crisis vs. normal period analysis -
Add annotations to highlight outlier periods Pitfall 4: Using Wrong Plot Type Problem: Correlation matrix shown as a line plot (nonsensical). Solution: Match plot type to data: - Heatmap →
Correlation matrices - Line plot →  Time series - Box plot →  Distributions across categories - Scatter plot →
Bivariate relationships Pitfall 5: Notebook Doesn’t Run Top-to-Bottom Problem: Cells depend on manual execution order; “Run All” fails. Solution: Restart kernel and run all cells
before submission. Fix any errors.
Testing Checklist (Run Before Submission) Restart kernel and run all: Kernel →  Restart & Run All. No errors?
2/25/26, 12:18 AM README ﬁle:///tmp/M2-EDA-Dashboard_README.html 6/8

## Page 7

All 8 visualizations present: Check list of required plots (or alternatives if no groups)
Every plot has title, labels, legend Captions explain insights, not just describe visuals M2_EDA_summary.md complete: Key ﬁndings, hypotheses, data quality ﬂags
AI Audit Appendix submitted All ﬁles saved to results/ﬁgures/ and results/reports/:
capstone_eda.ipynb M2_EDA_summary.md AI_AUDIT_APPENDIX.md All PNG ﬁles from plots Submission Instructions Shared Team Repository Submission
1. Work in your team’s shared private GitHub repository and submit by committing/pushing to main.
2. Add your ﬁles: capstone_eda.ipynb, M2_EDA_summary.md, AI_AUDIT_APPENDIX.md,
results/figures/M2_*.png. Commit and push to main.
3. Verify: Check GitHub repo online; Jupyter notebook should render with all plots, and your latest commit
should be on main.
4. Auto-checks: If repository checks run, use them to conﬁrm notebook execution and ﬁle completeness.
Deadline: Friday, Week 10 (Mar 27) by 11:59 PM Late penalty: 10% per day, up to 3 days. After 3 days, no
credit.
Resources and Support Ofﬁce Hours Dr. Seagraves: Monday & Wednesday, 3:00-5:00 PM (Helm 122-D) Focus for M2: Visualization design, economic interpretation, hypothesis formulation
Example Code Starter notebook: starter/capstone_eda.ipynb (incomplete template) seaborn gallery: Ofﬁcial examples matplotlib customization: User guide Debugging Tips Plot not showing? Add plt.show() at end of cell.
Colors are ugly? Use seaborn.color_palette("colorblind") for accessibility.
Lag analysis not working? Use groupby('[entity_id]').shift(lag) to avoid cross-entity leakage.
Next Steps After M2 Your EDA insights will directly inform: - Milestone 3 (Week 14): Econometric model speciﬁcations (which
lags? which interactions?) - Milestone 4 (Week 16): Investment memo narrative (which groups to recommend?)
2/25/26, 12:18 AM README ﬁle:///tmp/M2-EDA-Dashboard_README.html 7/8

## Page 8

Don’t treat M2 as a checkbox exercise. The better your EDA, the stronger your M3 models and M4 memo will
be.
Good luck! Great EDA is the bridge between data and insight.
Document prepared for QM 2023: Statistics II, Spring 2026, University of Tulsa Contact: Dr. Cayman Seagraves
(cayman-seagraves@utulsa.edu) 2/25/26, 12:18 AM README ﬁle:///tmp/M2-EDA-Dashboard_README.html 8/8
