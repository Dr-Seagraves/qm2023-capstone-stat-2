# Milestone 4: Figures + References + AI Audit Lead

## Deliverables for Memo
- Figure 1: `results/figures/M3_modelB_option3_actual_vs_predicted.png`
- Figure 2: `results/figures/M3_residuals_vs_fitted.png`
- References section with source URLs
- AI Audit appendix section

## References
- CoinGecko. (n.d.). Cryptocurrency market data. Retrieved April 2026, from https://www.coingecko.com/
- Federal Reserve Bank of St. Louis. (n.d.). VIXCLS: CBOE Volatility Index: VIX. FRED. Retrieved April 2026, from https://fred.stlouisfed.org/series/VIXCLS
- Federal Reserve Bank of St. Louis. (n.d.). DFF: Effective Federal Funds Rate. FRED. Retrieved April 2026, from https://fred.stlouisfed.org/series/DFF
- Federal Reserve Bank of St. Louis. (n.d.). USEPUINDXD: U.S. Economic Policy Uncertainty Index. FRED. Retrieved April 2026, from https://fred.stlouisfed.org/series/USEPUINDXD
- U.S. Securities and Exchange Commission. (n.d.). News & press releases. Retrieved April 2026, from https://www.sec.gov/news/press-releases

## Appendix: AI Audit Summary

[0.5-1 page. Summarize AI use across all milestones. MANDATORY]

### AI Tools Used
- GitHub Copilot
- ChatGPT (Raptor mini Preview)

### Key Verification Examples

**M1 Example:**
- **Prompt:** "Summarize the dataset characteristics and suggest candidate outcome and control variables for a crypto panel model."
- **Output:** Proposed key variables and descriptive checks for the dataset.
- **Verification:** Reviewed actual data files in `data/processed/` and confirmed the suggested variables matched the repo’s crypto panel structure.
- **Critique:** Added domain-specific context for crypto volatility and clarified that regulatory event variables needed manual coding.

**M2 Example:**
- **Prompt:** "Translate a REIT fixed effects model description into a crypto panel regression with regulatory controls."
- **Output:** Drafted a translated model specification and interpretation framework.
- **Verification:** Compared the output to the repo’s actual model files and data merges, especially `merge_final_with_macro_controls.py` and `crypto_analysis_panel.csv`.
- **Critique:** Corrected the variable interpretation to focus on volatility and macro controls rather than REIT-specific finance terms.

**M3 Example:**
- **Prompt:** "Help write figure captions and diagnostic narrative for regression results and residual plots."
- **Output:** Generated draft caption text for model fit and diagnostic plots.
- **Verification:** Confirmed the figure names, file contents, and caption relevance by checking `results/figures/M3_modelB_option3_actual_vs_predicted.png` and `results/figures/M3_residuals_vs_fitted.png`.
- **Critique:** Refined captions to be accessible to non-technical readers and aligned them with the project’s crypto volatility focus.

**M4 Example:**
- **Prompt:** "Draft the memo appendix content for Milestone 4 that describes AI use, verification steps, and critique of AI output."
- **Output:** Drafted an AI Audit appendix narrative with AI use, verification checks, and human oversight.
- **Verification:** Confirmed source URLs against the repository’s data source list, verified figure file names and paths in `results/figures/`, and checked that AI wording matched memo assignment expectations.
- **Critique:** Clarified AI wording to distinguish between "percentage points" and "percent," and ensured the appendix emphasized human review over AI authorship.

### Responsibility Statement
All code and analysis in this memo have been verified by our team. We used AI as a productivity tool, not as a substitute for understanding. We take full responsibility for any errors and do not claim "the AI did it" as an excuse.
