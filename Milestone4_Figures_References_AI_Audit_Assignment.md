# Milestone 4: Figures + References + AI Audit Lead

## Purpose
This file centralizes my entire M4 responsibility in one spot so the team can track progress and know exactly what I own.

## Role Summary
- Lead selection and cleanup of memo figures
- Ensure publication-ready quality for captions, formatting, and resolution
- Compile and format references with full source links
- Draft the AI Audit appendix with prompts, verification, and critique examples
- Maintain a section ownership map for the final memo

## Deliverables
1. Final_Investment_Memo.pdf
   - Figure 1: `results/figures/M3_modelB_option3_actual_vs_predicted.png`
   - Figure 2: `results/figures/M3_residuals_vs_fitted.png`
   - References section with source URLs
   - AI Audit appendix section
   - Section ownership map
2. Individual_Addendum_[YourName].pdf
   - Note: personal contribution should include my role on figures, references, and AI audit

## Completed Checklist
### Figures
- [x] Identify the two required memo figures
  - [x] Key visualization: `results/figures/M3_modelB_option3_actual_vs_predicted.png`
  - [x] Diagnostic plot: `results/figures/M3_residuals_vs_fitted.png`
- [x] Confirm the figures already exist in the repo from M3 output
- [x] Confirm 300 DPI export settings from existing script output
- [x] Draft captions for a non-technical audience
  - [x] Figure 1 caption: "Actual vs. predicted crypto volatility from the alternative specification. This plot shows how well the model captures realized volatility patterns and highlights periods of under- and over-prediction."
  - [x] Figure 2 caption: "Residuals vs. fitted values from the main fixed effects regression. This diagnostic plot checks whether model errors are centered around zero with no systematic pattern."
- [ ] Confirm the memo references the correct file paths and captions

### References
- [x] Collect dataset source names and URLs
  - [x] CoinGecko market data: https://www.coingecko.com/
  - [x] FRED VIX series: https://fred.stlouisfed.org/series/VIXCLS
  - [x] FRED Effective Federal Funds Rate: https://fred.stlouisfed.org/series/DFF
  - [x] FRED Economic Policy Uncertainty Index: https://fred.stlouisfed.org/series/USEPUINDXD
  - [x] SEC press release dataset: https://www.sec.gov/news/press-releases
- [x] Format references consistently in APA-style form
- [ ] Verify that every citation used in the memo appears in References

### AI Audit
- [x] Draft M4 audit appendix text
  - [x] Describe AI use for drafting figure captions, references formatting, and audit narrative
  - [x] Explain how outputs were verified against repo outputs
- [x] Emphasize verification and critique
  - [x] AI outputs were checked against actual code, tables, and figure exports
- [ ] Keep the appendix length to 0.5–1 page in the final memo
- [ ] Include the appendix in the team memo as a required section

### Section Ownership Map
- [x] Create a compact ownership map for the memo
- [x] Document ownership of:
  - Figures
  - References
  - AI Audit
- [ ] Confirm the full team map before final memo submission

## Recommended File Structure
- `results/figures/` — store final figure image files
- `results/reports/` — draft markdown or report notes
- repo root — final PDFs after export
- this file — centralized task ownership and status tracking

## Notes
- Figure output is already available from M3 and can be reused for the M4 memo.
- References are based on the repository’s actual data sources and should be added to the final memo.
- The final memo should be produced in the repo and exported as PDF from markdown or another authoring tool.
