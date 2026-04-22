# Individual Contribution Statement

Name: James Gawey  
Team: QM 2023 Capstone Team  
Date: April 22, 2026

## 1. Personal Contribution to Capstone Milestones

### Milestone 1: Data Pipeline (Week 5)

Tasks completed:

- Built and validated the CoinGecko merge and cleaning workflow, including rank metadata and consistent panel formatting.
- Cleaned and standardized macro series (VIX, Effective Federal Funds Rate, EPU), including date parsing, missing value handling, and duplicate removal.
- Helped structure final outputs and assignment-aligned file locations (`data/raw`, `data/processed`, `data/final`) for reproducible submission.

Hours spent: 16 hours

Key deliverable:

- Reproducible input-to-output data pipeline producing `data/final/crypto_analysis_panel.csv` with metadata and supporting documentation.

### Milestone 2: EDA Dashboard (Week 10)

Tasks completed:

- Developed and/or refined EDA visualizations in `capstone_eda.ipynb` (correlation heatmap, lag profiles, event-window plots, group comparisons).
- Verified plot exports and naming in `results/figures/` and aligned interpretation with visual evidence.
- Contributed to M2 narrative polish to match observed relationships and data-quality caveats.

Hours spent: 14 hours

Key deliverable:

- EDA figure package (M2 plot set) and narrative alignment in `M2_EDA_summary.md`.

### Milestone 3: Econometric Models (Week 14)

Tasks completed:

- Implemented/validated Model A FE setup with clustered standard errors and interaction-based identification for date-common SEC event drivers.
- Ran and documented required diagnostics (Breusch-Pagan, VIF, residual plots) plus robustness checks (lags, outlier trimming, group subsamples).
- Completed Model B Option 3 comparison (OLS vs Random Forest) and integrated findings into interpretation memo.

Hours spent: 18 hours

Key deliverable:

- M3 tables and diagnostics in `results/tables/` and `results/figures/`, with interpretation write-up in `M3_interpretation.md`.

### Milestone 4: Final Investment Memo (Week 14)

Tasks completed:

- Drafted and refined final memo text using verified model outputs and plain-language interpretation.
- Completed AI audit documentation and final reproducibility checks.
- Created final submission support docs (rubric checklist and reproducibility report).

Hours spent: 12 hours

Key deliverable:

- Draft memo and submission support files in `results/reports/`.

### Total Estimated Contribution

Total hours across all milestones: 60 hours  
Percentage of team workload: 20% (adjust if your team uses a different split)  
Role(s) on team: Data Pipeline and Modeling Support, Reproducibility and Documentation Lead

## 2. One Defended Methodological Decision

Decision:

I supported using two-way fixed effects with group interaction terms for the SEC event driver, rather than a standalone event-level coefficient.

Reasoning:

Because the SEC event indicator is common across tokens on a given date, its level effect is absorbed by date fixed effects. Interaction terms by token group preserve identification of differential exposure, which is the economically relevant question in our setting. This strategy also aligns with our M3 outputs, where stablecoin interaction effects are statistically meaningful while DeFi effects are less stable.

Alternative considered (and why rejected):

We considered dropping time fixed effects to estimate a direct event-level coefficient, but rejected that approach because it would reintroduce bias from common time shocks and weaken causal interpretation.

## 3. One Key Limitation of Our Analysis

Limitation:

High multicollinearity among market-cap and volume controls limits confidence in individual control-coefficient interpretation.

Why this matters:

When predictors are highly collinear, coefficient estimates can become unstable even if the model fits reasonably well. In our case, very high VIF values indicate that size/liquidity channels are difficult to disentangle at the coefficient level, so causal weight should not be over-attributed to one control term.

Potential mitigation:

Future work can reduce dimensional overlap using feature selection, orthogonalized controls, or principal components for liquidity/size factors, then compare stability across specifications.

## 4. AI Audit Notes (Individual)

AI tools used:

- GitHub Copilot

Specific AI use examples:

Example 1:

- Task: FE model specification and diagnostic workflow support
- Prompt: Asked for a complete M3 structure with FE, robust SE comparison, diagnostics, and robustness checks
- Output: Produced code pattern and export workflow used in `capstone_models.py`
- Verification: Re-ran scripts and validated generated outputs in `results/tables/` and `results/figures/`
- Critique: Suggestions were useful for structure, but all econometric choices and interpretation were validated against project context

Example 2:

- Task: Final memo and AI-audit documentation support
- Prompt: Requested drafting support for M4 memo sections and audit completeness
- Output: Produced draft text and checklist files under `results/reports/`
- Verification: Cross-checked all reported numbers against model output CSV files
- Critique: Drafting support improved speed; final wording and accountability remain human-reviewed

Overall AI use:

AI was used mainly for coding efficiency, document structuring, and editing support; empirical interpretation, methodological decisions, and final checks were reviewed by the team.

## 5. Self-Reflection

What I did particularly well:

I was strongest in keeping the workflow reproducible from raw inputs through final outputs while linking technical outputs to clear written interpretation.

What I could have improved:

I could have started final memo synthesis earlier to allow more time for iteration on recommendation framing and final formatting polish.

What I learned from this capstone project:

I learned how much of credible analysis depends on transparent data engineering and reproducibility, not just final model fit. I also improved in defending model design choices under realistic constraints and communicating technical findings for a non-technical audience.

## 6. Attestation

By submitting this individual addendum, I affirm that:

- The contributions listed above are accurate and honest.
- I have not exaggerated my role or minimized teammates' contributions.
- I understand this addendum may be used to adjust my individual grade.
- I take responsibility for my own contributions and any errors in the sections I authored.

Signature: James Gawey  
Date: April 22, 2026
