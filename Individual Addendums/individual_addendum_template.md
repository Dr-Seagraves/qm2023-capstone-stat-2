## Individual Addendum Template 

## **QM 2023 Capstone Project - Milestone 4** 

## **Individual Contribution Statement** 

**Name:** _________________________________ **Team:** _________________________________ **Date:** _________________________________ 

## 1. Personal Contribution to Capstone Milestones 

[Provide specific, quantifiable contributions to each milestone. Include tasks, hours, and deliverables.] 

Milestone 1: Data Pipeline (Week 5) 

## **Tasks completed:** 

- [Specific task 1, e.g., "Implemented FRED API integration for economic indicators"] 

- [Specific task 2, e.g., "Handled missing value imputation for REIT returns"] 

- [Specific task 3, e.g., "Verified merge logic and ensured no duplicate rows"] 

**Hours spent:** _____ hours 

**Key deliverable:** [e.g., "FRED data fetching function (Section 4 of capstone_data_pipeline.py)"] 

## Milestone 2: EDA Dashboard (Week 10) 

## **Tasks completed:** 

- [Specific task 1, e.g., "Created correlation heatmap and dual-axis plots"] 

- [Specific task 2, e.g., "Conducted lagged effect analysis to determine optimal lag structure"] 

- [Specific task 3, e.g., "Wrote economic interpretation captions for all visualizations"] 

**Hours spent:** _____ hours 

**Key deliverable:** [e.g., "Visualizations 1, 2, and 7 in capstone_eda.ipynb + captions"] 

## Milestone 3: Econometric Models (Week 14) 

## **Tasks completed:** 

- [Specific task 1, e.g., "Specified and estimated Fixed Effects model with clustered SEs"] 

- [Specific task 2, e.g., "Ran heteroskedasticity and VIF diagnostics"] 

1 / 8 

- [Specific task 3, e.g., "Conducted robustness checks: alternative lags, COVID exclusion, sector subsamples"] 

**Hours spent:** _____ hours 

**Key deliverable:** [e.g., "M3 Fixed Effects regression code (Section 3) and diagnostics (Section 5)"] 

## Milestone 4: Final Investment Memo (Week 14) 

## **Tasks completed:** 

- [Specific task 1, e.g., "Drafted Executive Summary and Investment Recommendations sections"] 

- [Specific task 2, e.g., "Compiled regression tables from M3 results"] 

- [Specific task 3, e.g., "Edited full memo for clarity and professional tone"] 

## **Hours spent:** _____ hours 

**Key deliverable:** [e.g., "Executive Summary, Conclusions & Recommendations sections (Sections 1 and 3 of final memo)"] 

## Total Estimated Contribution 

**Total hours across all milestones:** _____ hours 

**Percentage of team workload:** _____% (must sum to 100% across all team members) 

**Role(s) on team:** [e.g., "Data Engineer (M1 lead), Visualization Specialist (M2 lead), Communication Lead (M4 lead)"] 

## 2. One Defended Methodological Decision 

[Choose one methodological decision you made (or advocated for) during the capstone project. Explain your reasoning with evidence from the analysis or economic theory.] 

**Decision:** [State the decision clearly] 

**Example:** "I recommended using a **2-month lag** for the Federal Funds Rate in our Fixed Effects model." 

## **Reasoning:** 

[Provide 2-4 sentences explaining WHY this decision was correct, grounding your argument in:] 

- **Data evidence:** "M2 exploratory analysis showed the strongest correlation at lag 2 (r = -0.38 vs. r = -0.20 at lag 1)." 

- **Economic reasoning:** "REITs negotiate leases and refinance debt over 1-2 months, so immediate rate effects are muted." 

- **Robustness:** "M3 robustness checks confirmed that lag 2 coefficient is most statistically significant (p = 0.012 vs. p = 0.08 for lag 1)." 

**Alternative considered (and why rejected):** 

2 / 8 

[Optional but impressive: mention an alternative approach and why you chose your decision instead.] 

**Example:** "We considered using the contemporaneous rate (lag 0) for simplicity, but this produced weaker statistical significance (p = 0.15) and was less consistent with REIT financing timelines." 

## 3. One Key Limitation of Our Analysis 

[Identify the most important limitation or caveat of your capstone analysis. Be honest and substantive—this is not the place for trivial concerns.] 

**Limitation:** [State the limitation clearly] 

**Example:** "Our Fixed Effects model assumes that unobserved REIT characteristics (management quality, property mix) are **time-invariant** ." 

## **Why this matters:** 

[Explain the economic or statistical implication of this limitation in 2-4 sentences.] 

## **Example:** 

However, during the COVID-19 pandemic, many REITs restructured their portfolios (e.g., exited retail properties, entered industrial warehouses). If these portfolio shifts correlate with both REIT returns and interest rate exposure, our Fixed Effects estimates may be biased. For instance, a REIT that shifted to industrial properties in 2020 would appear less rate-sensitive in our model, but this could be due to the portfolio change, not the rate environment. 

## **Potential mitigation:** 

[Suggest how future work could address this limitation.] 

## **Example:** 

A robustness check using **two-way Fixed Effects with sector-time interactions** could partially 

address this concern by allowing for time-varying sector effects. Alternatively, a **dynamic panel model** (e.g., Arellano-Bond GMM) could account for portfolio restructuring over time, but this requires a longer time series and is beyond the scope of this capstone. 

## 4. AI Audit Notes (If Applicable) 

[If you used AI tools for any specific tasks in your portion of the work, document them here. This section is optional if AI use was fully documented in the team AI Audit Appendix.] 

## **AI Tools Used:** 

**==> picture [10 x 10] intentionally omitted <==**

**==> picture [10 x 10] intentionally omitted <==**

**==> picture [10 x 10] intentionally omitted <==**

**==> picture [10 x 10] intentionally omitted <==**

ChatGPT 

- GitHub Copilot Claude 

Other: _________________________________ 

**Specific AI Use Examples:** 

3 / 8 

Example 1: [Task description] 

**Prompt:** [What you asked the AI] 

**Output:** [What the AI produced] 

**Verification:** [How you tested it worked correctly] 

**Critique:** [What the AI got wrong, how you corrected it] 

## Example 2: [Repeat for each significant AI interaction] 

**Overall AI Use:** [Estimate % of your work that involved AI assistance] 

## **Example:** 

I used AI for approximately 30% of my coding tasks (syntax help, debugging) but wrote all interpretations and economic reasoning independently. I verified all AI-generated code by running it on our dataset and cross-checking outputs against expected values. 

## 5. Self-Reflection 

What did I do particularly well on this capstone? 

[1-2 sentences highlighting your strengths or proudest moments] 

## **Example:** 

I excelled at translating technical regression output into business language for the final memo. My Executive Summary clearly communicated complex findings to a non-technical audience without sacrificing accuracy. 

## What could I have improved? 

[1-2 sentences acknowledging areas for growth] 

## **Example:** 

I could have started M3 econometric modeling earlier instead of waiting until the week before the deadline. This would have given me more time to explore alternative specifications and conduct deeper robustness checks. 

## What did I learn from this capstone project? 

[2-3 sentences reflecting on skill development or conceptual insights] 

## **Example:** 

This capstone taught me that **data cleaning is 80% of the work** in real-world analysis. I also learned to defend methodological choices with evidence (not just intuition) and to communicate limitations honestly rather than hiding them. Most importantly, I developed confidence in my ability to execute an end-to-end data science workflow from raw data to professional deliverable. 

4 / 8 

## 6. Attestation 

## **By submitting this individual addendum, I affirm that:** 

**==> picture [10 x 10] intentionally omitted <==**

**==> picture [10 x 10] intentionally omitted <==**

   - All contributions listed above are accurate and honest 

   - I have not exaggerated my role or minimized teammates' contributions 

- I understand that this addendum may be used to adjust my individual grade relative to the team 

- grade 

**==> picture [10 x 10] intentionally omitted <==**

I take full responsibility for my work and any errors in the sections I authored 

**Signature:** _________________________________ **Date:** _____________ 

## Submission Instructions 

1. **Save as PDF:** `Individual_Addendum_[YourLastName].pdf` 

   - Example: `Individual_Addendum_Johnson.pdf` 

2. **Submit via GitHub Classroom** (in the M4 submission folder) 

   - Add to your team repo: `git add Individual_Addendum_[YourName].pdf` 

Commit: `git commit -m "Add individual addendum - [Your Name]"` Push: `git push origin main` 

3. **Verify submission:** Check GitHub repo; your PDF should appear alongside the team memo 

**Deadline:** Friday, Week 14 (May 1) by 11:59 PM (same as M4 team memo) 

## Grading Criteria (10 points) 

|**Component**|**Points**|**Criteria**|
|---|---|---|
|**Specific Contribution**|3|Tasks, hours, and deliverables are specific (not vague)|
|**Defended Decision**|3|Decision is clearly stated, reasoning is grounded in evidence|
|**Key Limitation**|3|Limitation is substantive and honestly discussed|
|**Overall Quality**|1|Professional writing, honest reflection|



**Total:** 10 points (part of M4 50-point grade) 

## FAQs 

## Q: What if I contributed equally to all milestones? 

**A:** That's fine. List specific tasks for each milestone and estimate equal hours. The key is **specificity** (not just "I helped with M2"). 

## Q: What if I contributed less than my teammates due to other commitments? 

5 / 8 

**A:** Be honest. Rate yourself appropriately in the peer evaluation and explain here: "I contributed fewer hours (30 vs. 50 for teammates) due to [reason]. I recognize I did not pull my full weight." 

**Consequence:** Instructor may reduce your individual grade by 5-15% depending on the disparity and your honesty. 

## Q: Can I defend a decision that the team disagreed with initially? 

**A:** Yes! If you advocated for a decision that the team eventually adopted, explain your reasoning. Example: "I proposed using robust standard errors, which the team initially resisted. After I showed that heteroskedasticity tests indicated violations, the team agreed and we applied clustered SEs." 

## Q: What if I can't think of a key limitation? 

**A:** Review M3 interpretation memo, Section "Caveats and Limitations." Choose the limitation you think is most important (e.g., parallel trends assumption, omitted variable bias, measurement error). 

## Q: How long should this addendum be? 

**A:** Exactly **1 page** (strict limit). Use 11-12 pt font, 1-inch margins. Be concise. 

## Example Individual Addendum (Full) 

## **Individual Contribution Statement** 

**Name:** Sarah Johnson **Team:** Team Alpha **Date:** May 1, 2026 

## 1. Personal Contribution to Capstone Milestones 

## **Milestone 1: Data Pipeline (Week 5)** 

- Implemented FRED API integration using `pandas-datareader` (Section 4 of capstone_data_pipeline.py) Handled missing value imputation for REIT returns (dropped rows with missing ret per economic reasoning) 

- Verified merge logic to ensure no duplicate rows (assertion checks added) 

- **Hours:** 20 hours 

- **Key deliverable:** FRED data fetching and merge sections 

## **Milestone 2: EDA Dashboard (Week 10)** 

Created correlation heatmap, dual-axis plot (returns vs. FEDFUNDS), and sector box plots 

- Conducted lagged effect analysis (tested lags 0-12 months) to inform M3 specification 

- Wrote economic interpretation captions for all 8 visualizations **Hours:** 15 hours 

- **Key deliverable:** Visualizations 1, 2, 5, 7 + captions 

6 / 8 

## **Milestone 3: Econometric Models (Week 12)** 

- Specified and estimated Fixed Effects model with `linearmodels.PanelOLS` 

- Ran heteroskedasticity (Breusch-Pagan) and VIF diagnostics 

- Conducted 3 robustness checks: alternative lags, COVID exclusion, sector subsamples **Hours:** 18 hours 

**Key deliverable:** M3 Fixed Effects code (Sections 3, 5, 6) and interpretation memo 

## **Milestone 4: Final Investment Memo (Week 14)** 

- Drafted Executive Summary and Investment Recommendations sections 

- Compiled and formatted regression tables (Tables 1 and 2) 

- Edited full memo for clarity and professional tone (removed jargon, simplified explanations) **Hours:** 12 hours 

- **Key deliverable:** Executive Summary, Conclusions & Recommendations (Sections 1 and 3) 

**Total:** 65 hours (32% of team total; highest contributor per peer evaluation) 

## 2. One Defended Methodological Decision 

**Decision:** I recommended using a **2-month lag** for the Federal Funds Rate in our Fixed Effects model. 

**Reasoning:** M2 exploratory analysis showed the strongest correlation at lag 2 (r = -0.38 vs. r = -0.20 at lag 1). Economically, this makes sense: REITs negotiate leases and refinance debt over 1-2 months, so immediate rate effects are muted. M3 robustness checks confirmed that lag 2 coefficient is most statistically significant (p = 0.012 vs. p = 0.08 for lag 1). 

**Alternative considered:** We considered using the contemporaneous rate (lag 0) for simplicity, but this produced weaker statistical significance (p = 0.15) and was inconsistent with REIT financing timelines. 

## 3. One Key Limitation of Our Analysis 

**Limitation:** Our Fixed Effects model assumes that unobserved REIT characteristics (management quality, property portfolio mix) are **time-invariant** . 

**Why this matters:** During the COVID-19 pandemic, many REITs restructured portfolios (e.g., exited retail, entered industrial). If these changes correlate with both returns and interest rate exposure, our FE estimates may be biased. For example, a REIT that shifted to industrial properties in 2020 would appear less ratesensitive, but this could be due to the portfolio change, not the rate environment. 

**Potential mitigation:** A robustness check using **two-way FE with sector-time interactions** could partially address this by allowing for time-varying sector effects. Alternatively, a **dynamic panel model** (Arellano-Bond GMM) could account for restructuring, but this requires a longer time series. 

## 4. Self-Reflection 

**What I did well:** I excelled at translating technical regression output into business language for the final memo. My Executive Summary clearly communicated complex findings without sacrificing accuracy. 

7 / 8 

**What I could improve:** I could have started M3 econometric modeling earlier (I waited until 1 week before deadline). More time would have allowed deeper robustness checks and alternative specifications. 

**What I learned:** This capstone taught me that **data cleaning is 80% of the work** in real-world analysis. I also learned to defend methodological choices with evidence and to communicate limitations honestly. Most importantly, I developed confidence in executing an end-to-end workflow from raw data to professional deliverable. 

**Attestation:** I affirm all contributions listed are accurate and honest. 

**Signature:** Sarah Johnson **Date:** May 1, 2026 

_This example demonstrates:_ 

- **Specificity:** Not "helped with M1" but "Implemented FRED API integration (20 hours)" **Evidence-based reasoning:** Defended decision cites M2 lag analysis + M3 robustness checks **Substantive limitation:** Acknowledges time-varying portfolio restructuring as a real concern **Honest reflection:** Admits procrastination on M3, but also highlights strengths (memo writing) 

8 / 8 

