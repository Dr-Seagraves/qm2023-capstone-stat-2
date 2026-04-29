## Milestone 4: Final Investment Memo 

## **QM 2023 Capstone Project** 

**Due:** Friday, Week 14 (May 1, 2026) by 11:59 PM **Points:** 50 (25% of capstone grade) **Format:** Hybrid - Team memo (40 pts) + Individual addendum (10 pts) 

## Overview 

Milestone 4 is the **culmination** of your semester-long capstone project. You will synthesize your data pipeline (M1), exploratory analysis (M2), and econometric models (M3) into a **professional investment memo** that communicates findings to a non-technical audience. 

**Real-world context:** Investment committees don't read code or regression output. They read concise memos that translate technical analysis into actionable recommendations. Your memo must: 

- State findings clearly (Executive Summary) 

- Explain methods transparently (Methodology) 

- Present evidence visually and numerically (Results) 

- Provide investment guidance with honest caveats (Conclusions) 

**Success criterion:** A portfolio manager with no econometrics background should be able to read your memo and make an informed investment decision. 

## Learning Objectives 

By completing M4, you will: 

1. **Translate technical results** into business language 

2. **Communicate visually** with publication-ready tables and figures 

3. **Provide actionable recommendations** grounded in empirical evidence 

4. **Acknowledge limitations** honestly (omitted variables, assumptions, external validity) 

5. **Demonstrate individual accountability** through personal contribution reflection 

6. **Practice professional writing** (clear, concise, jargon-free) 

## Deliverables 

**Note for Alternative Dataset Teams** : Adapt the "Investment Committee" framing to your research context. For example: 

- **Crypto teams →** "Risk Committee" (e.g., "We recommend reducing DeFi exposure during regulatory uncertainty periods") 

- **Housing teams →** "Policy Advisory" (e.g., "Markets with X characteristics are most vulnerable to rate increases") 

**Macro/economic teams →** "Economic Outlook" (e.g., "Our model forecasts Y under scenario Z") 

1 / 7 

**Open Data teams →** Frame your recommendations around whoever would act on your findings (policymakers, managers, investors, etc.) 

The core requirements (Executive Summary, Methodology, Results, Recommendations) remain the same regardless of your topic. Where the template below says "Investment Recommendation," replace with the appropriate action for your audience. 

## 1. Team Memo: `Final_Investment_Memo.pdf` (40 points) 

**Format:** 5-7 pages, professional PDF (not Word doc) **Structure:** See template in `memo_template.md` 

## **Required Sections:** 

## 1. **Executive Summary** (0.5 page) 

- 2-3 sentences stating the key finding 

- 1-2 sentences with investment recommendation 

- No tables or figures (pure text) 

## 2. **Methodology** (1 page) 

- Data sources (REIT Master, FRED, REIT Factors) with citations 

- Sample construction: n_reits, n_months, date range 

- Model specifications (equations for FE and DiD/ARIMA/ML) Variable definitions (what is FEDFUNDS_lag2? What is MOM?) 

## 3. **Results** (1.5-2 pages) 

- **Table 1:** Fixed Effects regression (main model) 

- **Table 2:** Alternative specification (DiD/ARIMA/ML) 

- **Figure 1:** Key visualization (e.g., sector returns over time, rate vs. return dual-axis plot) **Figure 2:** Diagnostic plot (residuals vs. fitted) 

- Interpretation prose: "The coefficient on FEDFUNDS (-0.025) implies that a 1 pp increase in the Federal Funds Rate reduces REIT returns by 2.5 pp, controlling for REIT and time fixed effects." 

## 4. **Conclusions & Recommendations** (1 page) 

- Investment implications: Which sectors to buy/hold/sell? 

- Risk assessment: What could go wrong with this analysis? Caveats: Assumptions, omitted variables, external validity concerns 

## 5. **References** (0.5 page) 

Data sources with URLs 

- Any academic papers cited (Lit-Anchor from Orbis) Format: APA or similar 

## 6. **Appendix: AI Audit** (0.5-1 page) 

Summary of AI use across all milestones 

Key verification and critique examples 

2 / 7 

## 2. Individual Addendum: `Individual_Addendum_[YourName].pdf` (10 points) 

**Format:** 1 page PDF per student **Content:** 

## 1. **Personal Contribution** (2-4 bullets) 

- What did you contribute to M1, M2, M3, M4? 

- Be specific: "Led M1 data cleaning, handled missing values and merge logic (15 hours)" not "Helped with data" 

## 2. **One Defended Decision** (2-4 sentences) 

- Choose one methodological decision you made (or advocated for) and defend it Example: "I recommended using a 2-month lag for FEDFUNDS based on M2 lag analysis showing strongest correlation at lag 2 (r = -0.38). This choice is grounded in economic reasoning: REITs negotiate leases and financing over 1-2 months, so immediate rate effects are muted." 

## 3. **One Key Limitation** (2-4 sentences) 

- What is the most important limitation or caveat of your analysis? 

- Example: "Our Fixed Effects model assumes parallel trends across sectors, but M2 EDA showed slight pre-shock divergence between Retail and Industrial REITs. If this trend continued absent the rate hike, our DiD estimate may overstate the causal effect." 

## 4. **AI Audit Notes** (if applicable) 

Any AI use specific to your work not covered in team appendix 

## Team Memo Template 

See `memo_template.md` for full template. Key guidance: 

## Executive Summary 

2-3 sentences: key finding (magnitude, significance, lag). 1-2 sentences: specific recommendation (e.g., "Overweight Industrial by 15%; underweight Retail/Office"). Write for non-economists—no jargon. 

## Methodology 

List data sources with citations. Sample construction (n, date range, observations after cleaning). Model equations with variable definitions. Standard errors clustered at entity level. 

## Results 

**Table 1:** Main FE regression (coefficients, SEs, t-stats, p-values; FE and N; significance stars). **Table 2:** Alternative spec (DiD/ARIMA/ML). **Figure 1:** Key viz (e.g., dual-axis outcome vs. driver). **Figure 2:** Diagnostic plot. Reference figures with `../figures/filename.png` when memo lives in `results/reports/` . Interpretation: translate coefficient to economic magnitude; note robustness and theory alignment. 

## Conclusions & Recommendations 

3 / 7 

**Investment implications:** Sector allocation (who to overweight/underweight), factor tilts, scenario analysis. **Risk assessment:** Model assumptions, omitted variables, external validity. **Caveats:** FE/DiD assumptions, data limitations. Be specific and honest. 

## Individual Addendum Template 

See `individual_addendum_template.md` for full template. 

**Personal Contribution:** 2-4 bullets, tasks + hours per milestone (e.g., "Led M1 data cleaning, FRED merge (20 hrs)"). 

**Defended Decision:** One methodological choice with evidence (e.g., lag choice based on M2 EDA + economic reasoning). 

**Key Limitation:** Substantive caveat with why it matters. 

**AI Audit Notes:** Any AI use not in team appendix; document prompt, output, verification. 

## Grading Rubric (50 points) 

See `rubric.md` for detailed breakdown. Summary: 

|**Component**|**Points**|**Criteria**|
|---|---|---|
|**Team Memo:**|10|M1–M3 repo runs end-to-end; memo tables/figures match code;|
|**Reproducibility & Rigor**||models and diagnostics sound (see note below)|
|**Team Memo: Structure &**|||
|**Clarity**|10|Sections organized; jargon-free; professional|
|**Team Memo: Results &**|||
|**Interpretation**|12|Tables/figures publication-ready; economic interpretation clear|
|**Team Memo:**|||
|**Recommendations &**|8|Actionable advice; honest limitations|
|**Caveats**|||
|**Individual Addendum**|10|Specific contribution; defended decision; key limitation; honesty|



**Total:** 50 points 

**Reproducibility note:** M4 is submitted as PDFs, but grading still expects your **existing** capstone code (M1 scripts + merge, M2 notebook, M3 `capstone_models.py` or equivalent) to run and to **match** the numbers and figures in the memo. You are not graded on new code written only for M4; you are graded on whether the documented analysis is reproducible from the team repository. Details: `rubric.md` (Reproducibility & Technical Rigor). 

## Common Pitfalls and How to Avoid Them 

Pitfall 1: Executive Summary is Too Technical 

4 / 7 

**Problem:** "We estimated a two-way fixed effects panel regression with clustered standard errors..." **Solution:** Write for a business audience. "We analyzed 500+ REITs and found that rising interest rates reduce REIT returns." 

## Pitfall 2: No Investment Recommendations 

**Problem:** Memo ends with "Results show negative correlation" (so what?) **Solution:** "Based on these findings, we recommend overweighting Industrial REITs and underweighting Retail REITs." 

## Pitfall 3: Ignoring Limitations 

**Problem:** Memo claims results are definitive with no caveats **Solution:** "Our analysis assumes parallel trends, which may be violated if pre-shock trends differ. A placebo test partially addresses this concern." 

## Pitfall 4: Vague Individual Addendum 

**Problem:** "I helped with coding and analysis" (no specifics) **Solution:** "I implemented the M3 Fixed Effects model, ran VIF diagnostics, and drafted the robustness checks section (18 hours)." 

## Pitfall 5: Tables are Unreadable 

**Problem:** Regression output copy-pasted from Python console (raw text, no formatting) **Solution:** Use markdown tables or export to CSV → format in Excel → save as PDF 

## Testing Checklist (Run Before Submission) 

## Team Memo Checklist 

**==> picture [10 x 10] intentionally omitted <==**

**==> picture [10 x 10] intentionally omitted <==**

**==> picture [10 x 10] intentionally omitted <==**

**==> picture [10 x 10] intentionally omitted <==**

**==> picture [10 x 10] intentionally omitted <==**

**==> picture [10 x 10] intentionally omitted <==**

**==> picture [10 x 10] intentionally omitted <==**

- All sections present (Executive Summary, Methodology, Results, Conclusions, References, AI Audit) Length: 5-7 pages (not 2 pages, not 15 pages) 

- Tables are formatted (no raw Python output) 

- Figures are high-resolution (300 DPI) 

- No jargon or unexplained acronyms 

- Investment recommendations are specific (not "maybe buy REITs") 

- Limitations and caveats are honest 

## Individual Addendum Checklist 

**==> picture [10 x 10] intentionally omitted <==**

**==> picture [10 x 9] intentionally omitted <==**

**==> picture [10 x 10] intentionally omitted <==**

**==> picture [10 x 9] intentionally omitted <==**

Personal contribution is specific (tasks + hours) 

- Defended decision explains reasoning with evidence 

- Key limitation is substantive (not trivial) 

- Length: exactly 1 page (not 0.5, not 2) 

## Final Quality Check 

**==> picture [10 x 10] intentionally omitted <==**

**==> picture [10 x 10] intentionally omitted <==**

Team member names on both Team Memo and Individual Addendum 

   - Submitted as PDF (not Word doc or Google Docs link) 

- Team memo includes the AI Audit appendix (required; omission is treated as incomplete capstone 

- documentation—see syllabus and `rubric.md` ) 

5 / 7 

## Submission Instructions 

## Shared Team Repository Submission 

1. **Work in your team's shared private GitHub repository** and submit by committing/pushing to `main` . 

2. **Add your files:** `Final_Investment_Memo.pdf` , `Individual_Addendum_[YourName].pdf` (each team member). Commit and push to `main` . 

3. **Verify:** Check GitHub repo; required PDFs should be visible, and your latest commit should be on `main` . 

**Deadline:** Friday, Week 14 (May 1) by 11:59 PM (aligned with `Capstone-Project/README.md` and 

`Syllabus-Schedule-Docs/Schedule/QM2023-Spring2026-Week-Date-Map.md` ) 

**Converting to PDF:** Your memo must be submitted as a PDF (not Word or Google Docs). Easy options: 

- **VS Code:** Install the "Markdown PDF" extension → right-click your .md file → "Markdown PDF: Export (pdf)" 

- **Google Docs:** Write in Google Docs → File → Download → PDF 

- **Word:** Write in Word → File → Save As → PDF 

- **Browser:** Open your rendered markdown → Print → "Save as PDF" 

- **Command line:** `pandoc Final_Investment_Memo.md -o Final_Investment_Memo.pdf` (if pandoc is installed) 

## Resources and Support 

## Office Hours 

- **Dr. Seagraves:** Monday & Wednesday, 3:00-5:00 PM 

- **Focus for M4:** Memo writing, investment recommendations, limitation discussions 

## Templates 

- **Team Memo:** `memo_template.md` (full structure) 

**Individual Addendum:** `individual_addendum_template.md` 

## Writing Guides 

- Strunk & White, _The Elements of Style_ (concise writing) 

- Williams & Colomb, _Style: Lessons in Clarity and Grace_ (professional prose) 

## Presentation Integration (Weeks 14–15) 

**Calendar alignment:** Final presentations run **Week 14–15** (see `QM2023-Spring2026-Week-Date-Map.md` ). **Milestone 4 is due Friday, May 1, 2026, 11:59 PM** —the same calendar date as **Final Presentations Day 1** (in class). **Presentations Day 2** is **Monday, May 4, 2026** for teams scheduled that day; the memo deadline is still **May 1** unless the instructor announces otherwise. 

**Coordination strategy:** 

6 / 7 

- Use **Week 14** hackathon meetings (Mon/Wed) to draft the memo and rehearse slides. 

- Finish a **submission-ready PDF** before **11:59 PM Friday, May 1** ; do not rely on post-presentation edits for the graded artifact. 

If you present on **May 1** , finalize talking points early that week so memo and deck stay consistent. 

**Tip:** Draft your Executive Summary and Conclusions first—they map directly to your presentation script. 

## **Good luck! Your capstone memo is the portfolio piece that demonstrates your analytical rigor and professional communication skills.** 

_Document prepared for QM 2023: Statistics II, Spring 2026, University of Tulsa Contact: Dr. Cayman Seagraves (cayman-seagraves@utulsa.edu)_ 

7 / 7 

