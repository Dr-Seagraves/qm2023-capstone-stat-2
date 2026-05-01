## QM 2023 Capstone Project - Milestone 4
## Individual Addendum

Name: Dani Gamboa  
Team: Stat 2  
Date: 05/01/2026

### 1. Personal Contribution
- **M1:**
Gathered cryptocurrency data manually coin-by-coin, uploaded and organized the files in GitHub and Codespace (2 hours), created the M1 scripts with GitHub Copilot support to clean, fetch, and merge raw data (10 minutes), added variable definitions to the data dictionary + Added CoinGecko Ranking Panel information to the data quality report (2 hours).

- **M2:**
Created the Jupyter notebook code with AI support to build the M2 EDA workflow and visualization pipeline + ran it end-to-end to generate all visualizations (40 minutes), wrote the M2 EDA summary, turning plots and diagnostics into conclusions (2.5 hours).

- **M3:**
Evaluated Model B options and decided to create the ML-comparison code with AI support, including the setup for OLS v. Random Forest evaluation + Ran it to verify the outputs (1 hour).

- **M4:**
Wrote the Executive Summary and the Conclusions and Recommendations points while reviewing all the final investment memo's information aligns with the group's conclusions (2 hours).


Total hours: 10 hours, 20 minutes  |  Estimated team workload share: 32.5%  
Role(s): Data pipeline & cleaning, EDA & visualization, modeling (ML comparison), and report/memo writing.

### 2. One Defended Methodological Decision
Decision: For the modeling part, where we had to choose between DiD, ARIMA, or ML Comparison, I told the group why I thought ML Comparison would be the best option given our dataset.  

Reasoning: We could not do DiD since we did not have a control v. treatment group (all tokens are equally affected by events); we could not do ARIMA because we would ignore most of the cross-sectional variables; and ML Comparison allowed us to better analyze our multi-token panel token groups, macro controls, and predicted nonlinear relationships between events and volatility.

### 3. One Key Limitation
Limitation: the 2020–February 2026 period may not consider regulatory regimes post Iran war, public debt skyrocketing, FED-chair uncertainty.

Why it matters + mitigation: since crypto behavior constantly changes under new macro regimes, the dataset should always be updated so it considers all new and future events. To mitigate this, we should extend the dataset over time and try to make it be updated automatically, including variables to capture all structural changes.

### 4. AI Audit Notes
- Tool(s): GitHub Copilot

**M1**
- Prompt: "help me merge all files (top 50 coins) into one raw dataset, and save one merged CSV. Also clean the merged CoinGecko panel by dropping missing core market fields, converting date types, keeping only the last 6 years, and saving to processed data".
- Output: The AI built a unified panel and cleaned the CSV file.
- Verification: I verified the output file merged all necessary rows into a single table. I also compared raw v. processed rows and confirmed the cleaning process was successful.
- Critique: None.

**M2**
- Prompt: Create the Jupyter notebook code to build the M2 EDA workflow and visualization pipeline + ran it end-to-end to generate all visualizations (long specifications were included in the prompt).
- Output: The AI created the code + run it + created all the required visualizations, which still had to be cleaned.
- Verification: I verified the code run correctly end-to-end & that visualizations were what it was required by our prompt.
- Critique: Some visualizations' labels were too broad / over-complicated, so I changed them to make them understadable.

**M3**
- Prompt: "for model B, create the code for option 3" (ML Comparison).
- Output: The AI created the code for Model B Option 3 (ML Comparison).
- Verification: I confirmed the script location, name, and labels match M3 deliverable expectations in the README file. I also confirmed the code is the one needed if we follow Option 3 (ML Comparison) in Model B.
- Critique: None.

- Estimated AI-assisted share of your work: 15%

### 5. Self-Reflection
What I did well: I led and organized the team.  

What I could improve: Communicate more efficiently with my teammates.  

What I learned: I learned how to apply statistics concepts like correlations, robustness, heteroskedasticity... to such an interesting world like cryptocurrencies. I also learned how to use be infinitely more efficient while make my coding tasks by using AI. 

### 6. Attestation
I affirm that this contribution summary is accurate and honest.

Name: Dani Gamboa  
Team: Stat 2  
Date: 05/01/2026