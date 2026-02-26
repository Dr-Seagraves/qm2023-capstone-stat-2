[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/gp9US0IQ)
[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-2e0aaae1b6195c2367325f4f02e2d04e9abb55f0b24a779b69b11b9e10269abc.svg)](https://classroom.github.com/online_ide?assignment_repo_id=22639577&assignment_repo_type=AssignmentRepo)
# QM 2023 Capstone Project: Stat 2

## Team Members
- Luke Birdseye
- Ben Brown
- Katie Koonts
- James Gawey
- Dani Gamboa

## Research Question
How have economic policy uncertainty and regulatory shocks (for example, SEC actions and interest-rate changes) affected cryptocurrency return volatility from 2020–2026?

## Dataset Overview
- **Primary Dataset:** CoinGecko daily market panel (top 50 cryptocurrencies)
  - Entities: 50 tokens | Time: daily | Period: 2020-02-19 to 2026-02-18 (cleaned sample)
- **Supplementary Data:**
  - SEC press/litigation crypto event indicators
  - FRED VIX (`VIXCLS`)
  - FRED Effective Federal Funds Rate (`DFF`)
  - FRED Economic Policy Uncertainty Index (`USEPUINDXD`)

## Hypotheses (Preliminary)
1. Higher policy uncertainty is associated with higher crypto realized volatility.
2. SEC event days are associated with changes in crypto volatility relative to non-event days.
3. Macro stress indicators (VIX, Fed Funds changes) explain part of cross-time volatility variation.

## Repository Structure
```text
QM-2023-Capstone-Repo/
├── code/
│   ├── config_paths.py
│   ├── fetch_coingecko_data.py
│   ├── fetch_sec_data.py
│   ├── fetch_macro_data.py
│   ├── fetch_crypto_event_data.py
│   └── merge_final_panel.py
├── data/
│   ├── raw/
│   ├── processed/
│   └── final/
│       ├── crypto_analysis_panel.csv
│       └── data_dictionary.md
├── results/
│   ├── figures/
│   ├── reports/
│   └── tables/
├── tests/
│   └── .gitkeep
├── README.md
├── M1_data_quality_report.md
└── AI_AUDIT_APPENDIX.md
```

## How to Run
1. Clone repository and open in GitHub Codespaces.
2. Run fetch scripts:
	- `python code/fetch_coingecko_data.py`
	- `python code/fetch_sec_data.py`
	- `python code/fetch_macro_data.py`
	- `python code/fetch_crypto_event_data.py`
3. Run merge script:
	- `python code/merge_final_panel.py`
4. Check final output:
	- `data/final/crypto_analysis_panel.csv`

Run `python code/run_all.py` to execute everything end-to-end.