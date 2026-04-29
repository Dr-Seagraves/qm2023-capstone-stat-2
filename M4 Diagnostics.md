# Model B Diagnostics

## 2.2 Model B Results

**Table 2: OLS vs. Random Forest Test-Set Performance**

| Model | Test R2 | Test RMSE | Train Rows | Test Rows |
|---|---:|---:|---:|---:|
| OLS | 0.1633 | 0.0364 | 15,641 | 3,911 |
| Random Forest | 0.1095 | 0.0375 | 15,641 | 3,911 |

OLS is the stronger Model B benchmark. It explains more variation in realized volatility and has lower out-of-sample error than random forest, so it is the better alternative model for this assignment.

**Table 3: Key Predictive Signals**

| Variable | OLS Coefficient | p-value | Random Forest Importance | Interpretation |
|---|---:|---:|---:|---|
| SEC event indicator | 0.00033 | 0.883 | 0.00002 | The raw event flag adds little standalone predictive value once controls are included. |
| BTC correlation | -0.02063 | < 0.001 | 0.16333 | Market structure remains an important driver of volatility. |
| Federal Funds Rate | -0.00385 | < 0.001 | 0.18361 | Macro rate conditions contribute meaningfully to the volatility forecast. |
| EPU index | -0.00000935 | < 0.001 | 0.02941 | Policy uncertainty adds smaller but measurable signal. |
| DeFi group | 0.00806 | < 0.001 | 0.00959 | DeFi differs from the baseline group, but it is not the dominant predictive feature. |
| Stablecoin group | -0.05044 | < 0.001 | 0.34510 | Stablecoin membership is the clearest structural predictor in the forest model. |

The main interpretation is that volatility is explained more by token structure and market regime than by the SEC event flag alone. That is consistent with Model A, where the event effect is identified through group interactions rather than a raw event-level term.

## 2.3 Diagnostic Figure Discussion

The actual-versus-predicted figure shows three clear patterns. Both models fit low-to-moderate volatility reasonably well, but both compress the upper tail and underpredict the largest spikes. Random forest comes closer on one of the biggest spike days, but it does not beat OLS on overall test performance.

The practical takeaway is that Model B is useful as a sanity check for average days, but it is less reliable in stressed periods. The forecast should not be treated as a stand-alone risk ceiling during event windows.

## 2.4 Cross-Check Against Model A

Model B is consistent with Model A on the main substantive point: stablecoins are the clearest high-sensitivity group. In Model A, that appears through the stablecoin event interaction. In Model B, it appears through the stablecoin feature’s importance in random forest and its strong significance in OLS.

The two models also agree that the raw SEC event indicator alone is not the full story. The event matters when it is combined with token structure and market conditions. That is the key consistency check between the two specifications.

## 3.3 Risk Assessment

The main Model B risks tied to the results are:

1. Tail underfit. Both models miss the largest volatility spikes, so they are weaker in crisis-like periods.
2. Prediction versus causation. Model B is a forecasting benchmark, not the identification strategy for the main claim.
3. Signal dilution. The raw SEC event indicator is too broad to explain volatility without token-group structure.
4. Stability risk. Stablecoin behavior dominates the forest model, so any regime change in stablecoin markets could shift the results materially.

## 3.4 Caveats and Limitations

The key caveat is that Model B confirms the direction of the main story, but it does not strengthen the causal claim on its own. The actual-versus-predicted plot shows the model is calibrated for routine periods, not for extreme stress. So the safest interpretation is that Model A provides the main effect estimate, while Model B supports the story through out-of-sample prediction and feature ranking.

Another limitation is that the SEC event flag alone is weak in the predictive setting. That is not a contradiction; it means the event signal is distributed through token type and market regime rather than appearing as a standalone effect. Any recommendation should therefore remain group-specific and conditional on the current market structure.
