#!/usr/bin/env python3
"""
QM 2023 Capstone: Milestone 3 Econometric Models
Team: Stat 2
Members: Luke Birdseye, Ben Brown, Katie Koonts, James Gawey, Dani Gamboa
Date: 04/24/2026

Current scope in this file:
- Model B (Option 3): Machine Learning comparison (Random Forest vs. OLS)

This script loads the final crypto panel, builds a common feature matrix,
fits OLS and Random Forest models on a chronological train/test split,
and saves comparison outputs to results/tables/ and results/figures/.
"""

from __future__ import annotations

from pathlib import Path
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score


# Section 1: Imports and data loading
ROOT = Path(__file__).resolve().parent
CODE_DIR = ROOT / "code"
if str(CODE_DIR) not in sys.path:
    sys.path.insert(0, str(CODE_DIR))

from config_paths import FINAL_DATA_DIR, FIGURES_DIR, TABLES_DIR  # noqa: E402

PANEL_FILE = FINAL_DATA_DIR / "crypto_analysis_panel.csv"


# Section 2: Feature engineering
TARGET = "outcome_realized_vol_30d"
FEATURES = [
    "driver_sec_event_indicator",
    "control_market_cap",
    "control_total_volume",
    "control_btc_corr_30d",
    "vix",
    "ffeffective_rate",
    "epu_index",
    "token_group",
]


def load_and_prepare_data() -> tuple[pd.DataFrame, pd.Series, pd.Series]:
    df = pd.read_csv(PANEL_FILE)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    numeric_cols = [
        TARGET,
        "driver_sec_event_indicator",
        "control_market_cap",
        "control_total_volume",
        "control_btc_corr_30d",
        "vix",
        "ffeffective_rate",
        "epu_index",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    required = ["date", TARGET, *FEATURES]
    model_df = df[required].dropna().sort_values("date").reset_index(drop=True)

    X = pd.get_dummies(model_df[FEATURES], columns=["token_group"], drop_first=True).astype(float)
    y = model_df[TARGET]
    dates = model_df["date"]
    return X, y, dates


# Section 4: Model B - ML comparison (Random Forest vs. OLS)
def fit_model_b_option3(X: pd.DataFrame, y: pd.Series, dates: pd.Series) -> None:
    split_idx = int(len(X) * 0.8)
    if split_idx <= 0 or split_idx >= len(X):
        raise ValueError("Insufficient rows after cleaning for train/test split.")

    X_train = X.iloc[:split_idx].copy()
    X_test = X.iloc[split_idx:].copy()
    y_train = y.iloc[:split_idx].copy()
    y_test = y.iloc[split_idx:].copy()
    test_dates = dates.iloc[split_idx:].copy()

    X_train_ols = sm.add_constant(X_train, has_constant="add")
    X_test_ols = sm.add_constant(X_test, has_constant="add")
    ols_model = sm.OLS(y_train, X_train_ols).fit()
    ols_pred = ols_model.predict(X_test_ols)

    rf_model = RandomForestRegressor(
        n_estimators=500,
        max_depth=12,
        min_samples_leaf=5,
        random_state=42,
        n_jobs=-1,
    )
    rf_model.fit(X_train, y_train)
    rf_pred = rf_model.predict(X_test)

    metrics = pd.DataFrame(
        {
            "model": ["OLS", "RandomForest"],
            "r2_test": [r2_score(y_test, ols_pred), r2_score(y_test, rf_pred)],
            "rmse_test": [
                np.sqrt(mean_squared_error(y_test, ols_pred)),
                np.sqrt(mean_squared_error(y_test, rf_pred)),
            ],
            "train_rows": [len(X_train), len(X_train)],
            "test_rows": [len(X_test), len(X_test)],
        }
    )

    feature_importance = pd.DataFrame(
        {
            "feature": X.columns,
            "rf_importance": rf_model.feature_importances_,
        }
    ).sort_values("rf_importance", ascending=False)

    ols_coefficients = (
        ols_model.params.rename("coefficient")
        .to_frame()
        .join(ols_model.pvalues.rename("p_value"))
        .reset_index()
        .rename(columns={"index": "term"})
    )

    comparison_series = pd.DataFrame(
        {
            "date": test_dates,
            "actual": y_test.values,
            "ols_pred": ols_pred.values,
            "rf_pred": rf_pred,
        }
    )

    metrics.to_csv(TABLES_DIR / "M3_modelB_option3_metrics.csv", index=False)
    feature_importance.to_csv(TABLES_DIR / "M3_modelB_option3_feature_importance.csv", index=False)
    ols_coefficients.to_csv(TABLES_DIR / "M3_modelB_option3_ols_coefficients.csv", index=False)
    comparison_series.to_csv(TABLES_DIR / "M3_modelB_option3_predictions.csv", index=False)

    plt.figure(figsize=(10, 6))
    plt.scatter(y_test, ols_pred, alpha=0.25, label="OLS", s=20)
    plt.scatter(y_test, rf_pred, alpha=0.25, label="Random Forest", s=20)
    y_min = min(y_test.min(), ols_pred.min(), rf_pred.min())
    y_max = max(y_test.max(), ols_pred.max(), rf_pred.max())
    plt.plot([y_min, y_max], [y_min, y_max], linestyle="--", linewidth=1)
    plt.xlabel("Actual realized volatility")
    plt.ylabel("Predicted realized volatility")
    plt.title("Model B Option 3: OLS vs Random Forest (Test Set)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "M3_modelB_option3_actual_vs_predicted.png", dpi=300)
    plt.close()

    print("Saved Model B Option 3 outputs:")
    print(f"- {TABLES_DIR / 'M3_modelB_option3_metrics.csv'}")
    print(f"- {TABLES_DIR / 'M3_modelB_option3_feature_importance.csv'}")
    print(f"- {TABLES_DIR / 'M3_modelB_option3_ols_coefficients.csv'}")
    print(f"- {TABLES_DIR / 'M3_modelB_option3_predictions.csv'}")
    print(f"- {FIGURES_DIR / 'M3_modelB_option3_actual_vs_predicted.png'}")


# Section 7: Save regression tables and diagnostic plots

def main() -> None:
    X, y, dates = load_and_prepare_data()
    fit_model_b_option3(X, y, dates)


if __name__ == "__main__":
    main()
