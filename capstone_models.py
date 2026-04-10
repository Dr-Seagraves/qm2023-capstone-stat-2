#!/usr/bin/env python3
"""
QM 2023 Capstone: Milestone 3 Econometric Models
Team: Stat 2
Members: Luke Birdseye, Ben Brown, Katie Koonts, James Gawey, Dani Gamboa
Date: 04/24/2026

Current scope in this file:
- Model A: Fixed Effects panel regression (required)

This script loads the final crypto panel, estimates Model A with entity and
time fixed effects, runs required diagnostics, and saves robustness checks and
publication-ready tables/figures for Milestone 3.
"""

from __future__ import annotations

from pathlib import Path
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from linearmodels.panel import PanelOLS
from scipy import stats
import statsmodels.api as sm
from statsmodels.stats.diagnostic import het_breuschpagan
from statsmodels.stats.outliers_influence import variance_inflation_factor


# Section 1: Imports and data loading
ROOT = Path(__file__).resolve().parent
CODE_DIR = ROOT / "code"
if str(CODE_DIR) not in sys.path:
    sys.path.insert(0, str(CODE_DIR))

from config_paths import FINAL_DATA_DIR, FIGURES_DIR, TABLES_DIR  # noqa: E402

PANEL_FILE = FINAL_DATA_DIR / "crypto_analysis_panel.csv"


# Section 2: Feature engineering
TARGET = "outcome_realized_vol_30d"
DRIVER_RAW = "driver_sec_event_indicator"
ENTITY_COL = "coin_symbol"
TIME_COL = "date"
GROUP_COL = "token_group"

CONTROLS = [
    "log_market_cap",
    "log_total_volume",
    "control_btc_corr_30d",
]

NUMERIC_INPUTS = [
    TARGET,
    DRIVER_RAW,
    "driver_sec_event_indicator",
    "control_market_cap",
    "control_total_volume",
    "control_btc_corr_30d",
    "vix",
    "ffeffective_rate",
]


def load_and_prepare_data() -> pd.DataFrame:
    df = pd.read_csv(PANEL_FILE)
    df[TIME_COL] = pd.to_datetime(df[TIME_COL], errors="coerce")
    for col in NUMERIC_INPUTS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values([ENTITY_COL, TIME_COL]).reset_index(drop=True)
    df["log_market_cap"] = np.log1p(df["control_market_cap"])
    df["log_total_volume"] = np.log1p(df["control_total_volume"])

    for lag in [0, 1, 2, 3]:
        lag_col = f"driver_sec_event_indicator_lag{lag}"
        if lag == 0:
            df[lag_col] = df[DRIVER_RAW]
        else:
            df[lag_col] = df.groupby(ENTITY_COL, observed=True)[DRIVER_RAW].shift(lag)

    return df


def build_panel_data(
    df: pd.DataFrame, driver_col: str
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame, list[str]]:
    needed = [ENTITY_COL, TIME_COL, GROUP_COL, TARGET, driver_col, *CONTROLS]
    model_df = df[needed].dropna().copy()
    model_df = model_df.sort_values([ENTITY_COL, TIME_COL]).reset_index(drop=True)

    group_dummies = pd.get_dummies(model_df[GROUP_COL], prefix="grp", drop_first=True, dtype=float)
    interaction_cols = []
    for dummy_col in group_dummies.columns:
        interaction_col = f"{driver_col}_x_{dummy_col}"
        model_df[interaction_col] = model_df[driver_col] * group_dummies[dummy_col]
        interaction_cols.append(interaction_col)

    panel = model_df.set_index([ENTITY_COL, TIME_COL]).sort_index()
    y = panel[TARGET]
    X = panel[[*interaction_cols, *CONTROLS]]
    return model_df, y, X, interaction_cols


def fit_fe_model(y: pd.Series, X: pd.DataFrame, clustered: bool, time_effects: bool = True) -> PanelOLS:
    model = PanelOLS(y, X, entity_effects=True, time_effects=time_effects, drop_absorbed=True)
    if clustered:
        return model.fit(cov_type="clustered", cluster_entity=True)
    return model.fit(cov_type="unadjusted")


def stars(p_value: float) -> str:
    if p_value < 0.01:
        return "***"
    if p_value < 0.05:
        return "**"
    if p_value < 0.10:
        return "*"
    return ""


def result_to_long_table(result, model_name: str) -> pd.DataFrame:
    rows = []
    for term in result.params.index:
        coef = float(result.params[term])
        se = float(result.std_errors[term])
        pval = float(result.pvalues[term])
        rows.append(
            {
                "model": model_name,
                "term": term,
                "coefficient": coef,
                "std_error": se,
                "t_stat": float(result.tstats[term]),
                "p_value": pval,
                "coef_with_stars": f"{coef:.6f}{stars(pval)}",
                "nobs": int(result.nobs),
                "r2_within": float(result.rsquared_within),
            }
        )
    return pd.DataFrame(rows)


def result_to_publishable_column(result, model_name: str) -> pd.DataFrame:
    terms = list(result.params.index)
    data = []
    for term in terms:
        coef = float(result.params[term])
        se = float(result.std_errors[term])
        pval = float(result.pvalues[term])
        data.append({"term": term, model_name: f"{coef:.6f}{stars(pval)} ({se:.6f})"})
    return pd.DataFrame(data)


def run_model_a() -> None:
    df = load_and_prepare_data()

    baseline_driver = "driver_sec_event_indicator_lag0"
    baseline_df, y, X, interaction_terms = build_panel_data(df, baseline_driver)
    key_driver_term = interaction_terms[0]

    model_standard = fit_fe_model(y, X, clustered=False)
    model_clustered = fit_fe_model(y, X, clustered=True)

    all_coef_tables = [
        result_to_long_table(model_standard, "FE_standard_SE"),
        result_to_long_table(model_clustered, "FE_clustered_SE"),
    ]

    regression_table = result_to_publishable_column(model_standard, "Model_1_FE_Standard")
    regression_table = regression_table.merge(
        result_to_publishable_column(model_clustered, "Model_2_FE_Clustered"),
        on="term",
        how="outer",
    )

    # Section 5: Diagnostics (heteroskedasticity, VIF, residual plots)
    residuals = model_clustered.resids
    fitted_df = model_clustered.fitted_values
    fitted = fitted_df.iloc[:, 0] if isinstance(fitted_df, pd.DataFrame) else fitted_df
    aligned = pd.concat([residuals.rename("residual"), fitted.rename("fitted")], axis=1).dropna()

    X_diag = X.loc[aligned.index]
    X_diag_bp = sm.add_constant(X_diag, has_constant="add")
    bp_lm, bp_lm_pvalue, bp_f, bp_f_pvalue = het_breuschpagan(aligned["residual"].values, X_diag_bp.values)
    bp_table = pd.DataFrame(
        {
            "metric": ["lm_stat", "lm_pvalue", "f_stat", "f_pvalue"],
            "value": [bp_lm, bp_lm_pvalue, bp_f, bp_f_pvalue],
        }
    )

    vif_table = pd.DataFrame({"variable": X_diag.columns})
    vif_table["vif"] = [variance_inflation_factor(X_diag.values, i) for i in range(X_diag.shape[1])]

    plt.figure(figsize=(10, 6))
    plt.scatter(aligned["fitted"], aligned["residual"], alpha=0.25, s=16)
    plt.axhline(0, color="red", linestyle="--", linewidth=1)
    plt.xlabel("Fitted values")
    plt.ylabel("Residuals")
    plt.title("Model A Residuals vs Fitted")
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "M3_residuals_vs_fitted.png", dpi=300)
    plt.close()

    plt.figure(figsize=(8, 8))
    stats.probplot(aligned["residual"], dist="norm", plot=plt)
    plt.title("Model A Q-Q Plot")
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "M3_qq_plot.png", dpi=300)
    plt.close()

    # Section 6: Robustness checks
    # 1) Alternative lag structures
    lag_rows = []
    for lag in [0, 1, 2, 3]:
        lag_col = f"driver_sec_event_indicator_lag{lag}"
        _, y_lag, X_lag, lag_interaction_terms = build_panel_data(df, lag_col)
        lag_model = fit_fe_model(y_lag, X_lag, clustered=True)
        lag_key_term = lag_interaction_terms[0]
        lag_rows.append(
            {
                "lag": lag,
                "driver_variable": lag_key_term,
                "coef": float(lag_model.params[lag_key_term]),
                "std_error": float(lag_model.std_errors[lag_key_term]),
                "p_value": float(lag_model.pvalues[lag_key_term]),
                "nobs": int(lag_model.nobs),
                "r2_within": float(lag_model.rsquared_within),
            }
        )

    lag_robustness = pd.DataFrame(lag_rows)

    # 2) Exclude outlier periods by trimming top 1% of outcome
    outlier_cutoff = baseline_df[TARGET].quantile(0.99)
    trimmed_df = df[df[TARGET] <= outlier_cutoff].copy()
    _, y_trim, X_trim, trimmed_interaction_terms = build_panel_data(trimmed_df, baseline_driver)
    key_driver_term_trim = trimmed_interaction_terms[0]
    model_trimmed = fit_fe_model(y_trim, X_trim, clustered=True)

    all_coef_tables.append(result_to_long_table(model_trimmed, "FE_clustered_trimmed_top1pct"))
    regression_table = regression_table.merge(
        result_to_publishable_column(model_trimmed, "Model_3_FE_Trimmed"),
        on="term",
        how="outer",
    )

    outlier_robustness = pd.DataFrame(
        {
            "specification": ["baseline_clustered", "trimmed_top1pct_clustered"],
            "driver_variable": [key_driver_term, key_driver_term_trim],
            "coef": [float(model_clustered.params[key_driver_term]), float(model_trimmed.params[key_driver_term_trim])],
            "std_error": [
                float(model_clustered.std_errors[key_driver_term]),
                float(model_trimmed.std_errors[key_driver_term_trim]),
            ],
            "p_value": [float(model_clustered.pvalues[key_driver_term]), float(model_trimmed.pvalues[key_driver_term_trim])],
            "nobs": [int(model_clustered.nobs), int(model_trimmed.nobs)],
            "r2_within": [float(model_clustered.rsquared_within), float(model_trimmed.rsquared_within)],
        }
    )

    # 3) Group subsamples
    group_rows = []
    for grp, grp_df in df.groupby(GROUP_COL, observed=True):
        try:
            grp_model_df = grp_df[[ENTITY_COL, TIME_COL, TARGET, baseline_driver, *CONTROLS]].dropna().copy()
            grp_panel = grp_model_df.set_index([ENTITY_COL, TIME_COL]).sort_index()
            y_grp = grp_panel[TARGET]
            X_grp = grp_panel[[baseline_driver, *CONTROLS]]

            # Subsample robustness uses entity FE only to keep a time-common driver identifiable.
            grp_model = fit_fe_model(y_grp, X_grp, clustered=True, time_effects=False)
            group_rows.append(
                {
                    "token_group": grp,
                    "driver_variable": baseline_driver,
                    "coef": float(grp_model.params[baseline_driver]),
                    "std_error": float(grp_model.std_errors[baseline_driver]),
                    "p_value": float(grp_model.pvalues[baseline_driver]),
                    "nobs": int(grp_model.nobs),
                    "r2_within": float(grp_model.rsquared_within),
                    "spec": "entity_FE_only",
                }
            )
        except Exception as exc:  # noqa: BLE001
            group_rows.append(
                {
                    "token_group": grp,
                    "driver_variable": baseline_driver,
                    "coef": np.nan,
                    "std_error": np.nan,
                    "p_value": np.nan,
                    "nobs": 0,
                    "r2_within": np.nan,
                    "spec": "entity_FE_only",
                    "error": str(exc),
                }
            )
    group_robustness = pd.DataFrame(group_rows)

    # Section 7: Save regression tables and diagnostic plots
    coefficient_long = pd.concat(all_coef_tables, ignore_index=True)
    coefficient_long.to_csv(TABLES_DIR / "M3_modelA_coefficients_long.csv", index=False)
    regression_table.to_csv(TABLES_DIR / "M3_modelA_regression_table.csv", index=False)
    regression_table.to_csv(TABLES_DIR / "M3_regression_table.csv", index=False)

    bp_table.to_csv(TABLES_DIR / "M3_modelA_breusch_pagan.csv", index=False)
    vif_table.to_csv(TABLES_DIR / "M3_modelA_vif.csv", index=False)
    lag_robustness.to_csv(TABLES_DIR / "M3_modelA_robustness_lags.csv", index=False)
    outlier_robustness.to_csv(TABLES_DIR / "M3_modelA_robustness_outlier_trim.csv", index=False)
    group_robustness.to_csv(TABLES_DIR / "M3_modelA_robustness_group_subsamples.csv", index=False)

    print("Saved Model A outputs:")
    print(f"- {TABLES_DIR / 'M3_modelA_coefficients_long.csv'}")
    print(f"- {TABLES_DIR / 'M3_modelA_regression_table.csv'}")
    print(f"- {TABLES_DIR / 'M3_regression_table.csv'}")
    print(f"- {TABLES_DIR / 'M3_modelA_breusch_pagan.csv'}")
    print(f"- {TABLES_DIR / 'M3_modelA_vif.csv'}")
    print(f"- {TABLES_DIR / 'M3_modelA_robustness_lags.csv'}")
    print(f"- {TABLES_DIR / 'M3_modelA_robustness_outlier_trim.csv'}")
    print(f"- {TABLES_DIR / 'M3_modelA_robustness_group_subsamples.csv'}")
    print(f"- {FIGURES_DIR / 'M3_residuals_vs_fitted.png'}")
    print(f"- {FIGURES_DIR / 'M3_qq_plot.png'}")


def main() -> None:
    run_model_a()


if __name__ == "__main__":
    main()
