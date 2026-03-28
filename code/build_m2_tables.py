"""Build M2 summary tables from the final crypto analysis panel."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PANEL_PATH = ROOT / "data" / "final" / "crypto_analysis_panel.csv"
TABLES_DIR = ROOT / "results" / "tables"


def main() -> None:
    TABLES_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(PANEL_PATH)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    key_vars = [
        "outcome_realized_vol_30d",
        "driver_sec_event_indicator",
        "control_market_cap",
        "control_total_volume",
        "control_btc_corr_30d",
        "vix",
        "ffeffective_rate",
        "epu_index",
    ]
    key_vars = [col for col in key_vars if col in df.columns]

    desc = df[key_vars].describe(percentiles=[0.01, 0.05, 0.5, 0.95, 0.99]).T
    ordered_cols = ["count", "mean", "std", "min", "1%", "5%", "50%", "95%", "99%", "max"]
    desc = desc[[col for col in ordered_cols if col in desc.columns]]
    desc.to_csv(TABLES_DIR / "M2_table1_descriptive_stats.csv")

    df[key_vars].corr(numeric_only=True).to_csv(TABLES_DIR / "M2_table2_correlation_matrix.csv")

    missing = pd.DataFrame({"variable": df.columns, "missing_count": df.isna().sum().values})
    missing["missing_pct"] = (missing["missing_count"] / len(df) * 100).round(4)
    missing = missing.sort_values(["missing_pct", "missing_count"], ascending=False)
    missing.to_csv(TABLES_DIR / "M2_table3_missingness_report.csv", index=False)

    required = {"driver_sec_event_indicator", "outcome_realized_vol_30d"}
    if required.issubset(df.columns):
        overall = (
            df[["driver_sec_event_indicator", "outcome_realized_vol_30d"]]
            .dropna()
            .groupby("driver_sec_event_indicator")["outcome_realized_vol_30d"]
            .agg(["mean", "median", "count"])
            .reset_index()
        )
        overall.columns = ["event_indicator", "mean_volatility", "median_volatility", "n_obs"]
        overall["scope"] = "overall"

        frames = [overall]

        if "token_group" in df.columns:
            by_group = (
                df[["token_group", "driver_sec_event_indicator", "outcome_realized_vol_30d"]]
                .dropna()
                .groupby(["token_group", "driver_sec_event_indicator"])["outcome_realized_vol_30d"]
                .agg(["mean", "median", "count"])
                .reset_index()
            )
            by_group.columns = [
                "token_group",
                "event_indicator",
                "mean_volatility",
                "median_volatility",
                "n_obs",
            ]
            by_group["scope"] = "token_group"
            frames.append(by_group)

        pd.concat(frames, ignore_index=True, sort=False).to_csv(
            TABLES_DIR / "M2_table4_event_vs_nonevent_volatility.csv", index=False
        )

    print("Created M2 tables in", TABLES_DIR)


if __name__ == "__main__":
    main()
