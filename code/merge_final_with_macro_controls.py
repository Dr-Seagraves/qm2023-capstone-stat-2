#!/usr/bin/env python3
"""
Merge cleaned macro controls (VIX, Effective Fed Funds Rate, EPU) onto final crypto panel.

Inputs:
    - data/processed/crypto_reg_event_panel.csv
  - data/processed/vix_cleaned.csv
  - data/processed/ffeffective_rate_cleaned.csv
    - data/processed/epu_index_cleaned.csv

Output:
    - data/final/crypto_analysis_panel.csv

Notes:
    - VIX is not published on market-closed days (weekends/holidays).
    - Before merging, macro controls are expanded to all panel dates using
      forward fill so each crypto observation date has the latest available
      macro reading.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from config_paths import FINAL_DATA_DIR, PROCESSED_DATA_DIR

FINAL_PANEL_INPUT = PROCESSED_DATA_DIR / "crypto_reg_event_panel.csv"
LEGACY_FINAL_PANEL_INPUT = FINAL_DATA_DIR / "crypto_reg_event_panel.csv"
VIX_INPUT = PROCESSED_DATA_DIR / "vix_cleaned.csv"
FED_INPUT = PROCESSED_DATA_DIR / "ffeffective_rate_cleaned.csv"
EPU_INPUT = PROCESSED_DATA_DIR / "epu_index_cleaned.csv"
OUTPUT_FILE = FINAL_DATA_DIR / "crypto_analysis_panel.csv"


def expand_to_panel_dates(series: pd.DataFrame, value_col: str, panel_dates: pd.Series) -> pd.DataFrame:
    """Expand a macro series to panel dates and forward-fill non-trading gaps."""
    expanded = (
        series[["observation_date", value_col]]
        .drop_duplicates(subset=["observation_date"], keep="last")
        .sort_values("observation_date")
        .set_index("observation_date")
        .reindex(panel_dates)
        .ffill()
        .reset_index()
        .rename(columns={"index": "observation_date"})
    )
    return expanded


def main() -> None:
    panel_input = FINAL_PANEL_INPUT if FINAL_PANEL_INPUT.exists() else LEGACY_FINAL_PANEL_INPUT

    for path in [panel_input, VIX_INPUT, FED_INPUT, EPU_INPUT]:
        if not path.exists():
            raise FileNotFoundError(f"Missing required input file: {path}")

    panel = pd.read_csv(panel_input)
    vix = pd.read_csv(VIX_INPUT)
    fed = pd.read_csv(FED_INPUT)
    epu = pd.read_csv(EPU_INPUT)

    if "date" not in panel.columns:
        raise ValueError("Final panel must include a 'date' column.")
    if "observation_date" not in vix.columns or "vix" not in vix.columns:
        raise ValueError("VIX cleaned file must include columns: observation_date, vix")
    if "observation_date" not in fed.columns or "ffeffective_rate" not in fed.columns:
        raise ValueError("Fed cleaned file must include columns: observation_date, ffeffective_rate")
    if "observation_date" not in epu.columns or "epu_index" not in epu.columns:
        raise ValueError("EPU cleaned file must include columns: observation_date, epu_index")

    panel["date"] = pd.to_datetime(panel["date"], errors="coerce")
    vix["observation_date"] = pd.to_datetime(vix["observation_date"], errors="coerce")
    fed["observation_date"] = pd.to_datetime(fed["observation_date"], errors="coerce")
    epu["observation_date"] = pd.to_datetime(epu["observation_date"], errors="coerce")

    panel = panel.dropna(subset=["date"]).copy()
    vix = vix.dropna(subset=["observation_date", "vix"]).drop_duplicates(subset=["observation_date"], keep="last")
    fed = fed.dropna(subset=["observation_date", "ffeffective_rate"]).drop_duplicates(subset=["observation_date"], keep="last")
    epu = epu.dropna(subset=["observation_date", "epu_index"]).drop_duplicates(subset=["observation_date"], keep="last")

    panel_dates = pd.Index(panel["date"].dropna().drop_duplicates().sort_values(), name="observation_date")

    vix_expanded = expand_to_panel_dates(vix, "vix", panel_dates)
    fed_expanded = expand_to_panel_dates(fed, "ffeffective_rate", panel_dates)
    epu_expanded = expand_to_panel_dates(epu, "epu_index", panel_dates)

    merged = panel.merge(
        vix_expanded,
        how="left",
        left_on="date",
        right_on="observation_date",
    ).drop(columns=["observation_date"])

    merged = merged.merge(
        fed_expanded,
        how="left",
        left_on="date",
        right_on="observation_date",
    ).drop(columns=["observation_date"])

    merged = merged.merge(
        epu_expanded,
        how="left",
        left_on="date",
        right_on="observation_date",
    ).drop(columns=["observation_date"])

    merged["outcome_vol_blank_reason"] = merged["outcome_realized_vol_30d"].isna().map(
        {True: "no_prior_ret_yet", False: "-"}
    )
    merged["btc_corr_blank_reason"] = merged["control_btc_corr_30d"].isna().map(
        {True: "no_paired_ret_yet", False: "-"}
    )

    merged["coin_rank_sort"] = pd.to_numeric(merged["coin_rank"], errors="coerce")
    merged = merged.sort_values(["date", "coin_rank_sort"], kind="mergesort").drop(columns=["coin_rank_sort"])

    merged["date"] = merged["date"].dt.strftime("%Y-%m-%d")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved merged final file: {OUTPUT_FILE}")
    print(f"Rows: {len(merged)}")
    print(f"Columns: {len(merged.columns)}")


if __name__ == "__main__":
    main()
