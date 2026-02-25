#!/usr/bin/env python3
"""
Clean macro time-series inputs (VIX, Effective Fed Funds Rate, EPU).

Inputs:
  - data/raw/VIX.csv
  - data/raw/FFEffective Rate.csv
    - data/raw/USEPUINDXD.csv (or USEPUINDXD (1).csv)

Outputs:
  - data/processed/vix_cleaned.csv
  - data/processed/ffeffective_rate_cleaned.csv
    - data/processed/epu_index_cleaned.csv

Cleaning decisions:
  - Keep only rows with valid date and numeric value
  - Treat blank strings and '.' as missing values
  - Drop duplicate dates (keep last occurrence)
  - Sort by date ascending
  - No interpolation or forward fill
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from config_paths import PROCESSED_DATA_DIR, RAW_DATA_DIR

VIX_INPUT = RAW_DATA_DIR / "VIX.csv"
FED_INPUT = RAW_DATA_DIR / "FFEffective Rate.csv"
VIX_OUTPUT = PROCESSED_DATA_DIR / "vix_cleaned.csv"
FED_OUTPUT = PROCESSED_DATA_DIR / "ffeffective_rate_cleaned.csv"
EPU_OUTPUT = PROCESSED_DATA_DIR / "epu_index_cleaned.csv"
EPU_INPUT_CANDIDATES = [
    RAW_DATA_DIR / "USEPUINDXD.csv",
    RAW_DATA_DIR / "USEPUINDXD (1).csv",
    RAW_DATA_DIR / "USEPUINDXD (2).csv",
]


def resolve_epu_input() -> Path:
    for candidate in EPU_INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Missing EPU input file. Expected one of: "
        + ", ".join(path.name for path in EPU_INPUT_CANDIDATES)
    )


def clean_series(input_file: Path, value_col: str, output_file: Path, out_value_col: str) -> pd.DataFrame:
    if not input_file.exists():
        raise FileNotFoundError(f"Missing input file: {input_file}")

    df = pd.read_csv(input_file)
    required_cols = {"observation_date", value_col}
    missing = [column for column in required_cols if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {input_file.name}: {missing}")

    out = df[["observation_date", value_col]].copy()
    out = out.replace({"": pd.NA, " ": pd.NA, ".": pd.NA})

    out["observation_date"] = pd.to_datetime(out["observation_date"], errors="coerce")
    out[out_value_col] = pd.to_numeric(out[value_col], errors="coerce")

    out = out.drop(columns=[value_col])
    out = out.dropna(subset=["observation_date", out_value_col])
    out = out.drop_duplicates(subset=["observation_date"], keep="last")
    out = out.sort_values("observation_date").reset_index(drop=True)

    out["observation_date"] = out["observation_date"].dt.strftime("%Y-%m-%d")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_file, index=False)
    return out


def main() -> None:
    vix = clean_series(
        input_file=VIX_INPUT,
        value_col="VIXCLS",
        output_file=VIX_OUTPUT,
        out_value_col="vix",
    )
    fed = clean_series(
        input_file=FED_INPUT,
        value_col="DFF",
        output_file=FED_OUTPUT,
        out_value_col="ffeffective_rate",
    )
    epu = clean_series(
        input_file=resolve_epu_input(),
        value_col="USEPUINDXD",
        output_file=EPU_OUTPUT,
        out_value_col="epu_index",
    )

    print(f"Saved: {VIX_OUTPUT} ({len(vix)} rows)")
    print(f"Saved: {FED_OUTPUT} ({len(fed)} rows)")
    print(f"Saved: {EPU_OUTPUT} ({len(epu)} rows)")


if __name__ == "__main__":
    main()
