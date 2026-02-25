#!/usr/bin/env python3
"""
Build top-10 CoinGecko daily returns/volatility panel required for event-study merge.

Inputs (first existing file wins):
  - data/processed/coingecko_ranking_cleaned.csv
  - data/final/finalcoingeckodata.csv

Output:
  - data/final/coingecko_top10_2020_returns_volatility.csv
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from config_paths import FINAL_DATA_DIR, PROCESSED_DATA_DIR

INPUT_CANDIDATES = [
    PROCESSED_DATA_DIR / "coingecko_ranking_cleaned.csv",
    FINAL_DATA_DIR / "finalcoingeckodata.csv",
]
OUTPUT_FILE = FINAL_DATA_DIR / "coingecko_top10_2020_returns_volatility.csv"


REQUIRED_COLUMNS = [
    "coin_rank",
    "coin_name",
    "coin_symbol",
    "price",
    "market_cap",
    "total_volume",
]


def pick_input_file() -> Path:
    for path in INPUT_CANDIDATES:
        if path.exists():
            return path
    candidates = "\n".join(f"  - {candidate}" for candidate in INPUT_CANDIDATES)
    raise FileNotFoundError(f"No input file found. Checked:\n{candidates}")


def parse_date_column(df: pd.DataFrame) -> pd.Series:
    if "snapped_at" in df.columns:
        raw = df["snapped_at"].astype(str).str.replace(" UTC", "", regex=False)
    elif "date" in df.columns:
        raw = df["date"].astype(str)
    else:
        raise ValueError("Input must contain either 'snapped_at' or 'date' column.")

    parsed = pd.to_datetime(raw, errors="coerce")
    if parsed.isna().all():
        raise ValueError("Unable to parse any dates from input date column.")
    return parsed.dt.normalize()


def main() -> None:
    input_file = pick_input_file()
    df = pd.read_csv(input_file)

    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Input file is missing required columns: {missing}")

    df = df.copy()
    df["date"] = parse_date_column(df)
    df = df.dropna(subset=["date"]).copy()

    df["coin_rank_num"] = pd.to_numeric(df["coin_rank"], errors="coerce")
    df = df[df["coin_rank_num"] <= 10].copy()
    if df.empty:
        raise ValueError("No rows found with coin_rank <= 10.")

    df["coin_symbol"] = df["coin_symbol"].astype(str).str.strip().str.lower()
    df = df.sort_values(["coin_symbol", "date"]) 

    price = pd.to_numeric(df["price"], errors="coerce")
    df["log_price"] = np.where(price > 0, np.log(price), np.nan)
    df["log_return"] = df.groupby("coin_symbol", sort=False)["log_price"].diff()
    df["rolling_vol_30d"] = (
        df.groupby("coin_symbol", sort=False)["log_return"]
        .rolling(window=30, min_periods=30)
        .std()
        .reset_index(level=0, drop=True)
    )

    out = df[
        [
            "date",
            "coin_symbol",
            "coin_name",
            "coin_rank_num",
            "market_cap",
            "total_volume",
            "rolling_vol_30d",
            "log_return",
        ]
    ].rename(columns={"coin_rank_num": "coin_rank"})

    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    out["coin_rank"] = out["coin_rank"].round().astype("Int64")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUTPUT_FILE, index=False)

    print(f"Input used: {input_file}")
    print(f"Output written: {OUTPUT_FILE}")
    print(f"Rows: {len(out)} | Tokens: {out['coin_symbol'].nunique()}")


if __name__ == "__main__":
    main()
