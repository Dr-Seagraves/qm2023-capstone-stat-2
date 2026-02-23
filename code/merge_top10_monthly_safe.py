#!/usr/bin/env python3
"""
Safely merge a top-10 subset onto a larger base dataset by entity + month.

Design goals:
  - Preserve all rows in the base dataset.
  - Only attempt enrichment for entities that appear in the top-10 dataset.
  - Merge on normalized monthly date keys.
  - Avoid overwriting base columns by default (adds prefixed columns).

Example:
  python code/merge_top10_monthly_safe.py \
    --base data/raw/reit_master_template.csv \
    --top data/final/coingecko_top10_2020_returns_volatility.csv \
    --base-entity-col company --base-date-col date \
    --top-entity-col coin_symbol --top-date-col date \
    --out data/final/base_with_top10_monthly_merge.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import pandas as pd


def normalize_entity(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower()


def normalize_month(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", utc=True)
    return parsed.dt.to_period("M").dt.to_timestamp("MS").dt.tz_localize(None)


def aggregate_top(top: pd.DataFrame, key_cols: List[str]) -> pd.DataFrame:
    value_cols = [column for column in top.columns if column not in key_cols]
    numeric_cols = [column for column in value_cols if pd.api.types.is_numeric_dtype(top[column])]
    text_cols = [column for column in value_cols if column not in numeric_cols]

    agg_map: Dict[str, str] = {column: "mean" for column in numeric_cols}
    agg_map.update({column: "last" for column in text_cols})

    return top.groupby(key_cols, as_index=False).agg(agg_map)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Safely merge top-10 data onto a larger dataset by entity + month."
    )
    parser.add_argument("--base", type=Path, required=True, help="Base CSV to preserve.")
    parser.add_argument("--top", type=Path, required=True, help="Top-10 CSV to merge in.")

    parser.add_argument("--base-entity-col", type=str, default=None, help="Entity column in base CSV.")
    parser.add_argument("--base-date-col", type=str, required=True, help="Date column in base CSV.")
    parser.add_argument("--top-entity-col", type=str, default=None, help="Entity column in top CSV.")
    parser.add_argument("--top-date-col", type=str, required=True, help="Date column in top CSV.")
    parser.add_argument(
        "--month-only",
        action="store_true",
        help="Merge by month only (ignore entity columns). Useful for SEC/event datasets.",
    )

    parser.add_argument(
        "--top-filter-col",
        type=str,
        default="coin_rank",
        help="Optional rank/filter column in top CSV (default: coin_rank).",
    )
    parser.add_argument(
        "--top-filter-max",
        type=float,
        default=10,
        help="Keep rows where top_filter_col <= this value (default: 10).",
    )
    parser.add_argument(
        "--disable-top-filter",
        action="store_true",
        help="Disable rank filtering and use all rows from the top CSV.",
    )

    parser.add_argument(
        "--prefix",
        type=str,
        default="top10_",
        help="Prefix for merged columns from top CSV when names overlap or are new.",
    )
    parser.add_argument("--out", type=Path, required=True, help="Output CSV path.")
    return parser.parse_args()


def validate_columns(df: pd.DataFrame, required: List[str], label: str) -> None:
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in {label}: {missing}")


def main() -> None:
    args = parse_args()

    if not args.base.exists():
        raise FileNotFoundError(f"Base CSV not found: {args.base}")
    if not args.top.exists():
        raise FileNotFoundError(f"Top CSV not found: {args.top}")

    base = pd.read_csv(args.base)
    top = pd.read_csv(args.top)

    if args.month_only:
        validate_columns(base, [args.base_date_col], "base")
        validate_columns(top, [args.top_date_col], "top")
    else:
        if not args.base_entity_col or not args.top_entity_col:
            raise ValueError("Entity columns are required unless --month-only is set.")
        validate_columns(base, [args.base_entity_col, args.base_date_col], "base")
        validate_columns(top, [args.top_entity_col, args.top_date_col], "top")

    if top.empty:
        raise ValueError("Top CSV is empty; nothing to merge.")

    if not args.disable_top_filter and args.top_filter_col in top.columns:
        filter_values = pd.to_numeric(top[args.top_filter_col], errors="coerce")
        top = top.loc[filter_values <= args.top_filter_max].copy()

    if top.empty:
        raise ValueError("No rows left in top CSV after filtering.")

    base_work = base.copy()
    top_work = top.copy()

    base_work["__month_key"] = normalize_month(base_work[args.base_date_col])
    top_work["__month_key"] = normalize_month(top_work[args.top_date_col])

    key_cols = ["__month_key"]
    if not args.month_only:
        base_work["__entity_key"] = normalize_entity(base_work[args.base_entity_col])
        top_work["__entity_key"] = normalize_entity(top_work[args.top_entity_col])
        key_cols = ["__entity_key", "__month_key"]

    base_work["__row_order"] = range(len(base_work))
    base_work = base_work.dropna(subset=key_cols)
    top_work = top_work.dropna(subset=key_cols)

    if args.month_only:
        base_top = base_work.copy()
        base_other = base_work.iloc[0:0].copy()
        top_entities = set()
    else:
        top_entities = set(top_work["__entity_key"].dropna().unique().tolist())
        base_top = base_work[base_work["__entity_key"].isin(top_entities)].copy()
        base_other = base_work[~base_work["__entity_key"].isin(top_entities)].copy()

    top_agg = aggregate_top(top_work, key_cols)

    protected = set(key_cols + [args.top_date_col])
    if args.top_entity_col:
        protected.add(args.top_entity_col)
    top_payload_cols = [column for column in top_agg.columns if column not in protected]

    rename_map: Dict[str, str] = {}
    for column in top_payload_cols:
        rename_map[column] = f"{args.prefix}{column}"

    top_payload = top_agg[key_cols + top_payload_cols].rename(columns=rename_map)

    merged_top = base_top.merge(top_payload, on=key_cols, how="left")
    merged_all = pd.concat([merged_top, base_other], ignore_index=True)
    merged_all = merged_all.sort_values("__row_order").drop(columns=["__row_order"])

    merged_all = merged_all.drop(columns=["__entity_key", "__month_key"], errors="ignore")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    merged_all.to_csv(args.out, index=False)

    print(f"Base rows: {len(base)}")
    print(f"Top rows used: {len(top_work)}")
    if args.month_only:
        print("Merge mode: month-only")
        print(f"Rows eligible for merge (all base rows with valid month): {len(base_top)}")
    else:
        print("Merge mode: entity + month")
        print(f"Top entities: {len(top_entities)}")
        print(f"Rows eligible for merge (base top-entity rows): {len(base_top)}")
        print(f"Rows left untouched (non-top entities): {len(base_other)}")
    print(f"Output written: {args.out}")


if __name__ == "__main__":
    main()