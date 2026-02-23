#!/usr/bin/env python3
"""
Build an analysis-ready REIT monthly panel.

Required input:
  - A REIT-level source CSV in data/raw (or a custom path)
    containing at least:
      * an entity identifier column (e.g., ticker/reit_id)
      * a date column (daily, weekly, or monthly)

Optional inputs:
  - Supplementary CSVs (macro/housing/labor/etc.) with a date column.
    If they also include the REIT entity column, merge by entity+month.
    Otherwise, merge by month only.

Outputs:
  - data/final/reit_panel_monthly.csv
  - data/final/reit_panel_metadata.json
  - data/final/reit_panel_metadata.md
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from config_paths import FINAL_DATA_DIR, RAW_DATA_DIR


DATE_CANDIDATES = [
    "date",
    "month",
    "time",
    "period",
    "observation_date",
    "event_month",
    "event_date",
]
ENTITY_CANDIDATES = ["reit", "reit_id", "ticker", "symbol", "entity", "company"]


def infer_column(columns: List[str], candidates: List[str]) -> str | None:
    lowered = {column.lower(): column for column in columns}
    for candidate in candidates:
        if candidate in lowered:
            return lowered[candidate]
    return None


def normalize_month(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", utc=True)
    return parsed.dt.to_period("M").dt.to_timestamp("MS").dt.tz_localize(None)


def load_reit_source(path: Path, entity_col: str | None, date_col: str | None) -> Tuple[pd.DataFrame, str, str]:
    df = pd.read_csv(path)
    if df.empty:
        raise ValueError(f"Input source is empty: {path}")

    entity = entity_col or infer_column(list(df.columns), ENTITY_CANDIDATES)
    date = date_col or infer_column(list(df.columns), DATE_CANDIDATES)

    if entity is None:
        raise ValueError(
            "Could not infer REIT entity column. Pass --entity-col explicitly (e.g., reit_id or ticker)."
        )
    if date is None:
        raise ValueError("Could not infer date column. Pass --date-col explicitly.")

    work = df.copy()
    work["entity"] = work[entity].astype(str).str.strip()
    work["month"] = normalize_month(work[date])
    work = work.dropna(subset=["entity", "month"])
    return work, entity, date


def aggregate_to_monthly(df: pd.DataFrame, original_entity_col: str, original_date_col: str, rule: str) -> pd.DataFrame:
    protected = {"entity", "month", original_entity_col, original_date_col}
    value_columns = [column for column in df.columns if column not in protected]
    numeric_columns = [column for column in value_columns if pd.api.types.is_numeric_dtype(df[column])]
    non_numeric_columns = [column for column in value_columns if column not in numeric_columns]

    agg: Dict[str, str] = {}
    for column in numeric_columns:
        agg[column] = "last" if rule == "last" else "mean"
    for column in non_numeric_columns:
        agg[column] = "last"

    monthly = (
        df.sort_values(["entity", "month"]) 
        .groupby(["entity", "month"], as_index=False)
        .agg(agg)
    )
    return monthly


def create_balanced_panel(df: pd.DataFrame) -> pd.DataFrame:
    entities = df["entity"].drop_duplicates().sort_values()
    months = pd.date_range(df["month"].min(), df["month"].max(), freq="MS")

    grid = (
        pd.MultiIndex.from_product([entities, months], names=["entity", "month"])
        .to_frame(index=False)
    )
    return grid.merge(df, on=["entity", "month"], how="left")


def merge_supplementary(panel: pd.DataFrame, paths: List[Path], entity_name: str) -> Tuple[pd.DataFrame, List[Dict[str, str]]]:
    merged = panel
    merge_notes: List[Dict[str, str]] = []

    for path in paths:
        extra = pd.read_csv(path)
        if extra.empty:
            merge_notes.append({"file": str(path), "status": "skipped_empty"})
            continue

        extra_date_col = infer_column(list(extra.columns), DATE_CANDIDATES)
        if extra_date_col is None:
            merge_notes.append({"file": str(path), "status": "skipped_no_date_col"})
            continue

        extra = extra.copy()
        extra["month"] = normalize_month(extra[extra_date_col])
        extra = extra.dropna(subset=["month"])

        extra_entity_col = entity_name if entity_name in extra.columns else infer_column(list(extra.columns), ENTITY_CANDIDATES)
        merge_keys = ["month"]
        if extra_entity_col and extra_entity_col in extra.columns:
            extra["entity"] = extra[extra_entity_col].astype(str).str.strip()
            merge_keys = ["entity", "month"]

        keep_columns = [column for column in extra.columns if column not in {extra_date_col, extra_entity_col}]
        extra = extra[keep_columns]

        grouped_keys = [key for key in ["entity", "month"] if key in extra.columns]
        if not grouped_keys:
            merge_notes.append({"file": str(path), "status": "skipped_no_merge_keys"})
            continue

        numeric_cols = [column for column in extra.columns if column not in grouped_keys and pd.api.types.is_numeric_dtype(extra[column])]
        non_numeric_cols = [column for column in extra.columns if column not in grouped_keys and column not in numeric_cols]

        agg_map = {column: "mean" for column in numeric_cols}
        agg_map.update({column: "last" for column in non_numeric_cols})
        extra = extra.groupby(grouped_keys, as_index=False).agg(agg_map)

        rename_map = {}
        stem = path.stem
        for column in extra.columns:
            if column in grouped_keys:
                continue
            if column in merged.columns:
                rename_map[column] = f"{stem}_{column}"
        extra = extra.rename(columns=rename_map)

        merged = merged.merge(extra, on=merge_keys, how="left")
        merge_notes.append({"file": str(path), "status": "merged", "keys": ",".join(merge_keys)})

    return merged, merge_notes


def fill_missing_numeric(panel: pd.DataFrame) -> pd.DataFrame:
    numeric_cols = [
        column
        for column in panel.columns
        if column not in {"entity", "month"} and pd.api.types.is_numeric_dtype(panel[column])
    ]
    if not numeric_cols:
        return panel

    filled = panel.sort_values(["entity", "month"]).copy()
    filled[numeric_cols] = (
        filled.groupby("entity", dropna=False)[numeric_cols]
        .transform(lambda group: group.ffill().bfill())
    )
    return filled


def build_metadata(
    panel: pd.DataFrame,
    source_file: Path,
    source_entity_col: str,
    source_date_col: str,
    aggregation_rule: str,
    fill_policy: str,
    supplemental_notes: List[Dict[str, str]],
) -> Dict[str, object]:
    missing_share = panel.isna().mean().sort_values(ascending=False)
    top_missing = {column: round(float(value), 4) for column, value in missing_share.head(15).items()}

    metadata: Dict[str, object] = {
        "dataset_name": "reit_panel_monthly",
        "entity_definition": "REIT",
        "time_definition": "Month (month start)",
        "source_file": str(source_file),
        "source_columns": {
            "entity_column": source_entity_col,
            "date_column": source_date_col,
        },
        "transformations": {
            "aggregation_to_month": aggregation_rule,
            "missing_value_policy": fill_policy,
            "supplementary_merges": supplemental_notes,
        },
        "panel_shape": {
            "rows": int(len(panel)),
            "columns": int(panel.shape[1]),
            "entities": int(panel["entity"].nunique(dropna=True)),
            "months": int(panel["month"].nunique(dropna=True)),
            "min_month": str(panel["month"].min().date()) if not panel.empty else None,
            "max_month": str(panel["month"].max().date()) if not panel.empty else None,
        },
        "missing_share_top15": top_missing,
    }
    return metadata


def write_metadata_markdown(path: Path, metadata: Dict[str, object]) -> None:
    panel_shape = metadata["panel_shape"]
    source_columns = metadata["source_columns"]
    transforms = metadata["transformations"]

    lines = [
        "# REIT Panel Metadata",
        "",
        "## Dataset",
        f"- Name: {metadata['dataset_name']}",
        f"- Entity: {metadata['entity_definition']}",
        f"- Time: {metadata['time_definition']}",
        f"- Source file: {metadata['source_file']}",
        "",
        "## Source Column Mapping",
        f"- Entity column: {source_columns['entity_column']}",
        f"- Date column: {source_columns['date_column']}",
        "",
        "## Transformations",
        f"- Aggregation to month: {transforms['aggregation_to_month']}",
        f"- Missing value policy: {transforms['missing_value_policy']}",
        "",
        "## Panel Shape",
        f"- Rows: {panel_shape['rows']}",
        f"- Columns: {panel_shape['columns']}",
        f"- Unique entities: {panel_shape['entities']}",
        f"- Unique months: {panel_shape['months']}",
        f"- Date range: {panel_shape['min_month']} to {panel_shape['max_month']}",
        "",
        "## Top Missingness",
    ]

    missing = metadata.get("missing_share_top15", {})
    if isinstance(missing, dict) and missing:
        for column, share in missing.items():
            lines.append(f"- {column}: {share}")
    else:
        lines.append("- No missing values detected.")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build monthly REIT panel dataset.")
    parser.add_argument(
        "--source",
        type=Path,
        default=RAW_DATA_DIR / "reit_master.csv",
        help="Path to required REIT-level source CSV.",
    )
    parser.add_argument(
        "--entity-col",
        type=str,
        default=None,
        help="Entity identifier column in source CSV (e.g., ticker).",
    )
    parser.add_argument(
        "--date-col",
        type=str,
        default=None,
        help="Date column in source CSV.",
    )
    parser.add_argument(
        "--aggregation",
        choices=["last", "mean"],
        default="last",
        help="How to aggregate numeric values to monthly frequency when source is higher frequency.",
    )
    parser.add_argument(
        "--supplementary",
        nargs="*",
        default=[],
        help="Optional supplementary CSV paths (macro/housing/labor, etc.).",
    )
    parser.add_argument(
        "--fill-forward-numeric",
        action="store_true",
        help="Forward/backward fill numeric variables within each REIT after panel balancing.",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=FINAL_DATA_DIR / "reit_panel_monthly.csv",
        help="Panel CSV output path.",
    )
    parser.add_argument(
        "--output-meta-json",
        type=Path,
        default=FINAL_DATA_DIR / "reit_panel_metadata.json",
        help="Metadata JSON output path.",
    )
    parser.add_argument(
        "--output-meta-md",
        type=Path,
        default=FINAL_DATA_DIR / "reit_panel_metadata.md",
        help="Metadata Markdown output path.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.source.exists():
        raise FileNotFoundError(
            f"Source file not found: {args.source}. Add a REIT source CSV and rerun."
        )

    source, source_entity_col, source_date_col = load_reit_source(
        args.source,
        args.entity_col,
        args.date_col,
    )
    monthly = aggregate_to_monthly(source, source_entity_col, source_date_col, args.aggregation)
    panel = create_balanced_panel(monthly)

    supplementary_paths = [Path(value) for value in args.supplementary]
    panel, merge_notes = merge_supplementary(panel, supplementary_paths, source_entity_col)

    fill_policy = "none"
    if args.fill_forward_numeric:
        panel = fill_missing_numeric(panel)
        fill_policy = "numeric columns forward/backward filled within each entity"

    panel = panel.sort_values(["entity", "month"]).reset_index(drop=True)

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    args.output_meta_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_meta_md.parent.mkdir(parents=True, exist_ok=True)

    panel.to_csv(args.output_csv, index=False)

    metadata = build_metadata(
        panel=panel,
        source_file=args.source,
        source_entity_col=source_entity_col,
        source_date_col=source_date_col,
        aggregation_rule=args.aggregation,
        fill_policy=fill_policy,
        supplemental_notes=merge_notes,
    )

    args.output_meta_json.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    write_metadata_markdown(args.output_meta_md, metadata)

    print(f"Saved panel CSV: {args.output_csv}")
    print(f"Saved metadata JSON: {args.output_meta_json}")
    print(f"Saved metadata MD: {args.output_meta_md}")


if __name__ == "__main__":
    main()
