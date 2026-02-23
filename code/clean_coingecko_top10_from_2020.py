#!/usr/bin/env python3
"""
Clean CoinGecko raw dataset for first 10 ranked coins and keep only 2020+ records.

Outputs:
  - data/processed/coingecko_top10_2020_clean.csv
  - data/processed/coingecko_top10_2020_clean_metadata.md

Missing-value policy (documented):
  1) Numeric parse invalid/blank values as missing.
  2) Within each coin, forward-fill then backward-fill over time.
  3) If still missing, fill with per-coin median.
  4) If still missing, fill with overall median (across selected data).
"""

from __future__ import annotations

import csv
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from config_paths import PROCESSED_DATA_DIR, RAW_DATA_DIR


INPUT_FILE = RAW_DATA_DIR / "coingecko_ranking.csv"
OUTPUT_FILE = PROCESSED_DATA_DIR / "coingecko_top10_2020_clean.csv"
METADATA_FILE = PROCESSED_DATA_DIR / "coingecko_top10_2020_clean_metadata.md"

NUMERIC_COLUMNS = ["price", "market_cap", "total_volume"]
DATE_FORMAT = "%Y-%m-%d %H:%M:%S UTC"
START_DATE = datetime(2020, 1, 1)


def parse_float(value: str) -> float | None:
    text = (value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_datetime(value: str) -> datetime | None:
    text = (value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, DATE_FORMAT)
    except ValueError:
        return None


def read_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_rows(path: Path, rows: List[Dict[str, str]], headers: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def median_or_none(values: List[float]) -> float | None:
    if not values:
        return None
    return float(statistics.median(values))


def first_10_symbols_in_order(rows: List[Dict[str, str]]) -> List[str]:
    selected: List[str] = []
    seen = set()
    for row in rows:
        symbol = (row.get("coin_symbol") or "").strip().lower()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        selected.append(symbol)
        if len(selected) == 10:
            break
    return selected


def fill_within_coin(records: List[Dict[str, object]], column: str) -> Dict[str, int]:
    ffill_count = 0
    bfill_count = 0
    coin_median_count = 0
    global_median_count = 0

    by_coin: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for record in records:
        by_coin[str(record["coin_symbol"])].append(record)

    all_non_missing: List[float] = [
        float(record[column])
        for record in records
        if record.get(column) is not None
    ]
    global_median = median_or_none(all_non_missing)

    for _, group in by_coin.items():
        group.sort(key=lambda r: r["_dt"])

        last_value = None
        for record in group:
            if record[column] is None and last_value is not None:
                record[column] = last_value
                ffill_count += 1
            if record[column] is not None:
                last_value = record[column]

        next_value = None
        for record in reversed(group):
            if record[column] is None and next_value is not None:
                record[column] = next_value
                bfill_count += 1
            if record[column] is not None:
                next_value = record[column]

        coin_values: List[float] = [float(record[column]) for record in group if record.get(column) is not None]
        coin_median = median_or_none(coin_values)
        if coin_median is not None:
            for record in group:
                if record[column] is None:
                    record[column] = coin_median
                    coin_median_count += 1

    if global_median is not None:
        for record in records:
            if record[column] is None:
                record[column] = global_median
                global_median_count += 1

    return {
        "ffill": ffill_count,
        "bfill": bfill_count,
        "coin_median": coin_median_count,
        "global_median": global_median_count,
    }


def format_float(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.12g}"


def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Missing input file: {INPUT_FILE}")

    raw_rows = read_rows(INPUT_FILE)
    selected_symbols = first_10_symbols_in_order(raw_rows)
    selected_symbol_set = set(selected_symbols)

    if len(selected_symbols) < 10:
        raise RuntimeError("Unable to find 10 unique coins in input file.")

    records: List[Dict[str, object]] = []
    dropped_before_2020 = 0
    dropped_bad_date = 0

    for row in raw_rows:
        symbol = (row.get("coin_symbol") or "").strip().lower()
        if symbol not in selected_symbol_set:
            continue

        rank_text = (row.get("coin_rank") or "").strip()
        if not rank_text.isdigit():
            continue
        rank = int(rank_text)

        dt = parse_datetime(row.get("snapped_at", ""))
        if dt is None:
            dropped_bad_date += 1
            continue
        if dt < START_DATE:
            dropped_before_2020 += 1
            continue

        record: Dict[str, object] = {
            "coin_rank": rank,
            "coin_id": (row.get("coin_id") or "").strip(),
            "coin_name": (row.get("coin_name") or "").strip(),
            "coin_symbol": (row.get("coin_symbol") or "").strip().lower(),
            "source_file": (row.get("source_file") or "").strip(),
            "snapped_at": (row.get("snapped_at") or "").strip(),
            "_dt": dt,
        }
        for column in NUMERIC_COLUMNS:
            record[column] = parse_float(row.get(column, ""))

        records.append(record)

    if not records:
        raise RuntimeError("No records available after filtering top 10 ranks and 2020+ date rule.")

    missing_before = {column: sum(1 for record in records if record[column] is None) for column in NUMERIC_COLUMNS}
    fill_stats = {column: fill_within_coin(records, column) for column in NUMERIC_COLUMNS}
    missing_after = {column: sum(1 for record in records if record[column] is None) for column in NUMERIC_COLUMNS}

    records.sort(key=lambda r: (int(r["coin_rank"]), str(r["coin_symbol"]), r["_dt"]))

    out_rows: List[Dict[str, str]] = []
    for record in records:
        out = {
            "coin_rank": str(record["coin_rank"]),
            "coin_id": str(record["coin_id"]),
            "coin_name": str(record["coin_name"]),
            "coin_symbol": str(record["coin_symbol"]),
            "source_file": str(record["source_file"]),
            "snapped_at": str(record["snapped_at"]),
            "price": format_float(record["price"]),
            "market_cap": format_float(record["market_cap"]),
            "total_volume": format_float(record["total_volume"]),
        }
        out_rows.append(out)

    headers = [
        "coin_rank",
        "coin_id",
        "coin_name",
        "coin_symbol",
        "source_file",
        "snapped_at",
        "price",
        "market_cap",
        "total_volume",
    ]
    write_rows(OUTPUT_FILE, out_rows, headers)

    coins = sorted({str(record["coin_symbol"]) for record in records})
    min_date = min(record["_dt"] for record in records).strftime("%Y-%m-%d")
    max_date = max(record["_dt"] for record in records).strftime("%Y-%m-%d")

    metadata_lines = [
        "# CoinGecko Top 10 (2020+) Cleaning Metadata",
        "",
        "## Scope",
        f"- Input file: {INPUT_FILE}",
        f"- Output file: {OUTPUT_FILE}",
        f"- Rule: keep first 10 unique coins by file order (symbols selected: {selected_symbols})",
        "- Rule: omit all rows before 2020-01-01",
        "",
        "## Result Summary",
        f"- Rows output: {len(out_rows)}",
        f"- Distinct coins: {len(coins)} ({', '.join(coins)})",
        f"- Date range: {min_date} to {max_date}",
        f"- Dropped rows (before 2020): {dropped_before_2020}",
        f"- Dropped rows (invalid datetime): {dropped_bad_date}",
        "",
        "## Missing-Value Decisions",
        "- Numeric columns: price, market_cap, total_volume",
        "- Order: forward-fill within coin -> backward-fill within coin -> coin median -> global median",
        "",
        "## Missing Counts",
    ]

    for column in NUMERIC_COLUMNS:
        stats = fill_stats[column]
        metadata_lines.extend(
            [
                f"- {column}: before={missing_before[column]}, after={missing_after[column]}, "
                f"ffill={stats['ffill']}, bfill={stats['bfill']}, "
                f"coin_median={stats['coin_median']}, global_median={stats['global_median']}",
            ]
        )

    METADATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    METADATA_FILE.write_text("\n".join(metadata_lines) + "\n", encoding="utf-8")

    print(f"Saved cleaned dataset: {OUTPUT_FILE}")
    print(f"Saved metadata: {METADATA_FILE}")
    print(f"Rows: {len(out_rows)} | Coins: {len(coins)} | Date range: {min_date} to {max_date}")


if __name__ == "__main__":
    main()
