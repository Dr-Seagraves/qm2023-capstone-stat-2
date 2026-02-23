#!/usr/bin/env python3
"""
Compute daily returns and rolling volatility for cleaned CoinGecko top-10 dataset.

Input:
  - data/processed/coingecko_top10_2020_clean.csv

Output:
  - data/final/coingecko_top10_2020_returns_volatility.csv
"""

from __future__ import annotations

import csv
import math
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from config_paths import FINAL_DATA_DIR, PROCESSED_DATA_DIR


INPUT_FILE = PROCESSED_DATA_DIR / "coingecko_top10_2020_clean.csv"
OUTPUT_FILE = FINAL_DATA_DIR / "coingecko_top10_2020_returns_volatility.csv"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S UTC"


def parse_datetime(value: str) -> datetime:
    return datetime.strptime(value.strip(), DATE_FORMAT)


def parse_float(value: str) -> float | None:
    text = (value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def rolling_std(values: List[float], window: int) -> float | None:
    if len(values) < window:
        return None
    segment = values[-window:]
    if len(segment) < 2:
        return None
    return float(statistics.stdev(segment))


def format_float(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.12g}"


def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Missing cleaned input file: {INPUT_FILE}")

    with INPUT_FILE.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    by_symbol: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in rows:
        symbol = (row.get("coin_symbol") or "").strip().lower()
        if not symbol:
            continue
        by_symbol[symbol].append(row)

    out_rows: List[Dict[str, str]] = []
    for symbol, group in by_symbol.items():
        group.sort(key=lambda r: parse_datetime(r["snapped_at"]))

        prev_price: float | None = None
        log_return_history: List[float] = []

        for row in group:
            price = parse_float(row.get("price", ""))

            simple_return: float | None = None
            log_return: float | None = None
            if price is not None and prev_price is not None and prev_price > 0:
                simple_return = (price / prev_price) - 1.0
                if price > 0:
                    log_return = math.log(price / prev_price)

            if log_return is not None:
                log_return_history.append(log_return)

            vol_7d = rolling_std(log_return_history, 7)
            vol_30d = rolling_std(log_return_history, 30)

            out = dict(row)
            out["date"] = row["snapped_at"][:10]
            out["simple_return"] = format_float(simple_return)
            out["log_return"] = format_float(log_return)
            out["rolling_vol_7d"] = format_float(vol_7d)
            out["rolling_vol_30d"] = format_float(vol_30d)
            out_rows.append(out)

            if price is not None:
                prev_price = price

    out_rows.sort(key=lambda r: (int(r["coin_rank"]), r["coin_symbol"], r["snapped_at"]))

    headers = list(rows[0].keys()) + ["date", "simple_return", "log_return", "rolling_vol_7d", "rolling_vol_30d"]
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_FILE.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"Saved returns/volatility dataset: {OUTPUT_FILE}")
    print(f"Rows: {len(out_rows)} | Coins: {len(by_symbol)}")


if __name__ == "__main__":
    main()
