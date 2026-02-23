#!/usr/bin/env python3
"""
Build a crypto regulatory event-study panel with required variables.

Requirements covered:
  - Outcome: 30-day realized volatility per token
  - Driver: SEC event-date indicator (binary)
  - Controls: market cap, total volume, BTC correlation (30-day rolling)
  - Groups: DeFi vs centralized exchange vs stablecoin

Inputs:
  - data/final/coingecko_top10_2020_returns_volatility.csv
  - data/processed/sec_press_litigation_crypto_only.csv

Outputs:
  - data/final/crypto_reg_event_panel.csv
  - data/final/crypto_reg_event_panel_metadata.md
"""

from __future__ import annotations

import csv
import math
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

from config_paths import FINAL_DATA_DIR, PROCESSED_DATA_DIR


RETURNS_INPUT = FINAL_DATA_DIR / "coingecko_top10_2020_returns_volatility.csv"
SEC_INPUT = PROCESSED_DATA_DIR / "sec_press_litigation_crypto_only.csv"
OUTPUT_FILE = FINAL_DATA_DIR / "crypto_reg_event_panel.csv"
METADATA_FILE = FINAL_DATA_DIR / "crypto_reg_event_panel_metadata.md"


STABLECOIN_SYMBOLS = {
    "usdt", "usdc", "dai", "busd", "tusd", "usdp", "ust", "frax", "lusd", "gusd"
}
CENTRALIZED_EXCHANGE_SYMBOLS = {
    "bnb", "ftt", "okb", "leo", "ht", "kcs", "cro", "bgb", "gt"
}
DEFI_SYMBOLS = {
    "uni", "aave", "mkr", "comp", "crv", "snx", "sushi", "yfi", "1inch", "bal", "cake"
}


@dataclass
class RowPoint:
    date: str
    coin_symbol: str
    coin_name: str
    coin_rank: str
    market_cap: str
    total_volume: str
    rolling_vol_30d: str
    log_return: float | None


def parse_float(value: str) -> float | None:
    text = (value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def format_float(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.12g}"


def classify_group(symbol: str) -> str:
    s = symbol.lower().strip()
    if s in STABLECOIN_SYMBOLS:
        return "stablecoin"
    if s in CENTRALIZED_EXCHANGE_SYMBOLS:
        return "centralized_exchange"
    if s in DEFI_SYMBOLS:
        return "defi"
    return "defi"


def pearson_corr(x: List[float], y: List[float]) -> float | None:
    if len(x) != len(y) or len(x) < 2:
        return None
    mean_x = statistics.fmean(x)
    mean_y = statistics.fmean(y)
    dx = [v - mean_x for v in x]
    dy = [v - mean_y for v in y]
    sum_prod = sum(a * b for a, b in zip(dx, dy))
    sum_x2 = sum(a * a for a in dx)
    sum_y2 = sum(b * b for b in dy)
    if sum_x2 <= 0 or sum_y2 <= 0:
        return None
    return sum_prod / math.sqrt(sum_x2 * sum_y2)


def rolling_corr_from_pairs(pairs: List[Tuple[float, float]], window: int) -> float | None:
    if len(pairs) < window:
        return None
    segment = pairs[-window:]
    x = [a for a, _ in segment]
    y = [b for _, b in segment]
    return pearson_corr(x, y)


def read_sec_event_dates(path: Path) -> set[str]:
    if not path.exists():
        raise FileNotFoundError(f"Missing SEC input file: {path}")

    event_dates: set[str] = set()
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            value = (row.get("event_date") or "").strip()
            if not value:
                continue
            try:
                dt = datetime.strptime(value, "%Y-%m-%d")
                event_dates.add(dt.strftime("%Y-%m-%d"))
            except ValueError:
                continue
    return event_dates


def read_return_rows(path: Path) -> Dict[str, List[RowPoint]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing returns/volatility input file: {path}")

    by_symbol: Dict[str, List[RowPoint]] = defaultdict(list)
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            symbol = (row.get("coin_symbol") or "").strip().lower()
            date = (row.get("date") or "").strip()
            if not symbol or not date:
                continue
            point = RowPoint(
                date=date,
                coin_symbol=symbol,
                coin_name=(row.get("coin_name") or "").strip(),
                coin_rank=(row.get("coin_rank") or "").strip(),
                market_cap=(row.get("market_cap") or "").strip(),
                total_volume=(row.get("total_volume") or "").strip(),
                rolling_vol_30d=(row.get("rolling_vol_30d") or "").strip(),
                log_return=parse_float(row.get("log_return", "")),
            )
            by_symbol[symbol].append(point)

    for symbol in by_symbol:
        by_symbol[symbol].sort(key=lambda point: point.date)
    return by_symbol


def build_panel(by_symbol: Dict[str, List[RowPoint]], sec_event_dates: set[str]) -> List[Dict[str, str]]:
    if "btc" not in by_symbol:
        raise RuntimeError("Bitcoin symbol 'btc' not found; cannot compute BTC correlation control.")

    btc_return_by_date: Dict[str, float] = {}
    for point in by_symbol["btc"]:
        if point.log_return is not None:
            btc_return_by_date[point.date] = point.log_return

    out_rows: List[Dict[str, str]] = []
    for symbol, group in by_symbol.items():
        pair_history: List[Tuple[float, float]] = []

        for point in group:
            token_lr = point.log_return
            btc_lr = btc_return_by_date.get(point.date)
            if token_lr is not None and btc_lr is not None:
                pair_history.append((token_lr, btc_lr))

            if symbol == "btc":
                btc_corr_30d = 1.0 if token_lr is not None else None
            else:
                btc_corr_30d = rolling_corr_from_pairs(pair_history, 30)

            out_rows.append(
                {
                    "date": point.date,
                    "coin_symbol": point.coin_symbol,
                    "coin_name": point.coin_name,
                    "coin_rank": point.coin_rank,
                    "token_group": classify_group(point.coin_symbol),
                    "outcome_realized_vol_30d": point.rolling_vol_30d,
                    "driver_sec_event_indicator": "1" if point.date in sec_event_dates else "0",
                    "control_market_cap": point.market_cap,
                    "control_total_volume": point.total_volume,
                    "control_btc_corr_30d": format_float(btc_corr_30d),
                }
            )

    out_rows.sort(key=lambda row: (row["date"], row["coin_symbol"]))
    return out_rows


def write_csv(path: Path, rows: List[Dict[str, str]]) -> None:
    if not rows:
        raise RuntimeError("No rows to write for event-study panel.")

    headers = [
        "date",
        "coin_symbol",
        "coin_name",
        "coin_rank",
        "token_group",
        "outcome_realized_vol_30d",
        "driver_sec_event_indicator",
        "control_market_cap",
        "control_total_volume",
        "control_btc_corr_30d",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def write_metadata(path: Path, rows: List[Dict[str, str]], sec_event_dates: set[str]) -> None:
    symbols = sorted({row["coin_symbol"] for row in rows})
    groups = sorted({row["token_group"] for row in rows})
    start = min(row["date"] for row in rows)
    end = max(row["date"] for row in rows)
    n_events = sum(1 for row in rows if row["driver_sec_event_indicator"] == "1")

    lines = [
        "# Crypto Regulatory Event Panel Metadata",
        "",
        "## Inputs",
        f"- Returns/volatility input: {RETURNS_INPUT}",
        f"- SEC events input: {SEC_INPUT}",
        "",
        "## Variables",
        "- Outcome: `outcome_realized_vol_30d` (from rolling_vol_30d)",
        "- Driver: `driver_sec_event_indicator` (1 if date is an SEC event_date else 0)",
        "- Controls: `control_market_cap`, `control_total_volume`, `control_btc_corr_30d`",
        "- Group variable: `token_group` in {defi, centralized_exchange, stablecoin}",
        "",
        "## Grouping Rule",
        "- Symbol list mapping for stablecoins and centralized-exchange tokens is hard-coded in script.",
        "- All remaining tokens default to `defi`.",
        "",
        "## Panel Summary",
        f"- Rows: {len(rows)}",
        f"- Tokens: {len(symbols)} ({', '.join(symbols)})",
        f"- Groups present: {', '.join(groups)}",
        f"- Date range: {start} to {end}",
        f"- Unique SEC action dates in input: {len(sec_event_dates)}",
        f"- Panel rows flagged with SEC indicator = 1: {n_events}",
        "",
        "## Missing-Value Decisions",
        "- Outcome/control fields are carried from upstream processed files.",
        "- No additional imputation is performed in this panel step.",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    sec_event_dates = read_sec_event_dates(SEC_INPUT)
    by_symbol = read_return_rows(RETURNS_INPUT)
    rows = build_panel(by_symbol, sec_event_dates)
    write_csv(OUTPUT_FILE, rows)
    write_metadata(METADATA_FILE, rows, sec_event_dates)

    print(f"Saved panel: {OUTPUT_FILE}")
    print(f"Saved metadata: {METADATA_FILE}")
    print(f"Rows: {len(rows)} | Tokens: {len(by_symbol)} | SEC event dates: {len(sec_event_dates)}")


if __name__ == "__main__":
    main()