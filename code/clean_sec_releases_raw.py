#!/usr/bin/env python3
"""
Clean SEC raw press/litigation pull into processed event files.

Inputs:
  - data/raw/sec_press_litigation_raw.csv

Outputs:
  - data/processed/sec_press_litigation_clean.csv
  - data/processed/sec_press_litigation_crypto_only.csv

This script does NOT merge with the crypto panel.
"""

from __future__ import annotations

import argparse
import csv
import re
from datetime import datetime
from html import unescape
from pathlib import Path

from config_paths import PROCESSED_DATA_DIR, RAW_DATA_DIR


NOISE_TITLE_PATTERNS = [
    r"^date sort",
    r"^sort\b",
    r"^filter\b",
]

CRYPTO_PATTERN = re.compile(
    (
        r"\bcrypto\b|\bcryptocurrency\b|digital\s+asset|\btoken\b|\bcoin\b|\bbitcoin\b|\bethereum\b|\bxrp\b|\bripple\b|"
        r"binance|coinbase|kraken|stablecoin|defi|nft|exchange\s+ban|staking|blockchain"
    ),
    re.IGNORECASE | re.VERBOSE,
)


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    text = unescape(str(value)).strip()
    return re.sub(r"\s+", " ", text)


def normalize_url(value: object) -> str:
    url = normalize_text(value)
    if not url:
        return ""
    url = url.replace("&amp;", "&")
    if url.startswith("/"):
        url = f"https://www.sec.gov{url}"
    return url


def parse_date_text(value: str) -> str:
    text = normalize_text(value)
    if not text:
        return ""

    for fmt in [
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y",
        "%B %d, %Y",
        "%b %d, %Y",
        "%b. %d, %Y",
    ]:
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def to_event_month(event_date: str) -> str:
    if not event_date:
        return ""
    return f"{event_date[:7]}-01"


def is_noise_row(title: str, url: str) -> bool:
    title_lower = title.lower().strip()
    if not title_lower:
        return True

    for pattern in NOISE_TITLE_PATTERNS:
        if re.search(pattern, title_lower):
            return True

    if url.startswith("?populate=") or "order=field_publish_date" in url:
        return True

    return False


def build_clean_rows(raw_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    clean_rows: list[dict[str, str]] = []
    seen = set()

    for row in raw_rows:
        source_type = normalize_text(row.get("source_type", ""))
        release_id = normalize_text(row.get("release_id", ""))
        title = normalize_text(row.get("title", ""))
        url = normalize_url(row.get("url", ""))
        raw_date_text = normalize_text(row.get("raw_date_text", ""))
        ingested_at_utc = normalize_text(row.get("ingested_at_utc", ""))

        if is_noise_row(title, url):
            continue

        key = (source_type, url, title)
        if key in seen:
            continue
        seen.add(key)

        event_date = parse_date_text(row.get("event_date", ""))
        if not event_date:
            event_date = parse_date_text(raw_date_text)

        event_family = {
            "sec_press_release": "press_release",
            "sec_litigation_release": "litigation_release",
        }.get(source_type, "other")

        text_for_match = f"{title} {url} {release_id}"
        is_crypto = bool(CRYPTO_PATTERN.search(text_for_match))

        clean_rows.append(
            {
                "event_date": event_date,
                "event_month": to_event_month(event_date),
                "event_family": event_family,
                "source_type": source_type,
                "release_id": release_id,
                "title": title,
                "url": url,
                "regulator": "SEC",
                "jurisdiction": "US",
                "is_probable_crypto_event": "true" if is_crypto else "false",
                "raw_date_text": raw_date_text,
                "ingested_at_utc": ingested_at_utc,
            }
        )

    clean_rows.sort(key=lambda item: (item["event_date"] or "9999-99-99", item["source_type"], item["title"]))
    for index, row in enumerate(clean_rows, start=1):
        row["event_id"] = f"SEC-{index:06d}"

    ordered_columns = [
        "event_id",
        "event_date",
        "event_month",
        "event_family",
        "source_type",
        "release_id",
        "title",
        "url",
        "regulator",
        "jurisdiction",
        "is_probable_crypto_event",
        "raw_date_text",
        "ingested_at_utc",
    ]
    return [{column: row.get(column, "") for column in ordered_columns} for row in clean_rows]


def read_raw_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    headers = [
        "event_id",
        "event_date",
        "event_month",
        "event_family",
        "source_type",
        "release_id",
        "title",
        "url",
        "regulator",
        "jurisdiction",
        "is_probable_crypto_event",
        "raw_date_text",
        "ingested_at_utc",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean SEC raw releases into processed event files.")
    parser.add_argument(
        "--input",
        type=Path,
        default=RAW_DATA_DIR / "sec_press_litigation_raw.csv",
        help="Path to SEC raw CSV.",
    )
    parser.add_argument(
        "--output-clean",
        type=Path,
        default=PROCESSED_DATA_DIR / "sec_press_litigation_clean.csv",
        help="Output path for cleaned all-events file.",
    )
    parser.add_argument(
        "--output-crypto",
        type=Path,
        default=PROCESSED_DATA_DIR / "sec_press_litigation_crypto_only.csv",
        help="Output path for probable crypto-only subset.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.input.exists():
        raise FileNotFoundError(f"Input file not found: {args.input}")

    raw_rows = read_raw_csv(args.input)
    clean_rows = build_clean_rows(raw_rows)
    crypto_rows = [row for row in clean_rows if row.get("is_probable_crypto_event") == "true"]

    write_csv(args.output_clean, clean_rows)
    write_csv(args.output_crypto, crypto_rows)

    print(f"Saved cleaned SEC events: {args.output_clean} ({len(clean_rows)} rows)")
    print(f"Saved crypto subset: {args.output_crypto} ({len(crypto_rows)} rows)")


if __name__ == "__main__":
    main()
