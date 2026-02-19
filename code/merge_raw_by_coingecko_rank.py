#!/usr/bin/env python3
import csv
import json
import re
import time
from html import unescape
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
OUTPUT_FILE = ROOT / "data" / "raw" / "raw_merged_coingecko_ranked.csv"
API_URL = "https://api.coingecko.com/api/v3/coins/markets"
WEB_URL = "https://www.coingecko.com/en"


def _request_text(url: str, timeout: int = 30) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
        },
    )
    with urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="ignore")


def fetch_ranked_symbols(max_pages: int = 10, per_page: int = 250) -> Dict[str, Dict[str, str]]:
    symbol_to_coin: Dict[str, Dict[str, str]] = {}

    for page in range(1, max_pages + 1):
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": per_page,
            "page": page,
            "sparkline": "false",
        }
        url = f"{API_URL}?{urlencode(params)}"

        payload = None
        for attempt in range(3):
            try:
                payload = _request_text(url, timeout=30)
                break
            except (HTTPError, URLError, TimeoutError):
                if attempt == 2:
                    raise
                time.sleep(1.5 * (attempt + 1))

        data = json.loads(payload)
        if not data:
            break

        for row in data:
            symbol = str(row.get("symbol", "")).lower().strip()
            if not symbol or symbol in symbol_to_coin:
                continue

            rank = row.get("market_cap_rank")
            if rank is None:
                continue

            symbol_to_coin[symbol] = {
                "coin_id": str(row.get("id", "")),
                "coin_name": str(row.get("name", "")),
                "coin_symbol": symbol,
                "coin_rank": str(rank),
            }

    return symbol_to_coin


def fetch_ranked_symbols_from_web(max_pages: int = 10) -> Dict[str, Dict[str, str]]:
    symbol_to_coin: Dict[str, Dict[str, str]] = {}
    row_pattern = re.compile(r"<tr[^>]*>(.*?)</tr>", re.DOTALL | re.IGNORECASE)
    rank_pattern = re.compile(
        r'<td class="tw-sticky tw-left-\[34px\] gecko-sticky">\s*(\d+)\s*</td>',
        re.IGNORECASE,
    )
    symbol_pattern = re.compile(r'alt="([A-Za-z0-9_\.-]+)"', re.IGNORECASE)
    name_pattern = re.compile(
        r'tw-text-gray-700 dark:tw-text-moon-100 tw-font-semibold tw-text-sm tw-leading-5">\s*([^<\n][^<]*?)\s*<',
        re.IGNORECASE,
    )

    for page in range(1, max_pages + 1):
        url = f"{WEB_URL}?page={page}"
        try:
            html = _request_text(url, timeout=30)
        except (HTTPError, URLError, TimeoutError):
            continue

        rows = row_pattern.findall(html)
        if not rows:
            continue

        page_added = 0
        for row_html in rows:
            rank_match = rank_pattern.search(row_html)
            symbol_match = symbol_pattern.search(row_html)
            if not rank_match or not symbol_match:
                continue

            symbol = symbol_match.group(1).lower().strip()
            if not symbol or symbol in symbol_to_coin:
                continue

            rank = rank_match.group(1)
            name_match = name_pattern.search(row_html)
            name = unescape(name_match.group(1).strip()) if name_match else ""

            symbol_to_coin[symbol] = {
                "coin_id": "",
                "coin_name": name,
                "coin_symbol": symbol,
                "coin_rank": rank,
            }
            page_added += 1

        if page_added == 0:
            break

    return symbol_to_coin


def list_input_files(raw_dir: Path) -> List[Path]:
    return sorted(raw_dir.glob("*-usd-max.csv"), key=lambda p: p.name.lower())


def parse_symbol_from_filename(path: Path) -> str:
    return path.name[: -len("-usd-max.csv")].lower()


def build_merged_rows(
    files: List[Path],
    symbol_to_coin: Dict[str, Dict[str, str]],
) -> Tuple[List[str], List[Dict[str, str]], List[str]]:
    all_rows: List[Dict[str, str]] = []
    unmatched_files: List[str] = []
    base_headers: List[str] = []

    for file_path in files:
        symbol = parse_symbol_from_filename(file_path)
        coin_meta = symbol_to_coin.get(symbol)

        if coin_meta is None:
            unmatched_files.append(file_path.name)
            coin_meta = {
                "coin_id": "",
                "coin_name": "",
                "coin_symbol": symbol,
                "coin_rank": "999999",
            }

        with file_path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            if not base_headers:
                base_headers = list(reader.fieldnames or [])

            for row in reader:
                merged = {
                    "coin_rank": coin_meta["coin_rank"],
                    "coin_id": coin_meta["coin_id"],
                    "coin_name": coin_meta["coin_name"],
                    "coin_symbol": coin_meta["coin_symbol"],
                    "source_file": file_path.name,
                    **{k: row.get(k, "") for k in base_headers},
                }
                all_rows.append(merged)

    all_rows.sort(
        key=lambda item: (
            int(item["coin_rank"]),
            item["coin_symbol"],
            item.get("snapped_at", ""),
        )
    )

    output_headers = [
        "coin_rank",
        "coin_id",
        "coin_name",
        "coin_symbol",
        "source_file",
        *base_headers,
    ]
    return output_headers, all_rows, unmatched_files


def main() -> None:
    input_files = list_input_files(RAW_DIR)
    if not input_files:
        raise FileNotFoundError(f"No '*-usd-max.csv' files found in {RAW_DIR}")

    try:
        symbol_to_coin = fetch_ranked_symbols()
    except (HTTPError, URLError, TimeoutError):
        symbol_to_coin = fetch_ranked_symbols_from_web()

    if not symbol_to_coin:
        raise RuntimeError("Unable to fetch CoinGecko ranking from API or website.")

    headers, merged_rows, unmatched = build_merged_rows(input_files, symbol_to_coin)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(merged_rows)

    print(f"Wrote merged file: {OUTPUT_FILE}")
    print(f"Input files merged: {len(input_files)}")
    print(f"Rows written: {len(merged_rows)}")
    if unmatched:
        print("Files not matched to CoinGecko symbols (placed last):")
        for name in unmatched:
            print(f"  - {name}")


if __name__ == "__main__":
    main()