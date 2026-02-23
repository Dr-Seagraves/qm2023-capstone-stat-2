#!/usr/bin/env python3
"""
Fetch SEC press releases + litigation releases into a standalone raw CSV.

Output file (separate from existing datasets):
  data/raw/sec_press_litigation_raw.csv

Notes:
- SEC applies fair-access rules and may block automated traffic.
- Set a contact user agent when running:
    export SEC_USER_AGENT="Your Name your_email@domain.com"
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import time
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from html import unescape
from pathlib import Path
from typing import Dict, List
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET

from config_paths import RAW_DATA_DIR


PRESS_URL = "https://www.sec.gov/news/pressreleases"
LITIGATION_URL = "https://www.sec.gov/litigation/litreleases"
PRESS_RSS_URL = "https://www.sec.gov/news/pressreleases.rss"
LITIGATION_RSS_URL = "https://www.sec.gov/enforcement-litigation/litigation-releases/rss"


def request_html(url: str, user_agent: str, timeout: int = 45) -> str:
    req = Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    with urlopen(req, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="ignore")


def request_text(url: str, user_agent: str, timeout: int = 45) -> str:
    req = Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept": "application/rss+xml,application/xml,text/xml,text/html,*/*",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    with urlopen(req, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="ignore")


def strip_tags(value: str) -> str:
    no_tags = re.sub(r"<[^>]+>", " ", value)
    return re.sub(r"\s+", " ", unescape(no_tags)).strip()


def parse_date(value: str) -> str:
    text = value.strip()
    patterns = [
        "%B %d, %Y",
        "%b. %d, %Y",
        "%b %d, %Y",
        "%m/%d/%Y",
        "%Y-%m-%d",
    ]
    for pattern in patterns:
        try:
            parsed = datetime.strptime(text, pattern)
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def parse_rss_pub_date(value: str) -> str:
    text = value.strip()
    if not text:
        return ""
    try:
        parsed = parsedate_to_datetime(text)
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(UTC).replace(tzinfo=None)
        return parsed.strftime("%Y-%m-%d")
    except (TypeError, ValueError):
        return parse_date(text)


def parse_rss_items(xml_text: str, source_type: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    root = ET.fromstring(xml_text)
    ns = {"dc": "http://purl.org/dc/elements/1.1/"}

    for item in root.findall("./channel/item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub_date_text = (item.findtext("pubDate") or "").strip()
        release_id = ""
        creator = item.find("dc:creator", ns)
        if creator is not None and creator.text:
            release_id = creator.text.strip()

        if not title and not link:
            continue

        rows.append(
            {
                "source_type": source_type,
                "event_date": parse_rss_pub_date(pub_date_text),
                "title": title,
                "url": link,
                "release_id": release_id,
                "raw_date_text": pub_date_text,
            }
        )

    return rows


def parse_press_releases(html: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    item_pattern = re.compile(r"<li[^>]*class=\"[^\"]*news-release[^\"]*\"[^>]*>(.*?)</li>", re.IGNORECASE | re.DOTALL)
    link_pattern = re.compile(r"<a[^>]+href=\"([^\"]+)\"[^>]*>(.*?)</a>", re.IGNORECASE | re.DOTALL)
    date_pattern = re.compile(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}", re.IGNORECASE)

    for block in item_pattern.findall(html):
        link_match = link_pattern.search(block)
        if not link_match:
            continue

        href = link_match.group(1).strip()
        title = strip_tags(link_match.group(2))
        date_match = date_pattern.search(strip_tags(block))
        event_date = parse_date(date_match.group(0)) if date_match else ""

        if href.startswith("/"):
            href = f"https://www.sec.gov{href}"

        rows.append(
            {
                "source_type": "sec_press_release",
                "event_date": event_date,
                "title": title,
                "url": href,
                "release_id": "",
                "raw_date_text": date_match.group(0) if date_match else "",
            }
        )

    return rows


def parse_litigation_releases(html: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    table_row_pattern = re.compile(r"<tr[^>]*>(.*?)</tr>", re.IGNORECASE | re.DOTALL)
    link_pattern = re.compile(r"<a[^>]+href=\"([^\"]+)\"[^>]*>(.*?)</a>", re.IGNORECASE | re.DOTALL)
    date_pattern = re.compile(r"\b\d{1,2}/\d{1,2}/\d{4}\b")
    release_pattern = re.compile(r"\bLR-?\d+\b", re.IGNORECASE)

    for block in table_row_pattern.findall(html):
        block_text = strip_tags(block)
        link_match = link_pattern.search(block)
        if not link_match:
            continue

        href = link_match.group(1).strip()
        title = strip_tags(link_match.group(2))

        date_match = date_pattern.search(block_text)
        event_date = parse_date(date_match.group(0)) if date_match else ""
        release_match = release_pattern.search(block_text)

        if href.startswith("/"):
            href = f"https://www.sec.gov{href}"

        rows.append(
            {
                "source_type": "sec_litigation_release",
                "event_date": event_date,
                "title": title,
                "url": href,
                "release_id": release_match.group(0).upper() if release_match else "",
                "raw_date_text": date_match.group(0) if date_match else "",
            }
        )

    return rows


def fetch_with_retry(url: str, user_agent: str, retries: int = 3, sleep_sec: float = 1.5) -> str:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return request_html(url, user_agent=user_agent)
        except (HTTPError, URLError, TimeoutError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(sleep_sec * attempt)
    raise RuntimeError(f"Failed to fetch {url}: {last_error}")


def fetch_rss_with_retry(url: str, user_agent: str, retries: int = 3, sleep_sec: float = 1.5) -> str:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return request_text(url, user_agent=user_agent)
        except (HTTPError, URLError, TimeoutError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(sleep_sec * attempt)
    raise RuntimeError(f"Failed to fetch {url}: {last_error}")


def deduplicate(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    unique_rows: List[Dict[str, str]] = []
    for row in rows:
        key = (row["source_type"], row["url"], row["title"])
        if key in seen:
            continue
        seen.add(key)
        unique_rows.append(row)
    unique_rows.sort(key=lambda r: (r["event_date"] or "9999-99-99", r["source_type"], r["title"]))
    return unique_rows


def write_csv(path: Path, rows: List[Dict[str, str]]) -> None:
    headers = [
        "source_type",
        "event_date",
        "release_id",
        "title",
        "url",
        "raw_date_text",
        "ingested_at_utc",
    ]

    ingested_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            out = dict(row)
            out["ingested_at_utc"] = ingested_at
            writer.writerow(out)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch SEC press + litigation releases into raw CSV.")
    parser.add_argument(
        "--output",
        type=Path,
        default=RAW_DATA_DIR / "sec_press_litigation_raw.csv",
        help="Output raw CSV path.",
    )
    parser.add_argument(
        "--user-agent",
        type=str,
        default=os.getenv("SEC_USER_AGENT", "Research Project research@example.com"),
        help="User-Agent string for SEC fair access (recommended: include name + email).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    rows: List[Dict[str, str]] = []

    try:
        press_rss = fetch_rss_with_retry(PRESS_RSS_URL, user_agent=args.user_agent)
        rows.extend(parse_rss_items(press_rss, "sec_press_release"))
    except RuntimeError:
        press_html = fetch_with_retry(PRESS_URL, user_agent=args.user_agent)
        rows.extend(parse_press_releases(press_html))

    try:
        litigation_rss = fetch_rss_with_retry(LITIGATION_RSS_URL, user_agent=args.user_agent)
        rows.extend(parse_rss_items(litigation_rss, "sec_litigation_release"))
    except RuntimeError:
        litigation_html = fetch_with_retry(LITIGATION_URL, user_agent=args.user_agent)
        rows.extend(parse_litigation_releases(litigation_html))

    rows = deduplicate(rows)

    if not rows:
        raise RuntimeError("No SEC rows parsed. SEC HTML structure may have changed.")

    write_csv(args.output, rows)
    print(f"Saved SEC raw file: {args.output}")
    print(f"Rows: {len(rows)}")


if __name__ == "__main__":
    main()
