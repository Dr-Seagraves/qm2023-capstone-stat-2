#!/usr/bin/env python3
"""
Run the full reproducible project pipeline in the required order.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYTHON = sys.executable

SCRIPTS = [
    "code/config_paths.py",
    "code/merge_raw_by_coingecko_rank.py",
    "code/clean_coingecko_data.py",
    "code/clean_macro_series.py",
    "code/fetch_sec_releases_raw.py",
    "code/clean_sec_releases_raw.py",
    "code/build_coingecko_top10_returns_volatility.py",
    "code/build_crypto_reg_event_panel.py",
    "code/merge_final_with_macro_controls.py",
]


def run_script(script_rel_path: str) -> None:
    script_path = ROOT / script_rel_path
    print(f"\n===== RUNNING {script_rel_path} =====")
    completed = subprocess.run([PYTHON, str(script_path)], cwd=ROOT)
    if completed.returncode != 0:
        raise RuntimeError(f"Script failed: {script_rel_path} (exit {completed.returncode})")


def main() -> None:
    for script in SCRIPTS:
        run_script(script)

    print("\nPipeline completed successfully.")


if __name__ == "__main__":
    main()
