#!/usr/bin/env python3
"""
QM 2023 Capstone Project: M1 - CoinGecko Data Fetch & Clean

Runs the primary crypto dataset pipeline:
1) merge raw CoinGecko exports + ranking metadata
2) clean primary panel
3) build top-10 returns/volatility panel for downstream event merge
"""

from __future__ import annotations

# Section 1: Imports and config_paths-compatible root detection
import subprocess
import sys
from pathlib import Path

# Section 2: Script pipeline configuration
ROOT = Path(__file__).resolve().parents[1]
PYTHON = sys.executable
SCRIPTS = [
    "code/merge_raw_by_coingecko_rank.py",
    "code/clean_coingecko_data.py",
    "code/build_coingecko_top10_returns_volatility.py",
]


# Section 3: Load raw data and clean (delegated to modular scripts)
def run_script(script_rel_path: str) -> None:
    completed = subprocess.run([PYTHON, str(ROOT / script_rel_path)], cwd=ROOT)
    if completed.returncode != 0:
        raise RuntimeError(f"Script failed: {script_rel_path} (exit {completed.returncode})")


# Section 4: Save processed outputs
def main() -> None:
    for script in SCRIPTS:
        run_script(script)
    print("CoinGecko fetch/clean pipeline completed.")


if __name__ == "__main__":
    main()
