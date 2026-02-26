#!/usr/bin/env python3
"""
QM 2023 Capstone Project: M1 - SEC Data Fetch & Clean

Runs SEC supplementary dataset pipeline:
1) fetch SEC press/litigation releases
2) clean and filter crypto-relevant events
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
    "code/fetch_sec_releases_raw.py",
    "code/clean_sec_releases_raw.py",
]


# Section 3: Fetch/load + clean supplementary SEC dataset
def run_script(script_rel_path: str) -> None:
    completed = subprocess.run([PYTHON, str(ROOT / script_rel_path)], cwd=ROOT)
    if completed.returncode != 0:
        raise RuntimeError(f"Script failed: {script_rel_path} (exit {completed.returncode})")


# Section 4: Save processed outputs
def main() -> None:
    for script in SCRIPTS:
        run_script(script)
    print("SEC fetch/clean pipeline completed.")


if __name__ == "__main__":
    main()
