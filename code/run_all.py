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
    "code/fetch_coingecko_data.py",
    "code/fetch_sec_data.py",
    "code/fetch_macro_data.py",
    "code/fetch_crypto_event_data.py",
    "code/merge_final_panel.py",
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
