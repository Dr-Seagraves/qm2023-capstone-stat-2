#!/usr/bin/env python3
"""
QM 2023 Capstone Project: M1 - Macro Controls Fetch & Clean

Runs macro supplementary dataset cleaning for VIX, effective Fed funds, and EPU.
"""

from __future__ import annotations

# Section 1: Imports and config_paths-based cleaning module
from clean_macro_series import main


# Section 2: Fetch/load + clean supplementary macro datasets
# Section 3: Save cleaned outputs to data/processed/
if __name__ == "__main__":
    main()
