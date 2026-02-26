#!/usr/bin/env python3
"""
QM 2023 Capstone Project: M1 - Final Panel Merge

Assignment-compatible entry point for final panel merging.
This script delegates to the existing merge logic in
`merge_final_with_macro_controls.py`.
"""

from __future__ import annotations

# Section 1: Imports and config_paths-compatible merge module
from merge_final_with_macro_controls import main


# Section 2: Load processed datasets and align time variables (delegated)
# Section 3: Merge processed datasets and verify integrity (delegated)
# Section 4: Save final panel to data/final/crypto_analysis_panel.csv (delegated)
# Section 5: Data dictionary is maintained at data/final/data_dictionary.md
if __name__ == "__main__":
    main()
