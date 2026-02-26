#!/usr/bin/env python3
"""
QM 2023 Capstone Project: M1 - Crypto Event Panel Build

Builds processed crypto event-study panel from cleaned CoinGecko and SEC inputs.
"""

from __future__ import annotations

# Section 1: Imports and config_paths-based build module
from build_crypto_reg_event_panel import main


# Section 2: Build processed event-study panel
# Section 3: Save panel + metadata outputs
if __name__ == "__main__":
    main()
