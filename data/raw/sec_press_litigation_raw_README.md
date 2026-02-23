# SEC Press + Litigation Raw Data

This file is intentionally kept separate from your current crypto panel data.

## Files

- `data/raw/sec_press_litigation_raw.csv` — standalone raw event dataset
- `code/fetch_sec_releases_raw.py` — fetcher script

## Run

Use a contact-style user agent to follow SEC fair-access guidance:

```bash
export SEC_USER_AGENT="Your Name your_email@domain.com"
python code/fetch_sec_releases_raw.py
```

Optional custom output path:

```bash
python code/fetch_sec_releases_raw.py --output data/raw/sec_press_litigation_raw.csv
```

## Current status

- The CSV is created with headers now.
- You can run the script to populate rows.
- Keep this dataset raw; clean later in a processed file.
