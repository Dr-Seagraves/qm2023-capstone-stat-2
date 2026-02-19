#!/usr/bin/env python3
"""
Clean Coingecko Ranking Data
============================

This script cleans the coingecko_ranking.csv dataset by:
1. Dropping rows with missing data
2. Filtering to include only data from the last 6 years (since Feb 19, 2020)
3. Saving the cleaned data to the processed data directory
"""

import pandas as pd
from pathlib import Path
from config_paths import RAW_DATA_DIR, PROCESSED_DATA_DIR

def clean_coingecko_data():
    # Input file
    input_file = RAW_DATA_DIR / 'coingecko_ranking.csv'
    
    # Output file
    output_file = PROCESSED_DATA_DIR / 'coingecko_ranking_cleaned.csv'
    
    # Read the CSV
    print("Reading data...")
    df = pd.read_csv(input_file)
    
    # Check initial shape
    print(f"Initial dataset shape: {df.shape}")
    
    
    # Drop rows with any missing values (treating empty strings as missing)
    print("Dropping rows with missing data...")
    df_clean = df.dropna(subset=["price", "market_cap", "total_volume"])
    
    print(f"After dropping missing data: {df_clean.shape}")
    
    # Convert snapped_at to datetime, removing ' UTC' if present
    print("Converting dates...")
    df_clean['snapped_at'] = pd.to_datetime(df_clean['snapped_at'].str.replace(' UTC', ''))
    
    # Filter to last 6 years: from Feb 19, 2020 onwards
    cutoff_date = pd.Timestamp('2020-02-19')
    df_filtered = df_clean[df_clean['snapped_at'] >= cutoff_date]
    
    print(f"After filtering to last 6 years: {df_filtered.shape}")
    
    # Save to processed directory
    print(f"Saving cleaned data to {output_file}...")
    df_filtered.to_csv(output_file, index=False)
    
    print("Cleaning complete!")

if __name__ == "__main__":
    clean_coingecko_data()
