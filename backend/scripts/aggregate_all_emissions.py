#!/usr/bin/env python3
"""
Aggregate all emissions CSV files into a single CSV file.
- Removes duplicates
- Sorts by date
- Adds date column for easy filtering
"""

import os
import sys
import pandas as pd
from datetime import datetime
import glob

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app.storage import DATA_DIR

def aggregate_all_emissions():
    print("🔍 Finding all emissions files...")
    emissions_files = sorted(glob.glob(os.path.join(DATA_DIR, "emissions_*.csv")))
    
    if not emissions_files:
        print("❌ No emissions files found!")
        return
    
    print(f"✅ Found {len(emissions_files)} emissions files\n")
    
    all_dataframes = []
    dates_processed = []
    
    for file_path in emissions_files:
        filename = os.path.basename(file_path)
        # Extract date from filename: emissions_YYYYMMDD.csv
        date_str = filename.replace("emissions_", "").replace(".csv", "")
        
        try:
            # Parse date to validate and sort
            date_obj = datetime.strptime(date_str, "%Y%m%d")
            
            print(f"  📂 Loading {filename}...", end=" ")
            
            # Load the CSV
            df = pd.read_csv(file_path, low_memory=False)
            
            if len(df) == 0:
                print("⚠️  Empty file, skipping")
                continue
            
            # Add date column for easy filtering
            df['date'] = date_obj.strftime("%Y-%m-%d")
            df['date_yyyymmdd'] = date_str
            
            all_dataframes.append(df)
            dates_processed.append(date_str)
            
            print(f"✅ {len(df):,} rows")
            
        except Exception as e:
            print(f"❌ Error loading {filename}: {e}")
            continue
    
    if not all_dataframes:
        print("\n❌ No valid emissions data found!")
        return
    
    print(f"\n📊 Aggregating {len(all_dataframes)} files...")
    
    # Combine all dataframes
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    
    print(f"  Total rows before deduplication: {len(combined_df):,}")
    
    # Remove duplicates
    # Check which columns to use for deduplication (typically flight identifiers)
    # Common columns that identify unique flights: icao24, callsign, dep, arr, firstSeen, lastSeen
    dedupe_columns = []
    if 'icao24' in combined_df.columns:
        dedupe_columns.append('icao24')
    if 'callsign' in combined_df.columns:
        dedupe_columns.append('callsign')
    if 'dep' in combined_df.columns:
        dedupe_columns.append('dep')
    if 'arr' in combined_df.columns:
        dedupe_columns.append('arr')
    if 'firstSeen' in combined_df.columns:
        dedupe_columns.append('firstSeen')
    if 'lastSeen' in combined_df.columns:
        dedupe_columns.append('lastSeen')
    if 'date' in combined_df.columns:
        dedupe_columns.append('date')
    
    if dedupe_columns:
        print(f"  Deduplicating based on: {', '.join(dedupe_columns)}")
        combined_df = combined_df.drop_duplicates(subset=dedupe_columns, keep='first')
    else:
        # Fallback: remove exact duplicates
        print("  Deduplicating: removing exact duplicate rows")
        combined_df = combined_df.drop_duplicates(keep='first')
    
    print(f"  Total rows after deduplication: {len(combined_df):,}")
    
    # Sort by date (chronological order)
    if 'date' in combined_df.columns:
        combined_df = combined_df.sort_values('date', ascending=True)
        print(f"  Sorted by date (chronological order)")
    
    # Save to single CSV file
    output_file = os.path.join(DATA_DIR, "all_emissions_combined.csv")
    combined_df.to_csv(output_file, index=False)
    
    file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
    
    # Get date range
    if 'date' in combined_df.columns:
        min_date = combined_df['date'].min()
        max_date = combined_df['date'].max()
        unique_dates = combined_df['date'].nunique()
    
    print(f"\n✅ Successfully created combined file!")
    print(f"  📁 File: {os.path.basename(output_file)}")
    print(f"  📊 Total rows: {len(combined_df):,}")
    print(f"  💾 File size: {file_size_mb:.2f} MB")
    if 'date' in combined_df.columns:
        print(f"  📅 Date range: {min_date} to {max_date}")
        print(f"  📅 Unique dates: {unique_dates}")
    print(f"  📋 Columns: {', '.join(combined_df.columns.tolist())}")
    
    print(f"\n📝 Processed dates ({len(dates_processed)}):")
    for date_str in sorted(dates_processed):
        date_obj = datetime.strptime(date_str, "%Y%m%d")
        print(f"  • {date_obj.strftime('%Y-%m-%d')}")

if __name__ == "__main__":
    aggregate_all_emissions()



