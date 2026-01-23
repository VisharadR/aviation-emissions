#!/usr/bin/env python3
"""
Script to compute emissions for flights files that don't have corresponding emissions files.
This helps recover from cases where flights were fetched but emissions computation failed.
"""
import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.storage import DATA_DIR, load_parquet
from app.main import compute_day_internal

def main():
    print("🔧 Computing missing emissions files...")
    print(f"Data directory: {DATA_DIR}\n")
    
    # Find flights files
    flights_files = [f for f in os.listdir(DATA_DIR) if f.startswith('flights_') and f.endswith('.csv')]
    
    missing_emissions = []
    for f_file in flights_files:
        date_str = f_file.replace('flights_', '').replace('.csv', '')
        e_file = f"emissions_{date_str}.csv"
        e_path = os.path.join(DATA_DIR, e_file)
        
        if not os.path.exists(e_path):
            # Check if flights file is valid
            f_path = os.path.join(DATA_DIR, f_file)
            try:
                file_size = os.path.getsize(f_path)
                if file_size > 0:
                    # Try to read it
                    import pandas as pd
                    df = pd.read_csv(f_path)
                    if len(df) > 0:
                        missing_emissions.append(date_str)
            except Exception as e:
                print(f"⚠️  Skipping {f_file}: {e}")
    
    if not missing_emissions:
        print("✅ All flights files have corresponding emissions files!")
        return
    
    print(f"Found {len(missing_emissions)} flights files without emissions files\n")
    
    # Compute emissions for each
    successful = 0
    failed = 0
    
    for i, date_key in enumerate(sorted(missing_emissions), 1):
        try:
            # Convert YYYYMMDD to YYYY-MM-DD
            date_str = f"{date_key[:4]}-{date_key[4:6]}-{date_key[6:8]}"
            
            print(f"[{i}/{len(missing_emissions)}] Computing emissions for {date_str}...", end=" ")
            
            count = compute_day_internal(date_str, progress_callback=None)
            successful += 1
            print(f"✅ {count:,} flights")
            
        except Exception as e:
            failed += 1
            print(f"❌ Failed: {str(e)}")
    
    print(f"\n📊 Summary:")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")
    print(f"  Total: {len(missing_emissions)}")

if __name__ == "__main__":
    main()



