#!/usr/bin/env python3
"""
Cleanup script to remove duplicate data files.
Removes old .parquet files since we're now using .csv format.
"""
import os
import sys

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

def cleanup_duplicates():
    """Remove duplicate parquet files (keep CSV versions)."""
    if not os.path.exists(DATA_DIR):
        print(f"Data directory not found: {DATA_DIR}")
        return
    
    parquet_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.parquet')]
    
    if not parquet_files:
        print("No parquet files found. Nothing to clean up.")
        return
    
    removed_count = 0
    total_size = 0
    
    for parquet_file in parquet_files:
        parquet_path = os.path.join(DATA_DIR, parquet_file)
        
        # Check if corresponding CSV exists
        csv_file = parquet_file.replace('.parquet', '.csv')
        csv_path = os.path.join(DATA_DIR, csv_file)
        
        if os.path.exists(csv_path):
            # CSV exists, remove parquet
            size = os.path.getsize(parquet_path)
            total_size += size
            os.remove(parquet_path)
            removed_count += 1
            print(f"✓ Removed {parquet_file} (CSV exists: {csv_file})")
        else:
            print(f"⚠ Kept {parquet_file} (no CSV equivalent found)")
    
    print(f"\n✓ Cleanup complete!")
    print(f"  Removed {removed_count} duplicate parquet file(s)")
    print(f"  Freed {total_size / (1024*1024):.2f} MB")

if __name__ == "__main__":
    print("Cleaning up duplicate data files...")
    print(f"Data directory: {DATA_DIR}\n")
    cleanup_duplicates()



