#!/usr/bin/env python3
"""
Helper script to ingest and compute emissions for a single date or date range.
Usage:
    python scripts/ingest_and_compute.py 2025-12-25
    python scripts/ingest_and_compute.py 2025-12-25 2025-12-30  (date range)
"""
import sys
from datetime import datetime, timedelta
from ingest_day import ingest_day
from compute_co2_day import compute_day

def parse_date(date_str: str) -> datetime:
    """Parse YYYY-MM-DD format."""
    return datetime.strptime(date_str, "%Y-%m-%d")

def date_range(start: str, end: str):
    """Generate dates from start to end (inclusive)."""
    start_dt = parse_date(start)
    end_dt = parse_date(end)
    current = start_dt
    while current <= end_dt:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python scripts/ingest_and_compute.py YYYY-MM-DD")
        print("  python scripts/ingest_and_compute.py YYYY-MM-DD YYYY-MM-DD  (date range)")
        sys.exit(1)
    
    start_date = sys.argv[1]
    end_date = sys.argv[2] if len(sys.argv) > 2 else start_date
    
    dates = list(date_range(start_date, end_date))
    print(f"Processing {len(dates)} date(s): {start_date} to {end_date}")
    print()
    
    for date_str in dates:
        print(f"\n{'='*60}")
        print(f"Processing: {date_str}")
        print(f"{'='*60}")
        
        try:
            # Step 1: Ingest flight data
            print(f"\n[1/2] Ingesting flight data for {date_str}...")
            ingest_day(date_str)
            
            # Step 2: Compute emissions
            print(f"\n[2/2] Computing emissions for {date_str}...")
            compute_day(date_str)
            
            print(f"\n✓ Successfully processed {date_str}")
            
        except Exception as e:
            print(f"\n✗ Error processing {date_str}: {e}")
            print(f"  Continuing with next date...")
            continue
    
    print(f"\n{'='*60}")
    print(f"Finished processing {len(dates)} date(s)")



