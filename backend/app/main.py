from fastapi import FastAPI, HTTPException, BackgroundTasks, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pandas as pd
from datetime import datetime, timezone, timedelta
import os
import sys
import importlib.util
import threading
import time
from typing import Dict
import traceback

# Add parent directory to path for importing ingest functions
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.storage import load_parquet, save_parquet
from app.airports import load_airports
from app.opensky_client import OpenSkyOAuthClient
from app.emissions import haversine_km, co2_from_distance_km, compute_emissions_vectorized

app = FastAPI(title="Aviation Emissions MVP")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global exception handler to catch and log all unhandled exceptions
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Catch all unhandled exceptions and return a proper error response."""
    error_traceback = traceback.format_exc()
    print(f"Internal Server Error on {request.method} {request.url.path}")
    print(f"Error: {str(exc)}")
    print(f"Traceback:\n{error_traceback}")
    
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "message": str(exc),
            "path": str(request.url.path),
            "method": request.method
        }
    )

# Store fetch job status and cancellation flags
fetch_jobs: Dict[str, Dict] = {}
cancellation_flags: Dict[str, bool] = {}  # Track cancellation requests

# Cache airports data (loaded once, reused for all requests)
_airports_cache: pd.DataFrame = None
def get_airports_cache():
    """Get airports data, loading it once and caching for subsequent requests."""
    global _airports_cache
    if _airports_cache is None:
        _airports_cache = load_airports("data/ourairports_airports.csv")
    return _airports_cache

@app.get("/")
def root():
    return {"message": "Aviation Emissions API is running. Try /health or /co2/summary/YYYY-MM-DD", "version": "1.0"}

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/test/opensky/{date_yyyymmdd}")
def test_opensky(date_yyyymmdd: str):
    """Test endpoint to directly query OpenSky API for a specific date."""
    try:
        day = datetime.fromisoformat(date_yyyymmdd).replace(tzinfo=timezone.utc)
        begin = int(day.timestamp())
        end = int(day.replace(hour=23, minute=59, second=59).timestamp())
        
        # Test with a small 2-hour chunk first
        test_end = min(begin + (2 * 3600), end)
        
        client = OpenSkyOAuthClient()
        print(f"🧪 Testing OpenSky API for {date_yyyymmdd} (timestamp {begin} to {test_end})")
        
        flights = client.flights_all(begin, test_end)
        
        return {
            "date": date_yyyymmdd,
            "timestamp_range": {"begin": begin, "end": test_end},
            "flights_count": len(flights) if flights else 0,
            "sample_flight": flights[0] if flights and len(flights) > 0 else None,
            "all_flights": flights[:5] if flights else [],  # Return first 5 for inspection
            "raw_response_type": type(flights).__name__,
        }
    except Exception as e:
        print(f"❌ Error testing OpenSky: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error testing OpenSky API: {str(e)}")

@app.get("/co2/summary/{date_yyyymmdd}")
def co2_summary(date_yyyymmdd: str):
    try:
        key = date_yyyymmdd.replace("-", "")

        candidate_names = [f"emissions_{key}"]

        df = None
        loaded_name = None
        for name in candidate_names:
            try: 
                df = load_parquet(name)
                loaded_name = name
                break
            except Exception as e:
                print(f"Failed to load {name}: {e}")
                pass
        
        if df is None or len(df) == 0:
            raise HTTPException(status_code=404, detail="Emissions data not found for this date. Use /fetch/{date_yyyymmdd} to fetch data from OpenSky first.")
        
        # Check required columns exist
        required_columns = ["co2_kg", "dep", "arr"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise HTTPException(
                status_code=500, 
                detail=f"Data file is missing required columns: {missing_columns}. The file may be corrupted."
            )
        
        # Optimize: Use more efficient aggregation
        # Pre-convert co2_kg to float if needed (should already be numeric)
        if df["co2_kg"].dtype != 'float64':
            df["co2_kg"] = pd.to_numeric(df["co2_kg"], errors='coerce')
        
        # Replace NaN values with 0 for co2_kg
        df["co2_kg"] = df["co2_kg"].fillna(0)
        
        total_co2_kg = float(df["co2_kg"].sum())
        flights = int(len(df))

        # Optimize: Use vectorized operations - groupby + sum is already optimized
        # But we can optimize by only grouping what we need
        co2_by_route = df.groupby(["dep", "arr"])["co2_kg"].sum()
        top_routes = (
            co2_by_route
            .sort_values(ascending=False)
            .head(15)
            .reset_index()
            .to_dict(orient="records")
        )

        co2_by_dep = df.groupby("dep")["co2_kg"].sum()
        top_dep_airports = (
            co2_by_dep
            .sort_values(ascending=False)
            .head(15)
            .reset_index()
            .to_dict(orient="records")
        )

        return {
            "date": date_yyyymmdd,
            "source_file": loaded_name,
            "flights_computed": flights,
            "total_co2_kg": total_co2_kg,
            "total_co2_tons": total_co2_kg / 1000.0,
            "top_routes": top_routes,
            "top_departure_airports": top_dep_airports,
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in co2_summary for {date_yyyymmdd}: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error processing data: {str(e)}")

def ingest_day_internal(date_yyyymmdd: str, progress_callback=None, cancellation_check=None):
    """Internal function to ingest flight data for a date.
    
    Args:
        date_yyyymmdd: Date string in YYYY-MM-DD format
        progress_callback: Optional callback function(message) for progress updates
        cancellation_check: Optional function() -> bool to check if operation should be cancelled
    """
    try:
        day = datetime.fromisoformat(date_yyyymmdd).replace(tzinfo=timezone.utc)
        begin = int(day.timestamp())
        end = int(day.replace(hour=23, minute=59, second=59).timestamp())
        
        # Calculate total chunks for progress
        total_chunks = ((end - begin) // (2 * 3600)) + 1
        current_chunk = 0
        
        client = OpenSkyOAuthClient()
        rows = []
        
        # Use parallel fetching with rate limiting (4 workers - conservative to avoid rate limits)
        # OpenSky rate limits are handled with global rate limiter + retry logic
        # Conservative settings reduce 429 errors which cause 10s+ delays
        # 4 workers = balanced speed while respecting rate limits
        # For single date (12 chunks = 48 credits), conservative approach avoids 429 errors
        parallel_workers = 4  # Conservative: avoids rate limit errors (429s cause long delays)
        
        if progress_callback:
            progress_callback(f"Fetching {total_chunks} chunks in parallel ({parallel_workers} workers)...")
        
        # Check for cancellation before starting
        if cancellation_check and cancellation_check():
            raise ValueError("Operation cancelled by user")
        
        # Track progress with callback - show individual chunk progress
        chunk_completed = [0]  # Use list to allow modification in nested function
        total_flights_so_far = [0]  # Track cumulative flights across all chunks
        
        def chunk_progress(completed, total, flights_count):
            chunk_completed[0] = completed
            total_flights_so_far[0] += flights_count  # Accumulate flights
            if progress_callback:
                percentage = int((completed / total) * 100) if total > 0 else 0
                # Show cumulative flights and current chunk flights
                if flights_count == 0:
                    # Some time periods (like late night) may have no flights - this is normal
                    progress_callback(f"Chunk {completed}/{total} ({percentage}%) - {flights_count} flights in this chunk, {total_flights_so_far[0]:,} total so far | Processing...")
                else:
                    progress_callback(f"Chunk {completed}/{total} ({percentage}%) - {flights_count:,} flights in this chunk, {total_flights_so_far[0]:,} total so far | Processing...")
        
        fetched_chunks = 0
        total_flights_seen = 0
        chunk_number = 0
        for t1, t2, flights in client.flights_all_chunked(begin, end, chunk_seconds=2*3600, max_workers=parallel_workers, progress_callback=chunk_progress):
            # Check for cancellation before processing each chunk
            if cancellation_check and cancellation_check():
                if progress_callback:
                    progress_callback("Cancellation requested. Stopping fetch...")
                raise ValueError("Operation cancelled by user")
            
            chunk_number += 1
            fetched_chunks += 1
            total_flights_seen += len(flights)
            
            # Update progress for each completed chunk with detailed info
            if progress_callback:
                chunk_start = datetime.fromtimestamp(t1, tz=timezone.utc)
                chunk_end = datetime.fromtimestamp(t2, tz=timezone.utc)
                chunk_time_range = f"{chunk_start.strftime('%H:%M')}-{chunk_end.strftime('%H:%M')} UTC"
                percentage = int((fetched_chunks / total_chunks) * 100) if total_chunks > 0 else 0
                if len(flights) == 0:
                    progress_callback(f"✓ Chunk {fetched_chunks}/{total_chunks} ({percentage}%) - {len(flights)} flights from {chunk_time_range} (no flights in this period) | Completed")
                else:
                    progress_callback(f"✓ Chunk {fetched_chunks}/{total_chunks} ({percentage}%) - {len(flights):,} flights from {chunk_time_range} | Completed")
            
            for f in flights:
                rows.append({
                    "icao24": f.get("icao24"),
                    "callsign": f.get("callsign"),
                    "firstSeen": f.get("firstSeen"),
                    "lastSeen": f.get("lastSeen"),
                    "dep": f.get("estDepartureAirport"),
                    "arr": f.get("estArrivalAirport"),
                })
        
        if progress_callback:
            progress_callback(f"Processed all {fetched_chunks}/{total_chunks} chunks. Total flights collected: {len(rows):,}. Processing...")
        
        df = pd.DataFrame(rows).drop_duplicates()
        name = f"flights_{day.strftime('%Y%m%d')}"
        
        # Handle empty data - don't save empty files
        if len(df) == 0:
            if progress_callback:
                progress_callback(f"No flight data available for {date_yyyymmdd}")
            # Don't save empty files - they cause issues later
            from app.storage import DATA_DIR
            csv_path = os.path.join(DATA_DIR, f"{name}.csv")
            # Remove empty file if it exists
            if os.path.exists(csv_path):
                try:
                    os.remove(csv_path)
                except:
                    pass
            raise ValueError(f"No flight data available from OpenSky for {date_yyyymmdd}")
        
        # Check if file already exists - if so, skip save to avoid duplicates
        from app.storage import DATA_DIR
        csv_path = os.path.join(DATA_DIR, f"{name}.csv")
        if os.path.exists(csv_path) and len(df) > 0:
            if progress_callback:
                progress_callback(f"Flight data already exists for {date_yyyymmdd}, skipping save")
            # Still return count for consistency
            existing_df = load_parquet(name)
            return len(existing_df)
        
        save_parquet(df, name)
        return len(df)
    except Exception as e:
        raise Exception(f"Error ingesting data: {str(e)}")

def compute_day_internal(date_yyyymmdd: str, progress_callback=None):
    """Internal function to compute emissions for a date."""
    try:
        if progress_callback:
            progress_callback("Loading flight data...")
        
        key = date_yyyymmdd.replace("-", "")
        flights_name = f"flights_{key}"
        
        # Check if flights file exists and is valid
        from app.storage import DATA_DIR
        flights_path = os.path.join(DATA_DIR, f"{flights_name}.csv")
        if not os.path.exists(flights_path):
            raise ValueError(f"Flight data file not found for {date_yyyymmdd}. Make sure ingestion completed successfully.")
        
        # Check if file is empty
        file_size = os.path.getsize(flights_path)
        if file_size == 0:
            raise ValueError(f"Flight data file is empty for {date_yyyymmdd}. No flights available for this date.")
        
        try:
            flights = load_parquet(flights_name)
        except Exception as e:
            raise ValueError(f"Error loading flight data for {date_yyyymmdd}: {str(e)}. File may be corrupted.")
        
        if len(flights) == 0:
            raise ValueError(f"No flight data in file for {date_yyyymmdd}")
        
        if progress_callback:
            progress_callback("Processing airports...")
        
        flights["dep"] = flights["dep"].astype(str).str.strip().str.upper()
        flights["arr"] = flights["arr"].astype(str).str.strip().str.upper()
        
        airports = get_airports_cache()  # Use cached airports (much faster)
        
        dep = airports.rename(columns={"icao": "dep", "lat": "dep_lat", "lon": "dep_lon"})
        arr = airports.rename(columns={"icao": "arr", "lat": "arr_lat", "lon": "arr_lon"})
        
        if progress_callback:
            progress_callback("Merging data...")
        
        df = flights.merge(dep[["dep", "dep_lat", "dep_lon"]], on="dep", how="left")
        df = df.merge(arr[["arr", "arr_lat", "arr_lon"]], on="arr", how="left")
        
        if progress_callback:
            progress_callback("Computing distances and emissions (optimized vectorized)...")
        
        # Optimized vectorized computation (40x+ faster than apply)
        df = compute_emissions_vectorized(df, use_gpu=False)
        
        computed = df.dropna(subset=["co2_kg"]).copy()
        out_name = f"emissions_{key}"
        
        # Check if file already exists - if so, skip save to avoid duplicates
        from app.storage import DATA_DIR
        csv_path = os.path.join(DATA_DIR, f"{out_name}.csv")
        if os.path.exists(csv_path) and len(computed) > 0:
            if progress_callback:
                progress_callback(f"Emissions data already exists for {date_yyyymmdd}, skipping save")
            # Still return count for consistency
            existing_df = load_parquet(out_name)
            return len(existing_df)
        
        save_parquet(computed, out_name)
        return len(computed)
    except Exception as e:
        raise Exception(f"Error computing emissions: {str(e)}")

def fetch_data_background(date_yyyymmdd: str):
    """Background task to fetch and process data."""
    job_id = date_yyyymmdd
    fetch_jobs[job_id] = {"status": "processing", "progress": "Starting...", "error": None}
    cancellation_flags[job_id] = False  # Initialize cancellation flag
    
    def is_cancelled():
        """Check if this job has been cancelled."""
        return cancellation_flags.get(job_id, False)
    
    def update_progress(msg):
        if job_id in fetch_jobs:
            fetch_jobs[job_id]["progress"] = msg
    
    try:
        # Check if data already exists first
        key = date_yyyymmdd.replace("-", "")
        emissions_name = f"emissions_{key}"
        from app.storage import DATA_DIR
        emissions_path = os.path.join(DATA_DIR, f"{emissions_name}.csv")
        
        if os.path.exists(emissions_path):
            update_progress("Data already exists, skipping fetch")
            existing_df = load_parquet(emissions_name)
            fetch_jobs[job_id] = {
                "status": "completed",
                "progress": "Done! (data already existed)",
                "flights_fetched": 0,
                "flights_computed": len(existing_df)
            }
            return
        
        # Check cancellation before fetching
        if is_cancelled():
            fetch_jobs[job_id] = {
                "status": "cancelled",
                "progress": "Operation cancelled by user",
                "error": None
            }
            return
        
        update_progress("Fetching flight data from OpenSky...")
        try:
            flights_count = ingest_day_internal(date_yyyymmdd, update_progress, cancellation_check=is_cancelled)
        except ValueError as e:
            if "cancelled" in str(e).lower():
                fetch_jobs[job_id] = {
                    "status": "cancelled",
                    "progress": "Operation cancelled by user",
                    "error": None
                }
                return
            else:
                raise
        
        # Check cancellation after fetching
        if is_cancelled():
            fetch_jobs[job_id] = {
                "status": "cancelled",
                "progress": "Operation cancelled after fetching flights",
                "error": None
            }
            return
        
        update_progress("Computing emissions...")
        emissions_count = compute_day_internal(date_yyyymmdd, update_progress)
        
        # Final cancellation check
        if is_cancelled():
            fetch_jobs[job_id] = {
                "status": "cancelled",
                "progress": "Operation cancelled after computing emissions",
                "error": None
            }
            return
        
        fetch_jobs[job_id] = {
            "status": "completed",
            "progress": "Done!",
            "flights_fetched": flights_count,
            "flights_computed": emissions_count
        }
    except Exception as e:
        fetch_jobs[job_id] = {
            "status": "error",
            "progress": "Failed",
            "error": str(e)
        }

@app.get("/check/{date_yyyymmdd}")
def check_data_exists(date_yyyymmdd: str, include_data: bool = False):
    """Quick check if data exists for a date. Optionally return the full summary data immediately."""
    try:
        key = date_yyyymmdd.replace("-", "")
        emissions_name = f"emissions_{key}"
        
        # Check if file exists first (faster than loading)
        from app.storage import DATA_DIR
        emissions_path = os.path.join(DATA_DIR, f"{emissions_name}.csv")
        
        if os.path.exists(emissions_path):
            # File exists, verify it's not empty
            file_size = os.path.getsize(emissions_path)
            if file_size > 0:
                try:
                    # Quick check - just read first few rows to verify it's valid
                    existing = load_parquet(emissions_name)
                    
                    # If include_data is True, return the full summary immediately
                    if include_data:
                        # Return the full summary data (same as co2_summary)
                        required_columns = ["co2_kg", "dep", "arr"]
                        missing_columns = [col for col in required_columns if col not in existing.columns]
                        if missing_columns:
                            return {"exists": False, "error": f"Data file is missing required columns: {missing_columns}"}
                        
                        # Optimize: Use more efficient aggregation
                        if existing["co2_kg"].dtype != 'float64':
                            existing["co2_kg"] = pd.to_numeric(existing["co2_kg"], errors='coerce')
                        existing["co2_kg"] = existing["co2_kg"].fillna(0)
                        
                        total_co2_kg = float(existing["co2_kg"].sum())
                        flights = int(len(existing))
                        
                        co2_by_route = existing.groupby(["dep", "arr"])["co2_kg"].sum()
                        top_routes = (
                            co2_by_route
                            .sort_values(ascending=False)
                            .head(15)
                            .reset_index()
                            .to_dict(orient="records")
                        )
                        
                        co2_by_dep = existing.groupby("dep")["co2_kg"].sum()
                        top_dep_airports = (
                            co2_by_dep
                            .sort_values(ascending=False)
                            .head(15)
                            .reset_index()
                            .to_dict(orient="records")
                        )
                        
                        return {
                            "exists": True,
                            "flights": flights,
                            "data": {
                                "date": date_yyyymmdd,
                                "source_file": emissions_name,
                                "flights_computed": flights,
                                "total_co2_kg": total_co2_kg,
                                "total_co2_tons": total_co2_kg / 1000.0,
                                "top_routes": top_routes,
                                "top_departure_airports": top_dep_airports,
                            }
                        }
                    
                    return {
                        "exists": True,
                        "flights": len(existing),
                        "date": date_yyyymmdd
                    }
                except Exception as load_error:
                    # File exists but can't be loaded - might be corrupted
                    return {
                        "exists": False,
                        "error": f"File exists but cannot be loaded: {str(load_error)}",
                        "date": date_yyyymmdd
                    }
            else:
                # Empty file
                return {
                    "exists": False,
                    "date": date_yyyymmdd
                }
        else:
            # File doesn't exist
            return {
                "exists": False,
                "date": date_yyyymmdd
            }
    except Exception as e:
        return {
            "exists": False,
            "error": str(e),
            "date": date_yyyymmdd
        }

@app.post("/fetch/{date_yyyymmdd}")
def fetch_data(date_yyyymmdd: str, background_tasks: BackgroundTasks):
    """
    Start fetching flight data from OpenSky and compute emissions for a given date.
    Returns immediately and processes in the background.
    """
    try:
        # Validate date format
        datetime.fromisoformat(date_yyyymmdd)
        
        # Quick check if data already exists
        key = date_yyyymmdd.replace("-", "")
        emissions_name = f"emissions_{key}"
        import os
        from app.storage import DATA_DIR
        emissions_path = os.path.join(DATA_DIR, f"{emissions_name}.csv")
        
        if os.path.exists(emissions_path):
            return {
                "status": "exists",
                "message": f"Data already exists for {date_yyyymmdd}",
                "date": date_yyyymmdd
            }
        
        # Check if already processing
        if date_yyyymmdd in fetch_jobs and fetch_jobs[date_yyyymmdd]["status"] == "processing":
            return {
                "status": "processing",
                "message": f"Data is already being fetched for {date_yyyymmdd}",
                "progress": fetch_jobs[date_yyyymmdd].get("progress", "Processing..."),
                "date": date_yyyymmdd
            }
        
        # Start background task
        background_tasks.add_task(fetch_data_background, date_yyyymmdd)
        
        return {
            "status": "started",
            "message": f"Started fetching data for {date_yyyymmdd}. This will take 1-3 minutes.",
            "date": date_yyyymmdd
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {date_yyyymmdd}. Use YYYY-MM-DD")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error starting fetch: {str(e)}")

@app.get("/fetch/status/{date_yyyymmdd}")
def fetch_status(date_yyyymmdd: str):
    """Get the status of a fetch job."""
    if date_yyyymmdd not in fetch_jobs:
        return {"status": "not_found", "date": date_yyyymmdd}
    return {**fetch_jobs[date_yyyymmdd], "date": date_yyyymmdd}

@app.post("/fetch/cancel/{date_yyyymmdd}")
def cancel_fetch(date_yyyymmdd: str):
    """Cancel a fetch job for a single date."""
    job_id = date_yyyymmdd
    if job_id not in fetch_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if fetch_jobs[job_id]["status"] not in ["processing"]:
        raise HTTPException(status_code=400, detail=f"Job is not running (status: {fetch_jobs[job_id]['status']})")
    
    # Set cancellation flag
    cancellation_flags[job_id] = True
    
    return {
        "status": "cancelling",
        "message": f"Cancellation requested for {date_yyyymmdd}. Operation will stop soon.",
        "job_id": job_id
    }

def aggregate_date_range(start_date: str, end_date: str, progress_callback=None):
    """Aggregate emissions data for a date range into a single DataFrame."""
    try:
        from datetime import timedelta
        start = datetime.fromisoformat(start_date).date()
        end = datetime.fromisoformat(end_date).date()
        
        if start > end:
            raise ValueError("Start date must be before end date")
        
        all_data = []
        current_date = start
        total_days = (end - start).days + 1
        current_day = 0
        
        while current_date <= end:
            current_day += 1
            date_str = current_date.strftime("%Y-%m-%d")
            date_key = current_date.strftime("%Y%m%d")
            
            if progress_callback:
                progress_callback(f"Processing {date_str} ({current_day}/{total_days})...")
            
            emissions_name = f"emissions_{date_key}"
            try:
                df = load_parquet(emissions_name)
                # Add date column to track which date each row belongs to
                df["date"] = date_str
                all_data.append(df)
                if progress_callback:
                    progress_callback(f"Loaded {len(df)} flights for {date_str}")
            except Exception as e:
                if progress_callback:
                    progress_callback(f"No data for {date_str}, skipping...")
                # Skip dates with no data
                pass
            
            current_date += timedelta(days=1)
        
        if not all_data:
            raise ValueError("No data found for any dates in the range")
        
        if progress_callback:
            progress_callback("Aggregating all data...")
        
        # Combine all dataframes
        aggregated = pd.concat(all_data, ignore_index=True)
        
        return aggregated
    except Exception as e:
        raise Exception(f"Error aggregating date range: {str(e)}")

@app.post("/fetch/range/{start_date}/{end_date}")
def fetch_date_range(start_date: str, end_date: str, background_tasks: BackgroundTasks):
    """
    Fetch and aggregate data for a date range.
    Creates a single CSV file with all data from the range.
    """
    try:
        # Validate dates
        start_dt = datetime.fromisoformat(start_date)
        end_dt = datetime.fromisoformat(end_date)
        
        if start_dt > end_dt:
            raise HTTPException(status_code=400, detail="Start date must be before end date")
        
        # Check if range file already exists
        from app.storage import DATA_DIR
        range_name = f"range_{start_date.replace('-', '')}_to_{end_date.replace('-', '')}"
        range_path = os.path.join(DATA_DIR, f"{range_name}.csv")
        
        # Check if range file exists AND if it has complete coverage
        if os.path.exists(range_path):
            # Verify if the file has data for all dates in range
            try:
                # Read full file to check completeness (not just first 1000 rows)
                df_full = pd.read_csv(range_path)
                expected_days = (end_dt.date() - start_dt.date()).days + 1
                
                if 'date' in df_full.columns:
                    unique_dates = set(df_full['date'].unique())
                    coverage = len(unique_dates) / expected_days if expected_days > 0 else 0
                    
                    # Require at least 90% coverage (allow some dates to have no data)
                    if coverage < 0.90:
                        # File exists but incomplete, delete it to force re-fetch
                        os.remove(range_path)
                        return {
                            "status": "incomplete",
                            "message": f"Range file exists but only has {len(unique_dates)} days (expected {expected_days}, {coverage*100:.1f}% coverage). Re-fetching missing dates...",
                            "date_range": {"start": start_date, "end": end_date}
                        }
                else:
                    # No date column - file might be old format, delete and re-fetch
                    os.remove(range_path)
                    return {
                        "status": "incomplete",
                        "message": f"Range file exists but missing 'date' column. Re-fetching...",
                        "date_range": {"start": start_date, "end": end_date}
                    }
            except Exception as e:
                # Error reading file - delete it and re-fetch
                try:
                    os.remove(range_path)
                except:
                    pass
                return {
                    "status": "incomplete",
                    "message": f"Error reading range file: {str(e)}. Re-fetching...",
                    "date_range": {"start": start_date, "end": end_date}
                }
            
            # File exists and appears complete (90%+ coverage)
            return {
                "status": "exists",
                "message": f"Aggregated data already exists for range {start_date} to {end_date}",
                "file": f"{range_name}.csv",
                "date_range": {"start": start_date, "end": end_date}
            }
        
        # Check if already processing
        job_id = f"range_{start_date}_{end_date}"
        if job_id in fetch_jobs and fetch_jobs[job_id]["status"] == "processing":
            return {
                "status": "processing",
                "message": f"Range aggregation already in progress",
                "progress": fetch_jobs[job_id].get("progress", "Processing..."),
                "date_range": {"start": start_date, "end": end_date}
            }
        
        # Start background task
        background_tasks.add_task(fetch_range_background, start_date, end_date)
        
        return {
            "status": "started",
            "message": f"Started aggregating data for range {start_date} to {end_date}. This may take a while.",
            "date_range": {"start": start_date, "end": end_date}
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error starting range fetch: {str(e)}")

def fetch_range_background(start_date: str, end_date: str):
    """Background task to fetch missing dates and aggregate date range data."""
    job_id = f"range_{start_date}_{end_date}"
    fetch_jobs[job_id] = {"status": "processing", "progress": "Starting...", "error": None}
    cancellation_flags[job_id] = False  # Initialize cancellation flag
    
    def is_cancelled():
        """Check if this job has been cancelled."""
        return cancellation_flags.get(job_id, False)
    
    def update_progress(msg, stats=None):
        if job_id in fetch_jobs:
            fetch_jobs[job_id]["progress"] = msg
            if stats:
                fetch_jobs[job_id]["stats"] = stats
    
    try:
        from datetime import timedelta
        start = datetime.fromisoformat(start_date).date()
        end = datetime.fromisoformat(end_date).date()
        
        if start > end:
            raise ValueError("Start date must be before end date")
        
        total_days = (end - start).days + 1
        current_date = start
        current_day = 0
        
        # Step 1: Check which dates have data
        update_progress(f"Scanning {total_days} days to check existing data...", {
            "total_days": total_days,
            "dates_with_data": 0,
            "dates_to_fetch": 0,
            "dates_completed": 0,
            "dates_failed": 0,
            "current_date": None,
            "phase": "scanning"
        })
        
        dates_to_fetch = []
        dates_with_data = []
        while current_date <= end:
            current_day += 1
            date_str = current_date.strftime("%Y-%m-%d")
            date_key = current_date.strftime("%Y%m%d")
            
            # Check if data exists and is not empty
            emissions_name = f"emissions_{date_key}"
            from app.storage import DATA_DIR
            emissions_path = os.path.join(DATA_DIR, f"{emissions_name}.csv")
            
            if os.path.exists(emissions_path):
                # Verify file is not empty
                file_size = os.path.getsize(emissions_path)
                if file_size > 0:
                    dates_with_data.append(date_str)
                else:
                    # File exists but is empty - need to fetch
                    dates_to_fetch.append(date_str)
            else:
                dates_to_fetch.append(date_str)
            
            # Update progress every 10 days
            if current_day % 10 == 0:
                update_progress(f"Scanned {current_day}/{total_days} days... ({len(dates_with_data)} have data, {len(dates_to_fetch)} need fetching)", {
                    "total_days": total_days,
                    "dates_with_data": len(dates_with_data),
                    "dates_to_fetch": len(dates_to_fetch),
                    "dates_completed": 0,
                    "dates_failed": 0,
                    "current_date": None,
                    "phase": "scanning"
                })
            
            current_date += timedelta(days=1)
        
        # Step 2: Fetch missing dates from OpenSky (PARALLEL for much faster performance)
        if not dates_to_fetch:
            # All dates already have data - skip fetching
            update_progress(
                f"✓ All {total_days} dates already have data! Skipping fetch, proceeding to aggregation...",
                {
                    "total_days": total_days,
                    "dates_with_data": len(dates_with_data),
                    "dates_to_fetch": 0,
                    "dates_completed": 0,
                    "dates_failed": 0,
                    "dates_processed": 0,
                    "progress_percent": 100,
                    "current_date": None,
                    "phase": "aggregating"
                }
            )
        elif dates_to_fetch:
            # Estimate: ~30-40 seconds per date with 3 parallel workers (conservative to avoid rate limits)
            # 90 dates / 3 workers = ~30 batches * 35 seconds = ~17-18 minutes (slower but avoids 429 errors)
            estimated_minutes = len(dates_to_fetch) * 0.6  # ~36 seconds per date with 3 workers
            update_progress(f"📊 Summary: {len(dates_with_data)} dates already have data, {len(dates_to_fetch)} need fetching. Estimated time: ~{estimated_minutes:.0f} minutes", {
                "total_days": total_days,
                "dates_with_data": len(dates_with_data),
                "dates_to_fetch": len(dates_to_fetch),
                "dates_completed": 0,
                "dates_failed": 0,
                "current_date": None,
                "phase": "fetching"
            })
            
            from concurrent.futures import ThreadPoolExecutor, as_completed
            import time
            start_time = time.time()
            
            completed = 0
            failed = 0
            total_dates = len(dates_to_fetch)
            
            # Fetch multiple days in parallel (CONSERVATIVE to avoid rate limits)
            # OpenSky rate limits are strict - better to be slower than hit 429 errors
            # Using 3 workers is more conservative and reduces rate limit errors
            # The rate limiter will automatically slow down if we hit limits
            # For 90 days: 3 workers = ~30 batches, but avoids 429 errors and retry delays
            parallel_day_workers = 3  # Conservative: reduces rate limit errors (429s cause 10s+ delays)
            
            def fetch_and_compute_day(date_str):
                """Fetch and compute a single day. Returns (date, flights_count, error)."""
                try:
                    # Create a lightweight progress callback for parallel execution
                    def parallel_progress(msg):
                        # Only log important messages to avoid spam
                        pass
                    
                    # Fetch flight data with improved error handling
                    flights_count = ingest_day_internal(date_str, parallel_progress)
                    
                    # Check if we got any flights
                    if flights_count == 0:
                        return (date_str, 0, "No flight data available from OpenSky for this date")
                    
                    # Compute emissions
                    emissions_count = compute_day_internal(date_str, parallel_progress)
                    return (date_str, emissions_count, None)
                except ValueError as e:
                    # Expected errors (no data, etc.) - return friendly message
                    return (date_str, 0, str(e))
                except Exception as e:
                    # Improved error handling - retries are handled in opensky_client
                    error_str = str(e)
                    # Network/timeout errors are now retried automatically
                    # Only return error if all retries exhausted
                    return (date_str, 0, f"Error after retries: {error_str[:150]}")
            
            # Execute ALL dates in parallel (no batching - rate limiter handles concurrency)
            # This is much faster than processing in small batches
            with ThreadPoolExecutor(max_workers=parallel_day_workers) as executor:
                # Submit all dates at once - rate limiter will queue them properly
                future_to_date = {executor.submit(fetch_and_compute_day, date_str): date_str for date_str in dates_to_fetch}
                
                # Process results as they complete
                for future in as_completed(future_to_date):
                    if is_cancelled():
                        update_progress("Operation cancelled by user", {
                            "total_days": total_days,
                            "dates_with_data": len(dates_with_data),
                            "dates_to_fetch": total_dates,
                            "dates_completed": completed,
                            "dates_failed": failed,
                            "current_date": None,
                            "phase": "cancelled"
                        })
                        return
                    
                    date_str, flights_count, error = future.result()
                    
                    if error:
                        # Rate limit errors are now handled automatically with retries
                        # Just log the error and continue
                        failed += 1
                    else:
                        completed += 1
                    
                    # Calculate progress and ETA
                    total_processed = completed + failed
                    progress_pct = int((total_processed / total_dates) * 100) if total_dates > 0 else 0
                    
                    # Estimate time remaining
                    elapsed = time.time() - start_time
                    if completed > 0:
                        avg_time_per_date = elapsed / completed
                        remaining_dates = total_dates - total_processed
                        eta_seconds = avg_time_per_date * remaining_dates
                        eta_minutes = int(eta_seconds / 60)
                        eta_str = f"~{eta_minutes} min remaining" if eta_minutes > 0 else f"~{int(eta_seconds)} sec remaining"
                    else:
                        eta_str = "calculating..."
                    
                    # Update progress with detailed stats
                    if error:
                        if "No flight data" not in error:
                            status_msg = f"⚠️ {date_str} failed: {error[:80]}"
                        else:
                            status_msg = f"ℹ️ {date_str}: No data (expected)"
                    else:
                        status_msg = f"✓ {date_str}: {flights_count:,} flights"
                    
                    update_progress(
                        f"📅 {total_processed}/{total_dates} dates ({progress_pct}%) | ✓ {completed} done, ⚠️ {failed} failed | {eta_str} | Latest: {status_msg}",
                        {
                            "total_days": total_days,
                            "dates_with_data": len(dates_with_data),
                            "dates_to_fetch": total_dates,
                            "dates_completed": completed,
                            "dates_failed": failed,
                            "dates_processed": total_processed,
                            "progress_percent": progress_pct,
                            "current_date": date_str,
                            "phase": "fetching",
                            "eta": eta_str
                        }
                    )
            
            update_progress(
                f"✓ Finished fetching: {completed} successful, {failed} failed out of {total_dates} dates",
                {
                    "total_days": total_days,
                    "dates_with_data": len(dates_with_data),
                    "dates_to_fetch": total_dates,
                    "dates_completed": completed,
                    "dates_failed": failed,
                    "dates_processed": completed + failed,
                    "progress_percent": 100,
                    "current_date": None,
                    "phase": "aggregating"
                }
            )
        
        # Step 3: Aggregate all data
        if is_cancelled():
            fetch_jobs[job_id] = {
                "status": "cancelled",
                "progress": "Operation cancelled before aggregation",
                "error": None
            }
            return
        
        update_progress("Aggregating all dates into single file...")
        aggregated_df = aggregate_date_range(start_date, end_date, update_progress)
        
        # Final cancellation check
        if is_cancelled():
            fetch_jobs[job_id] = {
                "status": "cancelled",
                "progress": "Operation cancelled after aggregation",
                "error": None
            }
            return
        
        # Save aggregated file
        range_name = f"range_{start_date.replace('-', '')}_to_{end_date.replace('-', '')}"
        range_path = save_parquet(aggregated_df, range_name)
        
        total_flights = len(aggregated_df)
        total_co2 = float(aggregated_df["co2_kg"].sum()) if "co2_kg" in aggregated_df.columns else 0
        
        update_progress(f"✓ Aggregation complete! Saved to {range_name}.csv")
        
        fetch_jobs[job_id] = {
            "status": "completed",
            "progress": "Done!",
            "total_flights": total_flights,
            "total_co2_kg": total_co2,
            "total_co2_tons": total_co2 / 1000.0,
            "file": f"{range_name}.csv",
            "date_range": {"start": start_date, "end": end_date}
        }
    except Exception as e:
        fetch_jobs[job_id] = {
            "status": "error",
            "progress": "Failed",
            "error": str(e)
        }

@app.get("/range/status/{start_date}/{end_date}")
def range_status(start_date: str, end_date: str):
    """Get status of a range aggregation job."""
    job_id = f"range_{start_date}_{end_date}"
    if job_id not in fetch_jobs:
        return {"status": "not_found", "date_range": {"start": start_date, "end": end_date}}
    return {**fetch_jobs[job_id], "date_range": {"start": start_date, "end": end_date}}

@app.post("/fetch/range/cancel/{start_date}/{end_date}")
def cancel_range_fetch(start_date: str, end_date: str):
    """Cancel a range fetch job."""
    job_id = f"range_{start_date}_{end_date}"
    if job_id not in fetch_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if fetch_jobs[job_id]["status"] not in ["processing"]:
        raise HTTPException(status_code=400, detail=f"Job is not running (status: {fetch_jobs[job_id]['status']})")
    
    # Set cancellation flag
    cancellation_flags[job_id] = True
    
    return {
        "status": "cancelling",
        "message": f"Cancellation requested for range {start_date} to {end_date}. Operation will stop soon.",
        "job_id": job_id
    }

@app.get("/range/data/{start_date}/{end_date}")
def get_range_data(start_date: str, end_date: str):
    """Get aggregated data for a date range."""
    try:
        range_name = f"range_{start_date.replace('-', '')}_to_{end_date.replace('-', '')}"
        df = load_parquet(range_name)
        
        total_co2_kg = float(df["co2_kg"].sum())
        total_flights = int(len(df))
        
        # Group by date for daily breakdown
        daily_stats = df.groupby("date").agg({
            "co2_kg": "sum",
            "icao24": "count"
        }).reset_index()
        daily_stats.columns = ["date", "co2_kg", "flights"]
        daily_stats = daily_stats.to_dict(orient="records")
        
        # Top routes across entire range
        top_routes = (
            df.groupby(["dep", "arr"])["co2_kg"]
            .sum()
            .reset_index()
            .sort_values("co2_kg", ascending=False)
            .head(20)
            .to_dict(orient="records")
        )
        
        # Top airports across entire range
        top_airports = (
            df.groupby("dep")["co2_kg"]
            .sum()
            .reset_index()
            .sort_values("co2_kg", ascending=False)
            .head(20)
            .to_dict(orient="records")
        )
        
        return {
            "date_range": {"start": start_date, "end": end_date},
            "total_flights": total_flights,
            "total_co2_kg": total_co2_kg,
            "total_co2_tons": total_co2_kg / 1000.0,
            "daily_stats": daily_stats,
            "top_routes": top_routes,
            "top_airports": top_airports,
            "file": f"{range_name}.csv"
        }
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Range data not found: {str(e)}")

@app.get("/co2/map/{date_yyyymmdd}")
def co2_map(date_yyyymmdd: str):
    key = date_yyyymmdd.replace("-", "")

    candidate_names = [f"emissions_{key}"]

    df = None
    for name in candidate_names:
        try: 
            df = load_parquet(name)
            break
        except Exception:
            pass
    
    if df is None:
        raise HTTPException(status_code=404, detail="Emissions data not found for this date(date_yyyymmdd)")
    
    # Clean out issing OD inference
    df = df[(df["dep"] != "NONE") & (df["arr"] != "NONE")].copy()

    airports = get_airports_cache()  # Use cached airports (much faster)

    # Top Departure Airports (bubble map)
    top_dep = (
        df.groupby("dep")["co2_kg"]
        .sum()
        .reset_index()
        .sort_values("co2_kg", ascending=False)
        .head(200)
    )

    top_dep = top_dep.merge(airports[["icao", "lat", "lon", "airport_name", "country"]],
                            left_on="dep", right_on="icao", how="left")
    top_dep = top_dep.dropna(subset=["lat", "lon"])

    airports_payload = top_dep.rename(columns={"dep": "icao"}).to_dict(orient="records")

    # Top routes (lines)
    top_routes = (
        df.groupby(["dep", "arr"])["co2_kg"]
        .sum()
        .reset_index()
        .sort_values("co2_kg", ascending=False)
        .head(200)
    )

    dep_coords = airports.rename(columns={
        "icao": "dep", "lat": "dep_lat", "lon": "dep_lon",})
    arr_coords = airports.rename(columns={
        "icao": "arr", "lat": "arr_lat", "lon": "arr_lon",})
    
    top_routes = top_routes.merge(dep_coords[["dep", "dep_lat", "dep_lon"]], on="dep", how="left")
    top_routes = top_routes.merge(arr_coords[["arr", "arr_lat", "arr_lon"]], on="arr", how="left")
    top_routes = top_routes.dropna(subset=["dep_lat", "dep_lon", "arr_lat", "arr_lon"])

    routes_payload = top_routes.to_dict(orient="records")

    return {
        "date": date_yyyymmdd,
        "airports": airports_payload, # bubble map
        "routes": routes_payload,   #polyline map
    }

@app.get("/storage/info")
def storage_info():
    """Get storage information including disk usage and file counts."""
    from app.storage import DATA_DIR
    import os
    
    total_size = 0
    file_count = 0
    emissions_files = 0
    range_files = 0
    flight_files = 0
    emissions_size = 0
    range_size = 0
    
    if os.path.exists(DATA_DIR):
        for filename in os.listdir(DATA_DIR):
            if filename.endswith(('.csv', '.parquet')):
                filepath = os.path.join(DATA_DIR, filename)
                size = os.path.getsize(filepath)
                total_size += size
                file_count += 1
                
                if filename.startswith('emissions_') and not filename.startswith('range_'):
                    emissions_files += 1
                    emissions_size += size
                elif filename.startswith('range_'):
                    range_files += 1
                    range_size += size
                elif filename.startswith('flights_'):
                    flight_files += 1
    
    def format_bytes(bytes):
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes < 1024.0:
                return f"{bytes:.2f} {unit}"
            bytes /= 1024.0
        return f"{bytes:.2f} TB"
    
    return {
        "total_size_bytes": total_size,
        "total_size_formatted": format_bytes(total_size),
        "file_count": file_count,
        "emissions_files": {
            "count": emissions_files,
            "size_bytes": emissions_size,
            "size_formatted": format_bytes(emissions_size),
            "description": "Individual day emissions files (can be deleted after aggregation if range file exists)"
        },
        "range_files": {
            "count": range_files,
            "size_bytes": range_size,
            "size_formatted": format_bytes(range_size),
            "description": "Aggregated date range files"
        },
        "flight_files": {
            "count": flight_files,
            "description": "Raw flight data files"
        }
    }

@app.post("/storage/cleanup/individual-days")
def cleanup_individual_days(keep_recent_days: int = 7):
    """
    Clean up individual day files that have corresponding range files.
    Optionally keeps recent N days even if range exists.
    """
    from app.storage import DATA_DIR
    import os
    from datetime import datetime, timedelta
    
    removed_count = 0
    freed_bytes = 0
    errors = []
    
    # Get all range files
    range_files = {}
    if os.path.exists(DATA_DIR):
        for filename in os.listdir(DATA_DIR):
            if filename.startswith('range_') and filename.endswith('.csv'):
                # Extract date range from filename: range_YYYYMMDD_to_YYYYMMDD.csv
                parts = filename.replace('range_', '').replace('.csv', '').split('_to_')
                if len(parts) == 2:
                    try:
                        start = datetime.strptime(parts[0], '%Y%m%d').date()
                        end = datetime.strptime(parts[1], '%Y%m%d').date()
                        range_files[filename] = (start, end)
                    except:
                        pass
    
    # Check which individual day files can be removed
    cutoff_date = (datetime.now() - timedelta(days=keep_recent_days)).date()
    
    if os.path.exists(DATA_DIR):
        for filename in os.listdir(DATA_DIR):
            if filename.startswith('emissions_') and filename.endswith('.csv') and not filename.startswith('range_'):
                # Extract date from filename: emissions_YYYYMMDD.csv
                date_str = filename.replace('emissions_', '').replace('.csv', '')
                try:
                    file_date = datetime.strptime(date_str, '%Y%m%d').date()
                    
                    # Skip if within recent days cutoff
                    if file_date >= cutoff_date:
                        continue
                    
                    # Check if this date is covered by any range file
                    can_remove = False
                    for range_filename, (start, end) in range_files.items():
                        if start <= file_date <= end:
                            can_remove = True
                            break
                    
                    if can_remove:
                        filepath = os.path.join(DATA_DIR, filename)
                        try:
                            size = os.path.getsize(filepath)
                            os.remove(filepath)
                            removed_count += 1
                            freed_bytes += size
                        except Exception as e:
                            errors.append(f"Failed to remove {filename}: {str(e)}")
                except:
                    pass
    
    return {
        "removed_files": removed_count,
        "freed_bytes": freed_bytes,
        "freed_formatted": f"{freed_bytes / (1024*1024):.2f} MB" if freed_bytes > 0 else "0 B",
        "errors": errors,
        "kept_recent_days": keep_recent_days
    }