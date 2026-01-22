import sys
import pandas as pd

from app.storage import load_parquet, save_parquet
from app.airports import load_airports
from app.emissions import compute_emissions_vectorized

def compute_day(date_yyyymmdd: str):
    flights_name = f"flights_{date_yyyymmdd.replace('-', '')}"
    flights  = load_parquet(flights_name)

    flights["dep"] = flights["dep"].astype(str).str.strip().str.upper()
    flights["arr"] = flights["arr"].astype(str).str.strip().str.upper()

    airports = load_airports("data/ourairports_airports.csv")

    # Join dep/arr to lat/lon
    dep = airports.rename(columns={
        "icao": "dep", "lat": "dep_lat", "lon": "dep_lon",})
    arr = airports.rename(columns={
        "icao": "arr", "lat": "arr_lat", "lon": "arr_lon",})
    
    df = flights.merge(dep[["dep", "dep_lat", "dep_lon"]], on="dep", how="left")
    df = df.merge(arr[["arr", "arr_lat", "arr_lon"]], on="arr", how="left")

    print("Columns after merge:", df.columns.tolist()[:50])
    print("Has dep_lat?", "dep_lat" in df.columns, "Has arr_lat?", "arr_lat" in df.columns)
    print("Sample dep/arr:", df[["dep","arr"]].dropna().head(5).to_dict(orient="records"))


    # Compute distance + CO2 where we have both endpoints (optimized vectorized)
    print("Computing distances and emissions (optimized vectorized)...")
    df = compute_emissions_vectorized(df, use_gpu=False)

    # Keep only flights where we could compute it
    computed = df.dropna(subset=["co2_kg"]).copy()

    out_name = "emissions_" + date_yyyymmdd.replace("-", "")
    path = save_parquet(computed, out_name)

    print(f"Computed emissions for {len(computed):,} flights -> {path}")
    print(f"Total CO2 (kg): {computed['co2_kg'].sum():,.0f}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python scripts/compute_co2_day.py YYYY-MM-DD")
        sys.exit(1)
    compute_day(sys.argv[1])

