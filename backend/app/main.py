from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from datetime import datetime
from app.storage import load_parquet
from app.airports import load_airports

app = FastAPI(title="Aviation Emissions MVP")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"message": "Aviation Emissions API is running. Try /health or /co2/summary/YYYY-MM-DD"}

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/summary/{date_yyyymmdd}")
def summary(date_yyyymmdd: str):
    """
    Returns simple counts for a day. Next step: compute CO2 using distances.
    """
    name = f"flights_{date_yyyymmdd.replace('-', '')}"
    df = load_parquet(name)

    flights = len(df)
    top_routes = (
        df.groupby(["dep", "arr"])
        .size()
        .reset_index(name="flights")
        .sort_values(by="flights", ascending=False)
        .head(10)
        .to_dict(orient="records")
    ) 
    return {"date": date_yyyymmdd, "flights": flights, "top_routes": top_routes}

@app.get("/co2/summary/{date_yyyymmdd}")
def co2_summary(date_yyyymmdd: str):
    key = date_yyyymmdd.replace("-", "")

    candidate_names = [f"emissions_{key}"]

    df = None
    loaded_name = None
    for name in candidate_names:
        try: 
            df = load_parquet(name)
            loaded_name = name
            break
        except Exception:
            pass
    
    if df is None:
        raise HTTPException(status_code=404, detail="Emissions data not found for this date(date_yyyymmdd)")
    
    total_co2_kg = float(df["co2_kg"].sum())
    flights = int(len(df))

    top_routes = (
        df.groupby(["dep", "arr"])["co2_kg"]
        .sum()
        .reset_index()
        .sort_values("co2_kg", ascending=False)
        .head(15)
        .to_dict(orient="records")
    )

    top_dep_airports = (
        df.groupby("dep")["co2_kg"]
        .sum()
        .reset_index()
        .sort_values("co2_kg", ascending=False)
        .head(15)
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

    airports = load_airports("data/ourairports_airports.csv")

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