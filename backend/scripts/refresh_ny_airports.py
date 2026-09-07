"""Refresh the live add-on catalog from public OurAirports data, preserving history."""
import csv
import io
import json
from datetime import datetime, timezone
from pathlib import Path
import requests
import argparse

URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
TARGET = Path(__file__).resolve().parents[1] / "data" / "ny_airports.csv"

def main():
    parser = argparse.ArgumentParser(description="Refresh public airport metadata without changing historical data")
    parser.add_argument("--world", action="store_true", help="Refresh worldwide airport catalog")
    args = parser.parse_args()
    target = TARGET.with_name("world_airports.csv") if args.world else TARGET
    response = requests.get(URL, timeout=60)
    response.raise_for_status()
    reader = csv.DictReader(io.StringIO(response.text))
    required = {"ident", "iso_region", "type", "name", "latitude_deg", "longitude_deg", "iata_code", "municipality", "scheduled_service"}
    if not required.issubset(reader.fieldnames or []):
        raise ValueError("Unexpected OurAirports schema; existing catalog preserved")
    rows = [r for r in reader if (args.world or r["iso_region"] == "US-NY") and r["type"] in ("large_airport", "medium_airport", "small_airport", "seaplane_base")]
    if len(rows) < (10000 if args.world else 20):
        raise ValueError("Unexpectedly small catalog; existing catalog preserved")
    for r in rows:
        lat, lon = float(r["latitude_deg"]), float(r["longitude_deg"])
        valid = (-90 <= lat <= 90 and -180 <= lon <= 180) if args.world else (40 <= lat <= 46 and -81 <= lon <= -71)
        if not valid:
            raise ValueError("Invalid airport coordinates; existing catalog preserved")
    temp = target.with_suffix(".tmp")
    with temp.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    temp.replace(target)
    target.with_suffix(".source.json").write_text(json.dumps({"source":URL,"retrieved_at":datetime.now(timezone.utc).isoformat(),"airports":len(rows)},indent=2),encoding="utf-8")
    print(f"Refreshed {len(rows)} {'worldwide' if args.world else 'New York'} airports. Restart backend to load the new catalog.")

if __name__ == "__main__":
    main()
