import sys
import pandas as pd
from datetime import datetime, timezone

from app.opensky_client import OpenSkyOAuthClient
from app.storage import save_parquet

def to_unix(dt: datetime) -> int:
    return int (dt.replace(tzinfo=timezone.utc).timestamp())

def ingest_day(date_yyyymmdd: str):
    # date_yyyymmdd like "2025-12-25"
    day = datetime.fromisoformat(date_yyyymmdd).replace(tzinfo=timezone.utc)
    begin = to_unix(day)
    end = to_unix(day.replace(hour=23, minute=59, second=59))

    client = OpenSkyOAuthClient()

    rows = []
    for t1, t2, flights in client.flights_all_chunked(begin, end, chunk_seconds=2*3600):
        for f in flights:
            # flights/all returns fields like:
            # icao24, callsign, firstSeen, lastSeen, estDepartureAirport, estArrivalAirport, ...
            rows.append({
                "icao24": f.get("icao24"),
                "callsign": f.get("callsign"),
                "firstSeen": f.get("firstSeen"),
                "lastSeen": f.get("lastSeen"),
                "dep": f.get("estDepartureAirport"),
                "arr": f.get("estArrivalAirport"),
                # "day_begin_utc": t1,
                # "day_end_utc": t2,
            })

    df = pd.DataFrame(rows).drop_duplicates()
    name = f"flights_{day.strftime('%Y%m%d')}"
    path = save_parquet(df, name)
    print(f"Saved {len(df):,} rows -> {path}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python scripts/ingest_day.py YYYY-MM-DD")
        sys.exit(1)
    ingest_day(sys.argv[1])
