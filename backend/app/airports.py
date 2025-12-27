import pandas as pd

def load_airports(path: str) -> pd.DataFrame:
    """
    Loads OurAirports airports.csv and returns a mapping table:
      icao -> lat/lon/name/country
    """
    df = pd.read_csv(path)

    icao = df["icao_code"].fillna(df["gps_code"]).fillna(df["ident"]).astype(str)

    out = pd.DataFrame({
        "icao": icao.str.strip().str.upper(),
        "lat": df["latitude_deg"],
        "lon": df["longitude_deg"],
        "airport_name": df["name"].astype(str),
        "country": df["iso_country"].astype(str),
    })

    # Remove blanks and missing coordinates
    out = out.replace({"icao": {"": None, "nan": None}}).dropna(subset=["icao", "lat", "lon"])
    out = out.drop_duplicates(subset=["icao"], keep="first")

    return out

