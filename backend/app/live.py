"""Live airport-vicinity observations; not confirmed routes or measured emissions."""
import copy
import csv
import math
import os
import threading
import time
from pathlib import Path
import requests
from fastapi import APIRouter
from app.opensky_client import OpenSkyOAuthClient, OPENSKY_API_BASE

router = APIRouter(prefix="/live", tags=["Live aviation"])
ROOT = Path(__file__).resolve().parents[1]
AIRPORTS_PATH = ROOT / "data" / "ny_airports.csv"
if not AIRPORTS_PATH.exists():
    AIRPORTS_PATH = ROOT / "data" / "ourairports_airports.csv"
MAX_AGE = 120
RADIUS_KM = 25
WORLD_AIRPORTS_PATH = ROOT / "data" / "world_airports.csv"
if not WORLD_AIRPORTS_PATH.exists():
    WORLD_AIRPORTS_PATH = ROOT / "data" / "ourairports_airports.csv"

def number(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)

def distance(lat, lon, a):
    p, q = math.radians(lat), math.radians(a["lat"])
    h = math.sin((q-p)/2)**2 + math.cos(p)*math.cos(q)*math.sin(math.radians(a["lon"]-lon)/2)**2
    return 12742 * math.asin(min(1, math.sqrt(h)))

def load_airport_catalog(path, region=None):
    with path.open(encoding="utf-8-sig", newline="") as stream:
        rows = [dict(id=r["ident"], code=r["iata_code"] or r["ident"], name=r["name"],
                     lat=float(r["latitude_deg"]), lon=float(r["longitude_deg"]),
                     municipality=r["municipality"], country=r["iso_country"], region=r["iso_region"],
                     type=r["type"], scheduled=r["scheduled_service"] == "yes")
                for r in csv.DictReader(stream) if (region is None or r["iso_region"] == region)
                and r["type"] in ("large_airport", "medium_airport", "small_airport", "seaplane_base")]
    return sorted(rows, key=lambda a: (not a["scheduled"], a["name"]))

def load_ny_airports():
    return load_airport_catalog(AIRPORTS_PATH, "US-NY")

class AirportIndex:
    """One-degree spatial buckets with dateline wrapping and polar support."""
    def __init__(self, airports):
        self.cells = {}
        for a in airports:
            key = (math.floor(a["lat"]), math.floor((a["lon"] + 180) % 360))
            self.cells.setdefault(key, []).append(a)

    def nearest(self, lat, lon):
        angular = RADIUS_KM / 6371
        dy = math.degrees(angular)
        cosine = math.cos(math.radians(lat))
        dx = 180 if cosine <= math.sin(angular) else math.degrees(math.asin(math.sin(angular)/cosine))
        center = (lon + 180) % 360
        columns = {x % 360 for x in range(math.floor(center-dx), math.floor(center+dx)+1)}
        best, km = None, RADIUS_KM
        for y in range(max(-90, math.floor(lat-dy)), min(90, math.floor(lat+dy))+1):
            for x in columns:
                for a in self.cells.get((y,x), []):
                    d = distance(lat,lon,a)
                    if d <= km:
                        best, km = a, d
        return best, km if best else None

def normalize(payload, airports, now, worldwide=False, index=None):
    if not isinstance(payload, dict) or not number(payload.get("time")):
        raise ValueError("Invalid provider timestamp")
    states = payload.get("states")
    if states is not None and not isinstance(states, list):
        raise ValueError("Invalid provider observations")
    result = {}
    for s in states or []:
        if not isinstance(s, list) or len(s) < 14:
            continue
        ident, pt, contact, lon, lat = s[0], s[3], s[4], s[5], s[6]
        if not isinstance(ident, str) or not all(number(v) for v in (pt, contact, lon, lat)):
            continue
        if not (-90 <= lat <= 90 and -180 <= lon <= 180) or not (-15 <= now-pt <= MAX_AGE and -15 <= now-contact <= MAX_AGE):
            continue
        if index is not None:
            a, km = index.nearest(lat, lon)
        else:
            a = min(airports, key=lambda a: distance(lat, lon, a)) if airports else None
            km = distance(lat, lon, a) if a else None
            if km is not None and km > RADIUS_KM:
                a, km = None, None
        if a is None and not worldwide:
            continue
        speed = s[9] if number(s[9]) and 0 <= s[9] <= 400 else None
        ground = s[8] if isinstance(s[8], bool) else None
        vertical = s[11] if number(s[11]) else None
        # Historical 3 kg fuel/km proxy differentiated over time, no fixed trip fuel.
        rate = round(speed * .06 * 3 * 3.16, 2) if ground is False and speed is not None else None
        phase = "Ground" if ground else ("Climb" if vertical is not None and vertical > 1 else "Descent" if vertical is not None and vertical < -1 else "Airborne")
        if ground is None:
            phase = "Unknown"
        f = dict(id=ident, callsign=str(s[1] or ident).strip(), lat=lat, lon=lon,
                 position_time=pt, last_contact=contact, airport_id=a["id"] if a else None, airport_code=a["code"] if a else None,
                 airport_country=a.get("country") if a else None,
                 distance_km=round(km, 1) if km is not None else None, on_ground=ground,
                 speed_kmh=round(speed*3.6) if speed is not None else None,
                 altitude_m=s[7] if number(s[7]) else None, heading=s[10] if number(s[10]) else 0,
                 phase=phase, co2_kg_min=rate)
        if ident not in result or result[ident]["position_time"] < pt:
            result[ident] = f
    return sorted(result.values(), key=lambda f: f["co2_kg_min"] or 0, reverse=True)

class LiveService:
    def __init__(self, scope="ny"):
        self.scope = scope
        self.index = None
        self.lock = threading.Lock()
        self.client = None
        self.airports = None
        self.snapshot = None
        self.next_fetch = 0
        self.error = None
        self.interval = 60

    def fetch(self):
        if self.client is None:
            self.client = OpenSkyOAuthClient(str(ROOT / "credentials.json"), os.getenv("OPENSKY_CLIENT_ID"), os.getenv("OPENSKY_CLIENT_SECRET"))
        auth = bool(self.client.client_id and self.client.client_secret)
        self.interval = (90 if auth else 1800) if self.scope == "world" else (60 if auth else 900)
        headers = {"Authorization": "Bearer " + self.client._ensure_token()} if auth else {}
        r = requests.get(OPENSKY_API_BASE + "/states/all", headers=headers,
            params={} if self.scope == "world" else dict(lamin=40.2, lomin=-80.1, lamax=45.3, lomax=-71.5), timeout=30)
        if r.status_code == 401 and auth:
            self.client._access_token = None
        if r.status_code == 429:
            retry = r.headers.get("X-Rate-Limit-Retry-After-Seconds", "900")
            self.next_fetch = time.time() + max(self.interval, int(retry) if retry.isdigit() else 900)
        r.raise_for_status()
        return r.json()

    def get(self):
        with self.lock:
            now = time.time()
            if self.airports is None:
                self.airports = load_airport_catalog(WORLD_AIRPORTS_PATH) if self.scope == "world" else load_ny_airports()
                self.index = AirportIndex(self.airports)
            if now >= self.next_fetch:
                try:
                    payload = self.fetch()
                    now = time.time()
                    flights = normalize(payload, self.airports, now, worldwide=self.scope == "world", index=self.index)
                    if not -15 <= now-payload["time"] <= MAX_AGE:
                        raise ValueError("Provider snapshot is out of date")
                    self.snapshot = dict(observed_at=payload["time"], fetched_at=now, flights=flights)
                    self.error = None
                except Exception as exc:
                    code = getattr(getattr(exc, "response", None), "status_code", None)
                    self.error = ("OpenSky rate limit reached. Waiting before retrying." if code == 429 else
                                  "OpenSky authentication failed. Check server credentials." if code in (401, 403) else
                                  "Live observations are unavailable. The server will retry automatically.")
                self.next_fetch = max(self.next_fetch, time.time()+self.interval)
            now = time.time()
            data = copy.deepcopy(self.snapshot or dict(observed_at=None, fetched_at=None, flights=[]))
            stale = data["observed_at"] is None or now-data["observed_at"] > MAX_AGE or bool(self.error)
            data["flights"] = [f for f in data["flights"] if now-f["position_time"] <= MAX_AGE and now-f["last_contact"] <= MAX_AGE]
            data.update(status="unavailable" if self.snapshot is None else "stale" if stale else "live",
                        error=self.error, airports=self.airports, server_time=now, next_fetch_at=self.next_fetch,
                        refresh_seconds=self.interval, radius_km=RADIUS_KM, source="OpenSky Network live state vectors",
                        airport_catalog_updated_at=(WORLD_AIRPORTS_PATH if self.scope == "world" else AIRPORTS_PATH).stat().st_mtime,
                        scope=self.scope, airport_count=len(self.airports))
            if self.scope == "world":
                ids = {f["airport_id"] for f in data["flights"] if f["airport_id"]}
                # Send nearby-airport metadata, not tens of thousands of unused records per poll.
                data["airports"] = [a for a in self.airports if a["id"] in ids]
                data["countries"] = sorted({a["country"] for a in self.airports if a.get("country")})
            return data

service = LiveService("world")

@router.get("/world")
def live_world():
    return service.get()

@router.get("/ny")
def live_new_york():
    data = service.get()
    airports = load_ny_airports()
    ids = {a["id"] for a in airports}
    data["flights"] = [f for f in data["flights"] if f["airport_id"] in ids]
    data.update(airports=airports, airport_count=len(airports), scope="ny", countries=["US"])
    return data
