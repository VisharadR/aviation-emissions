# New York live aviation add-on

The historical dashboard remains at `/`. The New York dashboard is at `/live/ny`, linked
from the historical page. Its same-origin `/api/live/ny` proxy calls the existing
FastAPI server's new `/live/ny` endpoint. No historical emissions files are changed.

## Run on Windows

From the repository root, create a virtual environment and install dependencies:

```powershell
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r backend/requirements.txt
```

In one terminal:

```powershell
cd backend
../.venv/Scripts/python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

In another terminal:

```powershell
cd frontend
npm install
npm run dev
```

Open http://localhost:3000/live/ny. The world view is at `/live`; see [LIVE_WORLD.md](LIVE_WORLD.md) for its shared feed and updated refresh policy. If the machine's npm shim is broken, the installed
frontend also runs with `node node_modules/next/dist/bin/next dev`.

## Data access

The live client reuses `backend/credentials.json`, resolved from the module path,
or accepts `OPENSKY_CLIENT_ID` and `OPENSKY_CLIENT_SECRET` environment variables.
Credentials remain on the server and never reach the browser. Existing historical
authentication code is unchanged. Without credentials the live client attempts
anonymous access, with a conservative 1,800-second interval; continuous monitoring
requires authenticated access because observations expire after 120 seconds.

The New York route now filters the shared worldwide snapshot by nearby airport.
See [LIVE_WORLD.md](LIVE_WORLD.md) for the current 90-second authenticated global
refresh policy, spatial indexing and shared rate limiter. No extra regional
provider request is made when switching views. Positions still expire after
120 seconds, and unavailable/stale states are never filled with demo data.
Use one backend worker; replicas require a shared cache and rate limiter.

The catalog was freshly retrieved from OurAirports and contains 412 open airport
and seaplane facilities in `US-NY`; heliports are excluded. Newark is in New Jersey
and is excluded. `backend/data/ny_airports.source.json` records retrieval provenance.
To refresh independently of the historical global catalog:

```powershell
cd backend
../.venv/Scripts/python.exe scripts/refresh_ny_airports.py
```

Restart the backend after a catalog refresh. If the NY file is absent, the live
service filters the existing global OurAirports catalog as a fallback.

## What the estimates mean

Each position is associated once with its nearest New York airport within 25 km.
This is proximity, not a confirmed arrival or departure. Overflights and positions
across state boundaries may occur in airport catchments. Receiver coverage is
incomplete; no observed flights is not proof of no airport activity.

The existing historical model uses 3 kg fuel/km and 3.16 kg CO₂/kg fuel. The add-on
uses the derivative of its distance term: speed in m/s × 0.06 × 3 × 3.16 gives
kg CO₂/min. The fixed 500 kg trip fuel allowance is not repeatedly added.
Ground, missing-speed, invalid-speed and unknown ground-state observations have no
rate estimate. This generic proxy is not calibrated for individual aircraft and
is especially limited for small aircraft. Aircraft type, fuel flow, engine, wind,
and actual flight paths are unknown. These are not measured emissions, full-flight
totals, or validated airport inventories; non-CO₂ effects are not modelled.

The session chart retains at most 30 distinct fresh provider snapshots in memory.
It resets on reload. It is not an emissions total or a persistent historical record.
Controls filter the map, table and KPIs; the trend stays statewide and is labeled so.

## Validation

```powershell
cd backend
../.venv/Scripts/python.exe -m pip install -r requirements-dev.txt
../.venv/Scripts/python.exe -m unittest discover -s tests -v
cd ../frontend
npm run build
```

Tests cover units, unmodelled flights, invalid/stale/future positions, deduplication,
empty provider responses, NY airport scope, caching/outages and historical routes.

## Assets and hosting

Higgsfield generation was attempted but rejected because the connected workspace
has no credits. See `frontend/public/live-assets/README.md` for the pending brief.
No generated assets have been substituted or fabricated. The map uses real,
attributed geographic tiles rather than generated imagery.

The app runs locally. Publishing requires hosting the Python backend and setting
the frontend server's `AVIATION_API_URL` to its reachable URL. A static export will
not support the live proxy. Sites' Cloudflare Worker runtime cannot directly run
the existing FastAPI application; it has not been deployed or replaced here.

Sources: [OpenSky REST documentation](https://github.com/openskynetwork/opensky-api/blob/master/docs/free/rest.rst),
[OurAirports public catalog](https://ourairports.com/data/),
[EUROCONTROL fuel conversion discussion](https://www.eurocontrol.int/publication/small-emitters-tool-set-2024).
