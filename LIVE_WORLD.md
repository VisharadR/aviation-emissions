# Worldwide live aviation

Open `/live` for the world dashboard and `/live/ny` for New York. The historical
dashboard remains at `/`. Use the existing backend/frontend start commands in
[LIVE_NEW_YORK.md](LIVE_NEW_YORK.md).

## Coverage and meaning

The worldwide feed requests all current OpenSky state vectors, without a regional
bounding box. Fresh aircraft outside airport catchments are retained as
**En route / unassociated**. This means no cataloged airport within 25 km, not a
confirmed flight phase or route. Aircraft type and actual fuel flow remain unknown.
The existing 3 kg fuel/km × 3.16 CO₂ conversion model remains a rough rate proxy.
World totals cover observed, modelled aircraft only—not all global aviation.
Receiver coverage varies, especially over oceans and remote regions.

The airport catalog contains 49,279 open airport/seaplane facilities from the latest
OurAirports download. Closed airports and heliports are excluded. The file and
retrieval provenance are separate from the existing historical global catalog.

```powershell
cd backend
../.venv/Scripts/python.exe scripts/refresh_ny_airports.py --world
```

Restart the backend after updating metadata. A missing refreshed file falls back
to the project's original worldwide OurAirports CSV.

## Refresh and scaling

`GET /live/world` and `GET /live/ny` share one provider snapshot and rate limiter.
New York filters that global snapshot by the associated airport. Authenticated
global requests run at most once per 90 seconds; anonymous requests use 1,800
seconds. Browsers check the backend every 30 seconds, or at its next provider refresh deadline if sooner. Under the current provider
credit schedule, a global query costs four credits, so continuous authenticated
polling uses roughly 3,840 credits/day before any other clients' usage.
Rate-limit backoff and stale warnings remain enabled. Do not run multiple backend
workers/replicas without adding a shared cache and limiter.

Positions expire after 120 seconds; the UI conservatively expires them up to five
seconds early. Brief stale intervals can occur between provider polls. No values
are simulated to fill coverage or freshness gaps. Times are displayed in UTC.

A one-degree spatial index finds nearby airports with exact Haversine checks;
longitude wraps across the date line, with wider searches near the poles. The
response sends metadata only for airports associated with current aircraft,
plus the total catalog count and country list. The airport selector therefore
lists observed airports in world mode. Airport-country filtering excludes
unassociated aircraft; choose **No nearby airport** to inspect those separately.

The map uses Leaflet canvas rendering and updates existing flight markers.
The table has 100 rows per page; the map and summary include every filtered match.
The trend is always for the entire selected scope (world or NY), independent of
country, airport and aircraft filters. The previously fixed map lifecycle supports
React Strict Mode cleanup/remount without reusing destroyed map instances.

## Validation and limitations

Run `python -m unittest discover -s tests -v` from `backend` and `npm run build`
from `frontend`. Tests include dateline/polar matching, randomized comparisons to
brute force, unassociated global flights, global request parameters, rate-limit
backoff, New York filtering and the existing data-quality tests.

Higgsfield imagery remains blocked by the connected workspace's lack of credits.
Hosting remains local; deployment requires a hosted FastAPI backend and the
frontend server's `AVIATION_API_URL` configured to reach it.

Sources: [OpenSky REST API](https://github.com/openskynetwork/opensky-api/blob/master/docs/free/rest.rst),
[OurAirports data](https://ourairports.com/data/).
