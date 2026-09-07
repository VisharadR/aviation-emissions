# GitHub Pages deployment

The static live dashboard is prepared for the repository's existing Pages origin:
https://visharadr.github.io/aviation-emissions/

Pages cannot run Python or Next.js API routes. The Pages build therefore exports
only the world and New York live views and fetches a separately hosted, read-only
API. The normal local Next.js application and historical dashboard are unchanged.

## Backend

The root Dockerfile packages only the live API, public airport catalogs and pinned
runtime requirements. It runs as a non-root user with one worker so the shared
OpenSky rate limiter remains effective. Historical mutation endpoints, private
historical data, and credentials files are excluded from the image.

`render.yaml` can create a free backend from this repository. Supply
`OPENSKY_CLIENT_ID` and `OPENSKY_CLIENT_SECRET` as private runtime variables on the
host. Never put these in a public GitHub variable or the browser bundle.
`LIVE_ALLOWED_ORIGINS` must include `https://visharadr.github.io` for browser access.
Render free services sleep when inactive and may take time to wake; use an
appropriate host/plan if uninterrupted service is required. No paid plan is selected.

Any HTTPS host supporting the Dockerfile or Python/uvicorn can be used instead.
Its command is `uvicorn app.live_main:app --host 0.0.0.0 --port $PORT --workers 1`
from `backend`, with `requirements-live.txt` installed.

## Publish

1. Deploy the backend and verify `/health` and `/live/world`, including CORS.
2. Add GitHub repository Actions **variable** `LIVE_API_URL` with its public HTTPS
   origin, with no trailing slash or path. This URL is public; OpenSky secrets are not.
3. Set Settings → Pages → Source to **GitHub Actions**.
4. Run **Deploy live dashboard to GitHub Pages** from Actions.

The workflow refuses to publish without a reachable backend. It builds into an
ignored staging directory, preserves the main application sources, sets the
repository base path, and deploys only generated static files.

Deployment is not complete until the Pages job succeeds and the published page
loads live observations from the configured backend.
