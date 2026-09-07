# Public live service only. Historical data and credentials never enter the image.
FROM python:3.12-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PORT=8000
WORKDIR /app
COPY backend/requirements-live.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt && useradd --create-home appuser
COPY backend/app/__init__.py backend/app/live.py backend/app/live_main.py backend/app/opensky_client.py ./app/
COPY backend/data/world_airports.csv backend/data/ny_airports.csv ./data/
USER appuser
EXPOSE 8000
CMD ["sh", "-c", "exec uvicorn app.live_main:app --host 0.0.0.0 --port ${PORT:-8000} --workers 1"]
