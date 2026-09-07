"""Deployment entrypoint for public, read-only live observations."""
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.live import router

app = FastAPI(title="Worldwide live aviation")
origins = [origin.strip() for origin in os.getenv("LIVE_ALLOWED_ORIGINS", "").split(",") if origin.strip()]
if origins:
    app.add_middleware(CORSMiddleware, allow_origins=origins, allow_methods=["GET"], allow_headers=["*"])
app.include_router(router)

@app.get("/health")
def health():
    return {"ok": True}
