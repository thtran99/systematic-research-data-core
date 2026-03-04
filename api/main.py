"""
api/main.py

FastAPI application entry point.

Start with:
    uvicorn api.main:app --reload

Configure registry backend via:
    DATABASE_URL=postgresql://user:pass@host/db uvicorn api.main:app
"""

from fastapi import FastAPI

from api.routes import datasets, health, metadata

app = FastAPI(
    title="Systematic Research Data Core",
    description=(
        "Read-only research-facing API for the canonical data registry. "
        "Exposes dataset registrations, health reports, and ingestion metadata."
    ),
    version="0.1.0",
)

app.include_router(datasets.router)
app.include_router(health.router)
app.include_router(metadata.router)
