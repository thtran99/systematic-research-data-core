"""
api/schemas.py

API-specific response schemas.

Domain models (DatasetRegistration, HealthReport, IngestionRecord) are reused
directly as response models. Only composite shapes that don't exist in the
domain layer are defined here.
"""

from typing import Optional

from pydantic import BaseModel

from core.models import DatasetRegistration, IngestionRecord


class DatasetMetadataResponse(BaseModel):
    """
    Combined response for GET /metadata/{dataset}.
    Joins the dataset registration with its most recent ingestion record.
    """

    registration: DatasetRegistration
    latest_ingestion: Optional[IngestionRecord] = None
