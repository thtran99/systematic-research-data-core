"""
api/schemas.py

API-specific response schemas.

Domain models (DatasetRegistration, HealthReport, IngestionRecord) are reused
directly as response models. Only composite shapes that don't exist in the
domain layer are defined here.
"""

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel

from core.models import DatasetRegistration, IngestionRecord


class DatasetMetadataResponse(BaseModel):
    """
    Combined response for GET /metadata/{dataset}.
    Joins the dataset registration with its most recent ingestion record.
    """

    registration: DatasetRegistration
    latest_ingestion: Optional[IngestionRecord] = None


class DataQueryResponse(BaseModel):
    """
    Response for GET /data/{dataset}?as_of=DATE&symbol=X.

    Returns rows from the latest ingestion whose ingested_at <= as_of,
    filtered to date <= as_of and optionally to a single symbol.
    This guarantees point-in-time correctness: no look-ahead bias.
    """

    dataset_name: str
    as_of: str                   # The requested as-of date (YYYY-MM-DD)
    symbol: Optional[str]        # Symbol filter applied, or None for all
    ingestion_run_id: str        # Which ingestion was used (audit trail)
    ingested_at: datetime        # When that ingestion ran
    row_count: int
    rows: list[dict[str, Any]]
