"""
api/routes/metadata.py

GET /metadata/{dataset} — dataset registration + latest ingestion record.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_registry
from api.schemas import DatasetMetadataResponse
from core.registry import MetadataRegistry

router = APIRouter()


@router.get(
    "/metadata/{dataset}",
    response_model=DatasetMetadataResponse,
    summary="Dataset metadata",
    description=(
        "Returns the registration record and most recent ingestion record "
        "for the given dataset. latest_ingestion is null if the dataset has "
        "never been ingested."
    ),
)
def get_metadata(
    dataset: str,
    registry: Annotated[MetadataRegistry, Depends(get_registry)],
) -> DatasetMetadataResponse:
    registration = registry.get_dataset(dataset)
    if registration is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset}' not found.")

    history = registry.get_ingestion_history(dataset, limit=1)
    latest_ingestion = history[0] if history else None

    return DatasetMetadataResponse(
        registration=registration,
        latest_ingestion=latest_ingestion,
    )
