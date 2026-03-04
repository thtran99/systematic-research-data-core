"""
api/routes/health.py

GET /health/{dataset} — latest health report for a dataset.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_registry
from core.models import HealthReport
from core.registry import MetadataRegistry

router = APIRouter()


@router.get(
    "/health/{dataset}",
    response_model=HealthReport,
    summary="Latest health report",
    description=(
        "Returns the most recent health report for the given dataset. "
        "404 if the dataset is not registered or has not been health-checked yet."
    ),
)
def get_health(
    dataset: str,
    registry: Annotated[MetadataRegistry, Depends(get_registry)],
) -> HealthReport:
    if registry.get_dataset(dataset) is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset}' not found.")

    report = registry.get_latest_health(dataset)
    if report is None:
        raise HTTPException(
            status_code=404,
            detail=f"No health report available for dataset '{dataset}'.",
        )

    return report
