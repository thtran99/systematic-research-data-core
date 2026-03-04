"""
api/routes/datasets.py

GET /datasets — list all registered datasets.
"""

from typing import Annotated

from fastapi import APIRouter, Depends

from api.dependencies import get_registry
from core.models import DatasetRegistration
from core.registry import MetadataRegistry

router = APIRouter()


@router.get(
    "/datasets",
    response_model=list[DatasetRegistration],
    summary="List registered datasets",
    description="Returns all datasets registered in the metadata registry.",
)
def list_datasets(
    registry: Annotated[MetadataRegistry, Depends(get_registry)],
) -> list[DatasetRegistration]:
    return registry.list_datasets()
