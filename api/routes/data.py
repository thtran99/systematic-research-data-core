"""
api/routes/data.py

Point-in-time data query endpoint.

GET /data/{dataset}?as_of=YYYY-MM-DD[&symbol=X]

Returns the rows from the latest ingestion that was available at as_of,
filtered to rows whose date <= as_of. If symbol is supplied, only rows
for that ticker are returned.

This prevents look-ahead bias in two ways:
  1. Ingestion selection: only ingestions that ran before as_of are visible
     (revised/adjusted prices re-ingested after as_of are excluded).
  2. Row filter: only rows with date <= as_of are returned
     (no future price data leaks into the result set).
"""

from datetime import date, datetime
from typing import Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query

from api.dependencies import get_registry
from api.schemas import DataQueryResponse
from core.registry import MetadataRegistry

router = APIRouter()


@router.get("/data/{dataset}", response_model=DataQueryResponse)
def get_data(
    dataset: str,
    as_of: str = Query(
        ...,
        description="Point-in-time date (YYYY-MM-DD). Only data known at this date is returned.",
    ),
    symbol: Optional[str] = Query(
        None,
        description="Filter rows by ticker symbol (e.g. AAPL). Omit to return all symbols.",
    ),
    registry: MetadataRegistry = Depends(get_registry),
) -> DataQueryResponse:
    # Validate as_of format
    try:
        as_of_date = date.fromisoformat(as_of)
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid as_of date {as_of!r}. Expected YYYY-MM-DD.",
        )

    # End-of-day on as_of: capture any ingestion that ran during that calendar day
    as_of_dt = datetime(as_of_date.year, as_of_date.month, as_of_date.day, 23, 59, 59)

    # Find the most recent ingestion available at as_of
    record = registry.get_ingestion_as_of(dataset, as_of_dt)
    if not record:
        raise HTTPException(
            status_code=404,
            detail=f"No ingestion found for dataset '{dataset}' at or before {as_of}.",
        )

    # Load canonical Parquet
    df = pd.read_parquet(record.partition_path)

    # Point-in-time row filter: exclude rows whose trading date is after as_of
    if "date" in df.columns:
        df = df[df["date"].astype(str) <= as_of]

    # Symbol filter
    if symbol is not None:
        if "symbol" not in df.columns:
            raise HTTPException(
                status_code=400,
                detail=f"Dataset '{dataset}' has no 'symbol' column.",
            )
        df = df[df["symbol"] == symbol]

    return DataQueryResponse(
        dataset_name=dataset,
        as_of=as_of,
        symbol=symbol,
        ingestion_run_id=record.run_id,
        ingested_at=record.ingested_at,
        row_count=len(df),
        rows=df.to_dict(orient="records"),
    )
