"""
tests/conftest.py

Shared fixtures for the test suite.

All database fixtures use sqlite:///:memory: — isolated per test,
no teardown required, no file system state.
"""

import textwrap
from pathlib import Path

import pytest

from core.models import (
    ColumnSchema,
    ColumnType,
    DatasetFrequency,
    DatasetRegistration,
    DatasetSchema,
)
from core.registry import MetadataRegistry
from pipelines.ingestion import IngestionConfig


@pytest.fixture
def registry() -> MetadataRegistry:
    """Fresh in-memory registry for each test."""
    return MetadataRegistry("sqlite:///:memory:")


@pytest.fixture
def sample_schema() -> DatasetSchema:
    """
    Canonical schema with three columns:
    - date: STRING, non-nullable
    - close: FLOAT, non-nullable
    - volume: INT, nullable
    """
    return DatasetSchema(
        name="prices",
        version="1.0",
        columns=[
            ColumnSchema(name="date", dtype=ColumnType.STRING, nullable=False),
            ColumnSchema(name="close", dtype=ColumnType.FLOAT, nullable=False),
            ColumnSchema(name="volume", dtype=ColumnType.INT, nullable=True),
        ],
    )


@pytest.fixture
def sample_registration(sample_schema: DatasetSchema) -> DatasetRegistration:
    return DatasetRegistration(
        name="prices",
        version="1.0",
        owner="test",
        expected_frequency=DatasetFrequency.DAILY,
        schema_hash=sample_schema.compute_hash(),
    )


@pytest.fixture
def sample_csv(tmp_path: Path) -> Path:
    """
    10-row CSV matching sample_schema, all rows complete.
    volume is declared nullable in the schema but test data has no nulls
    (null handling is covered in test_health_checks.py).
    """
    csv_path = tmp_path / "prices.csv"
    rows = "\n".join(
        f"2026-03-{day:02d},{100.0 + day:.1f},{1000 * day}"
        for day in range(1, 11)
    )
    csv_path.write_text(f"date,close,volume\n{rows}\n")
    return csv_path


@pytest.fixture
def ingestion_config(
    sample_schema: DatasetSchema,
    sample_csv: Path,
    tmp_path: Path,
) -> IngestionConfig:
    return IngestionConfig(
        dataset_name="prices",
        version="1.0",
        owner="test",
        expected_frequency=DatasetFrequency.DAILY,
        schema=sample_schema,
        source_path=str(sample_csv),
        db_url="sqlite:///:memory:",
        parquet_base=str(tmp_path / "canonical"),
    )
