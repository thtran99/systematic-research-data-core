"""
tests/test_health_checks.py

Unit tests for individual health check functions and HealthReporter orchestration.

Each check function is tested in isolation — no registry, no I/O.
HealthReporter tests verify orchestration, status aggregation, and persistence.
"""

import statistics
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import pytest

from core.health.checks import (
    CheckResult,
    check_freshness,
    check_null_ratio,
    check_schema_hash,
    check_volume,
)
from core.health.reporter import HealthReporter
from core.models import (
    ColumnSchema,
    ColumnType,
    DatasetFrequency,
    DatasetRegistration,
    DatasetSchema,
    HealthStatus,
    IngestionRecord,
)
from core.registry import MetadataRegistry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_registration(
    frequency: DatasetFrequency = DatasetFrequency.DAILY,
    schema_hash: str = "abc123",
) -> DatasetRegistration:
    return DatasetRegistration(
        name="prices",
        version="1.0",
        owner="test",
        expected_frequency=frequency,
        schema_hash=schema_hash,
    )


def _make_record(
    ingested_at: datetime,
    row_count: int = 100,
    schema_hash: str = "abc123",
    run_id: str = "run-1",
) -> IngestionRecord:
    return IngestionRecord(
        run_id=run_id,
        dataset_name="prices",
        version="1.0",
        ingested_at=ingested_at,
        row_count=row_count,
        checksum="checksum",
        source_path="data/raw/prices.csv",
        partition_path="data/canonical/prices/2026-03-04/data.parquet",
        schema_hash=schema_hash,
    )


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


# ---------------------------------------------------------------------------
# check_freshness
# ---------------------------------------------------------------------------


def test_freshness_on_demand_always_ok() -> None:
    registration = _make_registration(frequency=DatasetFrequency.ON_DEMAND)
    record = _make_record(ingested_at=_now() - timedelta(days=365))

    result = check_freshness(registration, record)

    assert result.ok is True
    assert result.details["reason"] == "on_demand_no_sla"


def test_freshness_no_record_fails() -> None:
    registration = _make_registration(frequency=DatasetFrequency.DAILY)

    result = check_freshness(registration, None)

    assert result.ok is False
    assert result.details["reason"] == "no_ingestion_record_found"


def test_freshness_within_window_ok() -> None:
    registration = _make_registration(frequency=DatasetFrequency.DAILY)
    record = _make_record(ingested_at=_now() - timedelta(hours=1))

    result = check_freshness(registration, record)

    assert result.ok is True


def test_freshness_stale_fails() -> None:
    registration = _make_registration(frequency=DatasetFrequency.DAILY)
    record = _make_record(ingested_at=_now() - timedelta(hours=30))

    result = check_freshness(registration, record)

    assert result.ok is False


def test_freshness_details_populated() -> None:
    registration = _make_registration(frequency=DatasetFrequency.DAILY)
    record = _make_record(ingested_at=_now() - timedelta(hours=1))

    result = check_freshness(registration, record)

    assert "age_seconds" in result.details
    assert "max_age_seconds" in result.details
    assert "ingested_at" in result.details
    assert result.details["expected_frequency"] == "daily"


# ---------------------------------------------------------------------------
# check_volume
# ---------------------------------------------------------------------------


def _make_history(counts: list[int]) -> list[IngestionRecord]:
    """Build ingestion records ordered newest→oldest (as registry returns them)."""
    now = _now()
    return [
        _make_record(
            ingested_at=now - timedelta(days=i),
            row_count=count,
            run_id=f"run-{i}",
        )
        for i, count in enumerate(counts)
    ]


def test_volume_empty_history_fails() -> None:
    result = check_volume([])

    assert result.ok is False
    assert result.details["reason"] == "no_ingestion_history"


def test_volume_insufficient_history_ok() -> None:
    history = _make_history([100, 105])  # only 2 records

    result = check_volume(history)

    assert result.ok is True
    assert result.details["reason"] == "insufficient_history"
    assert result.details["records_available"] == 2


def test_volume_within_baseline_ok() -> None:
    # 9 historical records + 1 current (within mean ± 3σ)
    baseline = [100, 102, 98, 101, 99, 103, 97, 100, 101]
    current = 100
    history = _make_history([current] + baseline)

    result = check_volume(history)

    assert result.ok is True
    assert "baseline_mean" in result.details
    assert "baseline_std" in result.details


def test_volume_anomaly_detected() -> None:
    baseline = [100, 102, 98, 101, 99, 103, 97, 100, 101]
    mean = statistics.mean(baseline)
    std = statistics.pstdev(baseline)
    anomalous_count = int(mean + 10 * std) + 1000  # well outside 3σ
    history = _make_history([anomalous_count] + baseline)

    result = check_volume(history)

    assert result.ok is False
    assert result.details["current_row_count"] == anomalous_count


def test_volume_constant_exact_match_ok() -> None:
    # All records have the same count → std=0, exact match required
    history = _make_history([100, 100, 100, 100, 100])

    result = check_volume(history)

    assert result.ok is True


def test_volume_constant_deviation_fails() -> None:
    # All historical records: 100. Current: 200. std=0 → any deviation fails.
    history = _make_history([200, 100, 100, 100, 100])

    result = check_volume(history)

    assert result.ok is False


# ---------------------------------------------------------------------------
# check_null_ratio
# ---------------------------------------------------------------------------


def _make_schema(nullable_volume: bool = True) -> DatasetSchema:
    return DatasetSchema(
        name="prices",
        version="1.0",
        columns=[
            ColumnSchema(name="date", dtype=ColumnType.STRING, nullable=False),
            ColumnSchema(name="close", dtype=ColumnType.FLOAT, nullable=False),
            ColumnSchema(name="volume", dtype=ColumnType.INT, nullable=nullable_volume),
        ],
    )


def _clean_df(rows: int = 20) -> pd.DataFrame:
    return pd.DataFrame({
        "date": [f"2026-03-{i:02d}" for i in range(1, rows + 1)],
        "close": [100.0 + i for i in range(rows)],
        "volume": [1000 * i for i in range(rows)],
    })


def test_null_ratio_empty_df_fails() -> None:
    schema = _make_schema()
    result = check_null_ratio(pd.DataFrame(), schema)

    assert result.ok is False
    assert result.details["reason"] == "empty_dataframe"


def test_null_ratio_clean_data_ok() -> None:
    schema = _make_schema()
    df = _clean_df()

    result = check_null_ratio(df, schema)

    assert result.ok is True
    assert result.details["violations"] == []


def test_null_ratio_non_nullable_violation() -> None:
    schema = _make_schema()
    df = _clean_df()
    df.loc[0, "close"] = None  # close is non-nullable

    result = check_null_ratio(df, schema)

    assert result.ok is False
    violation_cols = [v["column"] for v in result.details["violations"]]
    assert "close" in violation_cols


def test_null_ratio_nullable_within_threshold_ok() -> None:
    schema = _make_schema(nullable_volume=True)
    df = _clean_df(rows=100)
    # 5% nulls in volume (nullable) — below 10% threshold
    df.loc[:4, "volume"] = None

    result = check_null_ratio(df, schema, nullable_threshold=0.10)

    assert result.ok is True


def test_null_ratio_nullable_exceeds_threshold_fails() -> None:
    schema = _make_schema(nullable_volume=True)
    df = _clean_df(rows=100)
    # 15% nulls in volume (nullable) — above 10% threshold
    df.loc[:14, "volume"] = None

    result = check_null_ratio(df, schema, nullable_threshold=0.10)

    assert result.ok is False
    violation_cols = [v["column"] for v in result.details["violations"]]
    assert "volume" in violation_cols


# ---------------------------------------------------------------------------
# check_schema_hash
# ---------------------------------------------------------------------------


def test_schema_hash_match_ok() -> None:
    registration = _make_registration(schema_hash="abc123")
    record = _make_record(ingested_at=_now(), schema_hash="abc123")

    result = check_schema_hash(registration, record)

    assert result.ok is True
    assert result.details["match"] is True


def test_schema_hash_mismatch_fails() -> None:
    registration = _make_registration(schema_hash="abc123")
    record = _make_record(ingested_at=_now(), schema_hash="xyz999")

    result = check_schema_hash(registration, record)

    assert result.ok is False
    assert result.details["expected_hash"] == "abc123"
    assert result.details["actual_hash"] == "xyz999"
    assert result.details["match"] is False


# ---------------------------------------------------------------------------
# HealthReporter
# ---------------------------------------------------------------------------


def test_reporter_raises_if_not_registered(
    registry: MetadataRegistry,
    sample_schema: DatasetSchema,
) -> None:
    reporter = HealthReporter(registry)
    df = _clean_df()

    with pytest.raises(ValueError, match="not registered"):
        reporter.run("prices", df, sample_schema)


def test_reporter_healthy(
    registry: MetadataRegistry,
    sample_schema: DatasetSchema,
    sample_registration: DatasetRegistration,
) -> None:
    registry.register_dataset(sample_registration)
    registry.record_ingestion(
        _make_record(
            ingested_at=_now() - timedelta(hours=1),
            schema_hash=sample_schema.compute_hash(),
        )
    )

    reporter = HealthReporter(registry)
    df = _clean_df()
    report = reporter.run("prices", df, sample_schema)

    # With only 1 ingestion record, volume check returns ok=True ("insufficient_history")
    # freshness ok (ingested 1h ago, DAILY threshold = 26h)
    # null_ratio ok (no nulls)
    # schema_hash ok (hashes match)
    assert report.status == HealthStatus.HEALTHY
    assert report.freshness_ok is True
    assert report.null_ratio_ok is True
    assert report.schema_hash_ok is True


def test_reporter_failing_on_schema_drift(
    registry: MetadataRegistry,
    sample_schema: DatasetSchema,
    sample_registration: DatasetRegistration,
) -> None:
    registry.register_dataset(sample_registration)  # registered with hash A
    registry.record_ingestion(
        _make_record(
            ingested_at=_now() - timedelta(hours=1),
            schema_hash="different_hash_xyz",  # ingested with hash B → drift
        )
    )

    reporter = HealthReporter(registry)
    df = _clean_df()
    report = reporter.run("prices", df, sample_schema)

    assert report.status == HealthStatus.FAILING
    assert report.schema_hash_ok is False


def test_reporter_report_persisted(
    registry: MetadataRegistry,
    sample_schema: DatasetSchema,
    sample_registration: DatasetRegistration,
) -> None:
    registry.register_dataset(sample_registration)
    registry.record_ingestion(
        _make_record(
            ingested_at=_now() - timedelta(hours=1),
            schema_hash=sample_schema.compute_hash(),
        )
    )

    reporter = HealthReporter(registry)
    report = reporter.run("prices", _clean_df(), sample_schema)

    persisted = registry.get_latest_health("prices")
    assert persisted is not None
    assert persisted.status == report.status
    assert persisted.dataset_name == "prices"
