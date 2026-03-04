"""
core/health/checks.py

Individual health check functions. Each check is a pure function that takes
explicit inputs and returns a CheckResult — no registry I/O, no side effects.

Checks are designed to be composable and independently testable.
"""

import math
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd

from core.models import (
    DatasetFrequency,
    DatasetRegistration,
    DatasetSchema,
    IngestionRecord,
)


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class CheckResult:
    """
    Result of a single health check.
    Details must be JSON-serializable (str, int, float, bool, None, dict, list).
    """

    ok: bool
    details: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Freshness thresholds
# ---------------------------------------------------------------------------

_FRESHNESS_MAX_AGE: dict[DatasetFrequency, timedelta] = {
    DatasetFrequency.INTRADAY: timedelta(hours=4),
    DatasetFrequency.DAILY: timedelta(hours=26),   # buffer for late delivery
    DatasetFrequency.WEEKLY: timedelta(days=8),
    DatasetFrequency.MONTHLY: timedelta(days=33),
    DatasetFrequency.ON_DEMAND: None,               # no SLA
}


# ---------------------------------------------------------------------------
# Check functions
# ---------------------------------------------------------------------------


def check_freshness(
    registration: DatasetRegistration,
    latest_record: Optional[IngestionRecord],
) -> CheckResult:
    """
    Validates that the most recent ingestion is within the expected window
    derived from the dataset's declared frequency.

    ON_DEMAND datasets are always considered fresh.
    Missing ingestion record → stale by definition.
    """
    if registration.expected_frequency == DatasetFrequency.ON_DEMAND:
        return CheckResult(ok=True, details={"reason": "on_demand_no_sla"})

    if latest_record is None:
        return CheckResult(
            ok=False,
            details={"reason": "no_ingestion_record_found"},
        )

    max_age = _FRESHNESS_MAX_AGE[registration.expected_frequency]
    now = datetime.now(tz=timezone.utc)

    # Normalize ingested_at to UTC-aware for safe comparison
    ingested_at = latest_record.ingested_at
    if ingested_at.tzinfo is None:
        ingested_at = ingested_at.replace(tzinfo=timezone.utc)

    age = now - ingested_at
    ok = age <= max_age

    return CheckResult(
        ok=ok,
        details={
            "ingested_at": ingested_at.isoformat(),
            "age_seconds": round(age.total_seconds()),
            "max_age_seconds": round(max_age.total_seconds()),
            "expected_frequency": registration.expected_frequency.value,
        },
    )


def check_volume(
    ingestion_history: list[IngestionRecord],
) -> CheckResult:
    """
    Detects volume anomalies by comparing the latest row count against
    a rolling baseline computed from historical ingestion records.

    Baseline: mean ± 3 standard deviations of historical row_counts.
    Requires ≥3 records for a meaningful baseline; returns ok=True with a
    note when history is insufficient.

    The latest record is excluded from the baseline computation to avoid
    contaminating the reference window with the value under evaluation.
    """
    if len(ingestion_history) == 0:
        return CheckResult(ok=False, details={"reason": "no_ingestion_history"})

    current_count = ingestion_history[0].row_count

    if len(ingestion_history) < 4:  # <3 baseline records (latest + 3 historical)
        return CheckResult(
            ok=True,
            details={
                "reason": "insufficient_history",
                "records_available": len(ingestion_history),
                "current_row_count": current_count,
            },
        )

    # Baseline from all records except the latest
    historical_counts = [r.row_count for r in ingestion_history[1:]]
    mean = statistics.mean(historical_counts)
    std = statistics.pstdev(historical_counts)

    if std == 0:
        # Constant volume — any deviation is anomalous
        ok = current_count == mean
        return CheckResult(
            ok=ok,
            details={
                "current_row_count": current_count,
                "baseline_mean": mean,
                "baseline_std": 0.0,
                "lower_bound": mean,
                "upper_bound": mean,
            },
        )

    lower = mean - 3 * std
    upper = mean + 3 * std
    ok = lower <= current_count <= upper

    return CheckResult(
        ok=ok,
        details={
            "current_row_count": current_count,
            "baseline_mean": round(mean, 2),
            "baseline_std": round(std, 2),
            "lower_bound": round(lower, 2),
            "upper_bound": round(upper, 2),
        },
    )


def check_null_ratio(
    df: pd.DataFrame,
    schema: DatasetSchema,
    nullable_threshold: float = 0.10,
) -> CheckResult:
    """
    Validates null ratios against the schema contract:
    - Non-nullable columns (nullable=False): zero nulls tolerated.
    - Nullable columns: null ratio must be ≤ nullable_threshold.

    Per-column ratios are always included in details for observability.
    Columns present in the schema but absent from the DataFrame are flagged.
    """
    if df.empty:
        return CheckResult(ok=False, details={"reason": "empty_dataframe"})

    total_rows = len(df)
    column_map = {col.name: col for col in schema.columns}
    violations: list[dict] = []
    ratios: dict[str, float] = {}

    for col_name, col_def in column_map.items():
        if col_name not in df.columns:
            violations.append({"column": col_name, "reason": "column_missing_from_dataframe"})
            ratios[col_name] = None  # type: ignore[assignment]
            continue

        null_count = int(df[col_name].isna().sum())
        ratio = null_count / total_rows
        ratios[col_name] = round(ratio, 6)

        threshold = 0.0 if not col_def.nullable else nullable_threshold
        if ratio > threshold:
            violations.append(
                {
                    "column": col_name,
                    "null_ratio": round(ratio, 6),
                    "threshold": threshold,
                    "nullable": col_def.nullable,
                }
            )

    ok = len(violations) == 0
    return CheckResult(
        ok=ok,
        details={
            "total_rows": total_rows,
            "nullable_threshold": nullable_threshold,
            "column_null_ratios": ratios,
            "violations": violations,
        },
    )


def check_schema_hash(
    registration: DatasetRegistration,
    latest_record: Optional[IngestionRecord],
) -> CheckResult:
    """
    Compares the schema hash recorded at ingestion time against the hash
    registered in the dataset contract.

    A mismatch indicates silent schema drift — the data was parsed under
    a schema that no longer matches the registered contract.
    Missing ingestion record → schema cannot be verified → not ok.
    """
    if latest_record is None:
        return CheckResult(
            ok=False,
            details={"reason": "no_ingestion_record_found"},
        )

    expected = registration.schema_hash
    actual = latest_record.schema_hash
    ok = expected == actual

    return CheckResult(
        ok=ok,
        details={
            "expected_hash": expected,
            "actual_hash": actual,
            "match": ok,
        },
    )
