"""
core/health/reporter.py

Orchestrates the 4 health checks into a HealthReport and persists it
to the metadata registry.

HealthReporter is the single entry point for health evaluation.
It reads from the registry, delegates to pure check functions, and writes
the result back — no check function touches the registry directly.
"""

from datetime import datetime, timezone

import pandas as pd

from core.health.checks import (
    CheckResult,
    check_freshness,
    check_null_ratio,
    check_schema_hash,
    check_volume,
)
from core.models import DatasetSchema, HealthReport, HealthStatus
from core.registry import MetadataRegistry


class HealthReporter:
    """
    Runs all health checks for a dataset and produces a HealthReport.

    Usage:
        reporter = HealthReporter(registry)
        report = reporter.run("prices", df, schema)
    """

    def __init__(
        self,
        registry: MetadataRegistry,
        nullable_threshold: float = 0.10,
    ) -> None:
        self._registry = registry
        self._nullable_threshold = nullable_threshold

    def run(
        self,
        dataset_name: str,
        df: pd.DataFrame,
        schema: DatasetSchema,
    ) -> HealthReport:
        """
        Execute all health checks and persist the result.

        Args:
            dataset_name: Registered dataset name.
            df: Latest ingested partition as a DataFrame.
            schema: The canonical schema contract to validate against.

        Returns:
            A fully populated HealthReport that has been written to the registry.

        Raises:
            ValueError: If the dataset is not registered.
        """
        registration = self._registry.get_dataset(dataset_name)
        if registration is None:
            raise ValueError(
                f"Dataset '{dataset_name}' is not registered. "
                "Call registry.register_dataset() before running health checks."
            )

        history = self._registry.get_ingestion_history(dataset_name, limit=31)
        latest_record = history[0] if history else None

        # Run checks independently
        freshness = check_freshness(registration, latest_record)
        volume = check_volume(history)
        null_ratio = check_null_ratio(df, schema, self._nullable_threshold)
        schema_hash = check_schema_hash(registration, latest_record)

        status = _aggregate_status(freshness, volume, null_ratio, schema_hash)

        report = HealthReport(
            dataset_name=dataset_name,
            checked_at=datetime.now(tz=timezone.utc),
            status=status,
            freshness_ok=freshness.ok,
            volume_ok=volume.ok,
            null_ratio_ok=null_ratio.ok,
            schema_hash_ok=schema_hash.ok,
            details={
                "freshness": freshness.details,
                "volume": volume.details,
                "null_ratio": null_ratio.details,
                "schema_hash": schema_hash.details,
            },
        )

        self._registry.write_health_report(report)
        return report


# ---------------------------------------------------------------------------
# Status aggregation
# ---------------------------------------------------------------------------


def _aggregate_status(
    freshness: CheckResult,
    volume: CheckResult,
    null_ratio: CheckResult,
    schema_hash: CheckResult,
) -> HealthStatus:
    """
    Derive overall status from individual check results.

    Rules (in priority order):
    - FAILING: schema_hash fails (schema drift is critical and unacceptable)
    - FAILING: 3 or more checks fail simultaneously
    - DEGRADED: 1–2 non-critical checks fail
    - HEALTHY: all checks pass
    """
    if not schema_hash.ok:
        return HealthStatus.FAILING

    failures = sum([
        not freshness.ok,
        not volume.ok,
        not null_ratio.ok,
    ])

    if failures >= 3:
        return HealthStatus.FAILING
    if failures >= 1:
        return HealthStatus.DEGRADED
    return HealthStatus.HEALTHY
