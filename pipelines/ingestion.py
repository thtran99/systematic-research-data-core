"""
pipelines/ingestion.py

End-to-end ingestion orchestrator for the systematic research data platform.

Wires together: CSV loading → schema validation → Parquet storage →
metadata registry → health reporting.

Performance layer: when rust_core is installed, CSV parsing, type normalization,
Parquet writing, and checksum computation are delegated to the Rust engine.
If rust_core is not available, the pipeline falls back to the pure Python path
transparently — no change in behaviour or output contract.

Design principles:
- Idempotent: re-running with the same inputs produces the same state.
- Deterministic: run_id is derived from inputs, not a random UUID.
- Explicit failure: validation errors are captured in IngestionResult, not raised.
  Unexpected I/O or registry errors propagate normally.
- No side effects before validation passes.
"""

import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from core.health.reporter import HealthReporter
from core.models import (
    DatasetFrequency,
    DatasetRegistration,
    DatasetSchema,
    HealthReport,
    IngestionRecord,
)
from core.registry import MetadataRegistry
from core.validation.schema_validator import SchemaValidator

try:
    import rust_core as _rust_core
    _RUST_AVAILABLE = True
except ImportError:
    _rust_core = None  # type: ignore[assignment]
    _RUST_AVAILABLE = False

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class IngestionConfig:
    """
    All parameters required for a single ingestion run.
    One config instance = one deterministic pipeline execution.
    """

    dataset_name: str
    version: str
    owner: str
    expected_frequency: DatasetFrequency
    schema: DatasetSchema
    source_path: str                          # Path to raw CSV input
    db_url: str                               # Registry database URL
    parquet_base: str = "data/canonical"      # Root for partitioned Parquet store
    nullable_threshold: float = 0.10          # Null ratio threshold for health check
    strict_columns: bool = True               # Reject extra columns during validation


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------


@dataclass
class IngestionResult:
    """
    Outcome of a single ingestion run.

    On failure (success=False), error is populated and downstream fields
    (partition_path, checksum, health_report) may be None.
    On success (success=True), error is None and all fields are populated.
    """

    success: bool
    run_id: str
    dataset_name: str
    row_count: int
    partition_path: str
    schema_hash: str
    health_report: Optional[HealthReport] = None
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


class IngestionPipeline:
    """
    Orchestrates a full ingestion cycle for a single dataset.

    Usage:
        config = IngestionConfig(...)
        result = IngestionPipeline(config).run()
    """

    def __init__(self, config: IngestionConfig) -> None:
        self._config = config
        self._registry = MetadataRegistry(config.db_url)

    def run(self) -> IngestionResult:
        """
        Execute the full ingestion pipeline.

        Steps:
        1. Parse CSV, normalize types, write Parquet, compute checksum.
           → Rust engine (rust_core) when available, Python fallback otherwise.
        2. Upsert dataset registration in registry.
        3. Record ingestion (idempotent on run_id).
        4. Run health checks on the written Parquet and persist report.
        5. Return IngestionResult.
        """
        cfg = self._config
        schema_hash = cfg.schema.compute_hash()
        partition_date = datetime.now(tz=timezone.utc).date()
        run_id = _compute_run_id(cfg.dataset_name, cfg.source_path, schema_hash, partition_date)
        partition_path = str(
            Path(cfg.parquet_base) / cfg.dataset_name / str(partition_date) / "data.parquet"
        )

        logger.info(
            "Starting ingestion run_id=%s dataset=%s engine=%s",
            run_id, cfg.dataset_name, "rust" if _RUST_AVAILABLE else "python",
        )

        # Step 1 — Parse + write Parquet (Rust or Python)
        ingest_result = _ingest(cfg, partition_path, schema_hash)
        if not ingest_result["success"]:
            return IngestionResult(
                success=False,
                run_id=run_id,
                dataset_name=cfg.dataset_name,
                row_count=ingest_result.get("row_count", 0),
                partition_path=partition_path,
                schema_hash=schema_hash,
                error=ingest_result["error"],
            )

        row_count: int = ingest_result["row_count"]
        checksum: str = ingest_result["checksum"]

        # Step 2 — Register dataset (upsert)
        self._registry.register_dataset(DatasetRegistration(
            name=cfg.dataset_name,
            version=cfg.version,
            owner=cfg.owner,
            expected_frequency=cfg.expected_frequency,
            schema_hash=schema_hash,
        ))

        # Step 3 — Record ingestion (idempotent on run_id)
        self._registry.record_ingestion(IngestionRecord(
            run_id=run_id,
            dataset_name=cfg.dataset_name,
            version=cfg.version,
            ingested_at=datetime.now(tz=timezone.utc),
            row_count=row_count,
            checksum=checksum,
            source_path=cfg.source_path,
            partition_path=partition_path,
            schema_hash=schema_hash,
        ))

        # Step 4 — Health check on the written Parquet (canonical data, not raw CSV)
        df = pd.read_parquet(partition_path)
        reporter = HealthReporter(self._registry, nullable_threshold=cfg.nullable_threshold)
        health_report = reporter.run(cfg.dataset_name, df, cfg.schema)
        logger.info(
            "Health check complete dataset=%s status=%s",
            cfg.dataset_name, health_report.status.value,
        )

        return IngestionResult(
            success=True,
            run_id=run_id,
            dataset_name=cfg.dataset_name,
            row_count=row_count,
            partition_path=partition_path,
            schema_hash=schema_hash,
            health_report=health_report,
        )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _ingest(cfg: "IngestionConfig", partition_path: str, schema_hash: str) -> dict:
    """
    Parse CSV, normalize types, write Parquet, compute checksum.

    Returns a dict with keys:
      success (bool), row_count (int), checksum (str), error (str|None)

    Delegates to the Rust engine when available; falls back to Python otherwise.
    """
    if _RUST_AVAILABLE:
        return _ingest_rust(cfg, partition_path)
    return _ingest_python(cfg, partition_path)


def _ingest_rust(cfg: "IngestionConfig", partition_path: str) -> dict:
    """Rust path: parse + normalize + write via rust_core.ingest()."""
    try:
        result = _rust_core.ingest(
            source_path=cfg.source_path,
            schema_json=cfg.schema.model_dump_json(),
            output_path=partition_path,
        )
        return {
            "success": True,
            "row_count": result.row_count,
            "checksum": result.checksum,
            "error": None,
        }
    except ValueError as exc:
        return {"success": False, "row_count": 0, "checksum": "", "error": str(exc)}


def _ingest_python(cfg: "IngestionConfig", partition_path: str) -> dict:
    """Python fallback: pandas read_csv + SchemaValidator + to_parquet."""
    df = pd.read_csv(cfg.source_path)
    logger.debug("Loaded %d rows from %s (Python path)", len(df), cfg.source_path)

    validator = SchemaValidator(strict_columns=cfg.strict_columns)
    validation = validator.validate(df, cfg.schema)
    if not validation.valid:
        error_summary = "; ".join(
            f"[{e.error_type}] {e.column}: {e.details}" for e in validation.errors
        )
        logger.warning("Schema validation failed for %s: %s", cfg.dataset_name, error_summary)
        return {
            "success": False,
            "row_count": len(df),
            "checksum": "",
            "error": f"Schema validation failed ({len(validation.errors)} error(s)): {error_summary}",
        }

    _write_parquet(df, partition_path)
    checksum = _sha256_file(partition_path)
    return {"success": True, "row_count": len(df), "checksum": checksum, "error": None}


def _compute_run_id(
    dataset_name: str,
    source_path: str,
    schema_hash: str,
    partition_date: object,
) -> str:
    """
    Deterministic run_id: SHA-256 of (dataset, source, schema, date).
    Re-running with identical inputs on the same day yields the same run_id,
    making registry.record_ingestion() a no-op (idempotent re-runs).
    """
    payload = f"{dataset_name}|{source_path}|{schema_hash}|{partition_date}"
    return hashlib.sha256(payload.encode()).hexdigest()


def _write_parquet(df: pd.DataFrame, path: str) -> None:
    """Write DataFrame to Parquet, creating parent directories as needed."""
    dest = Path(path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(dest, engine="pyarrow", index=False)


def _sha256_file(path: str) -> str:
    """Compute SHA-256 checksum of a file. Used to detect post-write corruption."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()
