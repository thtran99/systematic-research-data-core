"""
core/registry.py

SQL-backed metadata registry for the systematic research data platform.

Single point of write for all dataset metadata, ingestion records, and health
reports. Consumers (health engine, API, pipelines) read from here; they do not
write directly to the underlying tables.

Backed by SQLite by default; PostgreSQL-ready via DATABASE_URL env var.
"""

import json
import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import Column, DateTime, Integer, String, Text, Boolean, desc, create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from core.models import (
    DatasetFrequency,
    DatasetRegistration,
    HealthReport,
    HealthStatus,
    IngestionRecord,
)


# ---------------------------------------------------------------------------
# ORM definitions
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
    pass


class DatasetRegistrationRow(Base):
    __tablename__ = "dataset_registry"

    name = Column(String, primary_key=True)
    version = Column(String, nullable=False)
    owner = Column(String, nullable=False)
    expected_frequency = Column(String, nullable=False)
    schema_hash = Column(String, nullable=False)
    description = Column(Text, default="")
    registered_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class IngestionRecordRow(Base):
    __tablename__ = "ingestion_records"

    run_id = Column(String, primary_key=True)
    dataset_name = Column(String, nullable=False, index=True)
    version = Column(String, nullable=False)
    ingested_at = Column(DateTime, nullable=False)
    row_count = Column(Integer, nullable=False)
    checksum = Column(String, nullable=False)
    source_path = Column(String, nullable=False)
    partition_path = Column(String, nullable=False)
    schema_hash = Column(String, nullable=False)


class HealthReportRow(Base):
    __tablename__ = "health_reports"

    id = Column(String, primary_key=True)
    dataset_name = Column(String, nullable=False, index=True)
    checked_at = Column(DateTime, nullable=False)
    status = Column(String, nullable=False)
    freshness_ok = Column(Boolean, nullable=False)
    volume_ok = Column(Boolean, nullable=False)
    null_ratio_ok = Column(Boolean, nullable=False)
    schema_hash_ok = Column(Boolean, nullable=False)
    details = Column(Text, nullable=False)  # JSON blob


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class MetadataRegistry:
    """
    Centralized metadata store for datasets, ingestion runs, and health reports.

    All writes go through this class. Direct table manipulation outside this
    interface is not permitted — schema evolution must go through here.
    """

    def __init__(self, db_url: str) -> None:
        self._engine = create_engine(db_url, echo=False, future=True)
        Base.metadata.create_all(self._engine)
        self._Session = sessionmaker(bind=self._engine, expire_on_commit=False)

    # ------------------------------------------------------------------
    # Dataset registration
    # ------------------------------------------------------------------

    def register_dataset(self, registration: DatasetRegistration) -> None:
        """
        Upsert a dataset registration.
        Safe to call on re-runs — updates schema_hash and version if changed.
        """
        with self._Session() as session:
            row = session.get(DatasetRegistrationRow, registration.name)
            if row:
                row.version = registration.version
                row.owner = registration.owner
                row.expected_frequency = registration.expected_frequency.value
                row.schema_hash = registration.schema_hash
                row.description = registration.description
            else:
                session.add(
                    DatasetRegistrationRow(
                        name=registration.name,
                        version=registration.version,
                        owner=registration.owner,
                        expected_frequency=registration.expected_frequency.value,
                        schema_hash=registration.schema_hash,
                        description=registration.description,
                    )
                )
            session.commit()

    def get_dataset(self, name: str) -> Optional[DatasetRegistration]:
        with self._Session() as session:
            row = session.get(DatasetRegistrationRow, name)
            if not row:
                return None
            return _to_registration(row)

    def list_datasets(self) -> list[DatasetRegistration]:
        with self._Session() as session:
            rows = session.query(DatasetRegistrationRow).all()
            return [_to_registration(r) for r in rows]

    # ------------------------------------------------------------------
    # Ingestion records
    # ------------------------------------------------------------------

    def record_ingestion(self, record: IngestionRecord) -> None:
        """
        Write an ingestion record.
        Idempotent: if the run_id already exists, the call is a no-op.
        """
        with self._Session() as session:
            if session.get(IngestionRecordRow, record.run_id):
                return
            session.add(
                IngestionRecordRow(
                    run_id=record.run_id,
                    dataset_name=record.dataset_name,
                    version=record.version,
                    ingested_at=record.ingested_at,
                    row_count=record.row_count,
                    checksum=record.checksum,
                    source_path=record.source_path,
                    partition_path=record.partition_path,
                    schema_hash=record.schema_hash,
                )
            )
            session.commit()

    def get_ingestion_history(
        self, dataset_name: str, limit: int = 10
    ) -> list[IngestionRecord]:
        with self._Session() as session:
            rows = (
                session.query(IngestionRecordRow)
                .filter_by(dataset_name=dataset_name)
                .order_by(desc(IngestionRecordRow.ingested_at))
                .limit(limit)
                .all()
            )
            return [_to_ingestion_record(r) for r in rows]

    # ------------------------------------------------------------------
    # Health reports
    # ------------------------------------------------------------------

    def write_health_report(self, report: HealthReport) -> None:
        """Append a health report. History is preserved — never overwritten."""
        with self._Session() as session:
            session.add(
                HealthReportRow(
                    id=str(uuid.uuid4()),
                    dataset_name=report.dataset_name,
                    checked_at=report.checked_at,
                    status=report.status.value,
                    freshness_ok=report.freshness_ok,
                    volume_ok=report.volume_ok,
                    null_ratio_ok=report.null_ratio_ok,
                    schema_hash_ok=report.schema_hash_ok,
                    details=json.dumps(report.details),
                )
            )
            session.commit()

    def get_latest_health(self, dataset_name: str) -> Optional[HealthReport]:
        with self._Session() as session:
            row = (
                session.query(HealthReportRow)
                .filter_by(dataset_name=dataset_name)
                .order_by(desc(HealthReportRow.checked_at))
                .first()
            )
            if not row:
                return None
            return _to_health_report(row)


# ---------------------------------------------------------------------------
# Private converters — keep ORM layer isolated from domain models
# ---------------------------------------------------------------------------


def _to_registration(row: DatasetRegistrationRow) -> DatasetRegistration:
    return DatasetRegistration(
        name=row.name,
        version=row.version,
        owner=row.owner,
        expected_frequency=DatasetFrequency(row.expected_frequency),
        schema_hash=row.schema_hash,
        description=row.description or "",
    )


def _to_ingestion_record(row: IngestionRecordRow) -> IngestionRecord:
    return IngestionRecord(
        run_id=row.run_id,
        dataset_name=row.dataset_name,
        version=row.version,
        ingested_at=row.ingested_at,
        row_count=row.row_count,
        checksum=row.checksum,
        source_path=row.source_path,
        partition_path=row.partition_path,
        schema_hash=row.schema_hash,
    )


def _to_health_report(row: HealthReportRow) -> HealthReport:
    return HealthReport(
        dataset_name=row.dataset_name,
        checked_at=row.checked_at,
        status=HealthStatus(row.status),
        freshness_ok=row.freshness_ok,
        volume_ok=row.volume_ok,
        null_ratio_ok=row.null_ratio_ok,
        schema_hash_ok=row.schema_hash_ok,
        details=json.loads(row.details),
    )
