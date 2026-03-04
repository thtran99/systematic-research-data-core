"""
tests/test_ingestion_determinism.py

Validates that the ingestion pipeline is deterministic and idempotent:
- Same input on the same day → same run_id, always.
- Schema hash is column-order-independent.
- Schema hash is insensitive to description changes.
- Re-running the pipeline does not duplicate registry records.
"""

from pathlib import Path

import pytest

from core.models import ColumnSchema, ColumnType, DatasetSchema
from core.registry import MetadataRegistry
from pipelines.ingestion import IngestionConfig, IngestionPipeline


def test_same_input_produces_same_run_id(ingestion_config: IngestionConfig) -> None:
    """
    Two runs with identical config on the same calendar day must produce
    the exact same run_id. This is the foundation of idempotent re-runs.
    """
    result_a = IngestionPipeline(ingestion_config).run()
    result_b = IngestionPipeline(ingestion_config).run()

    assert result_a.success
    assert result_b.success
    assert result_a.run_id == result_b.run_id


def test_schema_hash_is_column_order_independent(sample_schema: DatasetSchema) -> None:
    """
    Declaring columns in a different order must produce an identical schema hash.
    Hash stability must not depend on declaration order.
    """
    reversed_schema = DatasetSchema(
        name=sample_schema.name,
        version=sample_schema.version,
        columns=list(reversed(sample_schema.columns)),
    )

    assert sample_schema.compute_hash() == reversed_schema.compute_hash()


def test_schema_hash_ignores_descriptions(sample_schema: DatasetSchema) -> None:
    """
    Updating a column description must not change the schema hash.
    Descriptions are documentation; they must not affect contract identity.
    """
    annotated_columns = [
        ColumnSchema(
            name=col.name,
            dtype=col.dtype,
            nullable=col.nullable,
            description="added description",
        )
        for col in sample_schema.columns
    ]
    annotated_schema = DatasetSchema(
        name=sample_schema.name,
        version=sample_schema.version,
        columns=annotated_columns,
    )

    assert sample_schema.compute_hash() == annotated_schema.compute_hash()


def test_idempotent_rerun_no_duplicate_record(ingestion_config: IngestionConfig) -> None:
    """
    Running the pipeline twice with the same config must not create two
    ingestion records in the registry — the second write is a no-op.
    """
    # Each IngestionPipeline creates its own MetadataRegistry instance.
    # To share state, we must share the db_url pointing to a file-backed DB.
    # The ingestion_config fixture uses sqlite:///:memory:, which is per-connection.
    # We override to a file-backed DB for this test to share state across runs.
    from pathlib import Path
    import tempfile, os

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "shared.db"
        db_url = f"sqlite:///{db_path}"
        parquet_base = str(Path(tmpdir) / "canonical")

        from dataclasses import replace
        config = IngestionConfig(
            dataset_name=ingestion_config.dataset_name,
            version=ingestion_config.version,
            owner=ingestion_config.owner,
            expected_frequency=ingestion_config.expected_frequency,
            schema=ingestion_config.schema,
            source_path=ingestion_config.source_path,
            db_url=db_url,
            parquet_base=parquet_base,
        )

        result_a = IngestionPipeline(config).run()
        result_b = IngestionPipeline(config).run()

        assert result_a.success
        assert result_b.success
        assert result_a.run_id == result_b.run_id

        # Registry must contain exactly one ingestion record for this dataset
        shared_registry = MetadataRegistry(db_url)
        history = shared_registry.get_ingestion_history("prices")
        assert len(history) == 1, (
            f"Expected 1 ingestion record after idempotent re-run, got {len(history)}"
        )
