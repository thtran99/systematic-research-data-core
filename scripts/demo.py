"""
scripts/demo.py

End-to-end demo: ingest a dataset, inspect health, then start the API.

Run from the project root:
    python scripts/demo.py
"""

import sys
from pathlib import Path

# Ensure project root is on the path when running as a script
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.models import ColumnSchema, ColumnType, DatasetFrequency, DatasetSchema
from core.registry import MetadataRegistry
from pipelines.ingestion import IngestionConfig, IngestionPipeline

# ---------------------------------------------------------------------------
# 1. Define the canonical schema
# ---------------------------------------------------------------------------

schema = DatasetSchema(
    name="prices",
    version="2.0",
    columns=[
        ColumnSchema(name="symbol", dtype=ColumnType.STRING, nullable=False, description="Ticker symbol"),
        ColumnSchema(name="date",   dtype=ColumnType.STRING, nullable=False, description="Trading date"),
        ColumnSchema(name="close",  dtype=ColumnType.FLOAT,  nullable=False, description="Closing price"),
        ColumnSchema(name="volume", dtype=ColumnType.INT,    nullable=True,  description="Daily volume"),
    ],
)

# ---------------------------------------------------------------------------
# 2. Configure and run the ingestion pipeline
# ---------------------------------------------------------------------------

config = IngestionConfig(
    dataset_name="prices",
    version="2.0",
    owner="market-data",
    expected_frequency=DatasetFrequency.DAILY,
    schema=schema,
    source_path="data/raw/prices.csv",
    db_url="sqlite:///metadata.db",
    parquet_base="data/canonical",
)

print("─" * 60)
print("Running ingestion pipeline...")
result = IngestionPipeline(config).run()

print(f"  success:        {result.success}")
if not result.success:
    print(f"  error:          {result.error}")
    sys.exit(1)

print(f"  run_id:         {result.run_id[:16]}...")
print(f"  row_count:      {result.row_count}")
print(f"  partition_path: {result.partition_path}")
print(f"  schema_hash:    {result.schema_hash[:16]}...")

# ---------------------------------------------------------------------------
# 3. Inspect health report
# ---------------------------------------------------------------------------

print()
print("─" * 60)
print("Health report:")
h = result.health_report
print(f"  status:         {h.status.value.upper()}")
print(f"  freshness_ok:   {h.freshness_ok}")
print(f"  volume_ok:      {h.volume_ok}")
print(f"  null_ratio_ok:  {h.null_ratio_ok}")
print(f"  schema_hash_ok: {h.schema_hash_ok}")

# ---------------------------------------------------------------------------
# 4. Inspect registry
# ---------------------------------------------------------------------------

print()
print("─" * 60)
print("Registry state:")
registry = MetadataRegistry("sqlite:///metadata.db")
datasets = registry.list_datasets()
print(f"  registered datasets: {[d.name for d in datasets]}")
history = registry.get_ingestion_history("prices", limit=3)
print(f"  ingestion records:   {len(history)}")
print(f"  last row_count:      {history[0].row_count}")

# ---------------------------------------------------------------------------
# 5. Next step
# ---------------------------------------------------------------------------

print()
print("─" * 60)
print("Pipeline complete. Start the API with:")
print()
print("  DATABASE_URL=sqlite:///metadata.db uvicorn api.main:app --reload")
print()
print("Then query:")
print("  curl http://localhost:8000/datasets")
print("  curl http://localhost:8000/health/prices")
print("  curl http://localhost:8000/metadata/prices")
print("  open http://localhost:8000/docs")
print("─" * 60)
