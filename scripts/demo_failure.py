"""
scripts/demo_failure.py

Demonstrates the three ingestion failure modes:
  1. Missing column        — CSV is missing a required column
  2. Unexpected null       — non-nullable column contains a null value
  3. Type mismatch         — column has the wrong dtype

Run from the project root:
    python scripts/demo_failure.py
"""

import sys
import tempfile
import textwrap
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.models import ColumnSchema, ColumnType, DatasetFrequency, DatasetSchema
from pipelines.ingestion import IngestionConfig, IngestionPipeline

# ---------------------------------------------------------------------------
# Shared schema (same as main demo)
# ---------------------------------------------------------------------------

schema = DatasetSchema(
    name="prices",
    version="1.0",
    columns=[
        ColumnSchema(name="date",   dtype=ColumnType.STRING, nullable=False),
        ColumnSchema(name="close",  dtype=ColumnType.FLOAT,  nullable=False),
        ColumnSchema(name="volume", dtype=ColumnType.INT,    nullable=True),
    ],
)

SEP = "─" * 60


def run_case(label: str, csv_content: str) -> None:
    print()
    print(SEP)
    print(f"Case: {label}")
    print(SEP)

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, prefix="demo_fail_"
    ) as f:
        f.write(textwrap.dedent(csv_content).strip() + "\n")
        csv_path = f.name

    config = IngestionConfig(
        dataset_name="prices",
        version="1.0",
        owner="market-data",
        expected_frequency=DatasetFrequency.DAILY,
        schema=schema,
        source_path=csv_path,
        db_url="sqlite:///metadata_failure_demo.db",
        parquet_base="/tmp/demo_failure_canonical",
    )

    result = IngestionPipeline(config).run()

    print(f"  success: {result.success}")
    if not result.success:
        print(f"  error:   {result.error}")
    else:
        print("  (pipeline succeeded — unexpected for this case)")

    Path(csv_path).unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Case 1 — Missing required column (volume is absent)
# ---------------------------------------------------------------------------

run_case(
    "Missing column — 'volume' absent from CSV",
    """
    date,close
    2026-01-02,100.5
    2026-01-03,101.2
    """,
)

# ---------------------------------------------------------------------------
# Case 2 — Null in a non-nullable column (close is non-nullable)
# ---------------------------------------------------------------------------

run_case(
    "Unexpected null — 'close' is non-nullable but has an empty value",
    """
    date,close,volume
    2026-01-02,100.5,1500000
    2026-01-03,,1320000
    2026-01-06,99.8,1780000
    """,
)

# ---------------------------------------------------------------------------
# Case 3 — Type mismatch (close column contains text)
# ---------------------------------------------------------------------------

run_case(
    "Type mismatch — 'close' declared FLOAT but contains text",
    """
    date,close,volume
    2026-01-02,100.5,1500000
    2026-01-03,N/A,1320000
    2026-01-06,99.8,1780000
    """,
)

print()
print(SEP)
print("All failure cases exercised. No Parquet written, registry untouched.")
print(SEP)

# Clean up demo db if created
Path("metadata_failure_demo.db").unlink(missing_ok=True)
