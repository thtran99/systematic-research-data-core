# systematic-research-data-core

Production-grade core data infrastructure for a systematic research platform. Models the foundational data layer of a quantitative hedge fund: ingestion, normalization, storage, health monitoring, and research-facing API.

---

## Architecture

```
External Data Sources (CSV / semi-structured)
        │
        ▼
┌───────────────────┐
│  rust_core        │  ← Rust (PyO3)
│  - CSV parsing    │    Parallel ingestion via rayon
│  - Type norm.     │    Arrow RecordBatch → Parquet
│  - Schema hash    │    SHA-256 checksum
└────────┬──────────┘
         │ Parquet (partitioned by dataset/date)
         ▼
┌───────────────────┐
│  core/            │  ← Python
│  - models.py      │    Pydantic schema contracts
│  - registry.py    │    SQL-backed metadata registry
│  - health/        │    Freshness · Volume · Nulls · Schema hash
│  - validation/    │    Ingestion-time schema gate
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│  pipelines/       │  ← Orchestration
│  ingestion.py     │    End-to-end idempotent pipeline
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│  api/             │  ← FastAPI (read-only)
│  /datasets        │    List registered datasets
│  /health/{name}   │    Latest health report
│  /metadata/{name} │    Registration + last ingestion
└───────────────────┘
```

---

## Stack

| Layer | Technology |
|---|---|
| Performance ingestion | Rust 1.93 · PyO3 · rayon · arrow · parquet |
| Orchestration | Python 3.11 · pandas · pyarrow |
| Schema contracts | Pydantic v2 |
| Metadata registry | SQLAlchemy 2 · SQLite (PostgreSQL-ready) |
| API | FastAPI · uvicorn |
| Tests | pytest |
| Build | maturin |

---

## Project Structure

```
systematic-research-data-core/
│
├── rust_core/                   # Rust ingestion engine (PyO3 extension)
│   └── src/
│       ├── lib.rs               # rust_core.ingest() — PyO3 entry point
│       ├── schema/canonical.rs  # ColumnType, schema hash (matches Python)
│       └── ingestion/
│           ├── csv_parser.rs    # Parallel CSV parsing (rayon)
│           └── normalizer.rs    # Type normalization → Arrow RecordBatch
│
├── core/                        # Python domain layer
│   ├── models.py                # Schema contracts (single source of truth)
│   ├── registry.py              # Metadata registry (all writes go here)
│   ├── health/
│   │   ├── checks.py            # Freshness · Volume · Null ratio · Schema hash
│   │   └── reporter.py          # Orchestrates checks → HealthReport
│   └── validation/
│       └── schema_validator.py  # Ingestion-time gate (no silent coercion)
│
├── pipelines/
│   └── ingestion.py             # End-to-end pipeline (Rust + Python)
│
├── api/
│   ├── main.py                  # FastAPI app
│   ├── dependencies.py          # Registry DI
│   ├── schemas.py               # Composite response models
│   └── routes/                  # datasets · health · metadata
│
├── tests/
│   ├── conftest.py
│   ├── test_ingestion_determinism.py
│   └── test_health_checks.py
│
├── pyproject.toml
└── Cargo.toml
```

---

## Getting Started

### Prerequisites

- Python 3.11+
- Rust 1.70+ (`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`)
- maturin (`pip install maturin`)

### Install

```bash
# Clone
git clone https://github.com/thtran99/systematic-research-data-core.git
cd systematic-research-data-core

# Install Python dependencies
pip install -e ".[dev]"

# Build and install Rust extension
cd rust_core && maturin build --release
pip install ../target/wheels/rust_core-*.whl
cd ..
```

### Run tests

```bash
pytest
```

### Start API server

```bash
DATABASE_URL=sqlite:///./metadata.db uvicorn api.main:app --reload
```

Swagger UI available at `http://localhost:8000/docs`.

---

## Key Design Decisions

**Determinism** — The schema hash is computed identically in Rust and Python (SHA-256 of a canonically serialized JSON fingerprint, columns sorted by name). Same input always produces the same run ID, schema hash, and Parquet output.

**Idempotence** — `run_id = SHA256(dataset|source_path|schema_hash|date)`. Re-running the pipeline with the same inputs on the same day is a no-op at the registry level.

**No silent schema drift** — Every ingestion records a schema hash. The health engine compares it against the registered contract on every check. A mismatch immediately triggers `FAILING` status.

**Layered performance** — The Rust engine handles CSV parsing (rayon parallelism), type normalization, and Parquet writing. Python handles orchestration, metadata, health logic, and the API. If `rust_core` is not installed, the pipeline falls back to a pure Python path transparently.

**Single registry write path** — All metadata writes go through `MetadataRegistry`. Direct table manipulation is not permitted.

---

## Data Health Checks

| Check | Logic | Failure trigger |
|---|---|---|
| **Freshness** | Age of last ingestion vs. expected frequency | Age > threshold (DAILY: 26h, WEEKLY: 8d, …) |
| **Volume anomaly** | Current row count vs. rolling baseline (mean ± 3σ) | Outside 3 standard deviations |
| **Null ratio** | Per-column null % vs. schema contract | Non-nullable col has any null; nullable col exceeds threshold (default 10%) |
| **Schema hash** | Hash at ingest time vs. registered hash | Any mismatch → immediate `FAILING` |

**Status aggregation:** `FAILING` if schema hash fails or ≥3 checks fail · `DEGRADED` if 1–2 non-critical checks fail · `HEALTHY` if all pass.

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/datasets` | List all registered datasets |
| `GET` | `/health/{dataset}` | Latest health report for a dataset |
| `GET` | `/metadata/{dataset}` | Registration + most recent ingestion record |

---

## Extensibility

The architecture is designed to extend to:

- **S3-backed storage** — replace local Parquet paths with S3 URIs in `IngestionConfig`
- **Distributed ingestion workers** — `rust_core.ingest()` is stateless and safe to run in parallel across workers
- **Streaming ingestion** — the registry and health engine are decoupled from the ingest transport
- **Column-level lineage** — `IngestionRecord` already tracks `schema_hash` and `partition_path`
- **PostgreSQL** — set `DATABASE_URL=postgresql://...` (SQLAlchemy handles the rest)
