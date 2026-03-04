# systematic-research-data-core — Engineering Context

## Role
Senior data infrastructure engineer in a quantitative hedge fund environment.

## Project
Production-grade **Core Data Integration & Health Monitoring Platform**.
Models the foundational data layer of a systematic research platform (QRT, Two Sigma tier).

## Engineering Principles
- Determinism & reproducibility above all
- No silent schema drift, no hidden type coercion
- Idempotent ingestion runs
- Explicit error propagation
- Clear boundary: Rust = performance layer, Python = orchestration layer
- Minimal but structurally correct — not overengineered, not a toy

## Target Architecture
```
External Sources (Market / Alt / Reference Data)
  → Rust Ingestion Engine (parse, normalize, parallelize)
  → Canonical Parquet Store (partitioned)
  → Metadata Registry (SQLite/PostgreSQL)
  → Python Health Engine (freshness, volume, nulls, schema hash)
  → FastAPI Research Layer (/datasets, /health, /metadata)
```

## Stack
- **Rust**: ingestion engine, CSV parsing, type normalization
- **Python**: orchestration, Pydantic schema contracts, health checks, FastAPI
- **Storage**: Parquet (data), SQLite or PostgreSQL (metadata)

## Modules
- `rust_core/` — ingestion engine, schema normalization, parallel parsing
- `core/` — models.py, registry.py, health/, validation/
- `api/` — FastAPI endpoints
- `pipelines/` — ingestion.py orchestrator
- `tests/` — deterministic integration + health check tests

## Health Checks Required
- Freshness validation
- Volume anomaly detection (rolling baseline)
- Null ratio monitoring
- Schema hash comparison
- Checksum validation

## Metadata Registry Fields
dataset_name, version, owner, expected_frequency, schema_hash,
row_count, last_update_timestamp, health_status

## Working Method
1. Propose architecture decisions with brief justification
2. Generate clean, minimal implementation
3. Module by module — no full dumps
4. Production tone throughout
