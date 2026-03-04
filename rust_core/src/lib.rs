/// lib.rs — PyO3 module entry point for rust_core.
///
/// Exposes a single `ingest()` function to Python:
///
/// ```python
/// import rust_core
///
/// result = rust_core.ingest(
///     source_path="data/raw/prices.csv",
///     schema_json='{"name":"prices","version":"1.0","columns":[...]}',
///     output_path="data/canonical/prices/2026-03-04/data.parquet",
/// )
/// print(result.row_count, result.checksum, result.schema_hash)
/// ```

use std::fs;
use std::io::Read;
use std::path::Path;

use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use pyo3::prelude::*;
use sha2::{Digest, Sha256};

mod ingestion;
mod schema;

use ingestion::csv_parser::parse_csv_parallel;
use ingestion::normalizer::normalize;
use schema::canonical::{compute_schema_hash, parse_schema_json};

// ---------------------------------------------------------------------------
// Python-visible types
// ---------------------------------------------------------------------------

/// Result of a successful ingestion run.
#[pyclass]
pub struct IngestionOutput {
    #[pyo3(get)]
    pub row_count: usize,
    /// SHA-256 hex digest of the written Parquet file.
    #[pyo3(get)]
    pub checksum: String,
    /// Canonical schema hash — identical to Python's DatasetSchema.compute_hash().
    #[pyo3(get)]
    pub schema_hash: String,
}

// ---------------------------------------------------------------------------
// Public function
// ---------------------------------------------------------------------------

/// Parse a CSV, normalize types per schema, write Parquet, return metadata.
///
/// Args:
///     source_path: Path to the input CSV file.
///     schema_json: JSON string of the dataset schema
///                  (matches Python's DatasetSchema.model_dump_json()).
///     output_path: Destination path for the Parquet output file.
///                  Parent directories are created automatically.
///
/// Returns:
///     IngestionOutput with row_count, checksum, schema_hash.
///
/// Raises:
///     ValueError: On schema parse error, CSV error, type error, or I/O error.
#[pyfunction]
fn ingest(
    source_path: &str,
    schema_json: &str,
    output_path: &str,
) -> PyResult<IngestionOutput> {
    // Parse schema
    let schema = parse_schema_json(schema_json)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Invalid schema JSON: {}", e)))?;

    // Compute schema hash (before any I/O — deterministic)
    let schema_hash = compute_schema_hash(&schema);

    // Parse CSV in parallel
    let parsed = parse_csv_parallel(source_path, &schema)
        .map_err(pyo3::exceptions::PyValueError::new_err)?;

    let row_count = parsed.rows.len();

    // Normalize to Arrow RecordBatch
    let batch = normalize(&parsed, &schema)
        .map_err(pyo3::exceptions::PyValueError::new_err)?;

    // Write Parquet
    let dest = Path::new(output_path);
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            pyo3::exceptions::PyIOError::new_err(format!(
                "Cannot create output directory '{}': {}",
                parent.display(),
                e
            ))
        })?;
    }

    let file = fs::File::create(dest).map_err(|e| {
        pyo3::exceptions::PyIOError::new_err(format!(
            "Cannot create output file '{}': {}",
            output_path, e
        ))
    })?;

    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("Parquet writer error: {}", e)))?;

    writer
        .write(&batch)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("Parquet write error: {}", e)))?;

    writer
        .close()
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("Parquet close error: {}", e)))?;

    // Compute SHA-256 checksum of written file
    let checksum = sha256_file(output_path).map_err(|e| {
        pyo3::exceptions::PyIOError::new_err(format!("Checksum error: {}", e))
    })?;

    Ok(IngestionOutput {
        row_count,
        checksum,
        schema_hash,
    })
}

// ---------------------------------------------------------------------------
// Module registration
// ---------------------------------------------------------------------------

#[pymodule]
fn rust_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(ingest, m)?)?;
    m.add_class::<IngestionOutput>()?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn sha256_file(path: &str) -> Result<String, String> {
    let mut file =
        fs::File::open(path).map_err(|e| format!("Cannot open '{}': {}", path, e))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 65536];
    loop {
        let n = file
            .read(&mut buf)
            .map_err(|e| format!("Read error: {}", e))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}
