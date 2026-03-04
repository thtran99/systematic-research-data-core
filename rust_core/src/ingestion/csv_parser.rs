/// csv_parser.rs — Parallel CSV parsing.
///
/// Reads a CSV file, splits rows across threads with rayon, and returns
/// typed column data (as Vec<Option<String>> per column, pre-split).
///
/// Parallelism strategy:
/// - Read all rows into memory first (single-threaded I/O — disk is the bottleneck)
/// - Parse and type-convert each row in parallel (CPU-bound work via rayon)
/// - Collect back into column-oriented Vecs for Arrow RecordBatch construction

use std::collections::HashMap;

use rayon::prelude::*;

use crate::schema::canonical::Schema;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A single parsed row, column name → raw string value (None = missing/empty).
pub type RawRow = HashMap<String, Option<String>>;

/// All rows from the CSV, in original order.
pub struct ParsedCsv {
    pub rows: Vec<RawRow>,
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/// Read and parse a CSV file in parallel.
///
/// Steps:
/// 1. Read all records (single-threaded I/O)
/// 2. Parse each record into a RawRow in parallel (rayon)
///
/// Returns an error if the file cannot be opened or the CSV is malformed.
pub fn parse_csv_parallel(path: &str, schema: &Schema) -> Result<ParsedCsv, String> {
    let schema_columns: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();

    // Step 1: Read CSV headers + all raw records (single-threaded I/O)
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .trim(csv::Trim::All)
        .from_path(path)
        .map_err(|e| format!("Failed to open CSV '{}': {}", path, e))?;

    let headers: Vec<String> = reader
        .headers()
        .map_err(|e| format!("Failed to read CSV headers: {}", e))?
        .iter()
        .map(|s| s.to_string())
        .collect();

    // Validate that all schema columns are present in the CSV header
    for col in &schema_columns {
        if !headers.contains(&col.to_string()) {
            return Err(format!(
                "Schema column '{}' not found in CSV headers {:?}",
                col, headers
            ));
        }
    }

    // Read all records into memory
    let raw_records: Vec<csv::StringRecord> = reader
        .records()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("CSV parse error: {}", e))?;

    // Step 2: Parse records in parallel
    let rows: Vec<RawRow> = raw_records
        .par_iter()
        .map(|record| {
            let mut row = RawRow::new();
            for (i, header) in headers.iter().enumerate() {
                let value = record.get(i).map(|v| {
                    let trimmed = v.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed.to_string())
                    }
                });
                row.insert(header.clone(), value.flatten());
            }
            row
        })
        .collect();

    Ok(ParsedCsv { rows })
}
