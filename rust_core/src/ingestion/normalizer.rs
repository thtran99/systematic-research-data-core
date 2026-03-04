/// normalizer.rs — Type normalization and Arrow RecordBatch construction.
///
/// Converts raw string values from the CSV parser into typed Arrow arrays
/// according to the canonical schema. No silent coercion: if a non-nullable
/// value is missing or a value cannot be parsed as the declared type, an
/// explicit error is returned.

use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch,
    StringArray, TimestampNanosecondArray,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};

use crate::ingestion::csv_parser::ParsedCsv;
use crate::schema::canonical::{ColumnDef, ColumnType, Schema};

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Build an Arrow RecordBatch from parsed CSV rows, typed per the schema.
///
/// Column ordering in the RecordBatch matches the schema column order.
/// Returns an error if any required (non-nullable) value is missing or
/// if a value cannot be parsed as the declared type.
pub fn normalize(parsed: &ParsedCsv, schema: &Schema) -> Result<RecordBatch, String> {
    let row_count = parsed.rows.len();

    let mut fields: Vec<Field> = Vec::with_capacity(schema.columns.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.columns.len());

    for col_def in &schema.columns {
        let (field, array) = build_column(col_def, parsed, row_count)?;
        fields.push(field);
        arrays.push(array);
    }

    let arrow_schema = Arc::new(ArrowSchema::new(fields));
    RecordBatch::try_new(arrow_schema, arrays)
        .map_err(|e| format!("Failed to construct RecordBatch: {}", e))
}

// ---------------------------------------------------------------------------
// Per-column builders
// ---------------------------------------------------------------------------

fn build_column(
    col: &ColumnDef,
    parsed: &ParsedCsv,
    _row_count: usize,
) -> Result<(Field, ArrayRef), String> {
    let raw_values: Vec<Option<&str>> = parsed
        .rows
        .iter()
        .map(|row| row.get(&col.name).and_then(|v| v.as_deref()))
        .collect();

    // Validate nulls against schema contract
    if !col.nullable {
        for (i, val) in raw_values.iter().enumerate() {
            if val.is_none() {
                return Err(format!(
                    "Non-nullable column '{}' has null value at row {}",
                    col.name, i
                ));
            }
        }
    }

    match col.dtype {
        ColumnType::Int => {
            let values: Result<Vec<Option<i64>>, String> = raw_values
                .iter()
                .map(|v| match v {
                    None => Ok(None),
                    Some(s) => s
                        .parse::<i64>()
                        .map(Some)
                        .map_err(|_| format!("Column '{}': cannot parse '{}' as int", col.name, s)),
                })
                .collect();
            let arr = Int64Array::from(values?);
            let field = Field::new(&col.name, DataType::Int64, col.nullable);
            Ok((field, Arc::new(arr) as ArrayRef))
        }

        ColumnType::Float => {
            let values: Result<Vec<Option<f64>>, String> = raw_values
                .iter()
                .map(|v| match v {
                    None => Ok(None),
                    Some(s) => s.parse::<f64>().map(Some).map_err(|_| {
                        format!("Column '{}': cannot parse '{}' as float", col.name, s)
                    }),
                })
                .collect();
            let arr = Float64Array::from(values?);
            let field = Field::new(&col.name, DataType::Float64, col.nullable);
            Ok((field, Arc::new(arr) as ArrayRef))
        }

        ColumnType::String => {
            let values: Vec<Option<&str>> = raw_values;
            let arr = StringArray::from(values);
            let field = Field::new(&col.name, DataType::Utf8, col.nullable);
            Ok((field, Arc::new(arr) as ArrayRef))
        }

        ColumnType::Date => {
            // Store dates as string (Utf8) — compatible with pandas dtype.kind 'O'
            // which is what the Python validator accepts for DATE columns.
            let values: Vec<Option<&str>> = raw_values;
            let arr = StringArray::from(values);
            let field = Field::new(&col.name, DataType::Utf8, col.nullable);
            Ok((field, Arc::new(arr) as ArrayRef))
        }

        ColumnType::Timestamp => {
            // Parse ISO 8601 strings into nanoseconds since epoch.
            let values: Result<Vec<Option<i64>>, String> = raw_values
                .iter()
                .map(|v| match v {
                    None => Ok(None),
                    Some(s) => parse_timestamp_ns(s, &col.name).map(Some),
                })
                .collect();
            let arr = TimestampNanosecondArray::from(values?);
            let field = Field::new(
                &col.name,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                col.nullable,
            );
            Ok((field, Arc::new(arr) as ArrayRef))
        }

        ColumnType::Boolean => {
            let values: Result<Vec<Option<bool>>, String> = raw_values
                .iter()
                .map(|v| match v {
                    None => Ok(None),
                    Some(s) => parse_bool(s, &col.name).map(Some),
                })
                .collect();
            let arr = BooleanArray::from(values?);
            let field = Field::new(&col.name, DataType::Boolean, col.nullable);
            Ok((field, Arc::new(arr) as ArrayRef))
        }
    }
}

// ---------------------------------------------------------------------------
// Parsers
// ---------------------------------------------------------------------------

fn parse_timestamp_ns(s: &str, col_name: &str) -> Result<i64, String> {
    // Accept ISO 8601 with or without time component
    // Try full datetime first, then date-only
    let formats = ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"];
    for fmt in &formats {
        if let Ok(dt) =
            chrono::NaiveDateTime::parse_from_str(s, fmt).or_else(|_| {
                chrono::NaiveDate::parse_from_str(s, fmt)
                    .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
            })
        {
            return Ok(dt.and_utc().timestamp_nanos_opt().unwrap_or(0));
        }
    }
    Err(format!(
        "Column '{}': cannot parse '{}' as timestamp",
        col_name, s
    ))
}

fn parse_bool(s: &str, col_name: &str) -> Result<bool, String> {
    match s.to_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err(format!(
            "Column '{}': cannot parse '{}' as boolean",
            col_name, s
        )),
    }
}
