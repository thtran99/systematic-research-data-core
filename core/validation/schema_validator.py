"""
core/validation/schema_validator.py

Schema validation at ingestion time — before data is written to Parquet.

Responsibility: verify that an incoming DataFrame strictly conforms to the
canonical schema contract. This is a gate, not a fixer. No coercion, no
silent casting. Violations are returned as structured errors; the caller
decides whether to reject or quarantine.

Distinct from health/checks.py which runs post-storage on persisted data.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import pandas as pd

from core.models import ColumnSchema, ColumnType, DatasetSchema


# ---------------------------------------------------------------------------
# Error taxonomy
# ---------------------------------------------------------------------------


class ValidationErrorType(str, Enum):
    MISSING_COLUMN = "missing_column"
    UNEXPECTED_COLUMN = "unexpected_column"
    TYPE_MISMATCH = "type_mismatch"
    UNEXPECTED_NULL = "unexpected_null"


@dataclass
class ValidationError:
    column: str
    error_type: ValidationErrorType
    details: str


@dataclass
class ValidationResult:
    valid: bool
    errors: list[ValidationError] = field(default_factory=list)

    def raise_on_invalid(self) -> None:
        """Raise a SchemaViolationError if validation failed."""
        if not self.valid:
            summary = "; ".join(f"[{e.error_type}] {e.column}: {e.details}" for e in self.errors)
            raise SchemaViolationError(f"Schema validation failed — {len(self.errors)} error(s): {summary}")


class SchemaViolationError(Exception):
    """Raised when a DataFrame does not conform to its canonical schema contract."""


# ---------------------------------------------------------------------------
# dtype mapping
# ---------------------------------------------------------------------------

# Maps canonical ColumnType to sets of accepted pandas dtype kinds.
# kind codes: 'i'=signed int, 'u'=unsigned int, 'f'=float, 'O'=object,
#             'M'=datetime64, 'b'=bool
_DTYPE_KIND_MAP: dict[ColumnType, set[str]] = {
    ColumnType.INT: {"i", "u"},
    ColumnType.FLOAT: {"f"},
    ColumnType.STRING: {"O"},
    ColumnType.DATE: {"M", "O"},       # datetime64 or date-string object
    ColumnType.TIMESTAMP: {"M"},
    ColumnType.BOOLEAN: {"b"},
}

# Human-readable dtype descriptions for error messages
_DTYPE_LABEL: dict[ColumnType, str] = {
    ColumnType.INT: "integer (int8/16/32/64)",
    ColumnType.FLOAT: "float (float32/64)",
    ColumnType.STRING: "string (object)",
    ColumnType.DATE: "date (datetime64 or date-string object)",
    ColumnType.TIMESTAMP: "timestamp (datetime64)",
    ColumnType.BOOLEAN: "boolean",
}


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------


class SchemaValidator:
    """
    Validates a DataFrame against a canonical DatasetSchema.

    Checks (in order):
    1. No required columns are missing from the DataFrame.
    2. No unexpected columns are present (configurable — strict by default).
    3. Each column's dtype is compatible with its declared ColumnType.
    4. Non-nullable columns contain no null values.

    Does not mutate the DataFrame. Does not coerce types.
    """

    def __init__(self, strict_columns: bool = True) -> None:
        """
        Args:
            strict_columns: If True, reject DataFrames that contain columns
                not declared in the schema. Set to False when working with
                raw sources that may carry extra fields before projection.
        """
        self._strict_columns = strict_columns

    def validate(self, df: pd.DataFrame, schema: DatasetSchema) -> ValidationResult:
        """
        Validate df against schema. Returns a ValidationResult with all
        errors collected — does not short-circuit on first failure.
        """
        errors: list[ValidationError] = []
        schema_columns = {col.name: col for col in schema.columns}
        df_columns = set(df.columns)

        errors.extend(_check_missing_columns(schema_columns, df_columns))
        if self._strict_columns:
            errors.extend(_check_unexpected_columns(schema_columns, df_columns))

        # Only validate type and nulls for columns present in both
        present_columns = {
            name: col
            for name, col in schema_columns.items()
            if name in df_columns
        }
        errors.extend(_check_dtypes(df, present_columns))
        errors.extend(_check_nulls(df, present_columns))

        return ValidationResult(valid=len(errors) == 0, errors=errors)


# ---------------------------------------------------------------------------
# Individual check functions (private)
# ---------------------------------------------------------------------------


def _check_missing_columns(
    schema_columns: dict[str, ColumnSchema],
    df_columns: set[str],
) -> list[ValidationError]:
    errors = []
    for name in schema_columns:
        if name not in df_columns:
            errors.append(
                ValidationError(
                    column=name,
                    error_type=ValidationErrorType.MISSING_COLUMN,
                    details=f"Column '{name}' declared in schema but absent from DataFrame.",
                )
            )
    return errors


def _check_unexpected_columns(
    schema_columns: dict[str, ColumnSchema],
    df_columns: set[str],
) -> list[ValidationError]:
    errors = []
    for name in sorted(df_columns - set(schema_columns)):
        errors.append(
            ValidationError(
                column=name,
                error_type=ValidationErrorType.UNEXPECTED_COLUMN,
                details=f"Column '{name}' present in DataFrame but not declared in schema.",
            )
        )
    return errors


def _check_dtypes(
    df: pd.DataFrame,
    columns: dict[str, ColumnSchema],
) -> list[ValidationError]:
    errors = []
    for name, col_def in columns.items():
        actual_kind = df[name].dtype.kind
        expected_kinds = _DTYPE_KIND_MAP[col_def.dtype]
        if actual_kind not in expected_kinds:
            errors.append(
                ValidationError(
                    column=name,
                    error_type=ValidationErrorType.TYPE_MISMATCH,
                    details=(
                        f"Expected {_DTYPE_LABEL[col_def.dtype]}, "
                        f"got dtype='{df[name].dtype}' (kind='{actual_kind}')."
                    ),
                )
            )
    return errors


def _check_nulls(
    df: pd.DataFrame,
    columns: dict[str, ColumnSchema],
) -> list[ValidationError]:
    errors = []
    total_rows = len(df)
    for name, col_def in columns.items():
        if col_def.nullable:
            continue
        null_count = int(df[name].isna().sum())
        if null_count > 0:
            ratio = null_count / total_rows if total_rows > 0 else 1.0
            errors.append(
                ValidationError(
                    column=name,
                    error_type=ValidationErrorType.UNEXPECTED_NULL,
                    details=(
                        f"Non-nullable column has {null_count} null(s) "
                        f"({ratio:.2%} of {total_rows} rows)."
                    ),
                )
            )
    return errors
