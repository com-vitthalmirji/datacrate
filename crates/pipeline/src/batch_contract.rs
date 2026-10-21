//! Runtime validation for the fixed ingest schema shared by writer and digest.

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::DataType;

use crate::PipelineIoError;

pub(crate) const INGEST_FIELDS: [(&str, DataType, bool); 3] = [
    ("id", DataType::Int64, false),
    ("name", DataType::Utf8, false),
    ("note", DataType::Utf8, true),
];

/// A violation of the fixed ingest batch contract.
#[derive(Debug, PartialEq, Eq)]
pub enum BatchViolation {
    /// The batch has an unexpected number of columns.
    ColumnCount {
        /// Required number of columns.
        expected: usize,
        /// Actual number of columns.
        found: usize,
    },
    /// A column has the wrong name.
    FieldName {
        /// Zero-based column index.
        index: usize,
        /// Required field name.
        expected: &'static str,
        /// Actual field name.
        found: String,
    },
    /// A column has the wrong Arrow type.
    DataType {
        /// Zero-based column index.
        index: usize,
        /// Required logical type.
        expected: DataType,
        /// Actual logical type.
        found: DataType,
    },
    /// A column's declared nullability differs from the contract.
    Nullability {
        /// Zero-based column index.
        index: usize,
        /// Required nullability.
        expected: bool,
        /// Actual nullability.
        found: bool,
    },
    /// A required column contains null values.
    NullValues {
        /// Zero-based column index.
        index: usize,
        /// Number of forbidden null values.
        count: usize,
    },
    /// A column does not expose the concrete Arrow representation required by the digest.
    ArrayRepresentation {
        /// Zero-based column index.
        index: usize,
    },
}

impl std::fmt::Display for BatchViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ColumnCount { expected, found } => {
                write!(f, "expected {expected} columns, found {found}")
            }
            Self::FieldName {
                index,
                expected,
                found,
            } => write!(
                f,
                "column {index}: expected name {expected:?}, found {found:?}"
            ),
            Self::DataType {
                index,
                expected,
                found,
            } => write!(f, "column {index}: expected {expected}, found {found}"),
            Self::Nullability {
                index,
                expected,
                found,
            } => write!(
                f,
                "column {index}: expected nullable={expected}, found {found}"
            ),
            Self::NullValues { index, count } => {
                write!(f, "column {index}: found {count} forbidden nulls")
            }
            Self::ArrayRepresentation { index } => {
                write!(f, "column {index}: unsupported Arrow array representation")
            }
        }
    }
}

impl std::error::Error for BatchViolation {}

/// Checks field order, names, types, declared nullability and required non-null values.
///
/// The contract is exactly `id: Int64`, `name: Utf8`, `note: Utf8`, with only
/// `note` nullable. Schema metadata is not interpreted. This does not check business rules.
///
/// # Errors
///
/// Returns `PipelineIoError::InvalidBatch` for the first violated invariant.
pub fn validate_ingest_batch(batch: &RecordBatch) -> Result<(), PipelineIoError> {
    validated_columns(batch).map(|_| ())
}

fn invalid(violation: BatchViolation) -> PipelineIoError {
    PipelineIoError::InvalidBatch { violation }
}

fn validated_columns(
    batch: &RecordBatch,
) -> Result<(&Int64Array, &StringArray, &StringArray), PipelineIoError> {
    if batch.num_columns() != INGEST_FIELDS.len() {
        return Err(invalid(BatchViolation::ColumnCount {
            expected: INGEST_FIELDS.len(),
            found: batch.num_columns(),
        }));
    }
    let schema = batch.schema();
    for (index, (name, dtype, nullable)) in INGEST_FIELDS.into_iter().enumerate() {
        let field = schema.field(index);
        if field.name() != name {
            return Err(invalid(BatchViolation::FieldName {
                index,
                expected: name,
                found: field.name().clone(),
            }));
        }
        if field.data_type() != &dtype {
            return Err(invalid(BatchViolation::DataType {
                index,
                expected: dtype,
                found: field.data_type().clone(),
            }));
        }
        if field.is_nullable() != nullable {
            return Err(invalid(BatchViolation::Nullability {
                index,
                expected: nullable,
                found: field.is_nullable(),
            }));
        }
        if !nullable && batch.column(index).null_count() != 0 {
            return Err(invalid(BatchViolation::NullValues {
                index,
                count: batch.column(index).null_count(),
            }));
        }
    }
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| invalid(BatchViolation::ArrayRepresentation { index: 0 }))?;
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| invalid(BatchViolation::ArrayRepresentation { index: 1 }))?;
    let notes = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| invalid(BatchViolation::ArrayRepresentation { index: 2 }))?;
    Ok((ids, names, notes))
}

/// Owns a checked batch and concrete views used by the fixed-schema digest.
pub(crate) struct CheckedIngestBatch {
    batch: RecordBatch,
    ids: Int64Array,
    names: StringArray,
    notes: StringArray,
}

impl CheckedIngestBatch {
    pub(crate) fn try_new(batch: RecordBatch) -> Result<Self, PipelineIoError> {
        let (ids, names, notes) = validated_columns(&batch)?;
        // Arrow array clones share buffers; these typed views do not copy payloads.
        let (ids, names, notes) = (ids.clone(), names.clone(), notes.clone());
        Ok(Self {
            batch,
            ids,
            names,
            notes,
        })
    }

    pub(crate) fn batch(&self) -> &RecordBatch {
        &self.batch
    }
    pub(crate) fn columns(&self) -> (&Int64Array, &StringArray, &StringArray) {
        (&self.ids, &self.names, &self.notes)
    }
}
