//! M2 scaffold: convert CSV fixtures into typed Arrow `RecordBatch` values.
//!
//! Scope is deliberately narrow: only the CSV-fixture-to-typed-columns
//! conversion step ("convert deterministic fixtures into typed Arrow arrays
//! and RecordBatch values"). Kernels, compute functions, and the Arrow
//! rehearsal segment are out of scope — this is an explicit, recorded scope
//! decision to extend `pipeline` ahead of the M1 checklist passing.

#![warn(missing_docs)]

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

/// Errors that can occur while converting a CSV fixture into a `RecordBatch`.
#[derive(Debug)]
pub enum FixtureError {
    /// The input file could not be opened.
    OpenInput {
        /// The path that could not be opened.
        path: std::path::PathBuf,
        /// The underlying I/O error.
        source: std::io::Error,
    },
    /// A CSV record could not be read or parsed.
    ReadRecord {
        /// The underlying `csv` crate error.
        source: csv::Error,
    },
    /// The `id` column did not contain a valid integer.
    InvalidId {
        /// The row index (zero-based, excluding the header) where parsing failed.
        row: usize,
        /// The value that failed to parse.
        value: String,
    },
    /// The Arrow `RecordBatch` could not be assembled from the built arrays.
    BuildBatch {
        /// The underlying Arrow error.
        source: arrow::error::ArrowError,
    },
}

impl std::fmt::Display for FixtureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FixtureError::OpenInput { path, source } => {
                write!(f, "failed to open {}: {source}", path.display())
            }
            FixtureError::ReadRecord { source } => {
                write!(f, "failed to read CSV record: {source}")
            }
            FixtureError::InvalidId { row, value } => {
                write!(f, "row {row}: invalid id {value:?}, expected an integer")
            }
            FixtureError::BuildBatch { source } => {
                write!(f, "failed to build record batch: {source}")
            }
        }
    }
}

impl std::error::Error for FixtureError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FixtureError::OpenInput { source, .. } => Some(source),
            FixtureError::ReadRecord { source } => Some(source),
            FixtureError::InvalidId { .. } => None,
            FixtureError::BuildBatch { source } => Some(source),
        }
    }
}

/// Reads a headered CSV file with an `id,name,note` schema and converts it
/// into a single typed Arrow [`RecordBatch`].
///
/// `id` becomes an `Int64` column; `name` and `note` become `Utf8` columns.
/// Missing trailing fields become empty strings, not nulls — this function
/// does not build a null bitmap.
///
/// # Errors
///
/// Returns [`FixtureError`] if the file cannot be opened, a record cannot be
/// read (including a row with a different column count than the header), an
/// `id` field is not a valid integer, or the resulting arrays cannot be
/// assembled into a `RecordBatch`.
pub fn fixture_to_record_batch(path: &Path) -> Result<RecordBatch, FixtureError> {
    let file = File::open(path).map_err(|source| FixtureError::OpenInput {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = csv::Reader::from_reader(file);

    let mut ids = Vec::new();
    let mut names = Vec::new();
    let mut notes = Vec::new();

    for (row, record) in reader.records().enumerate() {
        let record = record.map_err(|source| FixtureError::ReadRecord { source })?;
        let id_field = record.get(0).unwrap_or_default();
        let id: i64 = id_field.parse().map_err(|_| FixtureError::InvalidId {
            row,
            value: id_field.to_string(),
        })?;
        ids.push(id);
        names.push(record.get(1).unwrap_or_default().to_string());
        notes.push(record.get(2).unwrap_or_default().to_string());
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("note", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(notes)),
        ],
    )
    .map_err(|source| FixtureError::BuildBatch { source })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_path(name: &str) -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/m1")
            .join(name)
    }

    #[test]
    fn converts_headers_fixture_into_typed_columns() {
        let batch = fixture_to_record_batch(&fixture_path("headers.csv"))
            .expect("fixture should convert cleanly");

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 3);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column should be Int64");
        assert_eq!(ids.values(), &[1, 2, 3]);

        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column should be Utf8");
        assert_eq!(names.value(0), "Ada");
        assert_eq!(names.value(1), "Zoë");
        assert_eq!(names.value(2), "");

        let notes = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("note column should be Utf8");
        assert_eq!(notes.value(0), "quoted, comma");
        assert_eq!(notes.value(1), "line one\nline two");
    }

    #[test]
    fn all_columns_share_the_batchs_logical_length() {
        let batch = fixture_to_record_batch(&fixture_path("headers.csv"))
            .expect("fixture should convert cleanly");

        for i in 0..batch.num_columns() {
            assert_eq!(batch.column(i).len(), batch.num_rows());
        }
    }

    #[test]
    fn rejects_a_short_row_before_building_a_batch() {
        let err = fixture_to_record_batch(&fixture_path("malformed.csv"))
            .expect_err("a row with fewer fields than the header should fail");
        assert!(matches!(err, FixtureError::ReadRecord { .. }));
    }
}
