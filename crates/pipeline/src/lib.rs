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

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;

/// Errors that can occur while converting a CSV fixture into a `RecordBatch`,
/// or while writing/reading that batch as Parquet.
#[derive(Debug)]
pub enum PipelineIoError {
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
    /// A Parquet output file could not be created.
    OpenOutput {
        /// The path that could not be created.
        path: std::path::PathBuf,
        /// The underlying I/O error.
        source: std::io::Error,
    },
    /// A `RecordBatch` could not be written to Parquet.
    WriteParquet {
        /// The underlying Parquet error.
        source: ParquetError,
    },
    /// A Parquet file could not be read back into a `RecordBatch`.
    ReadParquet {
        /// The underlying Parquet error.
        source: ParquetError,
    },
}

impl std::fmt::Display for PipelineIoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineIoError::OpenInput { path, source } => {
                write!(f, "failed to open {}: {source}", path.display())
            }
            PipelineIoError::ReadRecord { source } => {
                write!(f, "failed to read CSV record: {source}")
            }
            PipelineIoError::InvalidId { row, value } => {
                write!(f, "row {row}: invalid id {value:?}, expected an integer")
            }
            PipelineIoError::BuildBatch { source } => {
                write!(f, "failed to build record batch: {source}")
            }
            PipelineIoError::OpenOutput { path, source } => {
                write!(f, "failed to create {}: {source}", path.display())
            }
            PipelineIoError::WriteParquet { source } => {
                write!(f, "failed to write parquet: {source}")
            }
            PipelineIoError::ReadParquet { source } => {
                write!(f, "failed to read parquet: {source}")
            }
        }
    }
}

impl std::error::Error for PipelineIoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PipelineIoError::OpenInput { source, .. } => Some(source),
            PipelineIoError::ReadRecord { source } => Some(source),
            PipelineIoError::InvalidId { .. } => None,
            PipelineIoError::BuildBatch { source } => Some(source),
            PipelineIoError::OpenOutput { source, .. } => Some(source),
            PipelineIoError::WriteParquet { source } | PipelineIoError::ReadParquet { source } => {
                Some(source)
            }
        }
    }
}

/// The fixed three-column schema (`id: Int64`, `name: Utf8`, `note: Utf8`,
/// nullable) every [`fixture_to_record_batch`]/[`fixture_to_record_batches`]
/// batch shares.
pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("note", DataType::Utf8, true),
    ]))
}

/// A single parsed `id,name,note` row. `note` is `None` when the CSV field
/// was empty, so the resulting Arrow column carries a real validity bit
/// instead of an empty string standing in for "missing".
struct Row {
    id: i64,
    name: String,
    note: Option<String>,
}

/// Parses one already-read CSV record into a [`Row`], given its zero-based
/// row index (used only for [`PipelineIoError::InvalidId`]'s message).
fn parse_row(row: usize, record: &csv::StringRecord) -> Result<Row, PipelineIoError> {
    let id_field = record.get(0).unwrap_or_default();
    let id: i64 = id_field.parse().map_err(|_| PipelineIoError::InvalidId {
        row,
        value: id_field.to_string(),
    })?;
    let name = record.get(1).unwrap_or_default().to_string();
    let note = match record.get(2).unwrap_or_default() {
        "" => None,
        note => Some(note.to_string()),
    };
    Ok(Row { id, name, note })
}

fn parse_rows(path: &Path) -> Result<Vec<Row>, PipelineIoError> {
    let file = File::open(path).map_err(|source| PipelineIoError::OpenInput {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = csv::Reader::from_reader(file);

    let mut rows = Vec::new();
    for (row, record) in reader.records().enumerate() {
        let record = record.map_err(|source| PipelineIoError::ReadRecord { source })?;
        rows.push(parse_row(row, &record)?);
    }
    Ok(rows)
}

fn batch_from_rows(rows: &[Row]) -> Result<RecordBatch, PipelineIoError> {
    let ids: Int64Array = rows.iter().map(|r| r.id).collect();
    let names: StringArray = rows.iter().map(|r| Some(r.name.as_str())).collect();
    let notes: StringArray = rows.iter().map(|r| r.note.as_deref()).collect();

    let columns: Vec<ArrayRef> = vec![Arc::new(ids), Arc::new(names), Arc::new(notes)];
    RecordBatch::try_new(schema(), columns).map_err(|source| PipelineIoError::BuildBatch { source })
}

/// Reads a headered CSV file with an `id,name,note` schema and converts it
/// into a single typed Arrow [`RecordBatch`].
///
/// `id` and `name` are non-nullable; an empty `note` field becomes a real
/// null, not an empty string.
///
/// # Errors
///
/// Returns [`PipelineIoError`] if the file cannot be opened, a record cannot be
/// read (including a row with a different column count than the header), an
/// `id` field is not a valid integer, or the resulting arrays cannot be
/// assembled into a `RecordBatch`.
pub fn fixture_to_record_batch(path: &Path) -> Result<RecordBatch, PipelineIoError> {
    batch_from_rows(&parse_rows(path)?)
}

/// Reads the same `id,name,note` fixture as [`fixture_to_record_batch`], but
/// splits the rows into multiple batches of at most `batch_size` rows each,
/// all sharing the same [`schema`].
///
/// # Errors
///
/// Same conditions as [`fixture_to_record_batch`].
///
/// # Panics
///
/// Panics if `batch_size` is zero.
pub fn fixture_to_record_batches(
    path: &Path,
    batch_size: usize,
) -> Result<Vec<RecordBatch>, PipelineIoError> {
    assert!(batch_size > 0, "batch_size must be greater than zero");
    parse_rows(path)?
        .chunks(batch_size)
        .map(batch_from_rows)
        .collect()
}

/// Creates `path` and opens an [`ArrowWriter`] against it for `schema`,
/// compressed with `compression`. Shared by [`write_parquet`] and the bounded
/// pipeline's consumer, which both need the same open-file/writer-properties
/// setup before writing any batches.
///
/// # Errors
///
/// Returns [`PipelineIoError`] if `path` cannot be created or the writer
/// cannot be constructed for `schema`.
pub(crate) fn open_parquet_writer(
    path: &Path,
    schema: SchemaRef,
    compression: Compression,
) -> Result<ArrowWriter<File>, PipelineIoError> {
    let file = File::create(path).map_err(|source| PipelineIoError::OpenOutput {
        path: path.to_path_buf(),
        source,
    })?;
    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();
    ArrowWriter::try_new(file, schema, Some(props))
        .map_err(|source| PipelineIoError::WriteParquet { source })
}

/// Writes `batch` to `path` as a single-row-group Parquet file, compressed
/// with `compression`.
///
/// # Errors
///
/// Returns [`PipelineIoError`] if `path` cannot be created or the batch cannot
/// be encoded as Parquet.
pub fn write_parquet(
    batch: &RecordBatch,
    path: &Path,
    compression: Compression,
) -> Result<(), PipelineIoError> {
    let mut writer = open_parquet_writer(path, batch.schema(), compression)?;
    writer
        .write(batch)
        .map_err(|source| PipelineIoError::WriteParquet { source })?;
    writer
        .close()
        .map_err(|source| PipelineIoError::WriteParquet { source })?;
    Ok(())
}

/// Reads a Parquet file written by [`write_parquet`] back into a single
/// `RecordBatch`.
///
/// # Errors
///
/// Returns [`PipelineIoError`] if `path` cannot be opened or its contents
/// cannot be decoded as a `RecordBatch` matching [`schema`].
pub fn read_parquet(path: &Path) -> Result<RecordBatch, PipelineIoError> {
    let file = File::open(path).map_err(|source| PipelineIoError::OpenInput {
        path: path.to_path_buf(),
        source,
    })?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|source| PipelineIoError::ReadParquet { source })?
        .build()
        .map_err(|source| PipelineIoError::ReadParquet { source })?;

    let batches: Vec<RecordBatch> =
        reader
            .collect::<Result<_, _>>()
            .map_err(|source| PipelineIoError::ReadParquet {
                source: ParquetError::from(source),
            })?;
    arrow::compute::concat_batches(&schema(), &batches)
        .map_err(|source| PipelineIoError::BuildBatch { source })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

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
        assert!(matches!(err, PipelineIoError::ReadRecord { .. }));
    }

    #[test]
    fn empty_note_field_becomes_a_real_null_not_an_empty_string() {
        let batch = fixture_to_record_batch(&fixture_path("headers.csv"))
            .expect("fixture should convert cleanly");

        let notes = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("note column should be Utf8");

        assert!(!notes.is_null(0));
        assert!(!notes.is_null(1));
        assert!(notes.is_null(2));
        assert_eq!(notes.null_count(), 1);
    }

    #[test]
    fn null_propagates_through_the_is_null_kernel() {
        let batch = fixture_to_record_batch(&fixture_path("headers.csv"))
            .expect("fixture should convert cleanly");
        let notes = batch.column(2);

        let mask = arrow::compute::is_null(notes).expect("is_null kernel should run");
        assert!(!mask.value(0));
        assert!(!mask.value(1));
        assert!(mask.value(2));
    }

    #[test]
    fn record_batch_try_new_rejects_a_schema_mismatch() {
        let wrong_type_column: ArrayRef = Arc::new(StringArray::from(vec!["not an int"]));
        let columns: Vec<ArrayRef> = vec![
            wrong_type_column,
            Arc::new(StringArray::from(vec!["a name"])),
            Arc::new(StringArray::from(vec![Some("a note")])),
        ];

        let err = RecordBatch::try_new(schema(), columns)
            .expect_err("an Int64 schema column backed by a Utf8 array must fail, not panic");
        assert!(matches!(
            err,
            arrow::error::ArrowError::InvalidArgumentError(_)
        ));
    }

    #[test]
    fn record_batch_supports_zero_rows() {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
        ];
        let batch = RecordBatch::try_new(schema(), columns).expect("an empty batch is valid");

        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 3);
    }

    #[test]
    fn fixture_splits_into_multiple_batches_sharing_one_schema() {
        let batches = fixture_to_record_batches(&fixture_path("headers.csv"), 2)
            .expect("fixture should split cleanly");

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 1);
        for batch in &batches {
            assert_eq!(batch.schema(), schema());
        }

        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn parquet_round_trip_preserves_schema_nulls_and_values() {
        let original = fixture_to_record_batch(&fixture_path("headers.csv"))
            .expect("fixture should convert cleanly");

        let dir = tempfile::tempdir().expect("temp dir should be creatable");
        let path = dir.path().join("headers.parquet");

        write_parquet(&original, &path, Compression::SNAPPY)
            .expect("batch should write to parquet cleanly");
        let round_tripped = read_parquet(&path).expect("parquet should read back cleanly");

        assert_eq!(round_tripped.schema(), original.schema());
        assert_eq!(round_tripped.num_rows(), original.num_rows());

        for column in 0..original.num_columns() {
            assert_eq!(
                round_tripped.column(column).as_ref(),
                original.column(column).as_ref(),
                "column {column} should match exactly after a parquet round trip"
            );
        }

        let original_notes = original
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("note column should be Utf8");
        let round_tripped_notes = round_tripped
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("note column should be Utf8");
        assert_eq!(
            round_tripped_notes.null_count(),
            original_notes.null_count()
        );
        for row in 0..original.num_rows() {
            assert_eq!(
                round_tripped_notes.is_null(row),
                original_notes.is_null(row)
            );
        }
    }

    #[test]
    fn slicing_shares_the_underlying_buffer_instead_of_copying() {
        let ids = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let sliced = ids.slice(1, 3);

        // `data_ptr()` (not `as_ptr()`) points at the start of the underlying
        // allocation, ignoring the view offset a slice introduces — that
        // offset is exactly why `as_ptr()` differs between the two views
        // even though they share one allocation.
        let original_ptr = ids.to_data().buffers()[0].data_ptr();
        let sliced_ptr = sliced.to_data().buffers()[0].data_ptr();

        assert_eq!(
            original_ptr, sliced_ptr,
            "Array::slice should reuse the original buffer allocation, not copy it"
        );
        assert_eq!(sliced.values(), &[2, 3, 4]);
    }

    #[test]
    fn casting_to_a_different_width_allocates_a_new_buffer() {
        let ids = Int64Array::from(vec![1, 2, 3]);
        let casted = arrow::compute::cast(&ids, &DataType::Float64)
            .expect("Int64 to Float64 cast should succeed");

        let original_ptr = ids.to_data().buffers()[0].data_ptr();
        let casted_ptr = casted.to_data().buffers()[0].data_ptr();

        assert_ne!(
            original_ptr, casted_ptr,
            "cast must allocate a new buffer: Int64 and Float64 differ in byte width"
        );
    }
}
