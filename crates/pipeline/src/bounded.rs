//! Week 6 build contract: a bounded input -> Arrow transform -> Parquet
//! output pipeline. Synchronous by design (`CLAUDE.md`: no async until M3
//! creates the need) — backpressure and bounded memory come from a bounded
//! `mpsc::sync_channel`, not from an executor.
//!
//! Output is staged: the writer targets a `.tmp` sibling of the requested
//! output path and is renamed into place only after a full, successful
//! close. Any error or cancellation removes the staged file instead of
//! publishing partial output.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread;

use arrow::array::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::{FixtureError, Row, batch_from_rows, parse_row, schema};

/// Tuning for [`run_bounded_pipeline`]: how many rows make one `RecordBatch`,
/// and how many batches may be in flight between the reader and writer at
/// once. Both bound the pipeline's peak memory independently of input size.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Rows per `RecordBatch`. Smaller values bound memory more tightly at
    /// the cost of more, smaller Parquet writes.
    pub batch_size: usize,
    /// Capacity of the channel between the reader and writer threads. `0`
    /// makes it a rendezvous channel (strictest possible backpressure).
    pub channel_capacity: usize,
    /// Parquet compression for the output file.
    pub compression: Compression,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            batch_size: 1_000,
            channel_capacity: 4,
            compression: Compression::SNAPPY,
        }
    }
}

/// A shared, cloneable cancellation flag. Cancelling stops the reader from
/// producing further batches; the writer notices on its next received batch
/// (or immediately, if none was ever sent) and reports
/// [`PipelineError::Cancelled`] instead of publishing output.
#[derive(Debug, Clone, Default)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    /// Creates a token that is not yet cancelled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Requests cancellation. Idempotent.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    /// Reports whether [`cancel`](Self::cancel) has been called.
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// Summary of a successful [`run_bounded_pipeline`] run.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct PipelineReport {
    /// Number of `RecordBatch`es written.
    pub batches_written: usize,
    /// Total rows written across all batches.
    pub rows_written: usize,
}

/// Errors from [`run_bounded_pipeline`].
#[derive(Debug)]
pub enum PipelineError {
    /// The reader or writer failed for a reason [`FixtureError`] already
    /// models (I/O, malformed CSV, schema mismatch, Parquet errors).
    Fixture(FixtureError),
    /// The pipeline was cancelled via [`CancellationToken::cancel`] before
    /// output could be published.
    Cancelled,
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineError::Fixture(source) => write!(f, "{source}"),
            PipelineError::Cancelled => {
                write!(f, "pipeline was cancelled before output was published")
            }
        }
    }
}

impl std::error::Error for PipelineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PipelineError::Fixture(source) => Some(source),
            PipelineError::Cancelled => None,
        }
    }
}

impl From<FixtureError> for PipelineError {
    fn from(source: FixtureError) -> Self {
        PipelineError::Fixture(source)
    }
}

fn staging_path_for(output: &Path) -> PathBuf {
    let mut staging = output.as_os_str().to_owned();
    staging.push(".tmp");
    PathBuf::from(staging)
}

/// Reads `path` row by row, grouping every `batch_size` rows into a
/// `RecordBatch` and sending it on `sender`. Never holds more than
/// `batch_size` rows in memory at once. Stops early, without error, if
/// `cancel` is set or the receiving end has gone away.
fn produce_batches(
    path: &Path,
    batch_size: usize,
    cancel: &CancellationToken,
    sender: SyncSender<Result<RecordBatch, FixtureError>>,
) {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(source) => {
            let _ = sender.send(Err(FixtureError::OpenInput {
                path: path.to_path_buf(),
                source,
            }));
            return;
        }
    };
    let mut reader = csv::Reader::from_reader(file);
    let mut chunk: Vec<Row> = Vec::with_capacity(batch_size);

    for (row_index, record) in reader.records().enumerate() {
        if cancel.is_cancelled() {
            return;
        }
        let row = match record
            .map_err(|source| FixtureError::ReadRecord { source })
            .and_then(|record| parse_row(row_index, &record))
        {
            Ok(row) => row,
            Err(err) => {
                let _ = sender.send(Err(err));
                return;
            }
        };
        chunk.push(row);

        if chunk.len() == batch_size {
            let batch = batch_from_rows(&chunk);
            chunk.clear();
            if sender.send(batch).is_err() {
                return;
            }
        }
    }

    if !chunk.is_empty() && !cancel.is_cancelled() {
        let _ = sender.send(batch_from_rows(&chunk));
    }
}

/// Receives batches from `receiver`, writing each to a Parquet writer over
/// `staging_path`. Reports [`PipelineError::Cancelled`] if `cancel` is set
/// either before the first batch or between batches, without writing
/// anything further.
fn consume_batches(
    receiver: Receiver<Result<RecordBatch, FixtureError>>,
    staging_path: &Path,
    compression: Compression,
    cancel: &CancellationToken,
) -> Result<PipelineReport, PipelineError> {
    if cancel.is_cancelled() {
        return Err(PipelineError::Cancelled);
    }

    let file = File::create(staging_path).map_err(|source| FixtureError::OpenOutput {
        path: staging_path.to_path_buf(),
        source,
    })?;
    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema(), Some(props))
        .map_err(|source| FixtureError::WriteParquet { source })?;

    let mut report = PipelineReport::default();

    for received in receiver {
        if cancel.is_cancelled() {
            return Err(PipelineError::Cancelled);
        }
        let batch = received?;
        writer
            .write(&batch)
            .map_err(|source| FixtureError::WriteParquet { source })?;
        report.batches_written += 1;
        report.rows_written += batch.num_rows();
    }

    if cancel.is_cancelled() {
        return Err(PipelineError::Cancelled);
    }

    writer
        .close()
        .map_err(|source| FixtureError::WriteParquet { source })?;
    Ok(report)
}

/// Runs a bounded `input` (CSV) -> Arrow -> `output` (Parquet) pipeline.
///
/// The reader and writer run on separate scoped threads connected by a
/// channel of capacity `config.channel_capacity`: the reader blocks once
/// that many batches are unconsumed, bounding memory independently of input
/// size. Output is staged and renamed into place only on success — any
/// error or cancellation leaves `output` untouched and removes the staged
/// file.
///
/// # Errors
///
/// Returns [`PipelineError::Fixture`] for the same conditions as
/// [`crate::fixture_to_record_batch`], plus writer/staging I/O failures, or
/// [`PipelineError::Cancelled`] if `cancel` was set before output could be
/// published.
pub fn run_bounded_pipeline(
    input: &Path,
    output: &Path,
    config: &PipelineConfig,
    cancel: &CancellationToken,
) -> Result<PipelineReport, PipelineError> {
    let (sender, receiver) =
        sync_channel::<Result<RecordBatch, FixtureError>>(config.channel_capacity);
    let staging_path = staging_path_for(output);

    let result = thread::scope(|scope| {
        scope.spawn(|| produce_batches(input, config.batch_size, cancel, sender));
        consume_batches(receiver, &staging_path, config.compression, cancel)
    });

    match result {
        Ok(report) => {
            std::fs::rename(&staging_path, output).map_err(|source| FixtureError::OpenOutput {
                path: output.to_path_buf(),
                source,
            })?;
            Ok(report)
        }
        Err(err) => {
            let _ = std::fs::remove_file(&staging_path);
            Err(err)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read_parquet;
    use std::io::Write;

    fn fixture_path(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/m1")
            .join(name)
    }

    /// Writes a synthetic `id,name,note` CSV with `row_count` rows to a fresh
    /// temp file, returning the temp dir (kept alive for the file's
    /// lifetime) and the file path.
    fn synthetic_csv(row_count: usize) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().expect("temp dir should be creatable");
        let path = dir.path().join("synthetic.csv");
        let mut file = std::fs::File::create(&path).expect("temp csv should be creatable");
        writeln!(file, "id,name,note").expect("header should write");
        for i in 0..row_count {
            writeln!(file, "{i},name-{i},note-{i}").expect("row should write");
        }
        (dir, path)
    }

    #[test]
    fn bounded_pipeline_round_trips_the_headers_fixture() {
        let dir = tempfile::tempdir().expect("temp dir should be creatable");
        let output = dir.path().join("out.parquet");

        let report = run_bounded_pipeline(
            &fixture_path("headers.csv"),
            &output,
            &PipelineConfig::default(),
            &CancellationToken::new(),
        )
        .expect("bounded pipeline should succeed on a clean fixture");

        assert_eq!(report.rows_written, 3);
        assert!(output.exists());
        assert!(!staging_path_for(&output).exists());
    }

    #[test]
    fn bounded_pipeline_matches_the_synchronous_reference() {
        let dir = tempfile::tempdir().expect("temp dir should be creatable");
        let output = dir.path().join("out.parquet");

        run_bounded_pipeline(
            &fixture_path("headers.csv"),
            &output,
            &PipelineConfig::default(),
            &CancellationToken::new(),
        )
        .expect("bounded pipeline should succeed on a clean fixture");

        let bounded_result = read_parquet(&output).expect("staged parquet should read back");
        let synchronous_result = crate::fixture_to_record_batch(&fixture_path("headers.csv"))
            .expect("synchronous reference should convert cleanly");

        assert_eq!(bounded_result.schema(), synchronous_result.schema());
        assert_eq!(bounded_result.num_rows(), synchronous_result.num_rows());
        for column in 0..synchronous_result.num_columns() {
            assert_eq!(
                bounded_result.column(column).as_ref(),
                synchronous_result.column(column).as_ref(),
                "column {column} should match the synchronous reference exactly"
            );
        }
    }

    #[test]
    fn bounded_pipeline_streams_a_large_input_with_small_batches_and_a_narrow_channel() {
        let (_csv_dir, csv_path) = synthetic_csv(20_000);
        let out_dir = tempfile::tempdir().expect("temp dir should be creatable");
        let output = out_dir.path().join("out.parquet");

        let config = PipelineConfig {
            batch_size: 250,
            channel_capacity: 2,
            ..PipelineConfig::default()
        };

        let report = run_bounded_pipeline(&csv_path, &output, &config, &CancellationToken::new())
            .expect("bounded pipeline should succeed on a larger streamed input");

        assert_eq!(report.rows_written, 20_000);
        assert_eq!(report.batches_written, 80);

        let batch = read_parquet(&output).expect("staged parquet should read back");
        assert_eq!(batch.num_rows(), 20_000);
    }

    #[test]
    fn malformed_input_fails_and_publishes_nothing() {
        let dir = tempfile::tempdir().expect("temp dir should be creatable");
        let output = dir.path().join("out.parquet");

        let err = run_bounded_pipeline(
            &fixture_path("malformed.csv"),
            &output,
            &PipelineConfig::default(),
            &CancellationToken::new(),
        )
        .expect_err("a malformed row should fail the pipeline, not produce partial output");

        assert!(matches!(
            err,
            PipelineError::Fixture(FixtureError::ReadRecord { .. })
        ));
        assert!(!output.exists());
        assert!(!staging_path_for(&output).exists());
    }

    #[test]
    fn a_pre_cancelled_token_stops_the_pipeline_before_any_output_is_published() {
        let dir = tempfile::tempdir().expect("temp dir should be creatable");
        let output = dir.path().join("out.parquet");
        let cancel = CancellationToken::new();
        cancel.cancel();

        let err = run_bounded_pipeline(
            &fixture_path("headers.csv"),
            &output,
            &PipelineConfig::default(),
            &cancel,
        )
        .expect_err("a pre-cancelled token must stop the pipeline");

        assert!(matches!(err, PipelineError::Cancelled));
        assert!(!output.exists());
        assert!(!staging_path_for(&output).exists());
    }

    #[test]
    fn writer_open_failure_cleans_up_and_publishes_nothing() {
        let dir = tempfile::tempdir().expect("temp dir should be creatable");
        // A path inside a directory that doesn't exist: `File::create` for
        // the staging file must fail before any row is written.
        let output = dir.path().join("missing-subdir").join("out.parquet");

        let err = run_bounded_pipeline(
            &fixture_path("headers.csv"),
            &output,
            &PipelineConfig::default(),
            &CancellationToken::new(),
        )
        .expect_err("staging file creation should fail when the parent directory is missing");

        assert!(matches!(
            err,
            PipelineError::Fixture(FixtureError::OpenOutput { .. })
        ));
        assert!(!output.exists());
    }

    #[test]
    fn cancellation_token_defaults_to_not_cancelled() {
        let cancel = CancellationToken::new();
        assert!(!cancel.is_cancelled());
        cancel.cancel();
        assert!(cancel.is_cancelled());
    }
}
