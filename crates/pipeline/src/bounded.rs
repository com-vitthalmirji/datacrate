//! A bounded input -> Arrow transform -> Parquet output pipeline. Synchronous
//! by design — backpressure and bounded memory come from a bounded
//! count-limited channel or the optional byte-budget transport, not an executor.
//!
//! Each local run owns a unique staged file in the output directory. It is
//! published only after encoding and file synchronization succeed. A parent
//! sync error after publication reports uncertain durability explicitly.
//!
//! See docs/adr/0005-bounded-pipeline-backpressure.md.

use std::fs::File;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex, Weak};
use std::thread;

use crate::staging::LocalStagedOutput;
use arrow::array::RecordBatch;
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;

#[path = "bounded/transport.rs"]
mod transport;

use crate::batch_contract::CheckedIngestBatch;
use crate::manifest::{self, CompletionManifest};
use crate::object_store_io::{
    best_effort_delete, download_to_temp, publish_staged_object, put_manifest, upload_from_temp,
};
pub use crate::staging::{LocalPublicationOutcome, LocalPublicationPhase};
use crate::{
    CallbackStage, PipelineIoError, Row, batch_from_rows, parquet_writer, parse_row, schema,
};

/// Tuning for [`run_bounded_pipeline`]: how many rows make one `RecordBatch`,
/// and how many batches may be in flight between the reader and writer at
/// once. The optional byte budget limits admitted Arrow array charges, excluding
/// batch construction, callback internals, Parquet buffers and allocator overhead.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Rows per `RecordBatch`. Smaller values bound memory more tightly at
    /// the cost of more, smaller Parquet writes.
    pub batch_size: NonZeroUsize,
    /// Capacity of the channel between the reader and writer threads. `0`
    /// makes it a rendezvous channel (strictest possible backpressure).
    pub channel_capacity: usize,
    /// Optional ceiling on admitted Arrow array memory charges. Construction,
    /// callbacks, Parquet buffers and allocator overhead are outside this budget.
    pub max_admitted_bytes: Option<NonZeroUsize>,
    /// Parquet compression for the output file.
    pub compression: Compression,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            batch_size: NonZeroUsize::new(1_000).expect("default batch size is nonzero"),
            channel_capacity: 4,
            max_admitted_bytes: None,
            compression: Compression::SNAPPY,
        }
    }
}

/// A shared, cloneable cancellation flag. Cancelling stops the reader from
/// producing further batches; the writer notices on its next received batch
/// (or immediately, if none was ever sent) and reports
/// [`PipelineError::Cancelled`] instead of publishing output.
#[derive(Debug, Clone, Default)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
    transports: Arc<Mutex<Vec<Weak<transport::Control>>>>,
}

impl CancellationToken {
    /// Creates a token that is not yet cancelled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Requests cancellation. Idempotent.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        let controls = {
            let mut registry = self.transports.lock().unwrap_or_else(|p| p.into_inner());
            registry
                .drain(..)
                .filter_map(|weak| weak.upgrade())
                .collect::<Vec<_>>()
        };
        for control in controls {
            control.cancel();
        }
    }

    fn register(&self, control: &Arc<transport::Control>) {
        {
            let mut registry = self.transports.lock().unwrap_or_else(|p| p.into_inner());
            registry.retain(|weak| weak.strong_count() > 0);
            registry.push(Arc::downgrade(control));
        }
        // Cancellation sets the flag before visiting registrations. This second check
        // covers registration after that visit, without acquiring locks in reverse order.
        if self.is_cancelled() {
            control.cancel();
        }
    }

    /// Reports whether [`cancel`](Self::cancel) has been called.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

/// Summary of a successful [`run_bounded_pipeline`] run.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct PipelineReport {
    /// Number of `RecordBatch`es written.
    pub batches_written: usize,
    /// Total rows written across all batches.
    pub rows_written: usize,
    /// `manifest::accumulate_batch_digest`'s final value over every row
    /// written — order- and batch-boundary-independent.
    pub content_digest: u64,
    /// Retained charge at successful completion (zero); None when budgeting is disabled.
    pub admitted_bytes: Option<usize>,
    /// Highest simultaneous admitted Arrow charge; None when budgeting is disabled.
    pub peak_admitted_bytes: Option<usize>,
}

/// Errors from [`run_bounded_pipeline`].
#[derive(Debug)]
pub enum PipelineError {
    /// A local staging or publication operation failed with known visibility.
    LocalPublication {
        /// Failed operation.
        phase: LocalPublicationPhase,
        /// Final-path visibility known after failure.
        outcome: LocalPublicationOutcome,
        /// Requested final output path.
        output: PathBuf,
        /// Original I/O or encoding failure.
        source: Box<PipelineIoError>,
    },
    /// A finished producer batch cannot fit in the configured byte budget.
    BatchTooLarge {
        /// Estimated retained Arrow bytes.
        bytes: usize,
        /// Configured admission ceiling.
        limit: usize,
    },
    /// A transformed batch cannot grow without exceeding the retained byte budget.
    BatchGrowthExceedsBudget {
        /// Estimated transformed batch bytes.
        bytes: usize,
        /// Bytes available to this lease, including its existing reservation.
        available: usize,
    },
    /// The declared Arrow memory charges cannot be added in usize.
    ByteChargeOverflow,
    /// The reader or writer failed for a reason [`PipelineIoError`] already
    /// models (I/O, malformed CSV, schema mismatch, Parquet errors).
    Io(PipelineIoError),
    /// The pipeline was cancelled via [`CancellationToken::cancel`] before
    /// output could be published.
    Cancelled,
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineError::LocalPublication {
                phase,
                outcome,
                output,
                source,
            } => write!(
                f,
                "local publication {phase:?} for {} ({outcome:?}): {source}",
                output.display()
            ),
            PipelineError::BatchTooLarge { bytes, limit } => {
                write!(f, "batch charge {bytes} exceeds byte budget {limit}")
            }
            PipelineError::BatchGrowthExceedsBudget { bytes, available } => write!(
                f,
                "transformed batch charge {bytes} exceeds available budget {available}"
            ),
            PipelineError::ByteChargeOverflow => f.write_str("batch byte charge overflow"),
            PipelineError::Io(source) => write!(f, "{source}"),
            PipelineError::Cancelled => {
                write!(f, "pipeline was cancelled before output was published")
            }
        }
    }
}

impl std::error::Error for PipelineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PipelineError::Io(source) => Some(source),
            PipelineError::LocalPublication { source, .. } => Some(source.as_ref()),
            PipelineError::Cancelled
            | PipelineError::BatchTooLarge { .. }
            | PipelineError::BatchGrowthExceedsBudget { .. }
            | PipelineError::ByteChargeOverflow => None,
        }
    }
}

impl From<PipelineIoError> for PipelineError {
    fn from(source: PipelineIoError) -> Self {
        PipelineError::Io(source)
    }
}

fn accumulate_batch_stats(report: PipelineReport, checked: &CheckedIngestBatch) -> PipelineReport {
    PipelineReport {
        batches_written: report.batches_written + 1,
        rows_written: report.rows_written + checked.batch().num_rows(),
        content_digest: manifest::accumulate_batch_digest(report.content_digest, checked),
        ..report
    }
}

/// Deterministic remote staging remains separate from owned local files.
fn staging_key_for(output_key: &ObjectPath) -> ObjectPath {
    ObjectPath::from(format!("{output_key}.staging"))
}

enum BatchSender {
    Count(SyncSender<Result<RecordBatch, PipelineIoError>>),
    Budgeted(transport::Sender),
}

impl BatchSender {
    fn send(&self, batch: Result<RecordBatch, PipelineIoError>) -> Result<(), PipelineError> {
        match self {
            Self::Count(sender) => sender.send(batch).map_err(|_| PipelineError::Cancelled),
            Self::Budgeted(sender) => sender.send(batch),
        }
    }
}

enum BatchReceiver {
    Count(Receiver<Result<RecordBatch, PipelineIoError>>),
    Budgeted(transport::Receiver),
}

impl BatchReceiver {
    fn recv(&self) -> Result<Option<transport::BatchLease>, PipelineError> {
        match self {
            Self::Count(receiver) => match receiver.recv() {
                Ok(batch) => batch
                    .map(transport::BatchLease::uncharged)
                    .map(Some)
                    .map_err(PipelineError::Io),
                Err(_) => Ok(None),
            },
            Self::Budgeted(receiver) => receiver.recv(),
        }
    }
}

/// Reads `path` row by row, grouping every `batch_size` rows into a
/// `RecordBatch` and sending it on `sender`. Never holds more than
/// `batch_size` owned rows at once. Checks cancellation between CSV records;
/// reports admission failure, and stops when the receiving end closes.
fn produce_batches(
    path: &Path,
    batch_size: usize,
    cancel: &CancellationToken,
    sender: BatchSender,
) -> Result<(), PipelineError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(source) => {
            return sender.send(Err(PipelineIoError::OpenInput {
                path: path.to_path_buf(),
                source,
            }));
        }
    };
    let mut reader = csv::Reader::from_reader(file);
    let mut chunk: Vec<Row> = Vec::with_capacity(batch_size);

    for (row_index, record) in reader.records().enumerate() {
        if cancel.is_cancelled() {
            return Ok(());
        }
        let row = match record
            .map_err(|source| PipelineIoError::ReadRecord { source })
            .and_then(|record| parse_row(row_index, &record))
        {
            Ok(row) => row,
            Err(err) => {
                return sender.send(Err(err));
            }
        };
        chunk.push(row);

        if chunk.len() == batch_size {
            let batch = batch_from_rows(&chunk);
            chunk.clear();
            sender.send(batch)?;
        }
    }

    if !chunk.is_empty() && !cancel.is_cancelled() {
        sender.send(batch_from_rows(&chunk))?;
    }
    Ok(())
}

/// Receives batches from `receiver` and writes them through the owned writer. Reports [`PipelineError::Cancelled`] if `cancel` is set
/// either before the first batch or between batches, without writing
/// anything further.
///
/// `receiver` is taken by value so that every early return below drops it:
/// that drop is what unblocks [`produce_batches`] if it's parked in a
/// blocking `sender.send()` on a full channel, rather than deadlocking.
fn consume_batches<W: std::io::Write + Send>(
    receiver: BatchReceiver,
    mut writer: ArrowWriter<W>,
    output: &Path,
    transform: Option<&dyn Fn(RecordBatch) -> Result<RecordBatch, PipelineIoError>>,
    cancel: &CancellationToken,
) -> Result<PipelineReport, PipelineError> {
    if cancel.is_cancelled() {
        return Err(PipelineError::Cancelled);
    }

    let mut report = PipelineReport::default();

    while let Some(lease) = receiver.recv()? {
        if cancel.is_cancelled() {
            return Err(PipelineError::Cancelled);
        }
        // Applied before the digest, not after: the manifest must audit what
        // was actually written, not the raw input.
        let lease = match transform {
            Some(f) => lease.transform(|batch| {
                f(batch).map_err(|source| PipelineIoError::Callback {
                    stage: CallbackStage::Transform,
                    batch_index: report.batches_written,
                    source: Box::new(source),
                })
            })?,
            None => lease,
        };
        let (batch, _reservation) = lease.into_parts();
        let checked = CheckedIngestBatch::try_new(batch)?;
        writer.write(checked.batch()).map_err(|source| {
            crate::staging::failure(
                output,
                LocalPublicationPhase::Write,
                PipelineIoError::WriteParquet { source },
            )
        })?;
        report = accumulate_batch_stats(report, &checked);
    }

    if cancel.is_cancelled() {
        return Err(PipelineError::Cancelled);
    }

    drop(writer.into_inner().map_err(|source| {
        crate::staging::failure(
            output,
            LocalPublicationPhase::Close,
            PipelineIoError::WriteParquet { source },
        )
    })?);
    Ok(report)
}

/// Runs a bounded `input` (CSV) -> Arrow -> `output` (Parquet) pipeline.
///
/// The reader and writer run on separate scoped threads connected by a
/// channel of capacity `config.channel_capacity`: the reader blocks once
/// that many batches are unconsumed, bounding memory independently of input
/// size. Output is staged and renamed into place only on success — any
/// error before rename leaves `output` untouched and drops owned staging.
/// A parent-directory sync failure after rename reports published output with
/// uncertain durability. The destination directory must be on one filesystem.
///
/// `thread::scope` only returns once the producer thread has observed
/// `consume_batches`'s receiver drop (as a `SendError`, if it was blocked
/// on `send()`) and returned, so the rename/cleanup step below never races
/// a still-running producer.
///
/// # Errors
///
/// Returns [`PipelineError::Io`] for the same conditions as
/// [`crate::fixture_to_record_batch`], [`PipelineError::LocalPublication`] for
/// encoding/staging/publication failures, or
/// [`PipelineError::Cancelled`] if `cancel` was set before output could be
/// published.
pub fn run_bounded_pipeline(
    input: &Path,
    output: &Path,
    config: &PipelineConfig,
    transform: Option<&dyn Fn(RecordBatch) -> Result<RecordBatch, PipelineIoError>>,
    cancel: &CancellationToken,
) -> Result<PipelineReport, PipelineError> {
    if cancel.is_cancelled() {
        return Err(PipelineError::Cancelled);
    }
    let staging = LocalStagedOutput::new(output)?;
    let writer = parquet_writer(staging.file()?, schema(), config.compression)
        .map_err(|source| crate::staging::failure(output, LocalPublicationPhase::Write, source))?;
    let (sender, receiver, accounting) = match config.max_admitted_bytes {
        Some(limit) => {
            let (sender, receiver, control) =
                transport::channel(config.channel_capacity, limit.get(), cancel);
            (
                BatchSender::Budgeted(sender),
                BatchReceiver::Budgeted(receiver),
                Some(control),
            )
        }
        None => {
            let (sender, receiver) = sync_channel(config.channel_capacity);
            (
                BatchSender::Count(sender),
                BatchReceiver::Count(receiver),
                None,
            )
        }
    };
    let mut production = Ok(());
    let result = thread::scope(|scope| {
        scope
            .spawn(|| production = produce_batches(input, config.batch_size.get(), cancel, sender));
        consume_batches(receiver, writer, output, transform, cancel)
    });
    // A producer admission error must not publish a successfully closed partial file.
    let result = result.and_then(|mut report| {
        production?;
        if let Some(control) = accounting {
            let (current, peak) = control.metrics();
            report.admitted_bytes = Some(current);
            report.peak_admitted_bytes = Some(peak);
        }
        Ok(report)
    });

    let report = result?;
    if cancel.is_cancelled() {
        return Err(PipelineError::Cancelled);
    }
    staging.publish()?;
    Ok(report)
}

/// Runs [`run_bounded_pipeline`] against objects in `store` instead of local
/// paths: downloads `input_key` to a local temp CSV, runs the unmodified
/// local pipeline, then publishes the resulting Parquet to `output_key`.
/// Local disk staging keeps the bounded pipeline's memory envelope unchanged
/// from the local-file case — only the two ends of the pipe move.
///
/// Publishing mirrors [`run_bounded_pipeline`]'s local staging pattern on the
/// object-store side: the upload lands at a `.staging` sibling key first,
/// then [`publish_staged_object`] renames it to `output_key` — so a reader
/// never observes a partially-uploaded object at the final key. Only once
/// that rename succeeds is a [`CompletionManifest`] (row count and an
/// order-independent content digest) written to `output_key`'s
/// `.manifest.json` sibling; its presence is the completion signal a reader
/// should wait for. If the manifest write fails, the published output is
/// removed (best-effort) rather than left behind without a valid completion
/// signal.
///
/// # Errors
///
/// Returns [`PipelineError::Io`] if the download, the local pipeline run, the
/// input's metadata read, the publish, or the manifest write fails, or
/// [`PipelineError::Cancelled`] if `cancel` was set before output could be
/// published.
pub fn run_bounded_pipeline_s3(
    store: &dyn ObjectStore,
    input_key: &ObjectPath,
    output_key: &ObjectPath,
    config: &PipelineConfig,
    transform: Option<&dyn Fn(RecordBatch) -> Result<RecordBatch, PipelineIoError>>,
    cancel: &CancellationToken,
) -> Result<PipelineReport, PipelineError> {
    let staging_dir = tempfile::tempdir().map_err(|source| PipelineIoError::OpenOutput {
        path: std::env::temp_dir(),
        source,
    })?;
    let local_input = staging_dir.path().join("input.csv");
    let local_output = staging_dir.path().join("output.parquet");

    let input_meta = download_to_temp(store, input_key, &local_input)?;
    let report = run_bounded_pipeline(&local_input, &local_output, config, transform, cancel)?;

    let staging_key = staging_key_for(output_key);
    upload_from_temp(store, &local_output, &staging_key)?;
    if let Err(err) = publish_staged_object(store, &staging_key, output_key) {
        best_effort_delete(store, &staging_key);
        return Err(err.into());
    }

    let manifest = CompletionManifest::new(
        input_key,
        &input_meta,
        report.rows_written,
        report.content_digest,
    );
    if let Err(err) = put_manifest(store, &manifest, &CompletionManifest::key_for(output_key)) {
        best_effort_delete(store, output_key);
        return Err(err.into());
    }

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read_parquet;
    use object_store::ObjectStoreExt;
    use std::io::Write;

    fn assert_no_owned_staging(output: &Path) {
        let parent = output.parent().expect("output directory");
        for entry in std::fs::read_dir(parent).expect("directory") {
            let entry = entry.expect("entry");
            assert!(
                !entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".datacrate-")
            );
        }
    }

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
    fn accumulate_batch_stats_updates_counters() {
        let record = csv::StringRecord::from(vec!["1", "name-1", "note-1"]);
        let row = crate::parse_row(0, &record).expect("row should parse");
        let batch = CheckedIngestBatch::try_new(
            crate::batch_from_rows(&[row]).expect("single-row batch should build"),
        )
        .expect("valid ingest batch");
        let report = PipelineReport::default();

        let report = accumulate_batch_stats(report, &batch);
        let report = accumulate_batch_stats(report, &batch);

        assert_eq!(report.batches_written, 2);
        assert_eq!(report.rows_written, 2);
    }

    #[test]
    fn bounded_pipeline_round_trips_the_headers_fixture() {
        let dir = tempfile::tempdir().expect("temp dir should be creatable");
        let output = dir.path().join("out.parquet");

        let report = run_bounded_pipeline(
            &fixture_path("headers.csv"),
            &output,
            &PipelineConfig::default(),
            None,
            &CancellationToken::new(),
        )
        .expect("bounded pipeline should succeed on a clean fixture");

        assert_eq!(report.rows_written, 3);
        assert!(output.exists());
        assert_no_owned_staging(&output);
    }

    #[test]
    fn bounded_pipeline_matches_the_synchronous_reference() {
        let dir = tempfile::tempdir().expect("temp dir should be creatable");
        let output = dir.path().join("out.parquet");

        run_bounded_pipeline(
            &fixture_path("headers.csv"),
            &output,
            &PipelineConfig::default(),
            None,
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
            batch_size: NonZeroUsize::new(250).expect("positive batch size"),
            channel_capacity: 2,
            ..PipelineConfig::default()
        };

        let report =
            run_bounded_pipeline(&csv_path, &output, &config, None, &CancellationToken::new())
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
            None,
            &CancellationToken::new(),
        )
        .expect_err("a malformed row should fail the pipeline, not produce partial output");

        assert!(matches!(
            err,
            PipelineError::Io(PipelineIoError::ReadRecord { .. })
        ));
        assert!(!output.exists());
        assert_no_owned_staging(&output);
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
            None,
            &cancel,
        )
        .expect_err("a pre-cancelled token must stop the pipeline");

        assert!(matches!(err, PipelineError::Cancelled));
        assert!(!output.exists());
        assert_no_owned_staging(&output);
    }

    #[test]
    fn writer_open_failure_cleans_up_and_publishes_nothing() {
        let dir = tempfile::tempdir().expect("temp dir should be creatable");
        // The output directory is required before owned staging can be created.
        let output = dir.path().join("missing-subdir").join("out.parquet");

        let err = run_bounded_pipeline(
            &fixture_path("headers.csv"),
            &output,
            &PipelineConfig::default(),
            None,
            &CancellationToken::new(),
        )
        .expect_err("staging file creation should fail when the parent directory is missing");

        assert!(matches!(
            err,
            PipelineError::LocalPublication {
                phase: LocalPublicationPhase::Create,
                outcome: LocalPublicationOutcome::NotPublished,
                ..
            }
        ));
        assert!(!output.exists());
    }

    struct FailingWriter {
        file: File,
        remaining: usize,
    }

    impl std::io::Write for FailingWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let length = bytes.len().min(self.remaining);
            if length == 0 {
                return Err(std::io::Error::other("injected writer failure"));
            }
            self.file.write_all(&bytes[..length])?;
            self.remaining -= length;
            Ok(length)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.file.flush()
        }
    }

    #[test]
    fn owned_staging_write_and_close_failures_keep_previous_output() {
        use arrow::array::{Int64Array, StringArray};
        use parquet::file::properties::WriterProperties;
        for budgeted in [false, true] {
            for (row_group, phase) in [
                (1, LocalPublicationPhase::Write),
                (1000, LocalPublicationPhase::Close),
            ] {
                let dir = tempfile::tempdir().expect("directory");
                let output = dir.path().join("output");
                std::fs::write(&output, b"old").expect("old output");
                let staged = LocalStagedOutput::new(&output).expect("owner");
                let file = FailingWriter {
                    file: staged.file().expect("file"),
                    remaining: 4,
                };
                let properties = WriterProperties::builder()
                    .set_max_row_group_row_count(Some(row_group))
                    .set_compression(Compression::UNCOMPRESSED)
                    .build();
                let writer = ArrowWriter::try_new(file, schema(), Some(properties))
                    .expect("construct writer");
                // TrackedWrite buffers small writes. A large uncompressed dictionary
                // page forces actual I/O during write; the small fixture fails at close.
                let batch = if phase == LocalPublicationPhase::Write {
                    RecordBatch::try_new(
                        schema(),
                        vec![
                            Arc::new(Int64Array::from(vec![1])),
                            Arc::new(StringArray::from(vec!["x".repeat(20_000)])),
                            Arc::new(StringArray::from(vec![None::<&str>])),
                        ],
                    )
                    .expect("large batch")
                } else {
                    crate::fixture_to_record_batch(&fixture_path("headers.csv")).expect("fixture")
                };
                let (receiver, accounting) = if budgeted {
                    let (sender, receiver, control) =
                        transport::channel(1, 1_000_000, &CancellationToken::new());
                    sender.send(Ok(batch)).expect("admit");
                    drop(sender);
                    (BatchReceiver::Budgeted(receiver), Some(control))
                } else {
                    let (sender, receiver) = sync_channel(1);
                    sender.send(Ok(batch)).expect("send");
                    drop(sender);
                    (BatchReceiver::Count(receiver), None)
                };
                let error =
                    consume_batches(receiver, writer, &output, None, &CancellationToken::new())
                        .expect_err("writer error");
                assert!(
                    matches!(&error, PipelineError::LocalPublication { phase: actual, outcome: LocalPublicationOutcome::NotPublished, source, .. } if *actual == phase && matches!(source.as_ref(), PipelineIoError::WriteParquet { .. })),
                    "expected {phase:?}, got {error:?}"
                );
                if let Some(control) = accounting {
                    assert_eq!(
                        control.metrics().0,
                        0,
                        "failed encoding releases reservations"
                    );
                    assert!(control.metrics().1 > 0);
                }
                drop(staged);
                assert_eq!(std::fs::read(&output).expect("output"), b"old");
                assert_no_owned_staging(&output);
            }
        }
    }

    #[test]
    fn cancellation_token_defaults_to_not_cancelled() {
        let cancel = CancellationToken::new();
        assert!(!cancel.is_cancelled());
        cancel.cancel();
        assert!(cancel.is_cancelled());
    }

    fn test_block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread tokio runtime should build")
            .block_on(future)
    }

    #[test]
    fn s3_pipeline_downloads_transforms_and_uploads_through_an_in_memory_store() {
        use object_store::PutPayload;
        use object_store::memory::InMemory;
        use object_store::path::Path as ObjectPath;

        let store = InMemory::new();
        let input_key = ObjectPath::from("input.csv");
        let output_key = ObjectPath::from("output.parquet");

        let csv = std::fs::read(fixture_path("headers.csv")).expect("fixture should be readable");
        test_block_on(store.put(&input_key, PutPayload::from(csv)))
            .expect("seeding the in-memory store should succeed");

        let report = run_bounded_pipeline_s3(
            &store,
            &input_key,
            &output_key,
            &PipelineConfig::default(),
            None,
            &CancellationToken::new(),
        )
        .expect("s3 pipeline should succeed against an in-memory store");

        assert_eq!(report.rows_written, 3);

        let uploaded = test_block_on(async {
            store
                .get(&output_key)
                .await
                .expect("output object should exist")
                .bytes()
                .await
                .expect("output object bytes should be readable")
        });
        assert!(!uploaded.is_empty());
    }

    #[test]
    fn s3_pipeline_publishes_a_manifest_and_leaves_no_staging_key_behind() {
        use object_store::PutPayload;
        use object_store::memory::InMemory;
        use object_store::path::Path as ObjectPath;

        let store = InMemory::new();
        let input_key = ObjectPath::from("input.csv");
        let output_key = ObjectPath::from("output.parquet");

        let csv = std::fs::read(fixture_path("headers.csv")).expect("fixture should be readable");
        test_block_on(store.put(&input_key, PutPayload::from(csv)))
            .expect("seeding the in-memory store should succeed");

        let report = run_bounded_pipeline_s3(
            &store,
            &input_key,
            &output_key,
            &PipelineConfig::default(),
            None,
            &CancellationToken::new(),
        )
        .expect("s3 pipeline should succeed against an in-memory store");

        let manifest_key = CompletionManifest::key_for(&output_key);
        let manifest_bytes = test_block_on(async {
            store
                .get(&manifest_key)
                .await
                .expect("manifest object should exist at the .manifest.json sibling key")
                .bytes()
                .await
                .expect("manifest object bytes should be readable")
        });
        let manifest: CompletionManifest =
            manifest::decode_completion_manifest(&manifest_bytes).expect("supported manifest");

        assert_eq!(manifest.input_key, input_key.to_string());
        assert_eq!(manifest.row_count, report.rows_written);
        assert_eq!(
            manifest.content_digest,
            format!("{:016x}", report.content_digest)
        );

        let staging_key = staging_key_for(&output_key);
        let staging_result = test_block_on(store.head(&staging_key));
        assert!(
            staging_result.is_err(),
            "the staging key should be renamed away, not left behind after a successful publish"
        );
    }

    #[test]
    fn s3_pipeline_reports_download_object_when_the_input_key_is_missing_on_a_real_backend() {
        use object_store::path::Path as ObjectPath;

        let minio = crate::minio_test_support::MinioContainer::start("s3-missing-input-download");
        let store = minio.store();
        let input_key = ObjectPath::from("missing-input.csv");
        let output_key = ObjectPath::from("output.parquet");

        let err = run_bounded_pipeline_s3(
            store.as_ref(),
            &input_key,
            &output_key,
            &PipelineConfig::default(),
            None,
            &CancellationToken::new(),
        )
        .expect_err("a missing input key on a real backend must fail, not panic");

        assert!(matches!(
            err,
            PipelineError::Io(PipelineIoError::DownloadObject { .. })
        ));
    }

    #[test]
    fn s3_pipeline_leaves_no_partial_output_on_a_real_backend_when_local_processing_fails() {
        use object_store::PutPayload;
        use object_store::path::Path as ObjectPath;

        let minio = crate::minio_test_support::MinioContainer::start(
            "s3-pipeline-leaves-no-partial-output-on-a-real-backend",
        );
        let store = minio.store();
        let input_key = ObjectPath::from("malformed.csv");
        let output_key = ObjectPath::from("output.parquet");

        let malformed =
            std::fs::read(fixture_path("malformed.csv")).expect("fixture should be readable");
        test_block_on(store.put(&input_key, PutPayload::from(malformed)))
            .expect("seeding the real backend should succeed");

        let err = run_bounded_pipeline_s3(
            store.as_ref(),
            &input_key,
            &output_key,
            &PipelineConfig::default(),
            None,
            &CancellationToken::new(),
        )
        .expect_err("malformed input on a real backend must fail, not publish partial output");

        assert!(matches!(
            err,
            PipelineError::Io(PipelineIoError::ReadRecord { .. })
        ));

        let output_result = test_block_on(store.head(&output_key));
        assert!(
            output_result.is_err(),
            "the final key must not exist after a run that failed before any upload"
        );

        let staging_result = test_block_on(store.head(&staging_key_for(&output_key)));
        assert!(
            staging_result.is_err(),
            "no staging key should be left behind after a failed run"
        );
    }
}

#[cfg(test)]
mod config_regressions {
    use super::*;
    #[test]
    fn one_row_batches_complete_over_a_rendezvous_channel() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let input = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/m1/headers.csv");
        let config = PipelineConfig {
            batch_size: NonZeroUsize::new(1).expect("positive"),
            channel_capacity: 0,
            ..PipelineConfig::default()
        };
        let report = run_bounded_pipeline(
            &input,
            &dir.path().join("output.parquet"),
            &config,
            None,
            &CancellationToken::new(),
        )
        .expect("rendezvous pipeline completes");
        assert_eq!(report.rows_written, 3);
        assert_eq!(report.batches_written, 3);
    }
}
