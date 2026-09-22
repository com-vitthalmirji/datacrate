//! A bounded input -> Arrow transform -> Parquet output pipeline. Synchronous
//! by design — backpressure and bounded memory come from a bounded
//! `mpsc::sync_channel`, not from an executor.
//!
//! Output is staged: the writer targets a `.tmp` sibling of the requested
//! output path and is renamed into place only after a full, successful
//! close. Any error or cancellation removes the staged file instead of
//! publishing partial output.
//!
//! See docs/adr/0005-bounded-pipeline-backpressure.md.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread;

use arrow::array::RecordBatch;
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use parquet::basic::Compression;

use crate::manifest::{self, CompletionManifest};
use crate::object_store_io::{
    best_effort_delete, download_to_temp, head_object, publish_staged_object, put_manifest,
    upload_from_temp,
};
use crate::{PipelineIoError, Row, batch_from_rows, open_parquet_writer, parse_row, schema};

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
        self.0.store(true, Ordering::Relaxed);
    }

    /// Reports whether [`cancel`](Self::cancel) has been called.
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
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
}

/// Errors from [`run_bounded_pipeline`].
#[derive(Debug)]
pub enum PipelineError {
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
            PipelineError::Cancelled => None,
        }
    }
}

impl From<PipelineIoError> for PipelineError {
    fn from(source: PipelineIoError) -> Self {
        PipelineError::Io(source)
    }
}

fn accumulate_batch_stats(report: PipelineReport, batch: &RecordBatch) -> PipelineReport {
    PipelineReport {
        batches_written: report.batches_written + 1,
        rows_written: report.rows_written + batch.num_rows(),
        content_digest: manifest::accumulate_batch_digest(report.content_digest, batch),
    }
}

fn staging_path_for(output: &Path) -> PathBuf {
    let mut staging = output.as_os_str().to_owned();
    staging.push(".tmp");
    PathBuf::from(staging)
}

/// The object-store-side counterpart to [`staging_path_for`]: a `.staging`
/// sibling key, published to `output_key` only after the upload succeeds.
fn staging_key_for(output_key: &ObjectPath) -> ObjectPath {
    ObjectPath::from(format!("{output_key}.staging"))
}

/// Reads `path` row by row, grouping every `batch_size` rows into a
/// `RecordBatch` and sending it on `sender`. Never holds more than
/// `batch_size` rows in memory at once. Stops early, without error, if
/// `cancel` is set or the receiving end has gone away.
fn produce_batches(
    path: &Path,
    batch_size: usize,
    cancel: &CancellationToken,
    sender: SyncSender<Result<RecordBatch, PipelineIoError>>,
) {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(source) => {
            let _ = sender.send(Err(PipelineIoError::OpenInput {
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
            .map_err(|source| PipelineIoError::ReadRecord { source })
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
///
/// `receiver` is taken by value so that every early return below drops it:
/// that drop is what unblocks [`produce_batches`] if it's parked in a
/// blocking `sender.send()` on a full channel, rather than deadlocking.
fn consume_batches(
    receiver: Receiver<Result<RecordBatch, PipelineIoError>>,
    staging_path: &Path,
    compression: Compression,
    transform: Option<&dyn Fn(RecordBatch) -> RecordBatch>,
    cancel: &CancellationToken,
) -> Result<PipelineReport, PipelineError> {
    if cancel.is_cancelled() {
        return Err(PipelineError::Cancelled);
    }

    let mut writer = open_parquet_writer(staging_path, schema(), compression)?;

    let mut report = PipelineReport::default();

    for received in receiver {
        if cancel.is_cancelled() {
            return Err(PipelineError::Cancelled);
        }
        let batch = received?;
        // Applied before the digest, not after: the manifest must audit what
        // was actually written, not the raw input.
        let batch = match transform {
            Some(f) => f(batch),
            None => batch,
        };
        writer
            .write(&batch)
            .map_err(|source| PipelineIoError::WriteParquet { source })?;
        report = accumulate_batch_stats(report, &batch);
    }

    if cancel.is_cancelled() {
        return Err(PipelineError::Cancelled);
    }

    let file = writer
        .into_inner()
        .map_err(|source| PipelineIoError::WriteParquet { source })?;
    // Durability: the rename in `run_bounded_pipeline` only makes the staged
    // file visible atomically, it doesn't guarantee the file's bytes survive
    // a crash. fsync here forces them to disk before that rename happens.
    file.sync_all()
        .map_err(|source| PipelineIoError::OpenOutput {
            path: staging_path.to_path_buf(),
            source,
        })?;
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
/// `thread::scope` only returns once the producer thread has observed
/// `consume_batches`'s receiver drop (as a `SendError`, if it was blocked
/// on `send()`) and returned, so the rename/cleanup step below never races
/// a still-running producer.
///
/// # Errors
///
/// Returns [`PipelineError::Io`] for the same conditions as
/// [`crate::fixture_to_record_batch`], plus writer/staging I/O failures, or
/// [`PipelineError::Cancelled`] if `cancel` was set before output could be
/// published.
///
/// # Panics
///
/// Panics if `config.batch_size` is zero.
pub fn run_bounded_pipeline(
    input: &Path,
    output: &Path,
    config: &PipelineConfig,
    transform: Option<&dyn Fn(RecordBatch) -> RecordBatch>,
    cancel: &CancellationToken,
) -> Result<PipelineReport, PipelineError> {
    assert!(
        config.batch_size > 0,
        "config.batch_size must be greater than zero"
    );
    let (sender, receiver) =
        sync_channel::<Result<RecordBatch, PipelineIoError>>(config.channel_capacity);
    let staging_path = staging_path_for(output);

    let result = thread::scope(|scope| {
        scope.spawn(|| produce_batches(input, config.batch_size, cancel, sender));
        consume_batches(
            receiver,
            &staging_path,
            config.compression,
            transform,
            cancel,
        )
    });

    match result {
        Ok(report) => {
            std::fs::rename(&staging_path, output).map_err(|source| {
                PipelineIoError::OpenOutput {
                    path: output.to_path_buf(),
                    source,
                }
            })?;
            // Durability: fsync the directory entry so the rename itself
            // survives a crash, not just the file contents synced above.
            if let Some(parent) = output
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                let dir = File::open(parent).map_err(|source| PipelineIoError::OpenOutput {
                    path: parent.to_path_buf(),
                    source,
                })?;
                dir.sync_all()
                    .map_err(|source| PipelineIoError::OpenOutput {
                        path: parent.to_path_buf(),
                        source,
                    })?;
            }
            Ok(report)
        }
        Err(err) => {
            let _ = std::fs::remove_file(&staging_path);
            Err(err)
        }
    }
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
///
/// # Panics
///
/// Panics if `config.batch_size` is zero (same as [`run_bounded_pipeline`]).
pub fn run_bounded_pipeline_s3(
    store: &dyn ObjectStore,
    input_key: &ObjectPath,
    output_key: &ObjectPath,
    config: &PipelineConfig,
    transform: Option<&dyn Fn(RecordBatch) -> RecordBatch>,
    cancel: &CancellationToken,
) -> Result<PipelineReport, PipelineError> {
    let staging_dir = tempfile::tempdir().map_err(|source| PipelineIoError::OpenOutput {
        path: std::env::temp_dir(),
        source,
    })?;
    let local_input = staging_dir.path().join("input.csv");
    let local_output = staging_dir.path().join("output.parquet");

    let input_meta = head_object(store, input_key)?;
    download_to_temp(store, input_key, &local_input)?;
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
        let batch = crate::batch_from_rows(&[row]).expect("single-row batch should build");
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
            batch_size: 250,
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
            None,
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
            None,
            &CancellationToken::new(),
        )
        .expect_err("staging file creation should fail when the parent directory is missing");

        assert!(matches!(
            err,
            PipelineError::Io(PipelineIoError::OpenOutput { .. })
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

    #[test]
    #[should_panic(expected = "config.batch_size must be greater than zero")]
    fn zero_batch_size_panics_instead_of_silently_buffering_everything() {
        let dir = tempfile::tempdir().expect("temp dir should be creatable");
        let output = dir.path().join("out.parquet");
        let config = PipelineConfig {
            batch_size: 0,
            ..PipelineConfig::default()
        };

        let _ = run_bounded_pipeline(
            &fixture_path("headers.csv"),
            &output,
            &config,
            None,
            &CancellationToken::new(),
        );
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
            serde_json::from_slice(&manifest_bytes).expect("manifest should be valid JSON");

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
    fn s3_pipeline_reports_head_object_when_the_input_key_is_missing_on_a_real_backend() {
        use object_store::path::Path as ObjectPath;

        let minio = crate::minio_test_support::MinioContainer::start(
            "s3-pipeline-reports-head-object-when-the-input-key-is-missing",
        );
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
            PipelineError::Io(PipelineIoError::HeadObject { .. })
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
