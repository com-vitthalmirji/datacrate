//! The minimal object-store edge the M2 smoke test needs, pulled forward
//! from M4. `object_store::ObjectStore`'s API is `async fn`-only, so each
//! function here owns a short-lived, single-thread Tokio runtime just for
//! its own `block_on` — no async spreads past this module.
//! [`crate::bounded::run_bounded_pipeline`] itself stays fully synchronous.
//!
//! This only stays sound as long as nothing calls into this module from
//! inside an already-running Tokio runtime: nesting `block_on` inside async
//! code is the "async-blocking-async sandwich" Tokio's own docs warn
//! against, and it panics deep inside Tokio's internals rather than at this
//! module's boundary. `block_on` below asserts that invariant explicitly so
//! a violation fails immediately and points at the fix, instead of surfacing
//! as an unexplained panic somewhere else. The migration plan is to switch
//! to `tokio::task::spawn_blocking` once this module is reached from async
//! code.

use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use futures_util::{StreamExt, stream::FuturesUnordered};
use object_store::path::Path as ObjectPath;
use object_store::{GetOptions, ObjectMeta, ObjectStore, ObjectStoreExt};

use crate::PipelineIoError;
use crate::manifest::CompletionManifest;

/// File read size and multipart part size. Above the 5 MiB S3 part minimum.
const CHUNK_BYTES: usize = 8 * 1024 * 1024;
/// Limits scheduled upload parts independently of the pipeline batch channel.
const MAX_UPLOAD_PARTS: usize = 2;

pub(crate) fn block_on<F: std::future::Future>(future: F) -> F::Output {
    assert!(
        tokio::runtime::Handle::try_current().is_err(),
        "object_store_io::block_on called from inside an existing Tokio \
         runtime — this is the async-blocking-async sandwich and would \
         panic or deadlock. Once a caller runs inside async code, replace \
         this call with tokio::task::spawn_blocking instead."
    );
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread tokio runtime should build")
        .block_on(future)
}

/// A conditional download response did not match the captured object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotViolation {
    /// The backend did not supply a usable version or ETag.
    MissingIdentity,
    /// Metadata named a different object.
    Location,
    /// The returned version or ETag differs from the captured identity.
    Identity,
    /// The backend returned a different object size.
    Size {
        /// Captured size.
        expected: u64,
        /// Returned size.
        actual: u64,
    },
    /// The backend returned a different range.
    Range {
        /// Requested range.
        expected: std::ops::Range<u64>,
        /// Returned range.
        actual: std::ops::Range<u64>,
    },
    /// The response body length differs from the requested range length.
    Length {
        /// Requested length.
        expected: u64,
        /// Received length.
        actual: u64,
    },
}

impl std::fmt::Display for SnapshotViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingIdentity => f.write_str("no object version or ETag is available"),
            Self::Location => f.write_str("response names a different object"),
            Self::Identity => f.write_str("response identity differs from the captured identity"),
            Self::Size { expected, actual } => {
                write!(f, "expected object size {expected}, got {actual}")
            }
            Self::Range { expected, actual } => {
                write!(f, "expected range {expected:?}, got {actual:?}")
            }
            Self::Length { expected, actual } => {
                write!(f, "expected {expected} response bytes, got {actual}")
            }
        }
    }
}
impl std::error::Error for SnapshotViolation {}

/// Downloads `key` from `store` and writes it to `dest`, overwriting any
/// existing file. Returns the metadata captured once before reading. Every
/// range is bound to its version, or its ETag when no version is available;
/// unsupported conditions are errors, never unconditional retries.
/// Streams the object in `CHUNK_BYTES`-sized ranges rather
/// than materializing it whole in memory, so peak RSS stays flat as the
/// object grows — a whole-file `get`/`bytes()` here was measured to scale
/// peak memory linearly with input size. See
/// docs/adr/0005.1-stream-object-store-io-in-chunks.md.
///
/// # Errors
///
/// Returns [`PipelineIoError::DownloadObject`] if `key`'s metadata or any
/// range of it cannot be read from `store`, or [`PipelineIoError::OpenOutput`]
/// if `dest` cannot be created or written to. Missing identity or invalid
/// responses return [`PipelineIoError::InvalidSnapshot`]; a failed read
/// condition returns [`PipelineIoError::SnapshotPrecondition`]. A failed
/// download may leave a partial destination and must not be treated as input.
pub fn download_to_temp(
    store: &dyn ObjectStore,
    key: &ObjectPath,
    dest: &Path,
) -> Result<ObjectMeta, PipelineIoError> {
    let mut file = File::create(dest).map_err(|source| PipelineIoError::OpenOutput {
        path: dest.to_path_buf(),
        source,
    })?;

    block_on(async {
        let captured = store
            .head(key)
            .await
            .map_err(|source| PipelineIoError::DownloadObject {
                key: key.clone(),
                source,
            })?;
        download_captured(store, key, dest, &mut file, captured).await
    })
}

// This boundary shares the range validation with captured-input downloads. The
// caller supplies the metadata observed under the intended version/condition.
async fn download_captured(
    store: &dyn ObjectStore,
    key: &ObjectPath,
    dest: &Path,
    file: &mut File,
    captured: ObjectMeta,
) -> Result<ObjectMeta, PipelineIoError> {
    let invalid = |violation| PipelineIoError::InvalidSnapshot {
        key: key.clone(),
        violation,
    };
    if captured.location != *key {
        return Err(invalid(SnapshotViolation::Location));
    }
    let version = captured.version.as_ref().filter(|value| !value.is_empty());
    let etag = captured.e_tag.as_ref().filter(|value| !value.is_empty());
    if version.is_none() && etag.is_none() {
        return Err(invalid(SnapshotViolation::MissingIdentity));
    }
    let mut offset = 0u64;
    while offset < captured.size {
        let end = captured.size.min(offset.saturating_add(CHUNK_BYTES as u64));
        let requested = offset..end;
        let options = GetOptions {
            range: Some(requested.clone().into()),
            version: version.cloned(),
            if_match: if version.is_none() {
                etag.cloned()
            } else {
                None
            },
            ..Default::default()
        };
        let response = store.get_opts(key, options).await.map_err(|source| {
            if matches!(
                source,
                object_store::Error::Precondition { .. } | object_store::Error::NotModified { .. }
            ) {
                PipelineIoError::SnapshotPrecondition {
                    key: key.clone(),
                    source,
                }
            } else {
                PipelineIoError::DownloadObject {
                    key: key.clone(),
                    source,
                }
            }
        })?;
        if response.meta.location != *key {
            return Err(invalid(SnapshotViolation::Location));
        }
        if response.meta.size != captured.size {
            return Err(invalid(SnapshotViolation::Size {
                expected: captured.size,
                actual: response.meta.size,
            }));
        }
        let identity_matches = match version {
            Some(version) => response.meta.version.as_ref() == Some(version),
            None => response.meta.e_tag.as_ref() == etag,
        };
        if !identity_matches {
            return Err(invalid(SnapshotViolation::Identity));
        }
        if response.range != requested {
            return Err(invalid(SnapshotViolation::Range {
                expected: requested,
                actual: response.range,
            }));
        }
        let expected = end - offset;
        let mut received = 0u64;
        let mut stream = response.into_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|source| PipelineIoError::DownloadObject {
                key: key.clone(),
                source,
            })?;
            received = received.saturating_add(chunk.len() as u64);
            if received > expected {
                return Err(invalid(SnapshotViolation::Length {
                    expected,
                    actual: received,
                }));
            }
            file.write_all(&chunk)
                .map_err(|source| PipelineIoError::OpenOutput {
                    path: dest.to_path_buf(),
                    source,
                })?;
        }
        if received != expected {
            return Err(invalid(SnapshotViolation::Length {
                expected,
                actual: received,
            }));
        }
        offset = end;
    }
    Ok(captured)
}

/// Reads a supported completion record and downloads its recorded output identity.
///
/// Version 1 remains decodable but is rejected here because it has no output
/// identity. Versioned reads use that recorded version even if latest has changed;
/// ETag reads fail on replacement. Backends must honor these conditions.
/// This verifies identity-bound bytes, not the logical row count or fingerprint.
///
/// # Errors
/// Returns manifest/version errors, CompletionIdentityRequired for version 1,
/// InvalidManifest for a different output key, or the normal conditional download
/// errors. A range failure can leave a partial destination; error is not completion.
pub fn download_completed_output(
    store: &dyn ObjectStore,
    output_key: &ObjectPath,
    dest: &Path,
) -> Result<CompletionManifest, PipelineIoError> {
    block_on(async {
        let manifest_key = CompletionManifest::key_for(output_key);
        let response =
            store
                .get(&manifest_key)
                .await
                .map_err(|source| PipelineIoError::DownloadObject {
                    key: manifest_key.clone(),
                    source,
                })?;
        let bytes = response
            .bytes()
            .await
            .map_err(|source| PipelineIoError::DownloadObject {
                key: manifest_key,
                source,
            })?;
        let manifest = crate::manifest::decode_completion_manifest(&bytes)?;
        if manifest.schema_version != 2 {
            return Err(PipelineIoError::CompletionIdentityRequired {
                version: manifest.schema_version,
            });
        }
        let requested_key = output_key.to_string();
        if manifest.output_key.as_deref() != Some(requested_key.as_str()) {
            return Err(PipelineIoError::InvalidManifest {
                violation: crate::manifest::ManifestViolation::OutputKeyMismatch,
            });
        }
        let version = manifest.output_version.as_ref();
        let etag = manifest.output_e_tag.as_ref();
        let captured = store
            .get_opts(
                output_key,
                GetOptions {
                    head: true,
                    version: version.cloned(),
                    if_match: if version.is_none() {
                        etag.cloned()
                    } else {
                        None
                    },
                    ..Default::default()
                },
            )
            .await
            .map_err(|source| match source {
                source @ (object_store::Error::Precondition { .. }
                | object_store::Error::NotModified { .. }) => {
                    PipelineIoError::SnapshotPrecondition {
                        key: output_key.clone(),
                        source,
                    }
                }
                source => PipelineIoError::DownloadObject {
                    key: output_key.clone(),
                    source,
                },
            })?
            .meta;
        let identity_matches = match version {
            Some(version) => captured.version.as_ref() == Some(version),
            None => captured.e_tag.as_ref() == etag,
        };
        if !identity_matches {
            return Err(PipelineIoError::InvalidSnapshot {
                key: output_key.clone(),
                violation: SnapshotViolation::Identity,
            });
        }
        let mut file = File::create(dest).map_err(|source| PipelineIoError::OpenOutput {
            path: dest.to_owned(),
            source,
        })?;
        download_captured(store, output_key, dest, &mut file, captured).await?;
        Ok(manifest)
    })
}

/// Uploads `src` to `key` in `store`. Streams the local file in
/// `CHUNK_BYTES`-sized reads through a multipart upload rather than reading
/// it whole into memory first, for the same flat-peak-RSS reason as
/// [`download_to_temp`].
///
/// # Errors
///
/// Returns [`PipelineIoError::OpenInput`] if `src` cannot be opened or read,
/// or [`PipelineIoError::UploadObject`] if the multipart upload to `store`
/// fails. If explicit abort also fails, [`PipelineIoError::MultipartAbort`]
/// retains both the original failure and the abort failure.
pub fn upload_from_temp(
    store: &dyn ObjectStore,
    src: &Path,
    key: &ObjectPath,
) -> Result<(), PipelineIoError> {
    let mut file = File::open(src).map_err(|source| PipelineIoError::OpenInput {
        path: src.to_path_buf(),
        source,
    })?;

    block_on(async {
        let upload =
            store
                .put_multipart(key)
                .await
                .map_err(|source| PipelineIoError::UploadObject {
                    key: key.clone(),
                    source,
                })?;
        write_multipart(&mut file, src, key, upload).await
    })
}

/// Streams aligned parts, retaining the upload until completion or explicit abort.
async fn write_multipart(
    reader: &mut impl Read,
    src: &Path,
    key: &ObjectPath,
    mut upload: Box<dyn object_store::MultipartUpload>,
) -> Result<(), PipelineIoError> {
    let mut parts: FuturesUnordered<object_store::UploadPart> = FuturesUnordered::new();
    let upload_error = |source| PipelineIoError::UploadObject {
        key: key.clone(),
        source,
    };
    let result = async {
        loop {
            if parts.len() == MAX_UPLOAD_PARTS
                && let Some(result) = parts.next().await
            {
                result.map_err(&upload_error)?;
            }
            // Short reads must not become undersized non-final S3 parts.
            let mut buffer = vec![0; CHUNK_BYTES];
            let mut filled = 0;
            while filled < buffer.len() {
                match reader.read(&mut buffer[filled..]) {
                    Ok(0) => break,
                    Ok(read) => filled += read,
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(source) => {
                        return Err(PipelineIoError::OpenInput {
                            path: src.to_path_buf(),
                            source,
                        });
                    }
                }
            }
            if filled != 0 {
                buffer.truncate(filled);
                parts.push(upload.put_part(buffer.into()));
            }
            if filled < CHUNK_BYTES {
                break;
            }
        }
        while let Some(result) = parts.next().await {
            result.map_err(&upload_error)?;
        }
        upload.complete().await.map_err(&upload_error)?;
        Ok(())
    }
    .await;
    // Drop outstanding part futures before aborting their owning upload.
    drop(parts);
    match result {
        Ok(()) => Ok(()),
        Err(source) => match upload.abort().await {
            Ok(()) => Err(source),
            Err(abort_source) => Err(PipelineIoError::MultipartAbort {
                source: Box::new(source),
                abort_source,
            }),
        },
    }
}

/// Reads `key`'s metadata from `store`, without downloading its contents —
/// the input snapshot recorded in a [`CompletionManifest`].
///
/// # Errors
///
/// Returns [`PipelineIoError::HeadObject`] if `key`'s metadata cannot be read.
pub fn head_object(
    store: &dyn ObjectStore,
    key: &ObjectPath,
) -> Result<ObjectMeta, PipelineIoError> {
    block_on(store.head(key)).map_err(|source| PipelineIoError::HeadObject {
        key: key.clone(),
        source,
    })
}

/// Serializes `manifest` to JSON and writes it to `key` in `store`.
///
/// # Errors
///
/// Returns [`PipelineIoError::WriteManifest`] if `manifest` cannot be
/// serialized, or [`PipelineIoError::UploadObject`] if the write to `store`
/// fails.
pub fn put_manifest(
    store: &dyn ObjectStore,
    manifest: &CompletionManifest,
    key: &ObjectPath,
) -> Result<(), PipelineIoError> {
    manifest.validate()?;
    let body =
        serde_json::to_vec(manifest).map_err(|source| PipelineIoError::WriteManifest { source })?;
    block_on(store.put(key, body.into())).map_err(|source| PipelineIoError::UploadObject {
        key: key.clone(),
        source,
    })?;
    Ok(())
}

pub(crate) fn delete_object(
    store: &dyn ObjectStore,
    key: &ObjectPath,
) -> Result<(), PipelineIoError> {
    match block_on(store.delete(key)) {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(source) => Err(PipelineIoError::DeleteObject {
            key: key.clone(),
            source,
        }),
    }
}

pub(crate) fn copy_staged_object(
    store: &dyn ObjectStore,
    from: &ObjectPath,
    to: &ObjectPath,
) -> Result<(), PipelineIoError> {
    block_on(store.copy(from, to)).map_err(|source| PipelineIoError::UploadObject {
        key: to.clone(),
        source,
    })
}

/// Legacy copy-and-delete publication helper, without a completion protocol.
///
/// Backend rename may be implemented as separate copy and delete operations.
/// An error can follow successful final publication. The bounded pipeline instead
/// uses explicit copy, identity-bound completion and owned cleanup phases.
///
/// # Errors
/// Returns [`PipelineIoError::UploadObject`] when backend rename reports failure;
/// this error alone does not establish whether final output became visible.
pub fn publish_staged_object(
    store: &dyn ObjectStore,
    from: &ObjectPath,
    to: &ObjectPath,
) -> Result<(), PipelineIoError> {
    block_on(store.rename(from, to)).map_err(|source| PipelineIoError::UploadObject {
        key: to.clone(),
        source,
    })
}

/// Deletes `key` from `store`, ignoring the result. For cleanup after a
/// failure that has already been reported — a missing object at this point
/// isn't a new error worth surfacing.
pub fn best_effort_delete(store: &dyn ObjectStore, key: &ObjectPath) {
    let _ = block_on(store.delete(key));
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::PutPayload;
    use object_store::memory::InMemory;

    #[test]
    fn round_trips_bytes_through_the_store_and_a_local_temp_file() {
        let store = InMemory::new();
        let key = ObjectPath::from("input.csv");
        block_on(store.put(&key, PutPayload::from(b"id,name,note\n1,Ada,\n".to_vec())))
            .expect("seeding the in-memory store should succeed");

        let dir = tempfile::tempdir().expect("temp dir should be creatable");
        let downloaded = dir.path().join("downloaded.csv");
        download_to_temp(&store, &key, &downloaded).expect("download should succeed");
        let contents = std::fs::read_to_string(&downloaded).expect("temp file should be readable");
        assert_eq!(contents, "id,name,note\n1,Ada,\n");

        let output_key = ObjectPath::from("output.csv");
        upload_from_temp(&store, &downloaded, &output_key).expect("upload should succeed");

        let round_tripped = block_on(async {
            store
                .get(&output_key)
                .await
                .expect("uploaded object should be readable")
                .bytes()
                .await
                .expect("uploaded object bytes should be readable")
        });
        assert_eq!(round_tripped.as_ref(), contents.as_bytes());
    }

    #[test]
    fn download_of_a_missing_key_reports_download_object_not_a_panic() {
        let store = InMemory::new();
        let key = ObjectPath::from("missing.csv");
        let dir = tempfile::tempdir().expect("temp dir should be creatable");
        let dest = dir.path().join("dest.csv");

        let err =
            download_to_temp(&store, &key, &dest).expect_err("a missing key must fail, not panic");
        assert!(matches!(err, PipelineIoError::DownloadObject { .. }));
    }

    #[tokio::test]
    #[should_panic(expected = "async-blocking-async sandwich")]
    async fn block_on_from_inside_a_running_runtime_panics_immediately() {
        let store = InMemory::new();
        let key = ObjectPath::from("input.csv");
        let dir = tempfile::tempdir().expect("temp dir should be creatable");
        let dest = dir.path().join("dest.csv");

        let _ = download_to_temp(&store, &key, &dest);
    }

    #[test]
    fn download_of_a_missing_key_against_a_real_backend_reports_download_object() {
        let minio = crate::minio_test_support::MinioContainer::start(
            "download-of-a-missing-key-against-a-real-backend",
        );
        let store = minio.store();
        let key = ObjectPath::from("missing.csv");
        let dir = tempfile::tempdir().expect("temp dir should be creatable");
        let dest = dir.path().join("dest.csv");

        let err = download_to_temp(store.as_ref(), &key, &dest)
            .expect_err("a missing key on a real S3-compatible backend must fail, not panic");
        assert!(matches!(err, PipelineIoError::DownloadObject { .. }));
    }

    #[test]
    fn download_with_invalid_credentials_against_a_real_backend_reports_download_object() {
        let minio = crate::minio_test_support::MinioContainer::start(
            "download-with-invalid-credentials-against-a-real-backend",
        );
        block_on(minio.store().put(
            &ObjectPath::from("input.csv"),
            PutPayload::from(b"id,name,note\n1,Ada,\n".to_vec()),
        ))
        .expect("seeding the real backend should succeed with valid credentials");

        let store = minio.store_with_credentials("minioadmin", "wrong-secret-key");
        let key = ObjectPath::from("input.csv");
        let dir = tempfile::tempdir().expect("temp dir should be creatable");
        let dest = dir.path().join("dest.csv");

        let err = download_to_temp(store.as_ref(), &key, &dest)
            .expect_err("wrong credentials against a real backend must fail, not panic");
        assert!(matches!(err, PipelineIoError::DownloadObject { .. }));
    }

    #[test]
    fn download_with_a_near_zero_timeout_against_a_real_backend_reports_download_object() {
        let minio = crate::minio_test_support::MinioContainer::start(
            "download-with-a-near-zero-timeout-against-a-real-backend",
        );
        block_on(minio.store().put(
            &ObjectPath::from("input.csv"),
            PutPayload::from(b"id,name,note\n1,Ada,\n".to_vec()),
        ))
        .expect("seeding the real backend should succeed with a normal store");

        let store = minio.store_with_timeout(std::time::Duration::from_nanos(1));
        let key = ObjectPath::from("input.csv");
        let dir = tempfile::tempdir().expect("temp dir should be creatable");
        let dest = dir.path().join("dest.csv");

        let err = download_to_temp(store.as_ref(), &key, &dest)
            .expect_err("a near-zero timeout with retries exhausted must fail, not panic or hang");
        assert!(matches!(err, PipelineIoError::DownloadObject { .. }));
    }

    #[test]
    fn a_parquet_object_written_with_a_drifted_schema_is_rejected_after_a_real_download() {
        use arrow::array::{ArrayRef, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::basic::Compression;
        use std::sync::Arc;

        // A schema with only two of the three columns the pipeline expects —
        // the kind of drift a real upstream writer could introduce between
        // when an object was written and when this pipeline reads it back.
        let drifted_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["Ada"])),
        ];
        let drifted_batch = RecordBatch::try_new(drifted_schema, columns)
            .expect("a batch matching its own schema should build");

        let local_dir = tempfile::tempdir().expect("temp dir should be creatable");
        let local_drifted = local_dir.path().join("drifted.parquet");
        crate::write_parquet(&drifted_batch, &local_drifted, Compression::SNAPPY)
            .expect("writing the drifted batch locally should succeed");

        let minio = crate::minio_test_support::MinioContainer::start(
            "a-parquet-object-written-with-a-drifted-schema",
        );
        let store = minio.store();
        let key = ObjectPath::from("drifted.parquet");
        upload_from_temp(store.as_ref(), &local_drifted, &key)
            .expect("uploading the drifted parquet to a real backend should succeed");

        let downloaded = local_dir.path().join("downloaded.parquet");
        download_to_temp(store.as_ref(), &key, &downloaded)
            .expect("downloading the drifted parquet from a real backend should succeed");

        let err = crate::read_parquet(&downloaded)
            .expect_err("a schema-drifted object must be rejected, not silently coerced");
        assert!(matches!(err, PipelineIoError::BuildBatch { .. }));
    }
}

#[cfg(test)]
mod multipart_regressions {
    use super::*;
    use object_store::{MultipartUpload, PutPayload, PutResult, UploadPart};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    #[derive(Debug, Default)]
    struct State {
        active: AtomicUsize,
        lengths: std::sync::Mutex<Vec<usize>>,
        peak: AtomicUsize,
        bytes: AtomicUsize,
        valid_bytes: AtomicBool,
        complete_called: AtomicBool,
        aborted: AtomicBool,
        fail_part: bool,
        fail_complete: bool,
        fail_abort: bool,
    }
    #[derive(Debug)]
    struct Upload(Arc<State>);
    fn injected() -> object_store::Error {
        object_store::Error::Generic {
            store: "regression",
            source: std::io::Error::other("injected failure").into(),
        }
    }
    #[async_trait::async_trait]
    impl MultipartUpload for Upload {
        fn put_part(&mut self, data: PutPayload) -> UploadPart {
            let state = Arc::clone(&self.0);
            state
                .lengths
                .lock()
                .expect("lengths")
                .push(data.content_length());
            let active = state.active.fetch_add(1, Ordering::SeqCst) + 1;
            state.peak.fetch_max(active, Ordering::SeqCst);
            state.bytes.fetch_add(
                data.iter().map(|bytes| bytes.len()).sum::<usize>(),
                Ordering::SeqCst,
            );
            if !data.iter().all(|chunk| chunk.iter().all(|byte| *byte == 7)) {
                state.valid_bytes.store(false, Ordering::SeqCst);
            }
            Box::pin(async move {
                // Keep scheduled parts pending until the production loop yields.
                tokio::task::yield_now().await;
                drop(data);
                state.active.fetch_sub(1, Ordering::SeqCst);
                if state.fail_part {
                    Err(injected())
                } else {
                    Ok(())
                }
            })
        }
        async fn complete(&mut self) -> object_store::Result<PutResult> {
            self.0.complete_called.store(true, Ordering::SeqCst);
            if self.0.fail_complete {
                return Err(injected());
            }
            assert_eq!(self.0.active.load(Ordering::SeqCst), 0);
            Ok(PutResult {
                e_tag: None,
                version: None,
            })
        }
        async fn abort(&mut self) -> object_store::Result<()> {
            self.0.aborted.store(true, Ordering::SeqCst);
            if self.0.fail_abort {
                return Err(object_store::Error::Generic {
                    store: "abort",
                    source: std::io::Error::other("abort failed").into(),
                });
            }
            Ok(())
        }
    }
    #[derive(Debug)]
    struct GatedUpload {
        next: usize,
        submitted: std::sync::mpsc::Sender<(usize, tokio::sync::oneshot::Sender<()>)>,
        completed: Arc<Vec<AtomicBool>>,
        lengths: Arc<std::sync::Mutex<Vec<usize>>>,
    }

    #[async_trait::async_trait]
    impl MultipartUpload for GatedUpload {
        fn put_part(&mut self, payload: PutPayload) -> UploadPart {
            let index = self.next;
            self.next += 1;
            let (permit, received) = tokio::sync::oneshot::channel();
            self.lengths
                .lock()
                .expect("lengths")
                .push(payload.content_length());
            self.submitted.send((index, permit)).expect("observer");
            let completed = Arc::clone(&self.completed);
            Box::pin(async move {
                received.await.map_err(|_| injected())?;
                drop(payload);
                completed[index].store(true, Ordering::SeqCst);
                Ok(())
            })
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            assert!(
                self.completed
                    .iter()
                    .all(|part| part.load(Ordering::SeqCst))
            );
            Ok(PutResult {
                e_tag: None,
                version: None,
            })
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn multipart_progress_refills_before_straggler_finishes() {
        use std::time::Duration;
        let (submitted, observed) = std::sync::mpsc::channel();
        let completed = Arc::new((0..4).map(|_| AtomicBool::new(false)).collect::<Vec<_>>());
        let lengths = Arc::new(std::sync::Mutex::new(Vec::new()));
        let upload = GatedUpload {
            next: 0,
            submitted,
            completed: Arc::clone(&completed),
            lengths: Arc::clone(&lengths),
        };
        let (finished, result) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let mut reader = std::io::repeat(7).take((CHUNK_BYTES * 3 + 17) as u64);
            finished
                .send(block_on(write_multipart(
                    &mut reader,
                    Path::new("input"),
                    &ObjectPath::from("output"),
                    Box::new(upload),
                )))
                .expect("result observer");
        });
        let next = || {
            observed
                .recv_timeout(Duration::from_secs(5))
                .expect("next part submission")
        };
        let (first, hold) = next();
        let (second, release) = next();
        assert_eq!((first, second), (0, 1));
        release.send(()).expect("release second");
        let (third, release) = next();
        assert_eq!(third, 2);
        assert!(
            !completed[0].load(Ordering::SeqCst),
            "a free slot must refill with the first part still held"
        );
        release.send(()).expect("release third");
        let (last, release) = next();
        assert_eq!(last, 3);
        assert!(!completed[0].load(Ordering::SeqCst));
        release.send(()).expect("release partial final part");
        hold.send(()).expect("release straggler");
        result
            .recv_timeout(Duration::from_secs(5))
            .expect("upload completion")
            .expect("upload");
        worker.join().expect("upload thread");
        assert_eq!(
            *lengths.lock().expect("lengths"),
            [CHUNK_BYTES, CHUNK_BYTES, CHUNK_BYTES, 17]
        );
    }

    #[test]
    fn outstanding_parts_and_final_partial_part_stay_bounded() {
        for size in [0, CHUNK_BYTES / 2, CHUNK_BYTES * 3, CHUNK_BYTES * 3 + 17] {
            let state = Arc::new(State {
                valid_bytes: AtomicBool::new(true),
                ..State::default()
            });
            let mut reader = std::io::repeat(7).take(size as u64);
            block_on(write_multipart(
                &mut reader,
                Path::new("input"),
                &ObjectPath::from("output"),
                Box::new(Upload(Arc::clone(&state))),
            ))
            .expect("bounded upload");
            assert_eq!(state.bytes.load(Ordering::SeqCst), size);
            assert!(state.valid_bytes.load(Ordering::SeqCst));
            assert!(state.peak.load(Ordering::SeqCst) <= 2);
            if size > CHUNK_BYTES * 2 {
                assert_eq!(state.peak.load(Ordering::SeqCst), 2);
            }
            assert!(state.complete_called.load(Ordering::SeqCst));
            assert!(!state.aborted.load(Ordering::SeqCst));
        }
    }
    #[test]
    fn multipart_short_reads_form_aligned_parts() {
        let state = Arc::new(State {
            valid_bytes: AtomicBool::new(true),
            ..State::default()
        });
        let bytes = vec![7; CHUNK_BYTES * 2 + 17];
        // Chain forces an initial short read despite ample input remaining.
        let mut reader = bytes[..13].chain(&bytes[13..]);
        block_on(write_multipart(
            &mut reader,
            Path::new("input"),
            &ObjectPath::from("output"),
            Box::new(Upload(Arc::clone(&state))),
        ))
        .expect("short reads");
        assert_eq!(
            *state.lengths.lock().expect("lengths"),
            [CHUNK_BYTES, CHUNK_BYTES, 17]
        );
        assert_eq!(state.bytes.load(Ordering::SeqCst), bytes.len());
        assert_eq!(state.peak.load(Ordering::SeqCst), 2);
        assert!(state.valid_bytes.load(Ordering::SeqCst));
    }

    #[test]
    fn part_failures_preserve_upload_error_context() {
        let state = Arc::new(State {
            fail_part: true,
            ..State::default()
        });
        let mut reader = std::io::repeat(7).take((CHUNK_BYTES * 3) as u64);
        let key = ObjectPath::from("failed-part");
        let result = block_on(write_multipart(
            &mut reader,
            Path::new("input"),
            &key,
            Box::new(Upload(Arc::clone(&state))),
        ));
        assert!(
            matches!(result, Err(PipelineIoError::UploadObject { key: actual, .. }) if actual == key)
        );
        assert!(!state.complete_called.load(Ordering::SeqCst));
        assert!(state.aborted.load(Ordering::SeqCst));
    }
    #[test]
    fn completion_failure_is_reported_and_abort_is_attempted() {
        let state = Arc::new(State {
            fail_complete: true,
            ..State::default()
        });
        let key = ObjectPath::from("failed-completion");
        let result = block_on(write_multipart(
            &mut std::io::empty(),
            Path::new("input"),
            &key,
            Box::new(Upload(Arc::clone(&state))),
        ));
        assert!(
            matches!(result, Err(PipelineIoError::UploadObject { key: actual, .. }) if actual == key)
        );
        assert!(state.complete_called.load(Ordering::SeqCst));
        assert!(state.aborted.load(Ordering::SeqCst));
    }
    #[test]
    fn multipart_failed_abort_preserves_read_and_final_part_errors() {
        struct Broken;
        impl Read for Broken {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("read failed"))
            }
        }
        for fail_part in [false, true] {
            let state = Arc::new(State {
                fail_part,
                fail_abort: true,
                ..State::default()
            });
            let mut part_input = std::io::repeat(7).take(17);
            let reader: &mut dyn Read = if fail_part {
                &mut part_input
            } else {
                &mut Broken
            };
            let error = block_on(write_multipart(
                &mut { reader },
                Path::new("input"),
                &ObjectPath::from("output"),
                Box::new(Upload(Arc::clone(&state))),
            ))
            .expect_err("operation and abort fail");
            let PipelineIoError::MultipartAbort {
                source,
                abort_source,
            } = error
            else {
                panic!("both failures required");
            };
            if fail_part {
                assert!(matches!(*source, PipelineIoError::UploadObject { .. }));
            } else {
                assert!(matches!(*source, PipelineIoError::OpenInput { .. }));
            }
            assert!(abort_source.to_string().contains("abort failed"));
            assert!(!state.complete_called.load(Ordering::SeqCst));
            assert!(state.aborted.load(Ordering::SeqCst));
        }
    }

    #[test]
    fn multipart_abort_failure_retains_completion_failure() {
        let state = Arc::new(State {
            fail_complete: true,
            fail_abort: true,
            ..State::default()
        });
        let error = block_on(write_multipart(
            &mut std::io::empty(),
            Path::new("input"),
            &ObjectPath::from("output"),
            Box::new(Upload(Arc::clone(&state))),
        ))
        .expect_err("complete and abort fail");
        let display = error.to_string();
        assert!(
            display.contains("injected failure"),
            "primary missing: {display}"
        );
        assert!(display.contains("abort failed"), "abort missing: {display}");
        assert!(
            std::error::Error::source(&error)
                .expect("primary source")
                .to_string()
                .contains("injected failure")
        );
        assert!(state.aborted.load(Ordering::SeqCst));
    }

    #[test]
    fn local_read_failure_preserves_input_error_context() {
        struct Broken;
        impl Read for Broken {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("read failed"))
            }
        }
        let state = Arc::new(State::default());
        let result = block_on(write_multipart(
            &mut Broken,
            Path::new("broken-input"),
            &ObjectPath::from("output"),
            Box::new(Upload(Arc::clone(&state))),
        ));
        assert!(
            matches!(result, Err(PipelineIoError::OpenInput { path, .. }) if path == Path::new("broken-input"))
        );
        assert!(!state.complete_called.load(Ordering::SeqCst));
        assert!(state.aborted.load(Ordering::SeqCst));
    }
}

#[cfg(test)]
#[path = "object_store_io/snapshot_tests.rs"]
mod snapshot_tests;
