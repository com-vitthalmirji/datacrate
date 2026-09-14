//! M4 pulled forward (`decisions.md`, 2026-09-13): the minimal object-store
//! edge the M2 smoke test needs. `object_store::ObjectStore`'s API is
//! `async fn`-only, so each function here owns a short-lived, single-thread
//! Tokio runtime just for its own `block_on` — no async spreads past this
//! module. [`crate::bounded::run_bounded_pipeline`] itself stays fully
//! synchronous.
//!
//! This only stays sound as long as nothing calls into this module from
//! inside an already-running Tokio runtime: nesting `block_on` inside async
//! code is the "async-blocking-async sandwich" Tokio's own docs warn
//! against, and it panics deep inside Tokio's internals rather than at this
//! module's boundary. `block_on` below asserts that invariant explicitly so
//! a violation fails immediately and points at the fix, instead of surfacing
//! as an unexplained panic somewhere else. `decisions.md`'s 2026-09-14 entry
//! has the M3 migration plan (switch to `tokio::task::spawn_blocking` once
//! this module is reached from async code).

use std::fs::File;
use std::io::Write;
use std::path::Path;

use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use crate::PipelineIoError;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
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

/// Downloads `key` from `store` and writes it to `dest`, overwriting any
/// existing file.
///
/// # Errors
///
/// Returns [`PipelineIoError::DownloadObject`] if `key` cannot be read from
/// `store`, or [`PipelineIoError::OpenOutput`] if `dest` cannot be created.
pub fn download_to_temp(
    store: &dyn ObjectStore,
    key: &ObjectPath,
    dest: &Path,
) -> Result<(), PipelineIoError> {
    let bytes = block_on(async {
        let result = store.get(key).await?;
        result.bytes().await
    })
    .map_err(|source| PipelineIoError::DownloadObject {
        key: key.clone(),
        source,
    })?;

    let mut file = File::create(dest).map_err(|source| PipelineIoError::OpenOutput {
        path: dest.to_path_buf(),
        source,
    })?;
    file.write_all(&bytes)
        .map_err(|source| PipelineIoError::OpenOutput {
            path: dest.to_path_buf(),
            source,
        })
}

/// Uploads `src` to `key` in `store`.
///
/// # Errors
///
/// Returns [`PipelineIoError::OpenInput`] if `src` cannot be read, or
/// [`PipelineIoError::UploadObject`] if the upload to `store` fails.
pub fn upload_from_temp(
    store: &dyn ObjectStore,
    src: &Path,
    key: &ObjectPath,
) -> Result<(), PipelineIoError> {
    let bytes = std::fs::read(src).map_err(|source| PipelineIoError::OpenInput {
        path: src.to_path_buf(),
        source,
    })?;

    block_on(store.put(key, PutPayload::from(bytes)))
        .map_err(|source| PipelineIoError::UploadObject {
            key: key.clone(),
            source,
        })
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
