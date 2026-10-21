use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStoreExt, PutPayload};
use pipeline::bounded::{CancellationToken, PipelineConfig, run_bounded_pipeline_s3};
use pipeline::manifest::{CompletionManifest, decode_completion_manifest};

fn block_on<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(future)
}

#[test]
fn publication_phase_emits_identity_bound_version_two_completion() {
    let store = InMemory::new();
    let input = ObjectPath::from("input.csv");
    let output = ObjectPath::from("output.parquet");
    block_on(store.put(&input, PutPayload::from(b"id,name,note\n1,Ada,\n".to_vec())))
        .expect("seed input");
    let report = run_bounded_pipeline_s3(
        &store,
        &input,
        &output,
        &PipelineConfig::default(),
        None,
        &CancellationToken::new(),
    )
    .expect("publish");
    let bytes = block_on(async {
        store
            .get(&CompletionManifest::key_for(&output))
            .await?
            .bytes()
            .await
    })
    .expect("manifest");
    let manifest = decode_completion_manifest(&bytes).expect("supported completion");
    assert_eq!(manifest.schema_version, 2);
    assert_eq!(manifest.output_key.as_deref(), Some("output.parquet"));
    let meta = block_on(store.head(&output)).expect("published identity");
    assert_eq!(manifest.output_e_tag, meta.e_tag);
    assert_eq!(manifest.output_version, meta.version);
    assert!(manifest.run_id.is_some_and(|run| !run.is_empty()));
    assert_eq!(manifest.row_count, report.rows_written);
}

use futures_util::{StreamExt, TryStreamExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutResult,
};
use pipeline::PipelineIoError;
use pipeline::bounded::{
    PipelineError, RemotePublicationOutcome as Outcome, RemotePublicationPhase as Phase,
};
use pipeline::object_store_io::download_completed_output;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
struct FaultStore {
    inner: Arc<InMemory>,
    fault: Option<Phase>,
    after: bool,
    cleanup_also: bool,
    multipart_fails: bool,
    newer_output_on_failure: bool,
    missing_output_identity: bool,
    version_only: bool,
    replace_during_read: AtomicBool,
    events: Arc<Mutex<Vec<(Phase, ObjectPath)>>>,
    reads: Mutex<Vec<GetOptions>>,
}
impl std::fmt::Display for FaultStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("publication fault store")
    }
}
fn injected() -> object_store::Error {
    object_store::Error::Generic {
        store: "publication fault store",
        source: std::io::Error::other("injected operation failure").into(),
    }
}
fn output_key() -> ObjectPath {
    ObjectPath::from("output.parquet")
}
fn marker_key() -> ObjectPath {
    CompletionManifest::key_for(&output_key())
}

#[derive(Debug)]
struct FailedMultipart;
#[async_trait::async_trait]
impl MultipartUpload for FailedMultipart {
    fn put_part(&mut self, _: PutPayload) -> object_store::UploadPart {
        Box::pin(async { Err(injected()) })
    }
    async fn complete(&mut self) -> object_store::Result<PutResult> {
        panic!("failed part must prevent completion")
    }
    async fn abort(&mut self) -> object_store::Result<()> {
        Err(object_store::Error::Generic {
            store: "abort",
            source: std::io::Error::other("abort failed").into(),
        })
    }
}

#[test]
fn publication_phase_preserves_part_abort_and_staging_delete_failures() {
    let store = FaultStore {
        multipart_fails: true,
        cleanup_also: true,
        ..FaultStore::default()
    };
    seed(&store);
    let PipelineError::RemotePublication(error) = publish(&store).expect_err("upload fails") else {
        panic!("remote error required");
    };
    assert_eq!(error.phase, Phase::Upload);
    assert_eq!(error.outcome, Outcome::NotPublished);
    let PipelineIoError::MultipartAbort {
        source,
        abort_source,
    } = error.source
    else {
        panic!("primary and abort required");
    };
    assert!(matches!(*source, PipelineIoError::UploadObject { .. }));
    assert!(abort_source.to_string().contains("abort failed"));
    assert!(matches!(
        error.cleanup_error,
        Some(PipelineIoError::DeleteObject { .. })
    ));
}

#[async_trait::async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        key: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if *key == marker_key() {
            self.events
                .lock()
                .expect("events")
                .push((Phase::WriteCompletion, key.clone()));
            if self.fault == Some(Phase::WriteCompletion) {
                if self.newer_output_on_failure {
                    self.inner
                        .put(&output_key(), b"newer output".to_vec().into())
                        .await?;
                }
                if !self.after {
                    return Err(injected());
                }
            }
        }
        let result = self.inner.put_opts(key, payload, opts).await?;
        if *key == marker_key() && self.fault == Some(Phase::WriteCompletion) && self.after {
            return Err(injected());
        }
        Ok(result)
    }
    async fn put_multipart_opts(
        &self,
        key: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.events
            .lock()
            .expect("events")
            .push((Phase::Upload, key.clone()));
        if self.fault == Some(Phase::Upload) {
            return Err(injected());
        }
        if self.multipart_fails {
            return Ok(Box::new(FailedMultipart));
        }
        self.inner.put_multipart_opts(key, opts).await
    }
    async fn get_opts(
        &self,
        key: &ObjectPath,
        mut opts: GetOptions,
    ) -> object_store::Result<GetResult> {
        if *key == output_key() {
            self.reads.lock().expect("reads").push(opts.clone());
            if opts.head {
                self.events
                    .lock()
                    .expect("events")
                    .push((Phase::CaptureOutputIdentity, key.clone()));
                if self.fault == Some(Phase::CaptureOutputIdentity) {
                    return Err(injected());
                }
            } else if opts.range.is_some() && self.replace_during_read.swap(false, Ordering::SeqCst)
            {
                self.inner
                    .put(key, b"replacement during range read".to_vec().into())
                    .await?;
            }
        }
        if self.version_only && opts.version.is_some() {
            opts.if_match = opts.version.take();
        }
        let mut result = self.inner.get_opts(key, opts).await?;
        if self.version_only {
            result.meta.version = result.meta.e_tag.take();
        }
        if *key == output_key() && self.missing_output_identity {
            result.meta.version = None;
            result.meta.e_tag = None;
        }
        Ok(result)
    }
    fn delete_stream(
        &self,
        keys: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        let inner = Arc::clone(&self.inner);
        let events = Arc::clone(&self.events);
        let fault = self.fault;
        let after = self.after;
        let cleanup_also = self.cleanup_also;
        Box::pin(keys.then(move |key| {
            let inner = Arc::clone(&inner);
            let events = Arc::clone(&events);
            async move {
                let key = key?;
                let phase = if key == marker_key() {
                    Phase::InvalidateCompletion
                } else {
                    Phase::CleanupStaging
                };
                events.lock().expect("events").push((phase, key.clone()));
                let fail = fault == Some(phase) || (phase == Phase::CleanupStaging && cleanup_also);
                if fail && !after {
                    return Err(injected());
                }
                inner.delete(&key).await?;
                if fail {
                    return Err(injected());
                }
                Ok(key)
            }
        }))
    }
    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        opts: CopyOptions,
    ) -> object_store::Result<()> {
        self.events
            .lock()
            .expect("events")
            .push((Phase::PublishOutput, to.clone()));
        assert!(
            matches!(
                self.inner.head(&marker_key()).await,
                Err(object_store::Error::NotFound { .. })
            ),
            "old marker must be invalidated before final replacement"
        );
        if self.fault == Some(Phase::PublishOutput) && !self.after {
            return Err(injected());
        }
        self.inner.copy_opts(from, to, opts).await?;
        if self.fault == Some(Phase::PublishOutput) {
            return Err(injected());
        }
        Ok(())
    }
}

fn seed(store: &FaultStore) {
    block_on(store.inner.put(
        &ObjectPath::from("input.csv"),
        b"id,name,note\n1,Ada,\n".to_vec().into(),
    ))
    .expect("input");
}
fn publish(store: &dyn ObjectStore) -> Result<pipeline::bounded::PipelineReport, PipelineError> {
    run_bounded_pipeline_s3(
        store,
        &ObjectPath::from("input.csv"),
        &output_key(),
        &PipelineConfig::default(),
        None,
        &CancellationToken::new(),
    )
}
fn content(store: &dyn ObjectStore, key: &ObjectPath) -> Vec<u8> {
    block_on(async { store.get(key).await?.bytes().await })
        .expect("object bytes")
        .to_vec()
}

#[test]
fn publication_phase_orders_marker_invalidation_and_owned_cleanup() {
    let store = FaultStore::default();
    seed(&store);
    block_on(
        store
            .inner
            .put(&output_key(), b"old output".to_vec().into()),
    )
    .expect("old output");
    block_on(
        store
            .inner
            .put(&marker_key(), b"old completion".to_vec().into()),
    )
    .expect("old marker");
    publish(&store).expect("publication");
    let events = store.events.lock().expect("events");
    assert_eq!(
        events.iter().map(|event| event.0).collect::<Vec<_>>(),
        [
            Phase::Upload,
            Phase::InvalidateCompletion,
            Phase::PublishOutput,
            Phase::CaptureOutputIdentity,
            Phase::WriteCompletion,
            Phase::CleanupStaging
        ]
    );
    assert_eq!(events[0].1, events[5].1);
    assert_ne!(events[0].1, output_key());
    drop(events);
    let dir = tempfile::tempdir().expect("directory");
    let completion =
        download_completed_output(&store, &output_key(), &dir.path().join("out.parquet"))
            .expect("completed bytes");
    assert_eq!(completion.row_count, 1);
    assert_eq!(
        pipeline::read_parquet(&dir.path().join("out.parquet"))
            .expect("valid output")
            .num_rows(),
        1
    );
}

#[test]
fn publication_phase_preserves_primary_failure_and_recovery_outcome() {
    for (phase, outcome) in [
        (Phase::Upload, Outcome::NotPublished),
        (Phase::InvalidateCompletion, Outcome::NotPublished),
        (Phase::PublishOutput, Outcome::PublicationUnknown),
        (
            Phase::CaptureOutputIdentity,
            Outcome::PublishedWithoutCompletion,
        ),
        (Phase::WriteCompletion, Outcome::CompletionUnknown),
        (Phase::CleanupStaging, Outcome::CompletedCleanupUnknown),
    ] {
        for after in [false, true] {
            let store = FaultStore {
                fault: Some(phase),
                after,
                ..FaultStore::default()
            };
            seed(&store);
            block_on(
                store
                    .inner
                    .put(&output_key(), b"old output".to_vec().into()),
            )
            .expect("old output");
            block_on(
                store
                    .inner
                    .put(&marker_key(), b"old completion".to_vec().into()),
            )
            .expect("old marker");
            let error = publish(&store).expect_err("injected publication failure");
            let PipelineError::RemotePublication(error) = error else {
                panic!("missing recovery state");
            };
            assert_eq!((error.phase, error.outcome), (phase, outcome));
            assert_eq!(error.output_key, output_key());
            assert_eq!(error.manifest_key, marker_key());
            assert_ne!(error.staging_key, output_key());
            assert!(std::error::Error::source(error.as_ref()).is_some());
            if matches!(phase, Phase::Upload | Phase::InvalidateCompletion) {
                assert_eq!(content(store.inner.as_ref(), &output_key()), b"old output");
            }
            assert!(
                !store
                    .events
                    .lock()
                    .expect("events")
                    .iter()
                    .any(|(phase, key)| *phase == Phase::CleanupStaging && *key == output_key())
            );
        }
    }
}

#[test]
fn publication_phase_cleanup_failure_does_not_mask_primary_failure() {
    let store = FaultStore {
        fault: Some(Phase::WriteCompletion),
        cleanup_also: true,
        ..FaultStore::default()
    };
    seed(&store);
    let PipelineError::RemotePublication(error) =
        publish(&store).expect_err("manifest and cleanup failures")
    else {
        panic!("recovery state");
    };
    assert_eq!(error.phase, Phase::WriteCompletion);
    assert!(matches!(error.source, PipelineIoError::UploadObject { .. }));
    assert!(matches!(
        error.cleanup_error,
        Some(PipelineIoError::DeleteObject { .. })
    ));
    assert!(block_on(store.inner.head(&output_key())).is_ok());
}

#[test]
fn publication_phase_failed_older_run_never_deletes_newer_final_output() {
    // This checks cleanup restraint under an out-of-band overwrite; it does not
    // claim that the externally coordinated single-writer protocol is multiwriter.
    let store = FaultStore {
        fault: Some(Phase::WriteCompletion),
        newer_output_on_failure: true,
        ..FaultStore::default()
    };
    seed(&store);
    let error = publish(&store).expect_err("completion failure");
    assert!(matches!(error, PipelineError::RemotePublication(_)));
    assert_eq!(
        content(store.inner.as_ref(), &output_key()),
        b"newer output"
    );
}

#[test]
fn publication_phase_repeated_runs_own_different_staging_names() {
    let store = FaultStore::default();
    seed(&store);
    publish(&store).expect("first publication");
    publish(&store).expect("second publication");
    let events = store.events.lock().expect("events");
    let uploads = events
        .iter()
        .filter(|(phase, _)| *phase == Phase::Upload)
        .map(|(_, key)| key)
        .collect::<Vec<_>>();
    assert_eq!(uploads.len(), 2);
    assert_ne!(uploads[0], uploads[1]);
    drop(events);
    assert_eq!(
        block_on(store.list(None).try_collect::<Vec<_>>())
            .expect("remaining keys")
            .len(),
        3
    );
}

#[test]
fn publication_phase_missing_output_identity_prevents_completion() {
    let store = FaultStore {
        missing_output_identity: true,
        ..FaultStore::default()
    };
    seed(&store);
    let PipelineError::RemotePublication(error) = publish(&store).expect_err("missing identity")
    else {
        panic!("recovery state");
    };
    assert_eq!(
        (error.phase, error.outcome),
        (
            Phase::CaptureOutputIdentity,
            Outcome::PublishedWithoutCompletion
        )
    );
    assert!(matches!(
        error.source,
        PipelineIoError::InvalidSnapshot { .. }
    ));
    assert!(matches!(
        block_on(store.inner.head(&marker_key())),
        Err(object_store::Error::NotFound { .. })
    ));
}

#[test]
fn completed_reader_rejects_stale_etag_before_and_during_ranges() {
    for during in [false, true] {
        let store = FaultStore::default();
        seed(&store);
        publish(&store).expect("publish");
        if during {
            store.replace_during_read.store(true, Ordering::SeqCst);
        } else {
            block_on(
                store
                    .inner
                    .put(&output_key(), b"replacement".to_vec().into()),
            )
            .expect("replace");
        }
        let dir = tempfile::tempdir().expect("directory");
        let result = download_completed_output(&store, &output_key(), &dir.path().join("output"));
        assert!(
            matches!(result, Err(PipelineIoError::SnapshotPrecondition { .. })),
            "stale completion must not label replacement as success"
        );
    }
}

#[test]
fn completed_reader_forwards_version_only_identity_on_head_and_ranges() {
    let store = FaultStore {
        version_only: true,
        ..FaultStore::default()
    };
    seed(&store);
    publish(&store).expect("publish");
    store.reads.lock().expect("reads").clear();
    let dir = tempfile::tempdir().expect("directory");
    let completion = download_completed_output(&store, &output_key(), &dir.path().join("output"))
        .expect("version-bound read");
    assert!(completion.output_e_tag.is_none());
    let requests = store.reads.lock().expect("reads");
    assert_eq!(requests.len(), 2);
    assert!(requests[0].head && requests[1].range.is_some());
    assert!(
        requests
            .iter()
            .all(|opts| opts.version == completion.output_version && opts.if_match.is_none())
    );
}

#[test]
fn completed_reader_refuses_weak_malformed_or_wrong_key_records() {
    let store = InMemory::new();
    let dir = tempfile::tempdir().expect("directory");
    let destination = dir.path().join("output");
    let v1 = include_bytes!("fixtures/manifests/v1.json");
    block_on(store.put(&marker_key(), v1.to_vec().into())).expect("v1 record");
    assert!(matches!(
        download_completed_output(&store, &output_key(), &destination),
        Err(PipelineIoError::CompletionIdentityRequired { version: 1 })
    ));
    let original: serde_json::Value =
        serde_json::from_slice(include_bytes!("fixtures/manifests/v2.json")).expect("v2");
    let mut wrong_key = original.clone();
    wrong_key["output_key"] = "other.parquet".into();
    block_on(store.put(
        &marker_key(),
        serde_json::to_vec(&wrong_key).expect("JSON").into(),
    ))
    .expect("wrong key record");
    assert!(matches!(
        download_completed_output(&store, &output_key(), &destination),
        Err(PipelineIoError::InvalidManifest {
            violation: pipeline::manifest::ManifestViolation::OutputKeyMismatch
        })
    ));
    for (field, value) in [
        ("schema_version", serde_json::json!(3)),
        ("digest_scheme", serde_json::json!("unknown")),
    ] {
        let mut invalid = original.clone();
        invalid[field] = value;
        block_on(store.put(
            &marker_key(),
            serde_json::to_vec(&invalid).expect("JSON").into(),
        ))
        .expect("invalid record");
        assert!(download_completed_output(&store, &output_key(), &destination).is_err());
    }
    assert!(!destination.exists());
}

#[test]
fn publication_phase_acknowledgment_loss_keeps_inspectable_outcomes() {
    for (phase, expected) in [
        (Phase::PublishOutput, Outcome::PublicationUnknown),
        (Phase::WriteCompletion, Outcome::CompletionUnknown),
        (Phase::CleanupStaging, Outcome::CompletedCleanupUnknown),
    ] {
        let store = FaultStore {
            fault: Some(phase),
            after: true,
            ..FaultStore::default()
        };
        seed(&store);
        let PipelineError::RemotePublication(error) =
            publish(&store).expect_err("lost acknowledgment")
        else {
            panic!("recovery state");
        };
        assert_eq!(error.outcome, expected);
        assert!(
            block_on(store.inner.head(&output_key())).is_ok(),
            "copy took effect"
        );
        let dir = tempfile::tempdir().expect("directory");
        let read = download_completed_output(&store, &output_key(), &dir.path().join("output"));
        if phase == Phase::PublishOutput {
            assert!(read.is_err(), "completion was never attempted");
        } else {
            assert_eq!(read.expect("completion actually took effect").row_count, 1);
        }
    }
}

#[test]
fn publication_phase_precancelled_run_does_not_read_missing_input() {
    let store = InMemory::new();
    let cancel = CancellationToken::new();
    cancel.cancel();
    let result = run_bounded_pipeline_s3(
        &store,
        &ObjectPath::from("missing.csv"),
        &output_key(),
        &PipelineConfig::default(),
        None,
        &cancel,
    );
    assert!(matches!(result, Err(PipelineError::Cancelled)));
}
