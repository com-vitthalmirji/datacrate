use super::*;
use futures_util::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone, Copy)]
enum Scenario {
    Stable,
    ReplaceAt(usize),
    VersionOnly,
    VersionAndEtag,
    MissingIdentity,
    IgnoreConditionAt(usize),
    Short,
    Oversized,
    WrongRange,
    WrongSize,
    WrongLocation,
    Unsupported,
    BackendFailure,
}

#[derive(Debug)]
struct SnapshotStore {
    inner: InMemory,
    scenario: Scenario,
    requests: Mutex<Vec<GetOptions>>,
    heads: AtomicUsize,
    head_keys: Mutex<Vec<ObjectPath>>,
    gets: AtomicUsize,
    size: usize,
}

impl SnapshotStore {
    fn new(scenario: Scenario, size: usize) -> Self {
        let store = Self {
            inner: InMemory::new(),
            scenario,
            requests: Mutex::new(Vec::new()),
            heads: AtomicUsize::new(0),
            head_keys: Mutex::new(Vec::new()),
            gets: AtomicUsize::new(0),
            size,
        };
        block_on(
            store
                .inner
                .put(&ObjectPath::from("input"), vec![1u8; size].into()),
        )
        .expect("seed object");
        store
    }
}

impl std::fmt::Display for SnapshotStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("snapshot test store")
    }
}

#[async_trait::async_trait]
impl ObjectStore for SnapshotStore {
    async fn put_opts(
        &self,
        key: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(key, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        key: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(key, opts).await
    }
    async fn get_opts(
        &self,
        key: &ObjectPath,
        opts: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.requests.lock().expect("requests").push(opts.clone());
        let head = opts.head;
        let mut opts = opts;
        if head {
            self.heads.fetch_add(1, Ordering::SeqCst);
            self.head_keys.lock().expect("head keys").push(key.clone());
        } else {
            let index = self.gets.fetch_add(1, Ordering::SeqCst) + 1;
            if matches!(self.scenario, Scenario::ReplaceAt(at) | Scenario::IgnoreConditionAt(at) if index == at)
            {
                self.inner.put(key, vec![2u8; self.size].into()).await?;
            }
            if matches!(self.scenario, Scenario::IgnoreConditionAt(_)) {
                opts.if_match = None;
            }
            if matches!(self.scenario, Scenario::Unsupported) {
                return Err(object_store::Error::NotSupported {
                    source: "conditional requests unsupported".into(),
                });
            }
            if matches!(self.scenario, Scenario::BackendFailure) {
                return Err(object_store::Error::Generic {
                    store: "snapshot fake",
                    source: "injected read failure".into(),
                });
            }
        }
        if matches!(
            self.scenario,
            Scenario::VersionOnly | Scenario::VersionAndEtag
        ) {
            // The fake uses its ETag to emulate a versioned backend. Assert the
            // caller actually selected version rather than accidentally relying on ETag.
            if !head {
                assert!(opts.version.is_some());
                assert!(opts.if_match.is_none());
                opts.if_match = opts.version.take();
            }
        }
        let mut response = self.inner.get_opts(key, opts).await?;
        match self.scenario {
            Scenario::VersionOnly | Scenario::VersionAndEtag => {
                response.meta.version = response.meta.e_tag.clone();
                if matches!(self.scenario, Scenario::VersionOnly) {
                    response.meta.e_tag = None;
                }
            }
            Scenario::MissingIdentity => {
                response.meta.version = None;
                response.meta.e_tag = None;
            }
            _ => {}
        }
        if !head {
            match self.scenario {
                Scenario::Short | Scenario::Oversized => {
                    let length = (response.range.end - response.range.start) as usize;
                    let length = if matches!(self.scenario, Scenario::Short) {
                        length - 1
                    } else {
                        length + 1
                    };
                    response.payload = GetResultPayload::Stream(Box::pin(
                        futures_util::stream::iter(vec![Ok(vec![1u8; length].into())]),
                    ));
                }
                Scenario::WrongRange => response.range.end += 1,
                Scenario::WrongSize => response.meta.size += 1,
                Scenario::WrongLocation => response.meta.location = ObjectPath::from("other"),
                _ => {}
            }
        }
        Ok(response)
    }

    fn delete_stream(
        &self,
        keys: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(keys)
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
        self.inner.copy_opts(from, to, opts).await
    }
}

#[test]
fn snapshot_rejects_same_size_replacement_before_first_middle_or_final_range() {
    for index in [1, 2, 3] {
        let store = SnapshotStore::new(Scenario::ReplaceAt(index), CHUNK_BYTES * 2 + 1);
        let dir = tempfile::tempdir().expect("tempdir");
        let result = download_to_temp(
            &store,
            &ObjectPath::from("input"),
            &dir.path().join("input"),
        );
        assert!(
            matches!(result, Err(PipelineIoError::SnapshotPrecondition { .. })),
            "replacement before range {index} did not fail its read condition"
        );
        assert_eq!(store.heads.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn snapshot_stable_etag_and_version_reads_return_the_single_captured_metadata() {
    for scenario in [
        Scenario::Stable,
        Scenario::VersionOnly,
        Scenario::VersionAndEtag,
    ] {
        let store = SnapshotStore::new(scenario, CHUNK_BYTES + 3);
        let dir = tempfile::tempdir().expect("tempdir");
        let dest = dir.path().join("input");
        let meta = download_to_temp(&store, &ObjectPath::from("input"), &dest).expect("snapshot");
        assert_eq!(meta.size, (CHUNK_BYTES + 3) as u64);
        assert_eq!(
            std::fs::read(&dest).expect("downloaded"),
            vec![1; CHUNK_BYTES + 3]
        );
        assert_eq!(store.heads.load(Ordering::SeqCst), 1);
        assert_eq!(store.gets.load(Ordering::SeqCst), 2);
        let requests = store.requests.lock().expect("requests");
        for request in requests.iter().filter(|request| !request.head) {
            if matches!(scenario, Scenario::Stable) {
                assert_eq!(request.if_match, meta.e_tag);
                assert!(request.version.is_none());
            } else {
                assert_eq!(request.version, meta.version);
                assert!(request.if_match.is_none());
            }
        }
    }
}

#[test]
fn snapshot_empty_object_still_requires_and_returns_captured_identity() {
    for scenario in [
        Scenario::Stable,
        Scenario::VersionOnly,
        Scenario::MissingIdentity,
    ] {
        let store = SnapshotStore::new(scenario, 0);
        let dir = tempfile::tempdir().expect("tempdir");
        let dest = dir.path().join("empty");
        let result = download_to_temp(&store, &ObjectPath::from("input"), &dest);
        if matches!(scenario, Scenario::MissingIdentity) {
            assert!(matches!(
                result,
                Err(PipelineIoError::InvalidSnapshot {
                    violation: SnapshotViolation::MissingIdentity,
                    ..
                })
            ));
        } else {
            assert_eq!(result.expect("empty captured object").size, 0);
            assert!(std::fs::read(dest).expect("empty file").is_empty());
        }
        assert_eq!(store.heads.load(Ordering::SeqCst), 1);
        assert_eq!(store.gets.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn snapshot_rejects_invalid_response_identity_range_size_and_body() {
    for (scenario, expected) in [
        (
            Scenario::MissingIdentity,
            SnapshotViolation::MissingIdentity,
        ),
        (Scenario::IgnoreConditionAt(1), SnapshotViolation::Identity),
        (
            Scenario::Short,
            SnapshotViolation::Length {
                expected: 3,
                actual: 2,
            },
        ),
        (
            Scenario::Oversized,
            SnapshotViolation::Length {
                expected: 3,
                actual: 4,
            },
        ),
        (
            Scenario::WrongRange,
            SnapshotViolation::Range {
                expected: 0..3,
                actual: 0..4,
            },
        ),
        (
            Scenario::WrongSize,
            SnapshotViolation::Size {
                expected: 3,
                actual: 4,
            },
        ),
        (Scenario::WrongLocation, SnapshotViolation::Location),
    ] {
        let store = SnapshotStore::new(scenario, 3);
        let dir = tempfile::tempdir().expect("tempdir");
        let error = download_to_temp(
            &store,
            &ObjectPath::from("input"),
            &dir.path().join("input"),
        )
        .expect_err("invalid response");
        assert!(
            matches!(error, PipelineIoError::InvalidSnapshot { violation, .. } if violation == expected),
            "{scenario:?}"
        );
    }
}

#[test]
fn snapshot_unsupported_conditions_and_backend_failures_never_retry_unconditionally() {
    for scenario in [Scenario::Unsupported, Scenario::BackendFailure] {
        let store = SnapshotStore::new(scenario, 3);
        let dir = tempfile::tempdir().expect("tempdir");
        let error = download_to_temp(
            &store,
            &ObjectPath::from("input"),
            &dir.path().join("input"),
        )
        .expect_err("read failure");
        match (scenario, error) {
            (
                Scenario::Unsupported,
                PipelineIoError::DownloadObject {
                    source: object_store::Error::NotSupported { .. },
                    ..
                },
            ) => {}
            (
                Scenario::BackendFailure,
                PipelineIoError::DownloadObject {
                    source: object_store::Error::Generic { .. },
                    ..
                },
            ) => {}
            (_, error) => panic!("unexpected error: {error}"),
        }
        assert_eq!(store.gets.load(Ordering::SeqCst), 1);
        assert!(
            store
                .requests
                .lock()
                .expect("requests")
                .iter()
                .filter(|request| !request.head)
                .all(|request| request.if_match.is_some() || request.version.is_some())
        );
    }
}

#[test]
fn snapshot_equal_keys_in_different_stores_do_not_share_identity_or_bytes() {
    let first = SnapshotStore::new(Scenario::Stable, 3);
    let second = SnapshotStore::new(Scenario::Stable, 3);
    let key = ObjectPath::from("input");
    block_on(second.inner.put(&key, vec![2u8; 3].into())).expect("replace second store only");
    let dir = tempfile::tempdir().expect("tempdir");
    for (store, name, expected) in [
        (&first, "first", vec![1u8; 3]),
        (&second, "second", vec![2u8; 3]),
    ] {
        let dest = dir.path().join(name);
        download_to_temp(store, &key, &dest).expect("snapshot");
        assert_eq!(std::fs::read(dest).expect("downloaded"), expected);
        assert_eq!(store.heads.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn snapshot_minio_honors_etag_conditions() {
    let minio = crate::minio_test_support::MinioContainer::start("snapshot-minio-etag-conditions");
    let store = minio.store();
    let key = ObjectPath::from("input");
    block_on(store.put(&key, vec![1u8; 3].into())).expect("seed");
    let dir = tempfile::tempdir().expect("tempdir");
    let meta = download_to_temp(store.as_ref(), &key, &dir.path().join("input")).expect("snapshot");
    let etag = meta.e_tag.expect("MinIO supplies an ETag");
    block_on(store.put(&key, vec![2u8; 3].into())).expect("replace");
    let error = block_on(store.get_opts(
        &key,
        GetOptions {
            if_match: Some(etag),
            range: Some((0..3).into()),
            ..Default::default()
        },
    ))
    .expect_err("captured ETag must fail after replacement");
    assert!(matches!(error, object_store::Error::Precondition { .. }));
}

#[test]
fn snapshot_pipeline_manifest_uses_the_download_metadata_without_an_extra_head() {
    let csv = b"id,name,note\n1,Ada,\n";
    let store = SnapshotStore::new(Scenario::Stable, csv.len());
    let input = ObjectPath::from("input");
    block_on(store.inner.put(&input, csv.to_vec().into())).expect("CSV input");
    let captured = block_on(store.inner.head(&input)).expect("expected metadata");
    let output = ObjectPath::from("output.parquet");
    crate::bounded::run_bounded_pipeline_s3(
        &store,
        &input,
        &output,
        &crate::bounded::PipelineConfig::default(),
        None,
        &crate::bounded::CancellationToken::new(),
    )
    .expect("pipeline");
    assert_eq!(
        *store.head_keys.lock().expect("head keys"),
        [input.clone(), output.clone()]
    );
    let bytes = block_on(async {
        store
            .inner
            .get(&CompletionManifest::key_for(&output))
            .await
            .expect("manifest")
            .bytes()
            .await
            .expect("manifest bytes")
    });
    let manifest: CompletionManifest = serde_json::from_slice(&bytes).expect("manifest JSON");
    assert_eq!(manifest.input_size, captured.size);
    assert_eq!(manifest.input_e_tag, captured.e_tag);
    assert_eq!(
        manifest.input_last_modified,
        captured.last_modified.to_rfc3339()
    );
}
