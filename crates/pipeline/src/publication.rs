//! Single-writer remote publication with owned staging and explicit recovery states.

use crate::manifest::{CompletionManifest, OutputIdentity};
use crate::object_store_io::{
    copy_staged_object, delete_object, head_object, put_manifest, upload_from_temp,
};
use crate::{PipelineIoError, bounded::PipelineError};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectMeta, ObjectStore};
use std::path::Path;

/// The remote operation at which publication failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemotePublicationPhase {
    /// Uploading this run's unique staging object.
    Upload,
    /// Removing the old completion signal before replacing output.
    InvalidateCompletion,
    /// Copying completed staging to the final output key.
    PublishOutput,
    /// Reading and validating the published output identity.
    CaptureOutputIdentity,
    /// Publishing the identity-bound completion record.
    WriteCompletion,
    /// Removing this run's staging object after completion.
    CleanupStaging,
}

/// What is known about final output and completion after an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemotePublicationOutcome {
    /// This run did not attempt to replace final output.
    NotPublished,
    /// A failed copy may have replaced final output.
    PublicationUnknown,
    /// Copy succeeded, but completion was not published by this run.
    PublishedWithoutCompletion,
    /// Completion PUT failed and may nevertheless have taken effect.
    CompletionUnknown,
    /// Output and completion succeeded; staging deletion returned an ambiguous error.
    CompletedCleanupUnknown,
}

/// Recovery information retaining the primary error and any secondary cleanup error.
#[derive(Debug)]
pub struct RemotePublicationError {
    /// Failed operation.
    pub phase: RemotePublicationPhase,
    /// Known publication state, including ambiguous backend outcomes.
    pub outcome: RemotePublicationOutcome,
    /// Final output key. Recovery must never blindly delete this shared key.
    pub output_key: ObjectPath,
    /// Completion key associated with final output.
    pub manifest_key: ObjectPath,
    /// This run's unique staging key, safe for an owned cleanup attempt.
    pub staging_key: ObjectPath,
    /// Primary operation failure.
    pub source: PipelineIoError,
    /// A failed staging cleanup attempt following the primary failure.
    pub cleanup_error: Option<PipelineIoError>,
}

impl std::fmt::Display for RemotePublicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "remote publication {:?} for {} ({:?}): {}",
            self.phase, self.output_key, self.outcome, self.source
        )?;
        if let Some(error) = &self.cleanup_error {
            write!(f, "; owned staging cleanup also failed: {error}")?;
        }
        Ok(())
    }
}
impl std::error::Error for RemotePublicationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

struct RemoteStagedOutput {
    _namespace: tempfile::TempDir,
    run_id: String,
    key: ObjectPath,
}
impl RemoteStagedOutput {
    fn new(output: &ObjectPath) -> Result<Self, PipelineIoError> {
        let namespace = tempfile::Builder::new()
            .prefix("datacrate-run-")
            .rand_bytes(24)
            .tempdir()
            .map_err(|source| PipelineIoError::OpenOutput {
                path: std::env::temp_dir(),
                source,
            })?;
        let run_id = namespace
            .path()
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| PipelineIoError::OpenOutput {
                path: namespace.path().to_owned(),
                source: std::io::Error::other("temporary run namespace has no UTF-8 name"),
            })?
            .to_owned();
        let key = ObjectPath::from(format!("{output}.staging/{run_id}"));
        Ok(Self {
            _namespace: namespace,
            run_id,
            key,
        })
    }
}

/// No remote operation runs from Drop. Explicit cleanup only touches this run's key.
pub(crate) fn publish(
    store: &dyn ObjectStore,
    local_output: &Path,
    output_key: &ObjectPath,
    input_key: &ObjectPath,
    input_meta: &ObjectMeta,
    rows: usize,
    digest: u64,
) -> Result<(), PipelineError> {
    use RemotePublicationOutcome as Outcome;
    use RemotePublicationPhase as Phase;
    let staged = RemoteStagedOutput::new(output_key)?;
    let manifest_key = CompletionManifest::key_for(output_key);
    let failure = |phase, outcome, source, cleanup_error| {
        PipelineError::RemotePublication(Box::new(RemotePublicationError {
            phase,
            outcome,
            output_key: output_key.clone(),
            manifest_key: manifest_key.clone(),
            staging_key: staged.key.clone(),
            source,
            cleanup_error,
        }))
    };
    let result = (|| {
        upload_from_temp(store, local_output, &staged.key)
            .map_err(|source| (Phase::Upload, Outcome::NotPublished, source))?;
        delete_object(store, &manifest_key)
            .map_err(|source| (Phase::InvalidateCompletion, Outcome::NotPublished, source))?;
        // Copy and staging deletion are separate actions. A failed copy can have
        // taken effect, so cleanup must never target the final output key.
        copy_staged_object(store, &staged.key, output_key)
            .map_err(|source| (Phase::PublishOutput, Outcome::PublicationUnknown, source))?;
        let meta = head_object(store, output_key).map_err(|source| {
            (
                Phase::CaptureOutputIdentity,
                Outcome::PublishedWithoutCompletion,
                source,
            )
        })?;
        let identity = OutputIdentity::new(staged.run_id.clone(), output_key.clone(), meta)
            .map_err(|source| {
                (
                    Phase::CaptureOutputIdentity,
                    Outcome::PublishedWithoutCompletion,
                    source,
                )
            })?;
        let manifest = CompletionManifest::new_v2(input_key, input_meta, rows, digest, &identity)
            .map_err(|source| {
            (
                Phase::CaptureOutputIdentity,
                Outcome::PublishedWithoutCompletion,
                source,
            )
        })?;
        put_manifest(store, &manifest, &manifest_key).map_err(|source| {
            let outcome = if matches!(source, PipelineIoError::UploadObject { .. }) {
                Outcome::CompletionUnknown
            } else {
                Outcome::PublishedWithoutCompletion
            };
            (Phase::WriteCompletion, outcome, source)
        })?;
        Ok(())
    })();
    if let Err((phase, outcome, source)) = result {
        let cleanup_error = delete_object(store, &staged.key).err();
        return Err(failure(phase, outcome, source, cleanup_error));
    }
    delete_object(store, &staged.key).map_err(|source| {
        failure(
            Phase::CleanupStaging,
            Outcome::CompletedCleanupUnknown,
            source,
            None,
        )
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bounded::{CancellationToken, PipelineConfig, run_bounded_pipeline_s3};
    use crate::object_store_io::{block_on, download_completed_output};
    use object_store::ObjectStoreExt;

    #[test]
    fn publication_phase_minio_completion_reads_recorded_version_after_replacement() {
        let minio = crate::minio_test_support::MinioContainer::start("completion-version-read");
        minio.enable_versioning();
        let store = minio.store();
        let input = ObjectPath::from("input.csv");
        let output = ObjectPath::from("output.parquet");
        block_on(store.put(&input, b"id,name,note\n1,Ada,\n".to_vec().into())).expect("input");
        run_bounded_pipeline_s3(
            store.as_ref(),
            &input,
            &output,
            &PipelineConfig::default(),
            None,
            &CancellationToken::new(),
        )
        .expect("publish");
        let dir = tempfile::tempdir().expect("directory");
        let first = dir.path().join("first.parquet");
        let historical = dir.path().join("historical.parquet");
        let manifest =
            download_completed_output(store.as_ref(), &output, &first).expect("completed output");
        assert!(manifest.output_version.is_some());
        assert!(manifest.input_version.is_some());
        block_on(store.put(&output, b"newer output".to_vec().into())).expect("replacement");
        let latest = block_on(store.head(&output)).expect("latest identity");
        assert_ne!(latest.version, manifest.output_version);
        let again = download_completed_output(store.as_ref(), &output, &historical)
            .expect("recorded historical version");
        assert_eq!(again, manifest);
        assert_eq!(
            std::fs::read(first).expect("first bytes"),
            std::fs::read(&historical).expect("historical bytes")
        );
        assert_eq!(
            crate::read_parquet(&historical)
                .expect("valid recorded Parquet")
                .num_rows(),
            1
        );
    }

    #[test]
    fn publication_phase_minio_completion_refuses_replaced_etag() {
        let minio = crate::minio_test_support::MinioContainer::start("completion-etag-read");
        let store = minio.store();
        let input = ObjectPath::from("input.csv");
        let output = ObjectPath::from("output.parquet");
        block_on(store.put(&input, b"id,name,note\n1,Ada,\n".to_vec().into())).expect("input");
        run_bounded_pipeline_s3(
            store.as_ref(),
            &input,
            &output,
            &PipelineConfig::default(),
            None,
            &CancellationToken::new(),
        )
        .expect("publish");
        let dir = tempfile::tempdir().expect("directory");
        let first = download_completed_output(store.as_ref(), &output, &dir.path().join("first"))
            .expect("completed output");
        assert!(first.output_version.is_none());
        assert!(first.output_e_tag.is_some());
        block_on(store.put(&output, b"replacement".to_vec().into())).expect("replace");
        assert!(matches!(
            download_completed_output(store.as_ref(), &output, &dir.path().join("second")),
            Err(PipelineIoError::SnapshotPrecondition { .. })
        ));
    }
}
