//! Owned local output staging and explicit publication outcomes.

use crate::PipelineIoError;
use crate::bounded::PipelineError;
use std::fs::File;
use std::path::{Path, PathBuf};

/// The local operation that failed before or during publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalPublicationPhase {
    /// Creating or reopening owned staging storage.
    Create,
    /// Writing encoded batches.
    Write,
    /// Closing the encoded output, including its footer.
    Close,
    /// Synchronizing staged file contents before rename.
    FileSync,
    /// Moving the staged file into the destination.
    Rename,
    /// Synchronizing the destination directory after rename.
    ParentSync,
}

/// What is known about final-path visibility after a local error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalPublicationOutcome {
    /// The owned staged file was not published by this run.
    NotPublished,
    /// Rename succeeded, but directory durability could not be established.
    PublishedDurabilityUncertain,
}

pub(crate) struct LocalStagedOutput {
    file: tempfile::NamedTempFile,
    output: PathBuf,
}

impl LocalStagedOutput {
    pub(crate) fn new(output: &Path) -> Result<Self, PipelineError> {
        let mut builder = tempfile::Builder::new();
        builder.prefix(".datacrate-");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            // Preserve File::create's prior mode policy; the OS still applies umask.
            builder.permissions(std::fs::Permissions::from_mode(0o666));
        }
        let file = builder
            .tempfile_in(parent(output))
            .map_err(|source| io_failure(output, LocalPublicationPhase::Create, source))?;
        Ok(Self {
            file,
            output: output.to_owned(),
        })
    }

    pub(crate) fn file(&self) -> Result<File, PipelineError> {
        self.file
            .reopen()
            .map_err(|source| io_failure(&self.output, LocalPublicationPhase::Create, source))
    }

    pub(crate) fn publish(self) -> Result<(), PipelineError> {
        self.publish_with(&Filesystem)
    }

    // The storage edge is injectable so tests can observe real destination and
    // owned-file cleanup at each failure boundary, without a global failpoint.
    fn publish_with(self, io: &impl PublicationIo) -> Result<(), PipelineError> {
        let Self { file, output } = self;
        io.sync_file(file.as_file())
            .map_err(|source| io_failure(&output, LocalPublicationPhase::FileSync, source))?;
        let _published = io
            .persist(file, &output)
            .map_err(|error| io_failure(&output, LocalPublicationPhase::Rename, error.error))?;
        io.sync_parent(parent(&output))
            .map_err(|source| io_failure(&output, LocalPublicationPhase::ParentSync, source))?;
        Ok(())
    }
}

fn parent(output: &Path) -> &Path {
    output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn io_failure(
    output: &Path,
    phase: LocalPublicationPhase,
    source: std::io::Error,
) -> PipelineError {
    failure(
        output,
        phase,
        PipelineIoError::OpenOutput {
            path: output.to_owned(),
            source,
        },
    )
}

pub(crate) fn failure(
    output: &Path,
    phase: LocalPublicationPhase,
    source: PipelineIoError,
) -> PipelineError {
    PipelineError::LocalPublication {
        phase,
        outcome: if phase == LocalPublicationPhase::ParentSync {
            LocalPublicationOutcome::PublishedDurabilityUncertain
        } else {
            LocalPublicationOutcome::NotPublished
        },
        output: output.to_owned(),
        source: Box::new(source),
    }
}

trait PublicationIo {
    fn sync_file(&self, file: &File) -> std::io::Result<()>;
    fn persist(
        &self,
        file: tempfile::NamedTempFile,
        output: &Path,
    ) -> Result<File, tempfile::PersistError>;
    fn sync_parent(&self, parent: &Path) -> std::io::Result<()>;
}
struct Filesystem;
impl PublicationIo for Filesystem {
    fn sync_file(&self, file: &File) -> std::io::Result<()> {
        file.sync_all()
    }
    fn persist(
        &self,
        file: tempfile::NamedTempFile,
        output: &Path,
    ) -> Result<File, tempfile::PersistError> {
        file.persist(output)
    }
    fn sync_parent(&self, parent: &Path) -> std::io::Result<()> {
        File::open(parent)?.sync_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    struct Failing(LocalPublicationPhase);
    impl PublicationIo for Failing {
        fn sync_file(&self, file: &File) -> std::io::Result<()> {
            if self.0 == LocalPublicationPhase::FileSync {
                return Err(std::io::Error::other("file sync"));
            }
            Filesystem.sync_file(file)
        }
        fn persist(
            &self,
            file: tempfile::NamedTempFile,
            output: &Path,
        ) -> Result<File, tempfile::PersistError> {
            if self.0 == LocalPublicationPhase::Rename {
                return Err(tempfile::PersistError {
                    error: std::io::Error::other("rename"),
                    file,
                });
            }
            Filesystem.persist(file, output)
        }
        fn sync_parent(&self, parent: &Path) -> std::io::Result<()> {
            if self.0 == LocalPublicationPhase::ParentSync {
                return Err(std::io::Error::other("parent sync"));
            }
            Filesystem.sync_parent(parent)
        }
    }

    #[test]
    fn owned_staging_unique_creation_and_drop_preserve_other_owners() {
        let dir = tempfile::tempdir().expect("directory");
        let output = dir.path().join("output");
        std::fs::write(&output, b"old").expect("old output");
        std::thread::scope(|scope| {
            let first = scope.spawn(|| LocalStagedOutput::new(&output).expect("first owner"));
            let second = scope.spawn(|| LocalStagedOutput::new(&output).expect("second owner"));
            let first = first.join().expect("first thread");
            let second = second.join().expect("second thread");
            assert_ne!(first.file.path(), second.file.path());
            let first_path = first.file.path().to_owned();
            let second_path = second.file.path().to_owned();
            drop(first);
            assert!(!first_path.exists());
            assert!(second_path.exists());
            assert_eq!(std::fs::read(&output).expect("output"), b"old");
            drop(second);
            assert!(!second_path.exists());
        });
    }

    #[test]
    fn owned_staging_failure_outcomes_match_final_visibility() {
        for phase in [
            LocalPublicationPhase::FileSync,
            LocalPublicationPhase::Rename,
            LocalPublicationPhase::ParentSync,
        ] {
            let dir = tempfile::tempdir().expect("directory");
            let output = dir.path().join("output");
            std::fs::write(&output, b"old").expect("old output");
            let staged = LocalStagedOutput::new(&output).expect("owner");
            staged
                .file()
                .expect("file")
                .write_all(b"new")
                .expect("write");
            let error = staged
                .publish_with(&Failing(phase))
                .expect_err("injected error");
            let expected = if phase == LocalPublicationPhase::ParentSync {
                LocalPublicationOutcome::PublishedDurabilityUncertain
            } else {
                LocalPublicationOutcome::NotPublished
            };
            assert!(
                matches!(error, PipelineError::LocalPublication { phase: actual, outcome, .. } if actual == phase && outcome == expected)
            );
            assert_eq!(
                std::fs::read(&output).expect("final output"),
                if phase == LocalPublicationPhase::ParentSync {
                    b"new"
                } else {
                    b"old"
                }
            );
            assert_eq!(std::fs::read_dir(dir.path()).expect("directory").count(), 1);
        }
    }

    #[test]
    fn owned_staging_relative_output_syncs_current_directory() {
        let reserved = tempfile::Builder::new()
            .prefix(".datacrate-relative-")
            .tempfile_in(".")
            .expect("reserve output");
        let output = PathBuf::from(reserved.path().file_name().expect("filename"));
        let staged = LocalStagedOutput::new(&output).expect("relative owner");
        staged
            .file()
            .expect("file")
            .write_all(b"new")
            .expect("write");
        staged.publish().expect("publish relative path");
        assert_eq!(std::fs::read(output).expect("output"), b"new");
    }

    #[cfg(unix)]
    #[test]
    fn owned_staging_preserves_file_create_permission_policy() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("directory");
        let reference = dir.path().join("reference");
        File::create(&reference).expect("reference creation");
        let expected = std::fs::metadata(reference)
            .expect("reference mode")
            .permissions()
            .mode()
            & 0o777;
        let output = dir.path().join("output");
        LocalStagedOutput::new(&output)
            .expect("owner")
            .publish()
            .expect("publish");
        assert_eq!(
            std::fs::metadata(output)
                .expect("published mode")
                .permissions()
                .mode()
                & 0o777,
            expected
        );
    }

    #[test]
    fn owned_staging_reopen_failure_preserves_error_and_previous_output() {
        let dir = tempfile::tempdir().expect("directory");
        let output = dir.path().join("output");
        std::fs::write(&output, b"old").expect("old output");
        let staged = LocalStagedOutput::new(&output).expect("owner");
        std::fs::remove_file(staged.file.path()).expect("inject missing owned path");
        let error = staged.file().expect_err("reopen must fail");
        assert!(matches!(
            &error,
            PipelineError::LocalPublication {
                phase: LocalPublicationPhase::Create,
                outcome: LocalPublicationOutcome::NotPublished,
                ..
            }
        ));
        assert_eq!(
            std::error::Error::source(&error)
                .and_then(|source| source.source())
                .and_then(|source| source.downcast_ref::<std::io::Error>())
                .expect("original I/O source")
                .kind(),
            std::io::ErrorKind::NotFound
        );
        drop(staged);
        assert_eq!(std::fs::read(output).expect("output"), b"old");
    }

    #[test]
    fn owned_staging_create_error_keeps_original_source() {
        let dir = tempfile::tempdir().expect("directory");
        let result = LocalStagedOutput::new(&dir.path().join("missing/output"));
        let error = result.err().expect("creation error");
        assert_eq!(
            std::error::Error::source(&error)
                .and_then(|source| source.source())
                .and_then(|source| source.downcast_ref::<std::io::Error>())
                .expect("original I/O source")
                .kind(),
            std::io::ErrorKind::NotFound
        );
        assert!(matches!(
            error,
            PipelineError::LocalPublication {
                phase: LocalPublicationPhase::Create,
                outcome: LocalPublicationOutcome::NotPublished,
                ..
            }
        ));
    }
}
