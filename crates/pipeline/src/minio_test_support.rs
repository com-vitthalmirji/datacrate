//! Per-test MinIO container, for failure-path tests that need real
//! S3-compatible error behavior (auth failures, missing objects) that
//! `object_store::memory::InMemory` can't produce. Uses the MinIO image's own
//! bundled `mc` CLI (via `Container::exec`) to create buckets, so this stays
//! off `aws-sdk-s3` as a dependency.

use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use object_store::aws::AmazonS3Builder;
use object_store::{ClientOptions, ObjectStore, RetryConfig};
use testcontainers::core::{ContainerPort, ExecCommand, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

// Mirrored from quay.io/minio/minio, which now rejects anonymous pulls with
// a 401 on every tag. Pinned by digest so this can't silently drift.
const MINIO_IMAGE: &str = "ghcr.io/vim89/datacrate-minio";
const MINIO_TAG: &str = "RELEASE.2025-09-07T16-13-09Z@sha256:9966a92a734f9411e32f4f41d7d9d826fcdc0f68c4e20b70295bd4e7c11f8a2f";
const MINIO_ROOT_USER: &str = "minioadmin";
const MINIO_ROOT_PASSWORD: &str = "minioadmin";

/// Runs an `mc` command in `container` and panics with its stderr if it
/// didn't exit zero. `Container::exec` only reports a `Result` if the exec
/// call itself failed, not if the command inside exited non-zero, so a
/// failed `mc mb` would otherwise surface only much later as a confusing
/// `NoSuchBucket` from the test body instead of a clear setup failure here.
/// `exit_code()` reports `None` until the command has actually finished, so
/// stderr must be drained first — draining blocks until the command exits.
fn run_mc<const N: usize>(container: &Container<GenericImage>, cmd: [&str; N]) {
    let description = cmd.join(" ");
    let mut result = container
        .exec(ExecCommand::new(cmd))
        .unwrap_or_else(|err| panic!("`{description}` should exec: {err}"));
    let mut stderr = String::new();
    let _ = result.stderr().read_to_string(&mut stderr);
    let exit_code = result
        .exit_code()
        .unwrap_or_else(|err| panic!("`{description}` exit code should be readable: {err}"));
    if exit_code != Some(0) {
        panic!("`{description}` exited with {exit_code:?}: {stderr}");
    }
}

pub(crate) struct MinioContainer {
    container: Container<GenericImage>,
    bucket: String,
}

impl MinioContainer {
    /// Starts a MinIO container and creates `bucket` in it via `mc`.
    pub(crate) fn start(bucket: &str) -> Self {
        let image = GenericImage::new(MINIO_IMAGE, MINIO_TAG)
            .with_wait_for(WaitFor::message_on_either_std("API:"))
            .with_exposed_port(ContainerPort::Tcp(9000))
            .with_cmd(["server", "/data", "--console-address", ":9001"])
            .with_env_var("MINIO_ROOT_USER", MINIO_ROOT_USER)
            .with_env_var("MINIO_ROOT_PASSWORD", MINIO_ROOT_PASSWORD);

        let container = image.start().expect("MinIO container should start");

        run_mc(
            &container,
            [
                "mc",
                "alias",
                "set",
                "local",
                "http://127.0.0.1:9000",
                MINIO_ROOT_USER,
                MINIO_ROOT_PASSWORD,
            ],
        );
        run_mc(&container, ["mc", "mb", &format!("local/{bucket}")]);

        Self {
            container,
            bucket: bucket.to_string(),
        }
    }

    /// An `ObjectStore` handle for this container's bucket, using the correct
    /// MinIO root credentials.
    pub(crate) fn store(&self) -> Arc<dyn ObjectStore> {
        self.store_with_credentials(MINIO_ROOT_USER, MINIO_ROOT_PASSWORD)
    }

    /// An `ObjectStore` handle for this container's bucket, using whatever
    /// credentials the caller supplies — for tests that need to exercise an
    /// authentication failure against a real backend.
    pub(crate) fn store_with_credentials(
        &self,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> Arc<dyn ObjectStore> {
        let port = self
            .container
            .get_host_port_ipv4(9000)
            .expect("MinIO's exposed port should be mapped to a host port");
        Arc::new(
            AmazonS3Builder::new()
                .with_endpoint(format!("http://127.0.0.1:{port}"))
                .with_bucket_name(&self.bucket)
                .with_access_key_id(access_key_id)
                .with_secret_access_key(secret_access_key)
                .with_allow_http(true)
                .build()
                .expect("AmazonS3Builder should build against the MinIO container"),
        )
    }

    /// An `ObjectStore` handle for this container's bucket, using the correct
    /// root credentials but `timeout` as the request timeout and zero
    /// retries — for tests that need a genuine timeout/retry-exhaustion
    /// failure against a real backend, rather than one only `object_store`'s
    /// own unit tests can reach.
    pub(crate) fn store_with_timeout(&self, timeout: Duration) -> Arc<dyn ObjectStore> {
        let port = self
            .container
            .get_host_port_ipv4(9000)
            .expect("MinIO's exposed port should be mapped to a host port");
        Arc::new(
            AmazonS3Builder::new()
                .with_endpoint(format!("http://127.0.0.1:{port}"))
                .with_bucket_name(&self.bucket)
                .with_access_key_id(MINIO_ROOT_USER)
                .with_secret_access_key(MINIO_ROOT_PASSWORD)
                .with_allow_http(true)
                .with_client_options(ClientOptions::new().with_timeout(timeout))
                .with_retry(RetryConfig {
                    max_retries: 0,
                    ..Default::default()
                })
                .build()
                .expect("AmazonS3Builder should build against the MinIO container"),
        )
    }
}
