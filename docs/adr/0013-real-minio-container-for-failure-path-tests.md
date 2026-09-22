# 0013. Real MinIO container for object-store failure-path tests

## Status
Accepted

## Context
[0004](0004-object-store-io-scope-and-async-shim.md) built the object-store I/O path against
`object_store::memory::InMemory` for fast, dependency-free unit tests. That backend can't produce
real S3-compatible error responses - authentication failures, missing-bucket/-key XML errors,
timeout/retry exhaustion - so the failure paths a participant will actually hit against real
storage (missing credentials, inaccessible objects, timeouts, schema drift, partial output) had no
test coverage.

## Decision
`crates/pipeline/src/minio_test_support.rs` adds a `MinioContainer` test helper (`testcontainers`
crate, `blocking` feature, `SyncRunner`) that starts a real MinIO container per test and creates a
bucket via the image's own bundled `mc` CLI (`Container::exec`), rather than adding `aws-sdk-s3` as
a dev-dependency just to create a bucket. The `blocking` feature keeps this crate's synchronous test
code synchronous instead of spreading async further than the existing short-lived `block_on` in
`object_store_io.rs`. `scripts/preflight.sh` gained a Docker-reachability check so a missing daemon
fails with a clear message instead of a cryptic error from inside the tests.

## Consequences
Five failure-path tests now run against real S3-compatible error behavior: missing credentials,
inaccessible objects, near-zero timeout/retry exhaustion, schema-drift rejection, and partial-output
cleanup on failure. The schema-drift test caught a real bug this way - `read_parquet` panicked
(index-out-of-bounds in `concat_batches`) instead of returning `Err` on a batch with fewer columns
than expected, a genuine panic-on-untrusted-input gap since Parquet files downloaded from object
storage are external input; fixed with a schema-equality guard before the panicking call. These
tests require a reachable Docker daemon - not yet confirmed whether GitHub-hosted CI runners'
pre-installed Docker Engine is sufficient unmodified; if not, they will need `#[ignore]` gating or a
feature flag.
