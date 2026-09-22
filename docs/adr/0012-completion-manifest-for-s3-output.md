# 0012. Completion manifest for S3 output

## Status
Accepted

## Context
[0005.2](0005.2-staged-write-durability.md) made local-disk output crash-durable via
staged-write-then-rename plus `fsync`. The S3 side of the pipeline
([0004](0004-object-store-io-scope-and-async-shim.md)) already follows the same
staging-key-then-rename pattern, but a caller polling object storage after a run had no way to
confirm *what* was written - row count, or whether the bytes matched a known-good input - short of
re-downloading and re-parsing the output itself.

## Decision
`run_bounded_pipeline_s3` now publishes a `CompletionManifest` (`crates/pipeline/src/manifest.rs`)
as a `.manifest.json` sibling of the output key: the input object's key/size/ETag/last-modified
snapshot, the row count, and a content digest. The digest is an order-independent, duplicate-safe
multiset hash - each row hashed with a fixed-seed `ahash::RandomState`, combined by wrapping `u64`
addition rather than XOR (XOR cancels identical rows, which would let a run that silently dropped
a duplicate hash identically to the original) - computed one row at a time, never buffering more
than a single row's hash. `ahash`, `serde`, and `serde_json` were already resolved transitively via
`datafusion`, so this added no new crate to the dependency graph.

## Consequences
A caller can confirm a run's output without re-parsing the Parquet file: read the manifest, compare
row count and digest against expectation. The manifest write follows the same staging-then-rename
publish step as the Parquet output, with best-effort (non-retried) cleanup if either write fails -
a cleanup failure after an already-reported error isn't worth a second failure mode. The digest is
a content-drift fingerprint, not a security boundary; it does not detect adversarial tampering, only
accidental drift (dropped/duplicated/reordered rows).
