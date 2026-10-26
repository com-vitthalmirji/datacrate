# Zstd compatibility boundary

The dependency update initially left a second codec implementation in the graph:
Arrow IPC, Parquet, DataFusion and Polars still requested Zstd 0.13, while the
security fixes were released in 0.14.3 and zstd-safe 8.2.0. A lockfile update
cannot satisfy a `^0.13` requirement with 0.14.3. Disabling compression would
remove an existing capability; copying the unsafe fixes would leave a local
codec implementation to maintain.

This private Cargo patch keeps the legacy package identity and forwards its
public interface and features to the exact upstream `zstd = 0.14.3` release.
Its entire Rust implementation is:

```rust
#![forbid(unsafe_code)]
pub use zstd_patched::*;
```

No upstream codec implementation is copied here. The registry dependencies
retain their own licenses; this forwarding code uses the repository's MIT license.
The patch applies when building this repository. Cargo patches are chosen by
the root application, so publishing a library does not propagate this override
to downstream applications.

## Verification boundary

RustSec currently [skips non-registry packages when matching vulnerabilities](https://github.com/RustSec/rustsec/blob/010f12aea2d7c663704ddd4c22d351ea5629e402/rustsec/src/database.rs#L145).
An audit passing after a path patch is therefore insufficient evidence.
`scripts/check_zstd_dependencies.py` independently requires the two-line source,
pinned dependency, feature forwarding and a lockfile containing only this facade
and patched registry Zstd implementations. Its negative tests reject old or
non-registry implementations and changes to the reviewed boundary. This is a
regression guard, not a general source-code security scanner.

The normal Rust CI compiles the existing callers with all application features.
`crates/pipeline/tests/zstd_compat.rs` covers bulk/dictionary round trips,
abandoned streaming context reuse, corrupt legacy-frame output, Parquet ZSTD
metadata and decoded values, and compressed Arrow IPC values. These tests are
compatibility checks, not a complete upstream security or format-conformance suite.
The facade is excluded from workspace membership so `--all-features` does not
activate dependency build-tool flags such as cross-language LTO and doc-cfg.
Features actually requested by application dependencies still forward normally.

Compatibility is limited to the callers and feature combinations verified here;
this is not a promise that every external 0.13 consumer can use the 0.14 interface.
No advisory ignore is added. Remove the facade, guard and dedicated tests when
all parent requirements accept a fixed upstream release, then resolve the graph
and repeat audit and compressed-I/O checks in CI.

## Upstream evidence

- [Zstd 0.14.3 release](https://github.com/gyscos/zstd-rs/releases/tag/v0.14.3).
- [Old public features](https://github.com/gyscos/zstd-rs/blob/v0.13.3/Cargo.toml)
  and [patched features and dependency](https://github.com/gyscos/zstd-rs/blob/75bc74045a2f6ea0439aeed7e159a266da20aac0/Cargo.toml).
- [Context-reuse fix](https://github.com/gyscos/zstd-rs/commit/74ace2dcdca62a2ce0f034895d6079458311e003)
  and [legacy decoder error fix](https://github.com/gyscos/zstd-rs/commit/006e1d1ad1eeaf6b5d4c28329e70217adabcec43).
- [RUSTSEC-2026-0391](https://github.com/RustSec/advisory-db/blob/main/crates/zstd/RUSTSEC-2026-0391.md),
  [RUSTSEC-2026-0398](https://github.com/RustSec/advisory-db/blob/main/crates/zstd-safe/RUSTSEC-2026-0398.md),
  [RUSTSEC-2026-0403](https://github.com/RustSec/advisory-db/blob/main/crates/zstd-safe/RUSTSEC-2026-0403.md).
