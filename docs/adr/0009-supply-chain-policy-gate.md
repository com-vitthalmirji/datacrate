# 0009. Gate the dependency tree with cargo-audit and cargo-deny, with recorded exceptions

## Status
Accepted

## Context
Adding Ballista, Comet's benchmark tooling, and object-store clients (see
[0008](0008-ballista-and-comet-benchmark-engines.md)) expanded the dependency tree considerably.
`cargo-audit` alone catches known vulnerabilities but says nothing about license compatibility,
duplicate dependency versions, or where a dependency's source is allowed to come from. Two
advisories fired that needed a judgment call rather than being ignored or blocked outright:

- Two RUSTSEC advisories: a quick-xml denial-of-service pattern pulled transitively through the
  object-store crate's S3 XML response parsing. The XML being parsed comes from trusted
  infrastructure, not untrusted user input, so the DoS vector doesn't apply here.
- An unmaintained-crate flag on `bincode`, pulled transitively through a Polars dependency and
  never called directly by this pipeline's code.

## Decision
Added `cargo-deny` as a complementary gate to `cargo-audit`, covering license policy, a
banned/duplicate-dependency-version check, and a source allow-list restricting where dependencies
are permitted to come from. Both tools carry explicit, individually justified ignore entries for
the three advisories above, recorded with the reasoning rather than suppressed silently. The same
advisory IDs were also added to the CI audit workflow's ignore list, matching the local `cargo-deny`
configuration.

## Consequences
The dependency tree is checked on two axes (known vulnerabilities, and license/source/duplication
policy) instead of one, and every suppressed advisory has a written justification attached to it
rather than existing only as an unexplained ignore line - anyone auditing the policy later can see
why each one was judged safe. The tradeoff is that these ignore entries need periodic revisiting:
an advisory judged safe under "trusted infrastructure input only" stops being safe if the pipeline
ever starts parsing object-store responses from an untrusted source.

## 2026-10-26 amendment: audit reporting and constrained Zstd updates

The audit action also requires job-scoped `checks: write` to publish its check.
The existing `contents: read` and `issues: write` permissions alone do not grant
that access. The same-repository pull request exposed the missing permission;
the action's generic fork warning did not identify this pull request as a fork.
See the [action's permission requirements](https://github.com/rustsec/audit-check#granular-permissions).

Compatible lockfile updates select Tokio 1.53.3, zstd 0.14.3, zstd-safe 8.2.0
and zstd-sys 2.1.2. Tokio's patched version addresses the AsyncFd I/O-safety
advisory. The Zstd updates address the applicable advisories on the newer branch;
they do not eliminate the older transitive branch.

Arrow IPC, Parquet, DataFusion and Polars still resolve zstd 0.13.3, which uses
zstd-safe 7.3.0. The remaining vulnerability occurrences are:

- [RUSTSEC-2026-0391](https://github.com/RustSec/advisory-db/blob/main/crates/zstd/RUSTSEC-2026-0391.md)
  on zstd 0.13.3; the advisory lists patched releases starting at 0.14.3.
- [RUSTSEC-2026-0398](https://github.com/RustSec/advisory-db/blob/main/crates/zstd-safe/RUSTSEC-2026-0398.md)
  on zstd-safe 7.3.0; patched releases start at 8.2.0.
- [RUSTSEC-2026-0403](https://github.com/RustSec/advisory-db/blob/main/crates/zstd-safe/RUSTSEC-2026-0403.md)
  on zstd-safe 7.3.0; patched releases start at 8.2.0.

These findings remain audit failures. No new advisory ignores or disabled
compression features were added. Fixing the older branch requires a compatible
upstream backport or coordinated parent-dependency upgrades, with correctness
and compatibility verification. Informational unsoundness and unmaintained
warnings remain distinct from these vulnerability occurrences. Restoring check
publication is not evidence that the dependency audit is clean.
