# 0019. Bind remote inputs and own publication lifecycles

## Status

Proposed - 20 October 2026. No new snapshot, publication or manifest guarantee is delivered.
Refines ADRs 0005.2, 0012, 0014 and 0015. Their historical decisions remain unchanged.

## Context

Datacrate currently records input metadata before a helper performs another HEAD and
unconditional range GETs. An object can change between those operations. Local/remote staging
names are deterministic. Final output and its manifest are separate publications; a failed
manifest can trigger unconditional deletion of the final key.

Owned resources need explicit fallible completion, and read results need validation.
Snapshot enforcement must bind each request to captured identity; a range request alone
does not establish that guarantee.

## Decision

### Input identity

Capture one ObjectMeta and choose its version when available, otherwise its ETag.
Bind every range to that version or `if_match` through pinned object_store 0.13.2 GetOptions.
Reject missing identity, unsupported conditions, changed identity, short/oversized responses,
and invalid returned ranges. Never downgrade strict snapshot reads to unconditional GETs.

Return the captured metadata from `download_to_temp` and use that exact observation for
the manifest; remove the independent metadata HEAD in the caller. Handle empty objects
under the same captured-identity contract. A successful empty read describes the captured
empty version, not a promise that the latest key remains unchanged afterward.

First support/test the S3/MinIO route and deterministic fakes. Backend support must honor
conditional/version requests; a generic trait signature alone is insufficient evidence.
Keep analytical Parquet demand reads on DataFusion's existing reader. Do not claim this
sequential-download snapshot change automatically snapshots a multi-file analytical dataset.

### Local staging ownership

Create a unique staged file in the output's parent using the existing tempfile dependency.
A private owner hides its path/file and performs fallible close, file sync, rename/persist,
and parent sync. Preserve original error sources. Drop cleans only that owner's unpublished
local artifact, best effort. A directory-sync error after rename is explicitly a published
but durability-uncertain result, not an assertion that the old output is unchanged.

Keep the same-filesystem requirement explicit. Test relative output paths and concurrent
staging creation. Last-writer overwrite policy, process failure and filesystem-specific
durability remain distinct from exclusive staging ownership.

### Remote publication contract

Retain fixed output keys under an explicit externally enforced single-writer-per-output-key
precondition. Use unique staging keys per run; uniqueness does not supply a distributed lease.
Prefer a unique tempfile-owned random run namespace using existing dependencies, not wall-clock
time or a process-local counter. Clean only owned staging artifacts.

On overwrite, invalidate the previous completion marker before replacing final output.
Publish completed output, capture its identity, then publish a version-2 manifest bound to
that output key and identity. A protocol-aware reader obtains the manifest and conditionally
reads the recorded output version/ETag; a changed output requires retry/error.

Do not delete the shared final key on manifest failure. Return a typed partial-publication
error identifying output/manifest and the last known phase, preserving the original failure.
A failed remote request may have taken effect: distinguish definitely uncommitted output from
an unknown publication/completion outcome that recovery must inspect. Never describe that as
rollback or promise that an errored manifest PUT left no marker.

Make copy/publication and staging cleanup separate actions rather than hiding both in rename.
A copy error can leave final visibility unknown; a staging-delete error after successful copy
must not be reported as though no output was published. Cleanup never masks primary failure.
If invalidating the previous marker fails, do not replace output. Existing readers that
ignore identity checks retain their weaker contract.

```mermaid
sequenceDiagram
    participant W as Single writer
    participant O as Object store
    participant R as Protocol-aware reader
    W->>O: Upload unique owned staging object
    W->>O: Invalidate old completion marker
    W->>O: Publish final output and capture identity
    W->>O: Put manifest v2 with output identity
    R->>O: Read supported manifest
    R->>O: Read output bound to manifest identity
    Note over W,O: Manifest failure leaves uncommitted output; no final-key rollback
```

This is an ordered, detectable completion protocol with a single-writer precondition.
It is not atomic visibility of two objects, exactly-once processing, or a multiwriter commit
protocol. If multiwriter atomic visibility becomes required, replace this decision with an
immutable generation plus conditional commit-pointer protocol and reader migration.

### Persisted compatibility

Implement supported-version validation before emitting version 2. Preserve version-1 JSON
fixtures and its digest interpretation. Version 2 keeps current input/count/digest fields and
adds a run ID, optional captured input version, a required output key, an output version or
ETag, and an explicit legacy digest-scheme label. Do not infer compatibility merely because
Serde can ignore unknown fields. Updated readers accept versions 1 and 2, reject others,
and reject malformed/identity-less v2. Version 1 remains weaker completion evidence.

Keep legacy row hashing, fixed seeds, byte encoding and wrapping-sum outputs unchanged.
Label it `legacy-ahash-wrapping-sum-v1`; that names existing interpretation, not a stable
cross-version or cross-platform cryptographic format. Preserve pinned-version fixtures,
null/empty/field-boundary cases, duplicates and batch permutations. A future portable digest
needs its own canonical encoding and migration ADR.

The historical ADR 0012 wording about detecting reordered rows is narrower in reality:
an order-independent digest intentionally cannot detect reordering. Collisions can also hide
content changes. No completion manifest is a collision-free equality or authenticity proof.
Old unchecked readers are not retroactively made safe; publish migration instructions before
enabling v2 writing. Refuse unsupported write versions.

### Acceptance

- Replace an input between HEAD and the first/middle/final range, including same-size
  replacement; prove one captured identity or an explicit error, with no mixed successful file.
- Test version-only, ETag-only, missing/unsupported identity, empty objects, short responses,
  backend failures, and equal keys in separate stores. Record actual MinIO support separately.
- Inject local create/write/close/sync/rename/parent-sync failure and remote upload/publish/
  metadata/marker/manifest failure. Check published phase and cleanup ownership.
- Concurrent runs use distinct staging objects; a failed older run never deletes a newer
  final object. This tests cleanup restraint, not unsupported multiwriter completion.
- Legacy v1 fixtures still read with their old digest meaning. V2 reader conditional reads
  reject stale manifests, unsupported versions/schemes and missing output identity.
- Cleanup failure never masks the primary error. Explicit async abort attempts happen in the
  async action path; Drop cannot guarantee remote cleanup after cancellation/process failure.

## Consequences

Identity checks add metadata/conditional-request costs and expose backend limitations.
Unique ownership prevents staging collisions; single-writer coordination remains an external
operational requirement. Invalidation deliberately removes the old completion signal during
overwrite, and publication failure can leave recoverable orphan output.

Compare this bounded protocol with a full generation/commit-pointer scheme and with the
current best-effort deletion. The first avoids a larger reader/storage redesign while making
failure states explicit. The current deletion can remove someone else's output and hides a
partial-failure distinction. No remote transaction is implied by Rust ownership.

## Sources

- [Pinned GetOptions](https://docs.rs/object_store/0.13.2/object_store/struct.GetOptions.html)
- [Datacrate I/O](../../crates/pipeline/src/object_store_io.rs),
  [publication](../../crates/pipeline/src/bounded.rs), [manifest](../../crates/pipeline/src/manifest.rs)
