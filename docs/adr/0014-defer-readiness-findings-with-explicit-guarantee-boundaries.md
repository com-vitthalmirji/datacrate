# 0014. Defer readiness findings with explicit guarantee boundaries

## Status

Accepted — 18 October 2026. Remediation deferred; this records a scheduling and evidence decision, not completed fixes.

## Context

A source and documentation review found correctness defects and unverified operational assumptions.
Passing existing tests does not establish the affected guarantees.

| Finding | Evidence at review time | Affected boundary |
|---------|-------------------------|-------------------|
| Contract differences are truncated before policy filtering. | An explicit `SchemaConforms::CHECK` compiled for a producer with `id: String` against a contract with eight missing optional fields followed by `id: i64`, under `Backward`. | Diagnostic capacity can change the verdict, rather than only its wording. |
| Case-insensitive matching does not reject ambiguous field names. | `Exact` accepted producer fields `id: i64` and `ID: String` against a contract containing only `id: i64`. | Name matching does not establish an unambiguous one-to-one field correspondence. |
| Multipart uploads have no explicit capacity wait. | The upload loop calls synchronous `WriteMultipart::write` without `wait_for_capacity`; dependency source and documentation confirm that outstanding uploads are not limited by `write`. No upload RSS benchmark was rerun. | Fixed-size file reads do not establish bounded multipart memory or concurrency. |
| Publication and input snapshot assumptions need failure/concurrency tests. | Inspection found deterministic staging names, separate output/manifest writes, and metadata/range reads without a version or ETag condition tying them together. These scenarios were not reproduced in this review. | Concurrent writers, replacement/retry behaviour, and changing inputs have no established guarantee. |
| Scale-fixture timestamp arithmetic overflows its intermediate. | A standalone arithmetic probe at ID `6_599_999_999` found that multiplying by `86_400_000_000` exceeds `i64` before division. The full dataset was not regenerated. | Timestamp correctness of large fixtures is not established by aggregate results that ignore timestamps. |

Relevant implementations: [contract comparison](../../crates/contracts/src/lib.rs),
[object-store transfers](../../crates/pipeline/src/object_store_io.rs),
[publication](../../crates/pipeline/src/bounded.rs),
[orders generator](../../crates/pipeline/examples/scale_benchmark_dataset.rs), and
[shipments generator](../../crates/pipeline/examples/scale_benchmark_shipments.rs).
The multipart finding is checked against [object_store 0.13.2 documentation](https://docs.rs/object_store/0.13.2/object_store/struct.WriteMultipart.html).

There is also an integration gap: `contracts` checks declarations only when its check is explicitly evaluated.
`typestate` tracks stage presence and does not depend on `contracts`. The local builder reads one whole batch; the remote
builder wraps the bounded pipeline; DataFusion queries use another path. None of this establishes a general
contract-enforced source-to-sink execution path.

## Decision

Record these findings now and defer their implementation. Revisit contract verdict correctness first, multipart
backpressure next, then publication/input consistency. Repair fixture arithmetic before using the large generators as
timestamp evidence or regenerating scale evidence. This order is a recommendation for resumption, not a delivery date.

Keep correctness decisions independent of bounded diagnostics. Before closing a finding, add focused regression or
failure-path coverage and run the checks appropriate to the changed path. Do not select a publication protocol, promise
policy parity with CTDC, or introduce contract-aware builders through this ADR; those require their own implementation
decisions. Integration and documentation reconciliation remain follow-ups, not completed capabilities.

## Consequences

The code remains unchanged and the reproduced acceptance defects remain open. Compile-time execution of a checker does
not make its verdict correct. Do not claim complete schema-drift rejection, a hard end-to-end memory ceiling, safe
concurrent publication, a version-consistent input snapshot, or contract enforcement at every pipeline stage from the
current implementation.

This ADR qualifies the evidence behind [0001](0001-proc-macro-build-script-diagnostics.md)'s diagnostic-limit description,
[0005.1](0005.1-stream-object-store-io-in-chunks.md)'s upload-memory claim, and
[0012](0012-completion-manifest-for-s3-output.md)'s completion signal. Their underlying design decisions remain in place;
their stronger implications are not established for the scenarios above. A completion marker alone does not establish a
transaction across replacement of an output/manifest pair.

The existing aggregate measurements are not automatically invalidated by the timestamp finding. They remain scoped to
their actual inputs, query columns, versions, timing boundaries, and retained evidence; timestamp-sensitive claims need
separate verification. Neither a local multi-process benchmark nor a projection establishes multi-node or 1 TB behaviour.

Baseline checks on the reviewed checkout: contracts/derive, fundamentals, CLI, and typestate tests passed; pipeline had
66 passing tests and six failures at MinIO setup because Docker was unavailable. The book build, formatting check, and
whitespace check passed. Optional-engine suites and scale benchmarks were not rerun. These results are a dated baseline,
not closure evidence for any finding.
