# 0018. Preserve representations and query semantics

## Status

Proposed - 20 October 2026. Scalar preservation, aggregate precision and shared query
construction are implemented, with controlled ordered-stream spill/progress coverage.
Direct ingestion remains an experiment pending controlled allocation/performance evidence.
Refines ADR 0006 and the representation/query candidates extending ADR 0015.

## Context

Preserving constants, processing typed chunks, and refusing rewrites that change errors or
null behavior can avoid unnecessary work. Apply those principles through Datacrate's
existing Arrow/DataFusion interfaces.

Earlier, the timestamp UDF expanded inputs through `values_to_arrays`, row-query helpers
combined planning with collection, and the amount-range aggregate declared precision 10
for a difference that can need precision 11. Those boundaries are corrected as specified
below. Normal Parquet registration was already lazy. The bounded CSV producer still creates
owned rows; changing that path requires the separate controlled measurement gate.

## Decision

### Scalar and aggregate contracts

Handle `ColumnarValue::Scalar` before array expansion in `days_since_epoch_udf`.
For the registered timestamp-microsecond/no-timezone input, compute once and return
`ScalarValue::Int64`, preserving typed nulls. Arrays use typed Arrow iteration and retain
length/nulls. Reject wrong arity/types/timezone through DataFusion errors. Preserve Euclidean
division for negative microseconds and `Volatility::Immutable`.

Test the implementation directly and through SQL; constant folding can bypass a scalar
execution branch. Returning a scalar avoids expansion here, not necessarily downstream.

Reproduce the amount-range result-domain issue in an actual engine-returned Arrow batch.
For precision-10 scale-2 inputs, unscaled extrema `-9_999_999_999` and `9_999_999_999`
have range `19_999_999_998`. Prefer an exact precision-11/scale-2 result over retaining
precision 10 and rejecting valid inputs. Keep min/max partial state precision 10.
Change both declared return type and evaluated scalar, then test merged/partitioned output.
The regression checks an engine-returned batch, including its declared type and actual value.

### Direct construction, only where earned

Evaluate reusable `csv::StringRecord` plus typed Arrow builders against the bounded
producer's existing `Vec<Row>` path. Append borrowed fields before reusing the CSV record.
Share pure field parsing; do not duplicate ID/null/error rules. Retain the correct CSV parser,
quoted newlines/delimiters, Unicode and malformed-UTF8 behavior, row indices, empty-note/null
semantics, and final partial batches. Keep small fixture helpers readable.

Prefer direct construction only after equivalent independent results and measured allocation
savings justify it. Record array construction and end-to-end write costs separately.
This reduces intermediate ownership; CSV decoding/Arrow string construction remain copies.

### Query construction and semantic permission

Add one `row_query_plan(ctx: &SessionContext) -> Result<DataFrame, DataFusionError>`
async helper for the existing row query, shared by its collecting SQL wrapper and a real
streaming consumer. Preserve its predicate, columns, ordering and null semantics. Keep an
independent DataFrame expression path as the differential oracle. Retain collection for tiny
aggregates. Sorting may still buffer or spill before the first output; a streamed API does
not make a global sort online.

Pruning with absent/inexact statistics must retain candidate rows. Reuse DataFusion/Parquet
and compare pruning enabled/disabled rather than building another statistics engine.
Keep filter-only columns available until evaluation even when absent from projection.

Do not memoize/deduplicate the test-only volatile lookup, reorder arbitrary callbacks, or
reuse validation/statistics after data replacement. A future cache requires data identity,
policy/version scope, invalidation and an observed repeated cost. Derived facts are run/batch
local by default. A `Fn` bound or shared reference does not establish purity.

Treat the existing row-cap optimizer example as output policy, not resource protection.
Test repeated application for stability and preserve existing limits/offsets. Do not add
a Datacrate optimizer loop, adaptive predicate scheduler, custom masks, SIMD layer or codecs.

### Acceptance

- UDF: scalar/null scalar, empty/all-null/mixed arrays, zero, -1, -86_400_000_000,
  `i64::MIN/MAX`, wrong arity/type/timezone; exact values, dtype and representation.
- Aggregate: empty/all-null/single/opposite-sign inputs, partial-state merge and partition
  permutations; inspect output schema and validate decimal value precision.
- Ingest: independent schema/value/null/error parity, malformed fields, quoted delimiters and
  multiline records, Unicode, partial/empty batches, short/long strings, varied batch sizes.
- Query: complete expected results, projection with filter-only columns, null/boundary/missing
  statistics, requested ordering, empty/limited output, conditional errors, slow/early-dropped
  stream and spill cleanup. Cancellation claims match the engine/runtime actually exercised.
- Preserve legacy digest bytes. Existing digest downcasts already happen once per batch;
  only investigate per-row hasher-builder setup if profiling shows retained cost.

## Consequences

A few explicit branches and one query-construction boundary can avoid unnecessary work
without a framework migration. More specialized paths require broader semantic tests.
The aggregate correction changes a test extension's output type; it is correctness work,
not an engine speedup. Direct builders and setup hoisting remain measured retention decisions.

Compare scalar preservation with unconditional expansion, direct builders with owned rows,
and shared query construction with duplicated queries. Reject alternatives whose maintenance
cost exceeds demonstrated benefit. Record neutral results and keep baseline defaults when
evidence is inconclusive.

## Sources

- [Datacrate UDFs and queries](../../crates/pipeline/src/datafusion_query.rs),
  [producer](../../crates/pipeline/src/bounded.rs), [ADR 0006](0006-datafusion-query-parity-and-pushdown-proof.md)
