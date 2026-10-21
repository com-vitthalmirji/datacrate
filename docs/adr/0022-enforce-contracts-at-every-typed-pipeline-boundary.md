# 0022. Enforce contracts at every typed pipeline boundary

## Status

Proposed - 21 October 2026. Design for review; not implemented.

## Context

Contract comparisons and builder completeness currently work independently. Callers
must request declaration comparisons explicitly. Existing builders accept opaque
RecordBatch callbacks, so configuration completeness does not establish compatible
contracts between stages. The local and remote ingest builders retain a fixed schema.

The Scala project's Spark builder carries its current contract, pins declared
transform output at runtime, and requests SchemaConforms evidence at the sink.
A transform declares its output but not its required input. Datacrate's new API
must check every connection, including intermediate transform inputs. Replacing
opaque batches with phantom tags alone would not validate returned data.

## Requirements

- Automatically enforce source-output to first-transform-input, every subsequent
  transform connection, and final-output to sink-input declaration compatibility.
- Allow any finite number of declared transforms, including schema changes and a
  source directly connected to a sink. A sink is terminal; execution is unavailable
  without a source and sink. A second source cannot reset an existing chain.
- Preserve existing PipelineBuilder and RemotePipelineBuilder APIs and behavior.
- Process batches incrementally without collecting the input stream, introducing
  background workers, or claiming a process-wide memory bound.
- Validate actual source and transform output before passing it downstream.
- Report the failing stage, batch index, and original error. On failure, stop
  downstream callbacks and release the iterator and retained batches through ownership.
- Keep current fixed-ingest writing, digest, cancellation, publication and snapshot
  mechanisms intact. The new composition layer does not replace them.

## Alternatives

1. Retrofit contract parameters into existing builders. This changes existing
   signatures, couples generic contracts to a fixed schema and complicates migration.
2. Add contract-carrying batch stages with automatic connection checks. This keeps
   existing APIs stable and permits distinct nominal types with compatible shapes.
3. Accept only Rust value-to-value stages. Native parameter types provide strong
   nominal checking but do not cover the current Arrow/DataFusion callback boundary
   or structurally compatible declarations without separate conversions.

Recommend option 2. It adds a separate API surface in exchange for preserving the
existing APIs and making Arrow-stage declarations explicit.

## Proposed API and execution

The API sketch names the intended operations; final generic spelling is implementation detail:

```rust,ignore
let pipeline = ContractPipeline::source::<Bronze>(read_batches)
    .transform::<BronzeInput, Silver>(clean)
    .transform::<SilverInput, Gold>(aggregate)
    .sink::<GoldInput>(write_batch)
    .build()?;
pipeline.run()?;
```

Source factories produce a fallible iterator of RecordBatch values. Construction
stores the factory without opening input. Transform and sink callbacks are fallible
FnMut operations invoked per batch, allowing owned state without requiring Sync.
The source factory is invoked once when execution begins. Source iterator creation
and iteration errors retain separate stage context. A successful sink returns unit;
execution reports processed batch and row counts, without inventing a generic digest.

Each transform's input declaration is checked against the preceding stage's output
using SchemaConforms with ExactOrdered. Its output declaration becomes the current
contract. Sink attachment checks the final connection. Callers do not write enforce
or CHECK themselves: the builder forces the const checks during compilation of each
instantiated connection. Build fixtures must exercise real calls under cargo build;
cargo check or unused generic functions alone cannot prove the check is enforced.

ExactOrdered initially preserves names, order, scalar identity and nested optionality.
Transform input and preceding output may be distinct Rust types with equal shapes.
Do not silently reorder, project, cast or supply default values. Other compatibility
policies require an explicit physical adaptation design before this executor offers
them. Existing standalone contract policies remain available and unchanged.

Build preflights all declared Arrow mappings before source I/O. Unsupported physical
representations return typed mapping errors. Runtime output validation uses the
existing Contract-to-Arrow mapping rather than duplicating scalar/container mappings.
Schema construction must not be described as array validation.

Actual batch validation checks names, order, types and nested nullability declarations,
then rejects forbidden actual nulls at active struct/list/map positions. Children
masked by a null nullable parent are not independently required to be present.
Lists and maps must respect sliced offsets and visible ranges. Nullable map values
and list elements are independent of container validity; map keys are required.
Do not claim business-rule validation or map-key uniqueness. Unsupported adjacent
optional layers and types lacking physical metadata remain explicit mapping errors.

Validate source output before the first downstream callback and every transform output
before the next callback, including the final sink. Declared compatible shapes do not
prove an opaque callback returns its declared shape. No unchecked public constructor
may turn an arbitrary batch into evidence of successful validation.

Use pure mapping/comparison/validation calculations. Keep reading, callback execution
and writing as owned actions. Failed validation stops subsequent actions, but cannot
undo effects performed inside a callback or earlier sink calls. The pipeline does not
promise transactional publication, retries, idempotence or exactly-once effects.

## Delivery and acceptance

One additive feature PR, with three strategic commits:

1. Add general Arrow batch validation and runtime regressions.
2. Add contract-aware stage composition, examples and build fixtures.
3. Document automatic stage checks, migration boundaries and the final tested API.

Required build fixtures:

- Different nominal declarations with equal ordered shapes compile.
- Source/first-transform mismatch, middle-transform mismatch, and sink mismatch
  each fail compilation without a user-written CHECK or enforce call.
- Nested optionality, changed field order/name/type and a missing required field
  reject at the appropriate connection.
- Missing source/sink, duplicate source and appending after a sink reject.
- Source-to-sink and multiple-transform chains compile.
- Instantiated generic pipeline helpers enforce the same connection checks.

Required runtime regressions:

- Matching batches traverse all stages once in order; schema-changing transforms
  produce the declared result. Empty batches and an empty input iterator succeed.
- A lying source or transform fails before downstream invocation, including a
  schema mismatch on a later batch after earlier successful sink calls.
- Required nulls are rejected at root, struct child, list item and map value/key
  positions where safe Arrow construction permits the malformed case. If Arrow
  rejects construction first, record that precondition rather than adding unsafe
  fixtures. Null parent masking and sliced collection offsets remain valid.
- Unsupported Arrow representations fail preflight before source I/O.
- Factory, iterator, transform and sink failures retain stage/batch/source context,
  stop downstream work and release owned iterator state.
- Existing builder fixtures and fixed-ingest behavior remain unchanged.

Run focused tests and build fixtures for affected crates, formatting and Clippy.
No performance measurements are part of this feature. Final source review and green
CI are required before merging. The feature is incomplete until both compile-time
and runtime acceptance cases pass; documentation is not a substitute.

## Claim supported after acceptance

Every connection in the contract-aware pipeline automatically checks declared schema
compatibility during compilation. Actual source and transform batches are validated
at runtime before downstream processing. This does not automatically type-check
arbitrary SQL text, infer opaque callback semantics, or apply to the legacy builders.

## Sources

- [Contract enforcement](../../crates/contracts/src/lib.rs)
- [Arrow mapping](../../crates/contracts/src/arrow.rs)
- [Existing builders](../../crates/typestate/src/lib.rs)
- [Fixed ingest validation](../../crates/pipeline/src/batch_contract.rs)
- [Resolved shape and representation decisions](0021-keep-shape-and-arrow-migrations-explicit.md)
