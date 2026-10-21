# 0021. Resolve contract shapes through traits and map Arrow schemas explicitly

## Status

Accepted - 20 October 2026. Supersedes this ADR's earlier investigation-only
position. Implements the two migrations considered in ADR 0015.

## Context

Earlier, the derive recognized primitive and container names in source syntax.
Aliases could fail compilation and shadowed generic containers could receive an
incorrect shape. Arity checks prevented panics without solving type resolution.
A declaration tree also needs explicit representation choices before it can
configure Arrow builders or writers.

## Decision

Delegate every field to `<FieldType as Contract>::SHAPE`. Implement Contract for
standard scalars, Option, Vec, HashMap and BTreeMap, preserving declaration labels
and nested optionality. Keep generic derives unsupported. A private atomic-key
whitelist accepts aliases of supported keys and rejects structured or floating
keys. Custom container names use their own Contract implementation.

References transparently inherit the referent's shape. This consumes coherence
space: previous manual downstream Contract implementations for `&Local` and
`&mut Local` conflict, even if they declare the same shape as Local. Remove redundant
reference implementations. For a distinct reference declaration, put its former shape
on a reference-holding newtype and migrate the corresponding fields/checks. Do not
change Local's declaration to represent two different contracts. Shared and mutable
references follow the same policy, including aliases and repeated references.

Mark both contracts and contracts-derive as 0.2.0, and constrain the derive dependency
to that version line. This is a pre-1.0 breaking change, not a compatible 0.1 update.
Both crates remain unpublished. A downstream build fixture proves the newtype
migration preserves a distinct shape while Local and its ordinary references retain
Local's shape. The failing override fixture records the intentional coherence boundary.
Merging remains the reviewer's decision; these changes do not claim external consumer
approval or publish a release. Review manual reference implementations and declarations
that previously depended on aliases or shadowed names before adopting 0.2.

Add an opt-in `arrow` feature depending only on arrow-schema. Its pure schema and
field functions return typed errors with paths. The consumer is schema construction
for Arrow builders and writers, demonstrated by the contract_schema example.
The default crate remains independent of Arrow and execution engines.

Map fixed-width 8/16/32/64-bit numbers, bool and strings directly. Structs preserve
field order; optionality applies independently at each physical field. Lists use
32-bit offsets and an item field. Maps use unsorted non-null entries structs with
non-null keys and independently nullable values. Key kinds follow the declaration
whitelist. Default attributes do not imply physical nullability or construct values.

Reject adjacent optional layers, unknown primitive labels, platform-sized integers,
128-bit integers, chars and missing timestamp/decimal metadata. Reject duplicate
field names and recursion beyond 128 levels, including cyclic manually supplied
shapes. Do not invent units, scale, timezone or platform width.

Keep the fixed-ingest validator and checked-batch witness. Schema mapping does not
inspect arrays, certify nested validity, enforce map-key uniqueness or prove business
rules. String and str can map to identical Utf8 schemas while remaining distinct
declarations. List/map offset capacity remains a runtime construction constraint.

## Alternatives and trade-offs

Keeping syntax recognition avoids blanket implementation conflicts but retains
incorrect shadowed shapes. A separate field-shape trait adds another extension
boundary and still needs reference coherence rules. Resolved Contract implementations
reuse the existing abstraction at the cost of the explicit reference compatibility
change. Future standard-type implementations must consider the same coherence cost.

An optional schema-only adapter avoids coupling declaration checks to execution
engines. It refuses representations absent from the current tree rather than
silently flattening information. A general runtime array validator remains a separate
API; schema construction is deliberately not a checked-data witness.

## Validation

Alias and qualified-path regressions reproduced compilation failures with the prior
derive. Resolved-shape tests cover aliases, borrowed aliases, nested containers,
shadowed containers and aliased map keys. Build fixtures reject unsupported fields,
non-atomic keys and reference overrides. Existing policy and diagnostic fixtures
remain in place. Independent Arrow schema expectations cover nested nullability,
map layout, scalar mappings and typed refusal paths, including cyclic shapes.

## Sources

- [Contract declarations](../../crates/contracts/src/lib.rs)
- [Derive implementation](../../crates/contracts-derive/src/lib.rs)
- [Arrow mapping](../../crates/contracts/src/arrow.rs)
- [Fixed ingest checks](../../crates/pipeline/src/batch_contract.rs)
- [ADR 0015](0015-strengthen-contract-and-pipeline-boundaries-from-rust-trait-and-const-design.md)
