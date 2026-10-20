# 0021. Keep resolved shapes and general Arrow mapping behind explicit migration decisions

## Status

Proposed - 20 October 2026. Bounded stable-Rust experiments completed; no production
shape-resolution or general Arrow adapter migration is included. Refines ADR 0015.

## Context

The derive currently recognizes standard-looking type names in syntax. A local generic
type named `Option<T>` is consequently recorded as optional even when its own `Contract`
implementation declares another shape. Type aliases and qualified primitive paths can
also require implementations that the syntax-based derive does not provide.

A resolved-type implementation can replace this guessing with compiler trait resolution.
That does not make adding blanket implementations automatically compatible. A schema
adapter also needs representation choices beyond a tree of Rust declarations.

## Evidence

On pinned stable Rust 1.97.1, an isolated candidate made derive emit each field's
`<FieldType as Contract>::SHAPE`. Primitive, Option, Vec, map and reference implementations
supplied resolved shapes. It passed alias/canonical equality, qualified paths, nested
containers, a shadowed container's real Contract, borrowed views and aliased atomic map keys.
The existing derive failed the shadowed-container expected-shape assertion at runtime.

A sealed supported-key boundary rejected sequence map keys and downstream attempts to
extend atomic keys. Unsupported fields and generic derives failed compilation. These are
bounded probe results; they do not establish compatibility for every downstream type.

The candidate's blanket reference implementation produced E0119 for an existing legal
pattern: a downstream local type implements Contract and its reference has a different
manual Contract implementation. Replacing syntax recognition with standard/container
implementations therefore requires an explicit public coherence and reference policy.

A separate Arrow schema probe mapped known scalar, nullable and list shapes, and refused
unknown primitives, maps and nested optionality without chosen representations. It also
confirmed that distinct `Primitive("String")` and `Primitive("str")` declarations can map
to the same Arrow field. Arrow schema equality is not Rust declaration identity.

## Decision

Keep the existing fixed-ingest runtime validator and its checked batch witness. It has a
real consumer, validates actual batches and is re-established after arbitrary transforms.
Do not replace it with a general adapter based only on successful schema construction.

Do not silently install the probe's blanket reference implementation. A production migration
must choose whether references inherit the referent's contract, whether explicit downstream
reference implementations remain supported, and how aliases of borrowed types behave.
Document that choice as a compatibility change and add downstream build fixtures before
changing derive. Preserve the existing monomorphic derive boundary and explicit atomic key
restriction unless another decision deliberately changes them.

Keep Arrow mapping separate from declaration comparison. Before adding a general adapter,
name its consumer and specify nullable structures, nested optionality, list element validity,
map keys, unsupported primitives, timestamp units/timezones, decimal precision/scale and
platform-sized integers. Return typed unsupported-mapping errors where information is absent.
A schema mapping alone must not certify nested array values or arbitrary business rules.

## Alternatives and trade-offs

Delegating every field to Contract makes aliases and shadowing follow the compiler, but
broadens public implementations and consumes coherence space. A separate field-shape trait
can isolate responsibilities, but introduces another public extension boundary and needs its
own overlap/reference analysis. Keeping syntax recognition preserves the current API at the
cost of its documented limitations. It is retained pending a deliberate migration, not
presented as equivalent correctness.

A general Arrow adapter would support more consumers, but none currently requires the full
mapping. The fixed-ingest validator avoids speculative mappings and keeps runtime evidence
private. It remains intentionally narrower than an arbitrary Contract-to-Arrow bridge.

## Consequences

The investigations are evaluated with concrete passing and failing cases. The existing
syntax limitation remains unresolved in production. No generic contract runtime guarantee,
lossless Arrow representation or backwards-compatible blanket implementation is claimed.
These follow-up migrations require source changes and build/runtime evidence of their own.

## Sources

- [Contract declarations](../../crates/contracts/src/lib.rs)
- [Derive implementation](../../crates/contracts-derive/src/lib.rs)
- [Fixed ingest checks](../../crates/pipeline/src/batch_contract.rs)
- [ADR 0015](0015-strengthen-contract-and-pipeline-boundaries-from-rust-trait-and-const-design.md)
