# 0015. Strengthen contract and pipeline boundaries using Rust traits and const evaluation

## Status

Proposed - 18 October 2026. Design direction and implementation acceptance criteria;
no source changes, completed remediation, or delivery commitment are recorded here.

## Context

I attended [Andre Bogus's standard-library traits talk](https://eurorust.eu/talks/from-deref-into-borrow-a-primer-on-standard-library-traits/)
and [Oli Scherer's compile-time talk](https://eurorust.eu/talks/move-your-problems/)
at EuroRust, took notes and asked the speakers questions. This ADR applies those
learnings to Datacrate, informed by a full review of notes in
`from-deref-into-borrow.md` and `move-problems-to-compile-time.md`, source
inspection, official Rust documentation and isolated compiler probes. It
distinguishes features available on the workspace's pinned Rust 1.97.1 from
future language proposals.

The relevant lessons are broader than adding more typestate parameters:

| Material reviewed                                                                          | Application to Datacrate                                                                                                                                                                          |
|--------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Trait methods, associated types/constants, generic parameters, dynamic dispatch            | Resolve field shapes through types where possible; choose dispatch and associated outputs for actual API requirements.                                                                            |
| `Eq`, `Ord`, `Hash`, arithmetic/assignment operators                                       | Preserve trait laws; schema compatibility is a policy relation, not Rust equality. Avoid a pipeline operator DSL without a concrete need.                                                         |
| `From`, `TryFrom`, `FromStr`, `Default`                                                    | Distinguish infallible representation changes from validation; construct valid execution configuration.                                                                                           |
| `AsRef`, `AsMut`, `Borrow`, `ToOwned`, `Cow`, `Deref`                                      | Expose intentional views without promising false identity or allowing invariant-breaking mutation. Introduce borrowing optimizations only where ownership and measurements justify them.          |
| `Clone`, `Copy`, `Drop`, guards and ownership                                              | Staged publication needs exclusive ownership and explicit fallible completion; cheap shared Arrow clones are not deep copies.                                                                     |
| `FnOnce`, `FnMut`, `Fn`, marker traits, `Send`/`Sync`                                      | Express actual callback invocation and thread-crossing requirements.                                                                                                                              |
| Iterator laziness/size contracts, `Future`/`IntoFuture`, formatting/errors                 | A batch API is not inherently streaming; an iterator length is not a memory bound; awaiting a wrapper does not make blocking I/O asynchronous. Preserve useful error context and safe formatting. |
| Const functions, generic inline consts, forced evaluation                                  | A check needs an evaluation point and a correct algorithm; merely defining a const or satisfying a marker trait does not enforce compatibility.                                                   |
| Reflection, type construction, compile-time allocation/I/O, macro functions, pattern types | Future design research; do not make the current implementation depend on proposals shown in a talk. Reflection also has privacy, semver and parametricity costs.                                  |

Today, [`contracts`](../../crates/contracts/src/lib.rs) compares declarations when
its check is explicitly evaluated. [`typestate`](../../crates/typestate/src/lib.rs)
tracks whether source, transform and sink stages are configured; it has no
dependency on `contracts`. Its local `build` executes an eager CSV-to-batch path.
Its remote builder delegates to the bounded pipeline. Neither builder validates
arbitrary external batches against `Contract::SHAPE`.

[ADR 0014](0014-defer-readiness-findings-with-explicit-guarantee-boundaries.md)
records existing checker, backpressure, publication and fixture defects. This ADR
extends the design analysis; it does not close or supersede those findings.

### Rust semantics used in this decision

The following distinctions guide how the learnings apply to Datacrate; the
linked official documentation supplies the precise trait and language semantics:

- `AsMut<T>` does not require `AsRef<T>`. `AsRef` has forwarding implementations;
  its lack of a universal reflexive implementation is a different issue.
- `Borrow<T>` requires compatible equality, ordering and hashing semantics. A
  case-insensitive key must not implement `Borrow<str>` with ordinary `str`
  semantics. `Deref` is appropriate for pointer-like behaviour, not inheritance
  between domain concepts. See [Borrow](https://doc.rust-lang.org/std/borrow/trait.Borrow.html),
  [AsRef](https://doc.rust-lang.org/std/convert/trait.AsRef.html),
  [AsMut](https://doc.rust-lang.org/std/convert/trait.AsMut.html) and
  [Deref](https://doc.rust-lang.org/std/ops/trait.Deref.html).
- `move` specifies capture ownership; it does not make a closure exclusively
  `FnOnce`. Its body determines whether it consumes, mutates or only reads captures.
  See [closure call traits](https://doc.rust-lang.org/reference/types/closure.html#call-traits-and-coercions).
- `T: Sync` means `&T: Send`. `tokio::spawn` requires a `Send + 'static` future and
  a `Send + 'static` output, not a blanket `Sync` bound. Local spawning has
  different requirements. See [Sync](https://doc.rust-lang.org/std/marker/trait.Sync.html)
  and [spawn](https://docs.rs/tokio/latest/tokio/task/fn.spawn.html).
- Trait-method ambiguity is resolved with qualified syntax such as
  `<Concrete as Trait>::method(&value)`, not by supplying a trait as a method's
  generic argument. See [qualified paths](https://doc.rust-lang.org/reference/paths.html#qualified-paths).
- `Drop` releases resources beyond memory, but cannot report a normal completion
  error to its caller or await a remote abort. Trait-law violations are logic
  errors; they do not authorize unsafe code to assume every safe implementation
  obeys those laws. See [Drop](https://doc.rust-lang.org/std/ops/trait.Drop.html)
  and [Hash](https://doc.rust-lang.org/std/hash/trait.Hash.html).
- A `const fn` may run at runtime. Generic inline consts can use type parameters,
  but evaluation and monomorphization matter. Proposed reflection operations,
  compile-time heap globalization and macro functions are not APIs this ADR
  adopts. See [constant evaluation](https://doc.rust-lang.org/reference/const_eval.html)
  and [const blocks](https://doc.rust-lang.org/reference/expressions/block-expr.html#const-blocks).

## Decision

Propose incremental improvements within the existing crates, with separate
evidence for declaration compatibility, execution configuration and runtime data.
Prioritize correctness before making enforcement more convenient. Do not perform
a framework rewrite or add a generic source/sink trait hierarchy in anticipation
of future integrations.

### 1. Separate compatibility verdicts from diagnostic capacity

In [`contracts`](../../crates/contracts/src/lib.rs), `diagnose_all` fills a bounded
buffer before `filter_diffs` applies the policy. A buffer filled by tolerated
differences can hide a later rejection. Increasing `MAX_DIFFS` only moves the
failure threshold.

Compute the verdict over the complete relevant shape comparison independently of
the bounded diagnostic representation. Diagnostics may truncate; acceptance must
not. Reject ambiguous case-insensitive field matches before treating fields as
corresponding. Keep policy-specific matching explicit.

Do not implement `PartialEq` for `TypeShape` using `Backward`, `Forward` or
case-insensitive compatibility. Directional compatibility is not an equivalence
relation, and structural Rust equality already has a distinct meaning. If a
normalized lookup key is introduced later, its `Eq` and `Hash` must use the same
normalization. A hash index is optional optimization, not a prerequisite.

Acceptance: retain both ADR 0014 reproductions as regressions; cover rejected
differences beyond diagnostic capacity, nested paths, each policy, and ambiguous
names on either side. Confirm the runtime verdict and forced const check agree.

### 2. Make macro metadata strict; investigate resolved field shapes

[`has_default_attr`](../../crates/contracts-derive/src/lib.rs) discards
`parse_nested_meta` errors and marks `default` before validating its trailing
syntax. An isolated probe confirmed that `#[contract(default = false)]` is
accepted as a default declaration, allowing a missing required field under
`Backward` when `CHECK` is explicitly evaluated.

Return and propagate `syn::Result`, preserve the offending span, and reject
unknown keys, unsupported values and duplicate declarations. Document that
`#[contract(default)]` is compatibility metadata: it neither constructs a value
nor proves that a runtime reader supplies one. Do not infer it from `Default`;
having a default value is not permission to silently substitute it during ingest.

Separately, investigate delegating field-shape construction to resolved types:
implement shape metadata for supported primitives and containers, then have the
derive assemble field names and `<FieldType as Contract>::SHAPE`. The existing
macro guesses containers and primitives from token spelling; aliases and custom
types named like standard containers expose the limitations of that approach.
The compiler can resolve types without speculative reflection.

Compare this approach with the current syntax-based implementation before
adoption. Account for coherence, reference handling, supported map keys and
downstream implementation compatibility. Preserve the atomic-map-key restriction
through an explicit supported-key boundary. Keep rejecting generic structs until
their bounds and instantiated shapes are deliberately supported and tested.

Acceptance: compile-fail fixtures for malformed metadata; passing fixtures for
valid defaults; a separate shape-resolution experiment covering aliases,
qualified paths, shadowed names, nesting, maps and unsupported types. No silent
fallback to an opaque spelling for an unsupported resolved type.

### 3. Give declaration checks an explicit execution boundary

After repairing the checker, evaluate a small helper that hides mandatory const
plumbing at a typed API boundary:

```rust,ignore
use contracts::{Contract, SchemaConforms, SchemaPolicyMarker};

fn enforce<P: Contract, C: Contract, M: SchemaPolicyMarker>() {
    const { SchemaConforms::<P, C, M>::CHECK };
}
```

This is a proposed helper, not an existing public API. On pinned Rust 1.97.1,
an isolated called mismatch passed `cargo check` and failed `cargo build` with
E0080; the same generic function with no call built successfully. Therefore a
generic bound or a successful editor check is insufficient evidence. Preserve
explicit item-level `const _: () = ...::CHECK` for concrete checks that should
not depend on a generic call being instantiated.

Do not call a marker trait a proof of compatibility when its implementation is
unconditional and its assertion has not been evaluated. Add build-based negative
fixtures for this enforcement mechanism; a harness that only type-checks can
miss its failure mode. Cover compatible calls, incompatible instantiated calls,
and unused generic code to document the boundary.

This establishes a relation between declared shapes, not the contents of CSV,
Parquet, Delta tables, an object store or a SQL result. No runtime I/O belongs in
this const check. No nightly feature or toolchain upgrade is needed for the
demonstrated helper.

### 4. Validate execution inputs at construction and preserve the invariant

In [`bounded`](../../crates/pipeline/src/bounded.rs), batch size is a public
`usize` checked with an assertion at execution. Prefer validated execution
configuration, using `NonZeroUsize` for batch size or a fallible constructor with
private validated fields. Parse raw CLI/config values into it before opening
files or spawning workers. Channel capacity zero is a valid rendezvous setting;
do not reject it just to reuse the same numeric wrapper.

Apply the same rule to scale-generator batch size: zero must produce an input
error rather than a loop that makes no progress. Retain checked wide arithmetic
for fixture timestamps as required by ADR 0014. Runtime CLI input remains runtime
validation; moving the invariant into a type prevents later misuse, not earlier
knowledge of the user's input. Use `TryFrom` or named validation for fallible
conversions, not a panicking `From` implementation.

In [`parse_amount`](../../crates/pipeline/src/datafusion_query.rs), the fractional
part is parsed as a signed integer. A probe using the current function body
accepted `"1.-1"` as 99 and `"1.+1"` as 101 unscaled units. Define and enforce the
decimal grammar, scale and supported precision. Setting Arrow decimal metadata
alone does not validate every value's precision. This probe covers the parser,
not an end-to-end ingestion reproduction.

Keep the existing `OrderId` domain boundary. Introduce an amount newtype only if
it centralizes a real invariant across callers; otherwise a correct private
parser is sufficient. Do not add `Deref<Target = i64>` to an identifier merely to
avoid explicit access.

Acceptance: zero/valid/overflow configuration boundaries, legal rendezvous
capacity, rejected malformed fractional signs, signed zero and negative amounts,
precision limits, and generator arithmetic at large IDs. Invalid configuration
must fail before side effects.

### 5. Match callbacks and lifecycle names to what execution actually does

The local typestate builder consumes `self` and calls each callback once.
`FnOnce` is sufficient and permits callbacks that consume captured resources.
Repeated batch processing needs `Fn` or `FnMut`; choose `FnMut` if serialized
stateful callbacks are supported. Do not use `FnOnce` for a callback invoked for
every batch. Add `Send`/`Sync` only where the callback actually crosses or is
shared across a thread boundary.

Provide a fallible transform path returning `Result` where bad input is expected.
Currently an arbitrary transform can return a different `RecordBatch`, while
downstream fixture-specific digest code assumes fixed columns and uses `expect`.
Make that boundary explicit instead of converting ordinary validation failures
into panics. Preserve existing contextual error types and sources; a new error
framework is unnecessary.

The local `Sink` is another `RecordBatch -> RecordBatch` callback; the remote
sink names an output object. Align naming with those different responsibilities.
Also distinguish constructing an executable plan from executing I/O: consider a
consuming `run` entry point, or a `build`/`run` split when a reusable plan is
actually needed. Do not add an otherwise empty plan wrapper just for symmetry.

Acceptance: a one-shot callback consuming a capture, a stateful repeated callback
if supported, propagated transform errors, and clear migration examples for any
public signature/name changes. Keep boxed dispatch where useful; benchmark before
claiming a generic replacement improves throughput.

### 6. Add runtime contract evidence through a dedicated adapter

If contract-aware execution is pursued, keep `contracts` independent of Arrow.
Put the mapping from declared shapes to Arrow logical types, field names,
nullability and policy semantics in an adapter. Specify unsupported mappings and
the interpretation of nullable fields rather than assuming Rust types alone
settle them.

A validated-batch wrapper must own private evidence created by actual checking.
Use a fallible constructor or contextual validation function; a public
`PhantomData<C>` tag attached to any batch is not proof. Expose a named immutable
view or `AsRef<RecordBatch>` where appropriate. Do not expose mutable replacement
that preserves a stale contract witness. Revalidate after arbitrary transforms,
or require a separately justified typed transform boundary.

Keep the claims distinct:

| Evidence                         | What it establishes                                                         | What it leaves open                                                                                    |
|----------------------------------|-----------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|
| Missing/Present builder states   | Required stages were configured.                                            | Successful I/O, schema agreement, publication and data quality.                                        |
| Evaluated declaration check      | Declared shapes satisfy the chosen implemented policy.                      | External data matching either declaration; checker correctness still needs regression evidence.        |
| Runtime batch validation         | The checked batch satisfies the adapter's specified checks.                 | Unchecked semantic rules, future batches and post-validation transformations.                          |
| Successful publication operation | The chosen publication protocol completed under its documented assumptions. | Cross-object transactions, crash recovery or input snapshot consistency unless separately implemented. |

Acceptance before advertising integration: supported-type mapping tests, invalid
runtime batches, validation after transformation, and an end-to-end path that
actually invokes the check. This remains proposed work, not a current capability.

### 7. Hide publication mechanics behind an owned, fallible lifecycle

Refactor publication responsibilities toward an owner of a unique staged
artifact. Its explicit consuming `finish`/`commit` returns `Result`; a local
`Drop` guard may perform best-effort cleanup of an uncommitted artifact. Such a
resource should not be `Copy` or casually `Clone`.

Remote multipart completion/abort requires explicit asynchronous handling where
applicable. Destructors cannot provide guaranteed asynchronous cleanup, and a
guard does not resolve process crashes, conflicting writers, retries or separate
output/manifest replacement. Choose and test the actual publication protocol in
the ADR 0014 follow-up before adding typestate names that imply atomicity.

Within the existing pipeline crate, separate fixture-specific decoding/schema
knowledge, bounded batch transport and publication orchestration. The current
`Row`/fixed-schema/digest coupling limits reuse. Make those responsibilities
coherent modules first; extract crates or generic extension traits only when a
second concrete implementation establishes the required boundary. Producer
backpressure and multipart backpressure remain separate resource controls.

Acceptance: cleanup on ordinary failures, exclusive staging ownership,
completion/abort errors, replacement and concurrent-writer tests, and the
documented recovery story. Do not promise crash safety from RAII alone.

### 8. Define the manifest digest's portability and equality semantics

[`hash_row`](../../crates/pipeline/src/manifest.rs) manually feeds fixed-seed
AHash with an integer, variable-length strings and an optional-value tag; row
hashes are combined with wrapping addition. Fixed seeds alone do not specify a
portable persisted format: [AHash 0.8.12](https://docs.rs/ahash/0.8.12/ahash/)
does not promise identical outputs across hardware or versions.

First decide whether verification is scoped to the same supported build/platform
or must survive upgrades and exchange between platforms. For a persisted portable
digest, specify a canonical encoding with unambiguous field boundaries, a stable
algorithm, and encoding/algorithm version metadata. Changing digest semantics
requires an explicit compatibility/migration policy for existing manifests.

For process-local structural hashing, implementing/using `Hash` on the row's
structure can express field boundaries; it still does not make the result a
portable wire format. Do not infer a demonstrated collision merely from adjacent
`write` calls: [Hasher](https://doc.rust-lang.org/std/hash/trait.Hasher.html) does
not require multiple calls to equal one concatenated write. The issue here is an
unspecified persistence contract, not a reproduced collision.

The existing order-independent sum intentionally cannot detect row reordering.
A non-cryptographic fingerprint is not a collision-free equality proof or an
authenticity mechanism. Acceptance: document those limits, null/empty and field
boundary fixtures, chunking/order semantics, and cross-version/platform fixtures
if portability is promised. Select a stronger digest only for an identified
integrity requirement, not as an incidental trait refactor.

### Sequencing and scope

1. Repair verdict completeness, ambiguous matching and malformed macro metadata.
2. Validate runtime configuration/parsing; repair the already recorded arithmetic
   and multipart defects in their respective follow-ups.
3. Evaluate shape resolution and explicit enforcement ergonomics with isolated
   fixtures before changing public APIs.
4. Improve callback/error/lifecycle boundaries and specify digest compatibility.
5. Add a runtime adapter and builder integration only against concrete pipeline
   requirements, with separate declaration and runtime evidence.

Treat exported contract metadata as an intentional compatibility surface. Deriving
it for a type can expose private fields through public associated metadata;
changing those fields can then affect consumers even if the Rust fields remain
private. Prefer dedicated contract models when domain/internal representation
should evolve independently. Future reflection would not remove that obligation.

No change to dbt's role, lakehouse transaction semantics, distributed consensus,
or orchestration is proposed. Integration with those systems requires their own
runtime boundaries. Do not add custom operators, blanket conversion wrappers,
`IntoFuture` around blocking work, speculative reflection, unsafe code, or more
stage markers as substitutes for the concrete checks above.

## Consequences

The proposed direction gives each invariant one owner and makes the difference
between configuration, declarations, data and publication reviewable. It can
reduce misuse without claiming that Rust types establish external data quality
or distributed correctness.

Costs include stricter macro inputs, possible callback/API migrations, trait
coherence constraints, runtime validation work proportional to the checks, and
manifest compatibility decisions. These are trade-offs requiring focused
implementation reviews; the design is not a promise of free performance or
complete compile-time enforcement.

Evidence gathered on Rust 1.97.1: generic inline-const mismatch passed `check` but
failed `build`; an unused generic helper built; malformed default metadata built
with an explicit compatibility check; the copied amount parser accepted both
malformed fractional signs; `AsMut` without `AsRef` compiled. These were isolated
probes, not source fixes or new workspace regression tests. Proposed acceptance
checks above remain work to implement. ADR 0014's existing baseline and open
findings remain unchanged.
