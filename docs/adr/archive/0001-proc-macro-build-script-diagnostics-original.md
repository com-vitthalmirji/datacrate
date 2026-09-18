# ADR-0001: proc-macro/build-script diagnostics to reach literal parity with compile-time-data-contracts

- Status: proposed
- Date: 2026-08-18

## Context

`crates/contracts` checks producer/contract schema conformance inside a `const fn`, evaluated via
CTFE (`SchemaConforms::CHECK`), so a schema mismatch is a genuine `rustc` compile error (`E0080`)
anchored at the call site. This is deliberate: `const fn`/CTFE gives real compiler-anchored errors
with zero runtime cost and zero extra build steps, at the cost of two permanent, disclosed
limitations relative to the Scala reference implementation, `compile-time-data-contracts` (CTDC):

1. **Bounded diffs.** `Diffs` is a fixed `[Option<Diff>; MAX_DIFFS]` array (`MAX_DIFFS = 8`) because
   `const fn` has no heap — no `Vec`, no growable collection. A schema with more than 8 simultaneous
   diffs still correctly fails the check (`Diffs.len` saturates at 8 but never resets to 0), but only
   the first 8 are named in the message.
2. **Bounded message text.** Each diff's message pieces are fixed `&'static str` literals spliced
   into `const_panic::concat_panic!`. There is no `String`, no `format!`, no `mkString` — messages
   are close to CTDC's wording in *category* (missing/extra/mismatch, optional/default annotations,
   expected/found) but cannot do arbitrary formatting.

**Why CTDC doesn't have these limits.** CTDC's compile-time checks are Scala 3 `inline`/quote-splice
macros (`ContractsCore.scala`, `${ ... }` macro bodies), which execute as ordinary JVM code *inside
the Scala compiler process* during compilation. That gives them full `String`/`List`/`mkString` at
macro-expansion time — CTDC's "compile-time" execution environment is not a restricted evaluator
like Rust's CTFE, it's a real host-language macro with a real heap. This is the root reason the two
implementations can't be made byte-identical without changing Rust's execution model for the check,
not a gap in effort or care.

**What Rust's proc-macro system can and cannot do here.** A Rust proc-macro *does* run as native
host code with full `std` (this part is directly analogous to CTDC's macro). The blocker is
different: Rust proc-macros operate on raw token streams before type resolution, with no symbol
table. A macro invocation like `schema_conforms!(Producer, Contract, Exact)` sees only the tokens
`Producer`, `Contract`, `Exact` — it cannot "look up" `Producer`'s struct definition by name the way
CTDC's macro can via the Scala compiler's `Quotes`/`TypeRepr` API, which runs after typer with full
symbol resolution. Closing this gap needs one of two workarounds, both scoped below.

## Decision

**Do not replace the `const fn`/CTFE gate.** Keep it as the pass/fail source of truth — it is
already verified 100% functionally aligned with CTDC (every case CTDC accepts/rejects, Rust
accepts/rejects identically; see gap #1/#4 and Stage C work, all green under
`cargo test -p contracts -p contracts-derive --all-features`). The two remaining gaps are
diagnostic-completeness gaps, not correctness gaps, and closing them fully requires abandoning
CTFE for a materially larger, more fragile mechanism whose cost is not justified without a concrete
schema that actually needs it (no such schema exists in this repo today — confirmed via
`grep -rn "SchemaConforms::<" crates/` before writing this ADR).

If a real need for unbounded/fully-formatted diagnostics does arise, the two viable mechanisms are:

### Option A — build.rs pre-pass, full replacement of the CTFE gate

A `build.rs` script parses every source file with `syn`, re-derives each `#[derive(Contract)]`
struct's shape as normal (heap-backed) Rust data, finds every `SchemaConforms::<P, C, Policy>`
usage, and runs the CTDC-equivalent comparison with full `String`/`Vec`. On failure it either
(a) `eprintln!`s the full message and `std::process::exit(1)`, failing the Cargo build with a
build-script error, or (b) generates a source file containing `compile_error!("<full message>")`
that gets `include!()`d into the checked crate, which anchors the error to a real (if indirect)
span via a second compile pass.

**Effort:** large. Requires re-implementing shape derivation (today only in `contracts-derive`) a
second time in `build.rs`-space, keeping the two implementations in lockstep, handling multi-crate
producer/contract pairs (build scripts only see their own crate's sources), and handling
incremental-compilation correctness (build scripts must correctly declare `cargo:rerun-if-changed`
for every file they parse, or diagnostics go stale).

**Risk:** high. (a) loses `E0080`'s precise call-site span — `trybuild`'s entire existing fixture
suite (7 fixtures) would need to be rewritten around build-script failure output instead of rustc
diagnostics. (b) keeps a real span but adds a second compile pass and an `OUT_DIR`-generated file
dependency, which is exactly the kind of build-graph fragility this crate's `const fn` design was
chosen to avoid in the first place.

### Option B — OUT_DIR side-channel between two proc-macro passes

`#[derive(Contract)]` additionally serializes its shape (e.g. JSON) to a per-type file under
`OUT_DIR`. A second macro (e.g. `schema_conforms!(Producer, Contract, Policy)`) reads both files
back and compares them with full `String`/`Vec`, emitting `compile_error!` on failure.

**Effort:** medium-large. No second shape-derivation implementation (reuses the derive macro's
output), but requires a stable, versioned serialization format and a naming scheme for the
`OUT_DIR` files that survives crate renames/multiple producer-contract pairs per crate.

**Risk:** high, for a different reason: proc-macro expansion order within a crate is not guaranteed
by `rustc`, so there is no guarantee the derive macro's file-write has happened before the
`schema_conforms!` macro's file-read runs. This is the same fragility the `inventory`/`typetag`
crates accept for their own use cases; it is a known, documented footgun in the proc-macro
ecosystem, not a novel risk introduced here.

### Option C — additive, not a replacement (lowest cost, mentioned for completeness)

Keep the `const fn`/CTFE gate exactly as-is (it remains the actual pass/fail authority, with its
current bounded message). Add a `build.rs` pre-pass whose *only* job is to print a fuller,
unbounded, CTDC-style diagnostic to `cargo build`'s stderr *before* the real compile error fires,
as supplementary human-readable output, not a new gate. No change to `trybuild` fixtures, no second
source of pass/fail truth, no span-anchoring loss — the existing `E0080` error still fires and still
anchors correctly; the build-script output is purely additive context.

**Effort:** medium (same shape-re-derivation cost as Option A, but no `compile_error!` generation,
no `include!()`, no incremental-compilation correctness requirement beyond not crashing the build).

**Risk:** low. Worst case, the supplementary message is wrong or stale — the real compile error
still fires correctly regardless, so this cannot introduce a false pass/fail.

## Consequences

- Choosing not to implement any option now means the two disclosed gaps (bounded diff count,
  bounded message formatting) remain permanent under the current architecture — already accepted
  and documented in `crates/contracts/src/lib.rs`'s module doc.
- If diagnostic completeness becomes a real blocker later, **Option C is the recommended starting
  point** if this is revisited: it is additive, cannot regress correctness, and does not touch the
  already-verified, already-green `const fn` gate or its `trybuild` fixture suite. Options A and B
  should only be considered if Option C's supplementary-output UX turns out to be insufficient in
  practice (e.g., users need the full message in the actual compiler error, not adjacent
  build-script output).
- No code changes accompany this ADR. It exists to record the scoping so this question isn't
  re-litigated from scratch next time it comes up.

## Alternatives considered

- **Do nothing, no ADR.** Rejected — the question "are we 100% aligned with CTDC" has come up
  multiple times in this session; recording the answer and the cost of closing the gap avoids
  re-deriving it from scratch in a future session.
- **Raise `MAX_DIFFS` further / add more static message templates.** Already the mitigation in
  place (`MAX_DIFFS` raised 4 → 8 this session); it narrows the gap but cannot close it — any fixed
  bound is still a bound, and `&'static str` splicing is still not arbitrary formatting.
- **Options A/B/C above**, evaluated and not adopted now per the Decision section — no concrete
  schema in this repo needs them yet, and each carries real cost/risk that isn't justified without
  one.
