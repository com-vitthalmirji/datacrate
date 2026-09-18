# 0001. Proc-macro/build-script diagnostics for `contracts`

## Status

Accepted

## Context

`crates/contracts` checks schema conformance inside a `const fn`, evaluated at
compile time, so a mismatch is a real `rustc` compile error anchored at the
call site. This has no heap available, which gives it two limits: only the
first 8 diffs in a mismatch are named, and each diff's wording comes from
fixed string literals, not free-form formatting.

A Scala reference implementation this crate tracks does the equivalent check
with no such limits, because its macros run as ordinary host code with a real
heap. Rust proc-macros can also run as full host code, but they only see raw
tokens, not resolved types, so matching that would mean either a `build.rs`
pre-pass that re-parses every contract-carrying source file, or a second
proc-macro pass that reads shape data written by the first through `OUT_DIR`.
Both are real, working approaches, but both are meaningfully larger and more
fragile than the current `const fn` gate, and no schema in this repo has
actually hit the 8-diff or fixed-wording limit yet.

## Decision

Keep the `const fn` compile-time gate as the only pass/fail check. Do not
build a proc-macro or build-script replacement now. If a real schema someday
needs diagnostics beyond what fixed messages can say, add a `build.rs`
pre-pass that prints a fuller message to stderr before the real compile error
fires — additive output only, not a new source of truth, so it can't change
whether a build passes or fails.

## Consequences

The two limits (8 diffs, fixed wording) stay in place under this decision.
Anyone needing more will need a new ADR before writing the build-script pass,
since it touches how errors are reported, not just what they say.
