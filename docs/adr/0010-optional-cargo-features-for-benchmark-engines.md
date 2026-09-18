# 0010. Make Ballista and Polars optional Cargo features, not default dependencies

## Status
Accepted

## Context
Ballista and Polars (see [0008](0008-ballista-and-comet-benchmark-engines.md) for why Ballista is
here) were unconditional dependencies of the pipeline crate, even though only a handful of
benchmark-only binaries used either of them. Every default `cargo build` of the pipeline crate -
including builds needing only the core CSV/Arrow/Parquet/DataFusion path - compiled a full
distributed-execution stack and a separate DataFrame engine it never touches.

## Decision
Made both dependencies `optional = true` Cargo features (`dep:` syntax), gated behind
`--features ballista` / `--features polars`, with explicit `required-features` entries on the
affected benchmark-only binaries so they don't exist in a default build rather than failing to
compile. This surfaced a second issue: the pipeline crate's own `tokio` dependency was missing the
`rt-multi-thread` feature, previously satisfied only transitively through Ballista. Fixed by
declaring it directly, since a dependency's transitive features shouldn't be load-bearing for a
crate's own direct usage.

## Consequences
`cargo build -p pipeline` with no feature flags - the path every other crate in the workspace, and
the default release build, actually takes - no longer compiles either Ballista or Polars. Anyone
running the benchmark suite opts in explicitly with the relevant feature flag. The tradeoff is one
more thing to remember when adding a new benchmark binary: it needs its own `required-features`
entry, or it silently becomes part of the default build again.
