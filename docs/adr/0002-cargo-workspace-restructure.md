# 0002. Restructure into a Cargo workspace, name the domain crate `dtl-core`

## Status
Accepted

## Context
The project started as a single crate with a flat `src/` directory. As scope grew to cover a
streaming CSV CLI, an Arrow/Parquet pipeline, DataFusion queries, and typestate builders, a single
crate no longer matched the natural boundaries between these pieces — they have different
dependency footprints and different audiences (library code vs. binaries vs. exercises).

The workspace's domain-logic crate needed a name. The obvious choice, `core`, collides with Rust's
own built-in `core` crate — not a compile error by itself, but a silent footgun: any accidental
unqualified `core::` reference resolves to the wrong crate. The shadowing is easy to miss in
review.

## Decision
Split the project into a Cargo workspace under `crates/`, with each crate scoped to one concern
(domain fundamentals, CLI binaries, the Arrow/Parquet/DataFusion pipeline, typestate builders).
Named the domain-logic crate `dtl-core` instead of `core`, trading a slightly longer name for
avoiding the shadowing hazard entirely.

## Consequences
Each crate pulls in only the dependencies it needs — the CLI binary doesn't compile DataFusion,
and vice versa. New crates are added only when they have a clear boundary and will be used
independently, not speculatively. The cost is a small amount of workspace-plumbing overhead
(per-crate `Cargo.toml` files, workspace-level lint configuration) that a single-crate layout
wouldn't have.
