# Getting Started

<img src="images/cover.png" alt="datacrate" width="480">

`datacrate` is a Rust Cargo workspace for building a typed data pipeline:
streaming CSV tooling, a typestate pipeline builder, and compile-time schema
contracts today, growing toward a fuller Arrow/Parquet/DataFusion pipeline.

## Prerequisites

- [rustup](https://rustup.rs/) - the pinned toolchain in `rust-toolchain.toml`
  (stable, with `rustfmt` and `clippy`) is installed automatically on first
  `cargo` invocation in this directory.
- [`just`](https://github.com/casey/just) - task runner used for all dev
  commands (`cargo install just` or via your package manager).

## Workspace layout

```
crates/
├── dtl-core/          lib   - ownership/borrowing/slices fundamentals, zero-copy CSV batching
├── csv-cli/           bin   - streaming CSV column-selection CLI (csv-select)
├── pipeline/          lib   - Arrow/Parquet/DataFusion pipeline, object-store I/O,
│                              Ballista distribution, Comet acceleration
├── typestate/         lib   - typestate pipeline builder (source/transform/sink)
├── contracts/         lib   - compile-time schema-conformance checking
├── contracts-derive/  lib   - `#[derive(Contract)]` proc macro backing `contracts`
└── rusty-ready/       lib   - DSA-in-Rust practice, isolated from the other crates
fixtures/         deterministic test data, committed
exercises/        ownership/compiler exercises
```

Why `dtl-core` and not just `core`: Rust ships its own built-in `core`
crate (the `#![no_std]`-compatible base of the standard library). Naming a
workspace crate `core` would shadow it - any `use core::...` elsewhere in
the workspace would silently resolve to the wrong one instead of failing
loudly. `dtl-core` sidesteps the collision entirely.

## Quick start

```sh
git clone <this-repo>
cd datacrate
just verify   # fmt check, clippy (warnings denied), tests, release build
```

Run the CSV column-selection CLI directly:

```sh
cargo run -p csv-select-cli --bin csv-select -- fixtures/m1/headers.csv --column 1
```

Continue to [Usage](usage.md) for a walkthrough of `csv-select` and the
`dtl-core` library, or jump straight to any other chapter: the deep dive on
[ownership and streaming](ownership.md), the
[typestate pipeline builder](typestate.md), the
[DataFusion pipeline](datafusion.md), the
[object-store edge](object-store.md),
[distributing DataFusion and accelerating Spark](distributed-and-acceleration.md),
or [compile-time schema contracts](contracts.md).
