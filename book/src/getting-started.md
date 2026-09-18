# Getting Started

<img src="images/cover.png" alt="datacrate" width="480">

`datacrate` is an end-to-end data engineering platform in Rust: one typed
Cargo workspace spanning the whole pipeline lifecycle - streaming ingestion,
compile-time schema contracts, typed transforms, columnar storage (Parquet),
a SQL/DataFrame query engine (DataFusion), distributed execution (Ballista),
and cloud object storage - the same ground a JVM data stack covers, without
the JVM.

Schema contracts are the anchor: declare what a dataset's columns and types
must be once, and every stage that reads or writes it is checked against
that declaration at compile time. A schema mismatch fails `cargo build`,
not a pipeline run three hours in.

JVM data stacks pay for GC pauses and schema drift you only discover at
runtime - this proves the same job gets done without either. Rust's
ownership model and compile-time schema contracts catch memory and schema
bugs before a job ever runs, with Arrow, DataFusion, and Ballista supplying
the columnar engine and distributed execution - all in one Cargo workspace
covering ingestion, transforms, storage, query, and distributed execution
end to end.

Using Ballista or Comet feels just like Spark, so that part is easy. Rust
itself is the hard part - once you go past using the pipeline and start
building your own handlers and pieces.

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
└── rusty-ready/       lib   - general Rust practice and playground, isolated from the other crates
│                              (exercises/ subdir: workshop compiler-error drills)
fixtures/         deterministic test data, committed
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
