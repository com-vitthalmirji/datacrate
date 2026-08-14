# Getting Started

<img src="images/cover.png" alt="datacrate" width="480">

`datacrate` is a Rust Cargo workspace for building a typed data pipeline:
streaming CSV tooling, a typestate pipeline builder, and compile-time schema
contracts today, growing toward a fuller Arrow/Parquet/DataFusion pipeline.

## Prerequisites

- [rustup](https://rustup.rs/) — the pinned toolchain in `rust-toolchain.toml`
  (stable, with `rustfmt` and `clippy`) is installed automatically on first
  `cargo` invocation in this directory.
- [`just`](https://github.com/casey/just) — task runner used for all dev
  commands (`cargo install just` or via your package manager).

## Workspace layout

```
crates/
├── dtl-core/          lib   — ownership/borrowing/slices fundamentals, zero-copy CSV batching
├── csv-cli/           bin   — streaming CSV column-selection CLI (csv-select)
├── pipeline/          lib   — CSV fixture → Arrow RecordBatch conversion
├── typestate/         lib   — typestate pipeline builder (source/transform/sink)
├── contracts/         lib   — compile-time schema-conformance checking
├── contracts-derive/  lib   — `#[derive(Contract)]` proc macro backing `contracts`
└── rusty-ready/       lib   — DSA-in-Rust practice, isolated from the other crates
fixtures/         deterministic test data, committed
exercises/        ownership/compiler exercises
```

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
`dtl-core` library, or jump straight to the
[typestate pipeline builder](typestate.md) and
[compile-time schema contracts](contracts.md) pages.
