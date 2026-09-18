<img src="assets/cover.png" alt="datacrate" width="640">

[![CI](https://github.com/com-vitthalmirji/datacrate/actions/workflows/ci.yml/badge.svg)](https://github.com/com-vitthalmirji/datacrate/actions/workflows/ci.yml)
[![Release](https://github.com/com-vitthalmirji/datacrate/actions/workflows/release-plz.yml/badge.svg)](https://github.com/com-vitthalmirji/datacrate/actions/workflows/release-plz.yml)
[![Audit](https://github.com/com-vitthalmirji/datacrate/actions/workflows/audit.yml/badge.svg)](https://github.com/com-vitthalmirji/datacrate/actions/workflows/audit.yml)
[![Docs](https://github.com/com-vitthalmirji/datacrate/actions/workflows/docs.yml/badge.svg)](https://com-vitthalmirji.github.io/datacrate/)
[![crates.io](https://img.shields.io/crates/v/dtl-core.svg)](https://crates.io/crates/dtl-core)
[![Rust](https://img.shields.io/badge/rust-1.97%2B-orange.svg)](https://www.rust-lang.org)
[![License: MIT](https://img.shields.io/github/license/com-vitthalmirji/datacrate.svg)](LICENSE)
---

# datacrate

An end-to-end data engineering platform in Rust: one typed Cargo workspace
spanning the whole pipeline lifecycle - streaming ingestion, compile-time
schema contracts, typed transforms, columnar storage (Parquet), a
SQL/DataFrame query engine (DataFusion), distributed execution (Ballista),
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

See [Getting Started](https://com-vitthalmirji.github.io/datacrate/getting-started.html)
for a walkthrough of the workspace and each chapter.

## Prerequisites

- [rustup](https://rustup.rs/) - the pinned toolchain in `rust-toolchain.toml`
  (stable, with `rustfmt` and `clippy`) is installed automatically on first
  `cargo` invocation in this directory.
- [`just`](https://github.com/casey/just) - task runner used for all dev
  commands below (`cargo install just` or via your package manager).

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

## Development

| Command        | What it does                                              |
|----------------|------------------------------------------------------------|
| `just fmt`     | `cargo fmt --all --check`                                  |
| `just lint`    | `cargo clippy --all-targets --all-features -- -D warnings` |
| `just test`    | `cargo test --all-targets --all-features --locked`         |
| `just release` | `cargo build --release --locked`                            |
| `just verify`  | all of the above, plus `git diff --check`                   |

Run `just verify` before opening a pull request - it is the same check CI runs.

### Git hooks

Enable the repo's pre-commit hook (runs `just verify` before each commit) once per clone:

```sh
git config core.hooksPath .githooks
```

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for commit message conventions,
branching, and the pull request process.

## Documentation

- Guide (published): https://com-vitthalmirji.github.io/datacrate/
- API reference (rustdoc, published): https://com-vitthalmirji.github.io/datacrate/api/
- API reference (also on docs.rs): https://docs.rs/dtl-core
- API reference (local): `cargo doc -p dtl-core --open`
- Guide (local): `cargo install mdbook --locked && mdbook serve book`
