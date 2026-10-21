<img src="assets/cover.png" alt="datacrate" width="640">

[![CI](https://github.com/com-vitthalmirji/datacrate/actions/workflows/ci.yml/badge.svg)](https://github.com/com-vitthalmirji/datacrate/actions/workflows/ci.yml)
[![Release](https://github.com/com-vitthalmirji/datacrate/actions/workflows/release-plz.yml/badge.svg)](https://github.com/com-vitthalmirji/datacrate/actions/workflows/release-plz.yml)
[![Audit](https://github.com/com-vitthalmirji/datacrate/actions/workflows/audit.yml/badge.svg)](https://github.com/com-vitthalmirji/datacrate/actions/workflows/audit.yml)
[![Docs](https://github.com/com-vitthalmirji/datacrate/actions/workflows/docs.yml/badge.svg)](https://com-vitthalmirji.github.io/datacrate/)
[![crates.io](https://img.shields.io/badge/crates.io-vim89-orange.svg)](https://crates.io/users/vim89)
[![Rust](https://img.shields.io/badge/rust-1.97%2B-orange.svg)](https://www.rust-lang.org)
[![License: MIT](https://img.shields.io/github/license/com-vitthalmirji/datacrate.svg)](LICENSE)
---

# datacrate

`datacrate` is an end-to-end data-engineering platform in Rust, under active
development. One Cargo workspace brings together streaming CSV/Arrow ingestion,
compile-time schema contracts, typestate pipeline builders, Parquet storage,
DataFusion SQL and DataFrame queries, and cloud object-store I/O. Ballista examples
extend query execution across scheduler and executor processes; Comet examples
explore native acceleration inside Spark.

The engineering focus is on explicit pipeline boundaries: compiler-resolved type
shapes and const-evaluated compatibility checks, fallible callbacks with checked
batch output, bounded batch transport with optional byte admission, captured object
identity, and owned output publication with recoverable failure information. The
core pipeline runs natively in Rust; Comet operates within Spark's JVM runtime.

### Where the guarantees apply

- **Contracts** compare Rust declarations when an explicit `SchemaConforms::CHECK`
  or `contracts::enforce` check is evaluated during a build. The optional Arrow
  adapter maps supported declarations to physical schemas.
- **Typestate** makes source, transform and sink configuration a compile-time
  requirement. Callback errors remain explicit runtime results.
- **Ingest validation** checks actual callback output against the current fixed
  `id`, `name`, nullable `note` schema before accepting it at the pipeline boundary.

These layers complement each other. External data still needs runtime validation;
memory accounting, schema mappings and publication guarantees have defined scopes
explained in the guide.

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

| Command        | What it does                                               |
|----------------|------------------------------------------------------------|
| `just fmt`     | `cargo fmt --all --check`                                  |
| `just lint`    | `cargo clippy --all-targets --all-features -- -D warnings` |
| `just test`    | `cargo test --all-targets --all-features --locked`         |
| `just release` | `cargo build --profile ci-release --locked`                           |
| `just verify`  | all of the above, plus `git diff --check`                  |

Run `just verify` before opening a pull request - it is the same check CI runs.

### Git hooks

Enable the repo's pre-commit hook (runs `just verify` before each commit) once per clone:

```sh
git config core.hooksPath .githooks
```

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

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for commit message conventions,
branching, and the pull request process.

## Documentation

- Guide (published): https://com-vitthalmirji.github.io/datacrate/
- API reference (rustdoc, published): https://com-vitthalmirji.github.io/datacrate/api/
- API reference (also on docs.rs): https://docs.rs/dtl-core
- API reference (local): `cargo doc -p dtl-core --open`
- Guide (local): `cargo install mdbook --locked && mdbook serve book`

For the precise boundaries and migration steps, see:

- [Contract checks, Arrow mapping and the 0.2 reference migration](book/src/contracts.md)
- [Callback errors and fixed-ingest validation](book/src/typestate.md)
- [Memory, cancellation and local publication](book/src/datafusion.md)
- [Captured object reads and remote publication](book/src/object-store.md)
- [Dependency audit policy and recorded exceptions](docs/adr/0009-supply-chain-policy-gate.md)

A green dependency audit reflects the advisory database and configured exceptions
at that run. Dependency fixes must reach `main` before related issues are treated
as resolved there.
