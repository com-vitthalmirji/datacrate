# datacrate

A Rust Cargo workspace for building a typed data pipeline: streaming CSV
tooling today, growing toward an Arrow/Parquet/DataFusion pipeline and
typestate-based builder APIs.

## Workspace layout

```
crates/
├── dtl-core/     lib   — ownership/borrowing/slices fundamentals, zero-copy CSV batching
├── csv-cli/      bin   — streaming CSV column-selection CLI (csv-select)
├── pipeline/     lib   — Arrow/Parquet/DataFusion pipeline (in progress)
├── typestate/    lib   — typestate builder patterns (in progress)
└── rusty-ready/  lib   — DSA-in-Rust practice, isolated from the other crates
fixtures/         deterministic test data, committed
exercises/        ownership/compiler exercises
```

## Prerequisites

- [rustup](https://rustup.rs/) — the pinned toolchain in `rust-toolchain.toml`
  (stable, with `rustfmt` and `clippy`) is installed automatically on first
  `cargo` invocation in this directory.
- [`just`](https://github.com/casey/just) — task runner used for all dev
  commands below (`cargo install just` or via your package manager).

## Quick start

```sh
git clone <this-repo>
cd datacrate
just verify   # fmt check, clippy (warnings denied), tests, release build
```

Run the CSV column-selection CLI directly:

```sh
cargo run -p csv-cli --bin csv-select -- fixtures/m1/headers.csv --column 1
```

## Development

| Command        | What it does                                              |
|----------------|------------------------------------------------------------|
| `just fmt`     | `cargo fmt --all --check`                                  |
| `just lint`    | `cargo clippy --all-targets --all-features -- -D warnings` |
| `just test`    | `cargo test --all-targets --all-features --locked`         |
| `just release` | `cargo build --release --locked`                            |
| `just verify`  | all of the above, plus `git diff --check`                   |

Run `just verify` before opening a pull request — it is the same check CI runs.

### Git hooks

Enable the repo's pre-commit hook (runs `just verify` before each commit) once per clone:

```sh
git config core.hooksPath .githooks
```

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for commit message conventions,
branching, and the pull request process.

## Releasing

Versioning is automated by [release-plz](https://release-plz.dev). Commit
type (`feat:`, `fix:`, `feat!:`/`BREAKING CHANGE:`) determines the version
bump for each crate that changed. On every merge to `main`, release-plz opens
or updates a "release" pull request with the version bump and changelog for
affected crates. Merging that PR publishes `dtl-core` and `csv-cli` to
crates.io, tags the release, and creates a GitHub Release; a follow-up
workflow then attaches prebuilt `csv-select` binaries for Linux, macOS, and
Windows. `pipeline`, `typestate`, and `rusty-ready` are excluded from
publishing.

## License

Licensed under the [MIT license](LICENSE).
