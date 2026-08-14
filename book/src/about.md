# About & contributing

`datacrate` is a Rust Cargo workspace for building a typed data pipeline:
streaming CSV tooling, a typestate pipeline builder, and compile-time schema
contracts today, growing toward a fuller Arrow/Parquet/DataFusion pipeline.

## Contributing

See [CONTRIBUTING.md](https://github.com/com-vitthalmirji/datacrate/blob/main/CONTRIBUTING.md)
in the repository root for commit message conventions, branching, and the
pull request process. Run `just verify` before opening a pull request — it
is the same check CI runs.

## Releasing

Versioning is automated by [release-plz](https://release-plz.dev). Commit
type (`feat:`, `fix:`, `feat!:`/`BREAKING CHANGE:`) determines the version
bump for each crate that changed. Merging the release PR publishes
`dtl-core` and `csv-select-cli` to crates.io and creates a GitHub Release.

## License

Licensed under the [MIT license](https://github.com/com-vitthalmirji/datacrate/blob/main/LICENSE).
