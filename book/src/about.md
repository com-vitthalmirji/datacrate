# About & contributing

`datacrate` is a Rust Cargo workspace for building a typed data pipeline:
streaming CSV tooling, a typestate pipeline builder, and compile-time schema
contracts today, growing toward a fuller Arrow/Parquet/DataFusion pipeline.

## Contributing

See [CONTRIBUTING.md](https://github.com/com-vitthalmirji/datacrate/blob/main/CONTRIBUTING.md)
in the repository root for commit message conventions, branching, and the
pull request process. Run `just verify` before opening a pull request - it
is the same check CI runs.

## Releasing

Versioning is automated by [release-plz](https://release-plz.dev). Commit
type (`feat:`, `fix:`, `feat!:`/`BREAKING CHANGE:`) determines the version
bump for each crate that changed. Merging the release PR publishes
`dtl-core` and `csv-select-cli` to crates.io and creates a GitHub Release.

## License

Licensed under the [MIT license](https://github.com/com-vitthalmirji/datacrate/blob/main/LICENSE).

## Glossary

Rust- and DataFusion-specific terms as used in this book, for a reader who
wants the short version before deciding whether to go read the full chapter.

| Term | Short definition |
|---|---|
| **Ownership** | Every value has exactly one owner at a time; when the owner goes out of scope, the value is dropped. No garbage collector - this is what replaces it. |
| **Borrowing** | Temporarily accessing a value without taking ownership: `&T` (shared/read-only, any number at once) or `&mut T` (exclusive/mutable, only one at a time). Enforced at compile time. |
| **Lifetime** | The compiler's name for "how long a borrow is valid," written `'a`. Exists to stop a borrowed reference from outliving the data it points to - see [Ownership and streaming](ownership.md). |
| **`Option<T>`** | A value that may or may not be present (`Some(T)` / `None`) - Rust's `null` replacement, checked at compile time; identical in shape to Scala's `Option[T]`. |
| **`Result<T, E>`** | A fallible operation's outcome (`Ok(T)` / `Err(E)`) - analogous to `Either[E, A]`; the `?` operator propagates an `Err` early, similar to a `for`-comprehension short-circuiting. |
| **Trait** | Rust's interface mechanism - a set of methods a type can implement. Roughly a Scala `trait` used for typeclass-style ad-hoc polymorphism more often than for inheritance. |
| **Trait object (`dyn Trait`)** | A trait used as a type via dynamic dispatch (a vtable, like a JVM interface reference) - e.g. `Box<dyn Fn(RecordBatch) -> RecordBatch>` in [Typestate pipeline builder](typestate.md). The alternative, generics/`impl Trait`, is static dispatch (monomorphized, like specialized bytecode per call site). |
| **`enum`** | A real sum type - each variant can carry its own data, matching Scala's `sealed trait` + `case class`/`case object` family more closely than a C-style enum. |
| **Closure** | An anonymous function that can capture variables from its environment - same idea as a Scala lambda; `Box<dyn Fn(...)>` is needed when a closure must be stored (a bare `fn` pointer can't capture). |
| **`async`/`.await`** | Cooperative, non-blocking execution - needs an executor (this project uses Tokio) the same way a Scala `Future` needs an `ExecutionContext`. Used narrowly, at the object-store edge, not pervasively - see [The object-store edge](object-store.md). |
| **`unsafe`** | A block where the compiler's memory-safety guarantees are manually asserted rather than checked - not used anywhere in this codebase. |
| **Typestate pattern** | Encoding "which steps have completed" as distinct generic types so an invalid next step is a compile error (`no method named build`), not a runtime check. See [Typestate pipeline builder](typestate.md). |
| **`const fn` / CTFE** | "Constant function" / compile-time function evaluation - code that runs *while `cargo build` is compiling*, not at program runtime. The schema-conformance check runs here, so a mismatch is a build failure. See [Compile-time schema contracts](contracts.md). |
| **Proc(edural) macro** | Code that generates Rust code at compile time by operating on the token stream (via `syn`/`quote`) - what `#[derive(Contract)]` is. Roughly analogous to a Scala 3 macro or an annotation processor, not to a simple text-substitution macro. |
| **`RecordBatch`** | Arrow's columnar, in-memory batch of rows - the closest analogue to a small, local, non-lazy Spark `DataFrame`. See [Coming from Spark/Scala](spark-concept-map.md). |
| **Validity bitmap** | Arrow's per-column, per-row null tracking - one bit per row, not a sentinel value in the data itself. Surfaced in Rust as `Option<T>` at construction time. |
| **`SessionContext`** | DataFusion's entry point for registering tables and running SQL/DataFrame queries - the direct analogue of Spark's `SparkSession`. |
| **`RuntimeEnv`** | DataFusion's memory-limit/spill configuration, attached to a `SessionContext` - the analogue of Spark executor memory config and its own spill-to-disk behavior, though the two engines' spill behavior differs by operator - see [The DataFusion pipeline: failure paths](datafusion.md). |
| **Ballista** | A distributed query engine built as a scheduler + executor pair *around* DataFusion - not a different engine, the same DataFusion logical/physical plan machinery running across a cluster. Analogue: Spark's own cluster execution model, minus the JVM. |
| **DataFusion Comet** | A native (Rust/DataFusion-backed) Spark plugin - replaces JVM scan/shuffle/exec with native code inside a real Spark job. Not a Spark replacement; closer to Databricks' Photon or the Gluten project in spirit. |
| **Shuffle** | Redistributing data across partitions/executors so rows with the same key land together (needed for joins/aggregations) - same concept and cost profile as a Spark shuffle; Ballista's shuffle is hash-partitioned the same way. |
| **Spill (to disk)** | An operator writing intermediate state to disk when it would otherwise exceed a memory limit, to keep running instead of failing - see [The DataFusion pipeline: failure paths](datafusion.md) for which operators do and don't do this today. |
| **Workspace (Cargo)** | A group of crates built and versioned together, sharing one `Cargo.lock` - see the `sbt`/Maven comparison in [Coming from Spark/Scala](spark-concept-map.md). |
| **Crate** | A compilation unit / package - a library crate (`src/lib.rs`) or a binary crate (`src/main.rs` or `src/bin/*.rs`). Roughly one sbt sub-project or one Maven module. |
