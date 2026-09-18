# Coming from Spark/Scala

This chapter is for one reader: someone who already thinks in Spark/Scala
terms - DataFrames, the Catalyst optimizer, executors, shuffles, `sbt` - and
is meeting Rust here for the first time. If that's not you, skip ahead to
[Rust fundamentals](rust-fundamentals.md).

If you're new to Rust *and* have no Spark background, that's fine too - read
the right-hand column of the table below as plain definitions and ignore the
left-hand column; every later chapter explains its own terms as it goes, and
the [glossary](about.md#glossary) is there whenever a term doesn't stick.

## The workspace, in `sbt`/Maven terms

| Rust / Cargo | Scala / sbt (or Maven) equivalent |
|---|---|
| Workspace `Cargo.toml` | Root `build.sbt` (multi-project build) |
| A crate's own `Cargo.toml` | A sub-project's `build.sbt` block / a Maven module's `pom.xml` |
| `[dependencies]` | `libraryDependencies` |
| `cargo build` / `cargo build --release` | `sbt compile` / `sbt assembly` (debug vs. optimized artifact, not "dev vs. prod config") |
| `cargo test` | `sbt test` |
| `cargo run -p <crate> --bin <name> -- <args>` | `sbt "<project>/run <args>"` |
| `cargo run -p <crate> --example <name>` | no direct sbt equivalent - a runnable demo file that ships with the crate but isn't its main binary |
| `cargo clippy` | `scalafix` / `scalastyle` (lint, not just format) |
| `cargo fmt` | `scalafmt` |
| `Cargo.lock` (always committed) | `build.sbt` + resolved versions being deterministic - Cargo makes this an explicit, committed file rather than resolver-time reproducibility |

The biggest structural difference from a typical Spark/Scala monorepo: the
crates in this workspace **don't** form a shared-kernel dependency chain the
way a Spark app depends on a shared `common` module. Each crate is
dependency-isolated from the others on purpose - there's no `common` crate
here to reach for out of habit. See [Getting started](getting-started.md) for
the actual crate list and what each one is for.

## Concept map

This is the fast path in. Concepts you already have; this just gives you
their name on this side.

| You know this from Spark/Scala | The equivalent here | Where |
|---|---|---|
| `SparkSession` | `datafusion::execution::context::SessionContext` | [The DataFusion pipeline](datafusion-basics.md) |
| `Dataset[T]` / `DataFrame` | `arrow::record_batch::RecordBatch` - a columnar, in-memory batch, not lazy, not distributed by itself | [Ownership and streaming](ownership.md), [The DataFusion pipeline](datafusion-basics.md) |
| Catalyst's logical/physical plan | DataFusion's `LogicalPlan` / `ExecutionPlan` - same two-phase optimizer shape, different implementation | [The DataFusion pipeline](datafusion-basics.md) |
| `df.sql(...)` vs. the DataFrame builder API both compiling to the same plan | The same claim, but tested, not assumed: queries are written both ways and asserted equal | [The DataFusion pipeline](datafusion-basics.md) |
| Spark executors + shuffle across a cluster | Ballista's scheduler/executor pair - a distributed *DataFusion*, not a different engine, with the same hash-partitioned shuffle idea | [Distributing DataFusion](distributed-and-acceleration.md) |
| `spark.sql.shuffle.partitions` / broadcast-join threshold tuning | `spark.sql.autoBroadcastJoinThreshold=-1` used on the Spark side of a comparison for the same reason you'd use it: force a real shuffle join, not a silent broadcast | [Proving it at scale](scale-and-parity.md) |
| JVM heap / `spark.executor.memory` and spill-to-disk under memory pressure | DataFusion `RuntimeEnv`'s `max_bytes` + its own spill-to-disk path - but spill behavior differs *by operator* here, a real divergence from "Spark just spills," not a detail to gloss over | [The DataFusion pipeline: failure paths](datafusion.md) |
| Spark-native execution accelerators (Photon on Databricks, Gluten, etc.) | DataFusion Comet - a native (Rust/DataFusion-backed) Spark plugin that replaces JVM scan/shuffle/exec with native code while staying inside a real Spark job, not a Spark replacement | [Accelerating Spark](distributed-and-acceleration.md) |
| Parquet, columnar storage, predicate/projection pushdown | Same Parquet format, same pushdown ideas - Arrow is the in-memory columnar model Spark's Tungsten row/columnar format plays a similar role for | [Ownership and streaming](ownership.md) |
| `null` in a Spark column / `Option` in idiomatic Scala | Arrow's per-column **validity bitmap** (a real bit per row, not a sentinel value) surfaced in Rust as `Option<T>` at construction time | [Ownership and streaming](ownership.md) |
| A case class with a compile-time-checked schema, like Spark's `Encoders.product[T]` | `contracts`' `#[derive(Contract)]` - but pushed further: a schema mismatch between two independently-evolving types fails `cargo build` itself, not just serialization | [Compile-time schema contracts](contracts.md) |
| A fluent/typed builder that only compiles when required fields are set (rare in Scala without a library) | `typestate`'s `PipelineBuilder` - a builder whose `.build()` method **only exists** in the type-state where every stage is present | [Typestate pipeline builder](typestate.md) |
| Scala's `Future`/`ExecutionContext`, or ZIO/Cats-Effect fibers | Rust's `async`/`.await` + Tokio - used narrowly here, not pervasively, because this codebase treats async as an edge concern rather than the default | [The object-store edge](object-store.md) |
| `sealed trait` + `case class`/`case object` ADT | Rust `enum` - a real sum type, each variant can carry its own data - used throughout for errors and states | [Typestate pipeline builder](typestate.md) |
| `Option[T]` / `Either[E, A]` | `Option<T>` / `Result<T, E>` - same shape, but `Result`'s `?` operator is Rust's terser analogue to a `for`-comprehension over `Either` | throughout |
| Ownership/borrowing - the one concept with **no** Scala/JVM analogue | The compiler enforces, at compile time, that data has exactly one owner (or many read-only borrowers, or one mutable borrower) at a time - nothing to unlearn from Scala, this is genuinely new | [Rust fundamentals](rust-fundamentals.md), [Ownership and streaming](ownership.md) |

## Suggested reading order

Not the book's table-of-contents order - this is *learning* order for a
Spark/Scala reader specifically:

1. **This chapter** - orient before reading any code.
2. **[Rust fundamentals](rust-fundamentals.md)**, focused on ownership and
   borrowing - the one prerequisite with no Spark analogue; everything else
   assumes it. Budget real time here.
3. **[Ownership and streaming](ownership.md)** - the closest thing to
   familiar ground: a `RecordBatch` looks and behaves like a small, local
   `DataFrame`.
4. **[The DataFusion pipeline](datafusion-basics.md)** - this is where a
   Spark background pays off fastest: `SessionContext`, SQL vs. DataFrame-API
   parity, and later, distributed execution via Ballista and the direct
   Spark/Comet comparison.
5. **[Typestate pipeline builder](typestate.md)** and
   **[Compile-time schema contracts](contracts.md)** - two compile-time
   guarantees with no direct Scala equivalent, but both solve problems a
   Spark/Scala engineer has hit at runtime before: an incompletely-configured
   job, a schema drift caught in production instead of CI.
6. **[The DataFusion pipeline: failure paths](datafusion.md)** and
   **[The object-store edge](object-store.md)** - the concurrency/I/O
   internals; read once the above feels solid, not before.

Then use the [glossary](about.md#glossary) whenever a term isn't landing.
