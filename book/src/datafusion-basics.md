# The DataFusion pipeline: from CSV to a running query

The previous chapters got you a typed `RecordBatch` in memory
(`ownership.md`) and a typestate builder that assembles a pipeline
(`typestate.md`). This chapter is the missing middle step: how a CSV fixture
actually becomes a Parquet-backed DataFusion table you can `SELECT` from,
join, and extend with your own function. Everything here lives in
`crates/pipeline/src/lib.rs` and `crates/pipeline/src/datafusion_query.rs`.
The next chapter (`datafusion.md`) assumes you already know this and jumps
straight to failure paths and memory limits — read this one first if you
haven't written a DataFusion query before.

## From CSV to a typed `RecordBatch`

`schema()` (`lib.rs:171`) fixes the shape every batch in this crate shares:
`id: Int64` (not null), `name: Utf8` (not null), `note: Utf8` (nullable).
That nullability isn't decoration — an empty `note` field in the CSV becomes
a real Arrow null, not an empty string standing in for "missing":

```rust
pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("note", DataType::Utf8, true),
    ]))
}
```

`fixture_to_record_batch` (`lib.rs:259`) does the actual conversion in two
steps you can trace directly: `read_csv_rows` (`lib.rs:216`) opens the file
and turns each CSV record into a typed `Row` via a caller-supplied parse
function, and `batch_from_rows` (`lib.rs:238`) builds one Arrow array per
column by mapping over the parsed rows —

```rust
let ids: Int64Array = rows.iter().map(|r| r.id).collect();
let names: StringArray = rows.iter().map(|r| Some(r.name.as_str())).collect();
let notes: StringArray = rows.iter().map(|r| r.note.as_deref()).collect();
```

— then hands all three columns to `RecordBatch::try_new(schema(), columns)`,
which checks each array's type and length against the schema and fails
rather than building a batch that would panic later. Run the fixture through
it:

```console
$ cargo test -p pipeline converts_headers_fixture_into_typed_columns -- --nocapture
```

**Scala/Spark bridge**: this is `spark.read.csv(...).schema(mySchema)`
compressed down to library-call granularity, with one difference worth
noticing: Spark infers or is handed a schema and lazily builds a DataFrame
over partitions you never see directly. Here, `batch_from_rows` is the whole
"infer, then materialize" step, visible and synchronous — you're looking at
the exact code that walks every row and builds every column, not a planner
that does it for you behind an API boundary.

If one file is too large for a single batch, `fixture_to_record_batches`
(`lib.rs:274`) does the same conversion but chunks the parsed rows into
multiple same-schema batches — the shape every streaming/bounded pipeline in
this crate (`bounded.rs`, covered in `datafusion.md`) is built from.

## Writing and reading Parquet

`write_parquet` (`lib.rs:317`) takes a batch, a path, and a `Compression`
setting, and writes a single-row-group Parquet file:

```rust
pub fn write_parquet(
    batch: &RecordBatch,
    path: &Path,
    compression: Compression,
) -> Result<(), PipelineIoError> {
    let mut writer = open_parquet_writer(path, batch.schema(), compression)?;
    writer.write(batch)...;
    writer.close()...;
    Ok(())
}
```

`read_parquet` (`lib.rs:339`) reverses it — open the file, build a reader,
collect every batch the reader yields, and concatenate them back into one
`RecordBatch` with `arrow::compute::concat_batches`. The round-trip test
(`lib.rs:496`, `parquet_round_trip_preserves_schema_nulls_and_values`) is the
part worth internalizing: it doesn't just check row counts, it checks that
every column — including which specific `note` values are null — comes back
identical:

```console
$ cargo test -p pipeline parquet_round_trip_preserves_schema_nulls_and_values -- --nocapture
```

**Scala/Spark bridge**: `df.write.parquet(path)` / `spark.read.parquet(path)`
minus the distributed-filesystem bookkeeping (partition directories,
`_SUCCESS` markers, multiple part-files) — one process, one file, one row
group. The nullability guarantee is the same one Parquet always gives you;
what's different is that the test above *proves* it in-process instead of
trusting the format's spec.

## Registering a table and running your first query

DataFusion doesn't query `RecordBatch` values directly — it queries
*tables*, which are usually Parquet files registered against a
`SessionContext`. `register_orders` (`datafusion_query.rs:146`) is the whole
registration step:

```rust
pub async fn register_orders(ctx: &SessionContext, path: &Path) -> Result<(), DataFusionError> {
    ctx.register_parquet(
        "orders",
        path.to_string_lossy().as_ref(),
        datafusion::prelude::ParquetReadOptions::default(),
    )
    .await
}
```

Once `orders` is registered, `row_query_sql` (`datafusion_query.rs:457`)
runs an ordinary filter/projection/ordering query through the SQL string
API:

```rust
pub async fn row_query_sql(ctx: &SessionContext) -> Result<Vec<RecordBatch>, DataFusionError> {
    ctx.sql(
        "SELECT id, amount, placed_at, note FROM orders \
         WHERE amount > 50.00 ORDER BY placed_at ASC",
    )
    .await?
    .collect()
    .await
}
```

`row_query_dataframe` (`datafusion_query.rs:473`) runs the *identical* query
through the DataFrame builder API instead — `.filter(...)`, `.select(...)`,
`.sort(...)` — and a test
(`row_query_sql_and_dataframe_paths_agree`, `datafusion_query.rs:714`)
asserts both paths produce exactly the same batches. That pairing (one SQL
function, one DataFrame function, one agreement test) repeats for every
query in this file — aggregation, joins, windows, grouped aggregation — so
once you've read one pair you've read the pattern for all of them.

```console
$ cargo test -p pipeline row_query_sql_and_dataframe_paths_agree -- --nocapture
```

**Scala/Spark bridge**: `ctx.sql("...")` vs `ctx.table("orders").filter(...)`
is exactly `spark.sql("...")` vs `df.filter(...).select(...)` — two front
ends over the same logical plan, and DataFusion proves it the same way Spark
does: both compile down to the same optimized plan before execution. The
`SessionContext` itself is the `SparkSession` analogue — one per query
context, tables registered against it by name.

## Aggregation and grouping

`aggregate_query_sql` (`datafusion_query.rs:496`) collapses the filtered
rows to a single `COUNT(*)`/`SUM(amount)` row:

```rust
ctx.sql(
    "SELECT COUNT(*) AS order_count, SUM(amount) AS total_amount \
     FROM orders WHERE amount > 50.00",
)
```

`group_by_bucket_query_sql` (`datafusion_query.rs:567`) does the grouped
version — `GROUP BY id % 1000`, one row per bucket — and its DataFrame
counterpart shows how a computed column gets added before aggregating:

```rust
ctx.table("orders")
    .await?
    .with_column("bucket", col("id") % lit(1000_i64))?
    .aggregate(
        vec![col("bucket")],
        vec![count(lit(1)).alias("order_count"), sum(col("amount")).alias("total_amount")],
    )?
```

**Scala/Spark bridge**: `.with_column("bucket", ...)` is `.withColumn(...)`,
`.aggregate(groupExprs, aggExprs)` is `.groupBy(...).agg(...)` — same shape,
same order of operations (compute the grouping key, then reduce).

## Window functions

Aggregation collapses rows down to one per group. A window function keeps
every row and adds a value computed *across* a set of rows related to it —
here, a rank relative to every other order. `window_query_sql`
(`datafusion_query.rs:285`) ranks every order by `amount`, highest first,
without losing any rows:

```rust
ctx.sql(
    "SELECT id, amount, RANK() OVER (ORDER BY amount DESC) AS amount_rank \
     FROM orders ORDER BY amount_rank, id",
)
```

`window_query_dataframe` (`datafusion_query.rs:300`) builds the identical
`RANK()` through the DataFrame API — `rank().order_by(...).build()`, added as
a column via `.window(...)` rather than a `SELECT`-level expression:

```rust
let rank_expr = rank()
    .order_by(vec![col("amount").sort(false, false)])
    .build()?
    .alias("amount_rank");

ctx.table("orders")
    .await?
    .window(vec![rank_expr])?
    .select(vec![col("id"), col("amount"), col("amount_rank")])?
```

Same agreement-test pattern as every other query pair in this file:

```console
$ cargo test -p pipeline window_query_sql_and_dataframe_paths_agree -- --nocapture
```

**Scala/Spark bridge**: `RANK() OVER (ORDER BY amount DESC)` is
`Window.orderBy(desc("amount"))` plus `rank().over(windowSpec)` in Spark —
same concept (a per-row value computed over an ordered/partitioned set of
rows without collapsing them), same SQL keyword.

## Multi-predicate filtering

Real queries rarely filter on one column. `multi_predicate_query_sql`
(`datafusion_query.rs:615`) combines two conditions with `AND` before
aggregating — orders over 50.00 *and* with a non-null `note`:

```rust
ctx.sql(
    "SELECT COUNT(*) AS order_count, SUM(amount) AS total_amount \
     FROM orders WHERE amount > 50.00 AND note IS NOT NULL",
)
```

`multi_predicate_query_dataframe` (`datafusion_query.rs:633`) chains the same
two conditions with `.and(...)` on the DataFrame's `.filter(...)`:

```rust
.filter(
    col("amount")
        .gt(lit(50.0_f64))
        .and(col("note").is_not_null()),
)?
```

There's nothing new mechanically here beyond `row_query_sql` — the point is
that predicates compose the same way in both APIs, so adding a second
condition to either is a one-line change, not a restructure.

```console
$ cargo test -p pipeline multi_predicate_query_sql_and_dataframe_paths_agree -- --nocapture
```

**Scala/Spark bridge**: `WHERE a > 50.00 AND b IS NOT NULL` is
`.filter($"a" > 50.00 && $"b".isNotNull)` — the `.and(...)` chain on
DataFusion's `Expr` plays the same role as Spark's `&&` on `Column`.

## What can go wrong

Every conversion and I/O function in `lib.rs` returns `Result<_,
PipelineIoError>`. The variants map directly to the steps you just read
through:

| Variant | Produced when |
|---|---|
| `OpenInput` | the CSV fixture path can't be opened |
| `ReadRecord` | the `csv` crate fails to read or parse a record |
| `InvalidId` | the `id` column isn't a valid integer |
| `InvalidAmount` | the `amount` column isn't a fixed-scale-2 decimal |
| `InvalidTimestamp` | the `placed_at` column isn't a valid `YYYY-MM-DDTHH:MM:SS` timestamp |
| `BuildBatch` | `RecordBatch::try_new` rejects the assembled arrays (mismatched type/length against the schema) |
| `OpenOutput` | the Parquet output file can't be created (covered in `datafusion.md`'s failure-path tests) |
| `WriteParquet` | the `ArrowWriter` fails while writing or closing |
| `ReadParquet` | the Parquet reader fails while opening or reading |
| `DownloadObject` / `UploadObject` | an object-store transfer fails (covered in `object-store.md`) |

Each variant carries the context you'd need to act on it — a path, a row
index and the offending value, or the underlying library error — rather than
collapsing everything into one opaque message.

## Joins

Two fixtures are involved once joins enter the picture: `orders` (the table
above) and `shipments`, registered the same way via `register_shipments`
(`datafusion_query.rs:225`) against `shipments_schema()`
(`datafusion_query.rs:159`) — `order_id: Int64`, `carrier: Utf8`,
`shipped_at: Timestamp`. `order_id` is a foreign key into `orders.id`, not a
primary key: an order can have zero shipments (unshipped) or more than one
(split shipment).

`join_query_sql` (`datafusion_query.rs:242`) runs a `LEFT JOIN` so unshipped
orders survive with `NULL` shipment columns instead of disappearing:

```rust
ctx.sql(
    "SELECT orders.id, orders.amount, shipments.carrier, shipments.shipped_at \
     FROM orders LEFT JOIN shipments ON orders.id = shipments.order_id \
     ORDER BY orders.id, shipments.carrier",
)
```

`join_query_dataframe` (`datafusion_query.rs:258`) is the same join via
`.join(shipments, JoinType::Left, &["id"], &["order_id"], None)`. The test
`join_keeps_unshipped_orders_and_duplicates_multi_shipment_orders`
(`datafusion_query.rs:842`) is the one to read closely — it names both
effects a LEFT JOIN has that an INNER JOIN wouldn't: unshipped orders (ids
5, 6, 7) keep a single null row each, and a multi-shipment order (id 3,
shipped via both DHL and FedEx) appears twice.

```console
$ cargo test -p pipeline join_keeps_unshipped_orders_and_duplicates_multi_shipment_orders -- --nocapture
```

**Scala/Spark bridge**: `orders.join(shipments, orders("id") === shipments("order_id"), "left_outer")`
— same join key, same semantics for unmatched rows on the preserved side.
The specific thing to watch for in both engines is the same trap:
`JOIN`/inner join silently drops unmatched rows, which is exactly why
`join_aggregate_query_sql` (`datafusion_query.rs:520`) — an *inner* join used
deliberately to compute shipped-orders-only totals — is a different function
from `join_query_sql`, not a flag on it.

## Writing your own scalar function

`days_since_epoch_udf` (`datafusion_query.rs:338`) is a one-argument scalar
UDF — `days_since_epoch(placed_at) -> BIGINT` — built with `create_udf`:

```rust
pub fn days_since_epoch_udf() -> ScalarUDF {
    let implementation = Arc::new(|args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let timestamps = args[0].as_any().downcast_ref::<TimestampMicrosecondArray>()...;
        let days: Int64Array = timestamps
            .iter()
            .map(|micros| micros.map(|m| m.div_euclid(MICROS_PER_DAY)))
            .collect();
        Ok(ColumnarValue::from(Arc::new(days) as ArrayRef))
    });

    create_udf(
        "days_since_epoch",
        vec![DataType::Timestamp(TimeUnit::Microsecond, None)],
        DataType::Int64,
        Volatility::Immutable,
        implementation,
    )
}
```

Two things matter here beyond "how do I write a UDF." First, the
implementation operates on whole `ArrayRef`s, not row-by-row callbacks —
`timestamps.iter().map(...)` walks one Arrow array and produces another,
which is what makes this vectorized rather than a per-row function call.
Second, `Volatility::Immutable` isn't a formality: it tells DataFusion's
optimizer the function is safe to constant-fold. `register_udf` puts it on
the context, then it's callable from SQL like any built-in:

```rust
ctx.register_udf(days_since_epoch_udf());
ctx.sql("SELECT id, days_since_epoch(placed_at) AS days FROM orders ORDER BY id")
```

```console
$ cargo test -p pipeline days_since_epoch_udf_computes_expected_value_on_real_data -- --nocapture
```

The companion test
`days_since_epoch_udf_gets_same_constant_folding_as_builtin_equivalent`
(`datafusion_query.rs:1115`) proves the `Volatility::Immutable` claim by
comparing this UDF's `EXPLAIN` plan against the equivalent built-in
`CAST(...)/86400000000` expression on the same literal input — both fold to
the same literal at plan time, so a custom Rust UDF doesn't cost you
optimizer treatment a built-in gets for free.

**Scala/Spark bridge**: `create_udf` plus `register_udf` is
`spark.udf.register("days_since_epoch", ...)` — register once, call by name
from SQL or the DataFrame API afterward. The optimizer-folding guarantee is
the one thing Spark doesn't give you automatically: a Spark UDF is a
Catalyst-opaque black box by default (no constant folding, no predicate
pushthrough) unless you go out of your way with a Catalyst expression
extension. DataFusion's `Volatility` flag is that opt-in made explicit and
ordinary.

## Where this leads

You now have the mechanics `datafusion.md` assumes: a table registered, a
query run through SQL or DataFrame, a join, an aggregation, a custom
function. That chapter picks up from here and asks what happens when things
go wrong — cancellation, memory limits, spilling, the specific asymmetry
between how a hash join and a hash aggregate behave once memory runs out.
`context_with_filter_pushdown` (`datafusion_query.rs:417`), which turns on
row-level Parquet filter pushdown, and `explain_analyze`
(`datafusion_query.rs:404`), which reads `EXPLAIN ANALYZE` plans for runtime
metrics, both live in that next chapter too.
