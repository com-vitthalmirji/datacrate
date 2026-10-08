# Extending DataFusion: six extension points, proved not assumed

Every query so far in this book runs *through* DataFusion - CSV in, SQL
against a registered table, `RecordBatch`es out. This chapter is about the
other direction: the points where **you** plug code *into* DataFusion's own
planning and execution machinery, rather than just calling it. All six live
in `crates/pipeline/src/datafusion_query.rs`, behind `#[cfg(test)]` - they
exist to prove the extension API end-to-end, not to ship as production
UDFs, so each one has a paired test that checks its output against an
independently computed expected value, never against itself.

Same rule as every other chapter in this book: claim, code, live test,
then the bridge back to something you already know from Spark/Scala.

## A custom `TableProvider`: your own data source, not just a format

**Claim**: DataFusion doesn't require your data to come from a file format
it already understands. A `TableProvider` can hand back rows from anywhere
- including, trivially, memory you already hold - and the planner treats
it exactly like a built-in table.

**Code**: `OrdersMemoryTableProvider` wraps a `Vec<RecordBatch>` already
held in memory. Its `scan()` method does the one thing every
`TableProvider` must do: given an optional column projection, return an
`ExecutionPlan`. `OrdersMemoryExec` is that plan - a single-partition leaf
node that replays the batches through DataFusion's own `MemoryStream` (the
same stream `MemTable` uses internally), applying the projection rather
than hand-rolling a second one.

```rust
async fn scan(
    &self,
    _state: &dyn Session,
    projection: Option<&Vec<usize>>,
    _filters: &[Expr],
    _limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let projection = projection.cloned();
    let projected_schema = match &projection {
        Some(indices) => Arc::new(self.schema.project(indices)?),
        None => Arc::clone(&self.schema),
    };
    Ok(Arc::new(OrdersMemoryExec::new(
        self.batches.clone(),
        projected_schema,
        projection,
    )))
}
```

Three tests prove three different claims about the same provider, each
against an independent source of truth:

```console
$ cargo test -p pipeline custom_table_provider_plan_names_its_own_exec -- --nocapture
$ cargo test -p pipeline custom_table_provider_pushes_projection_into_scan -- --nocapture
$ cargo test -p pipeline custom_table_provider_matches_parquet_provider_row_count -- --nocapture
```

- `custom_table_provider_plan_names_its_own_exec` - runs `EXPLAIN SELECT *
  FROM orders_mem` and checks the physical plan text actually names
  `OrdersMemoryExec`, proving the planner picked up the custom node rather
  than silently falling back to something built-in.
- `custom_table_provider_pushes_projection_into_scan` - selects two of four
  columns and checks the *returned* schema has exactly those two, proving
  `scan()`'s `projection` argument actually narrowed the output rather than
  being ignored.
- `custom_table_provider_matches_parquet_provider_row_count` - runs
  `COUNT(*)` against the memory-backed table and the Parquet-backed one
  over the same fixture, and asserts the two counts are equal - the
  cross-check that the hand-rolled path and the built-in path agree.

**The trade-off, named explicitly**: `OrdersMemoryTableProvider`
materializes every batch up front and keeps all of them resident for the
table's whole lifetime. That's the right choice for a fixture-sized table.
A table larger than memory would need `scan()` to return a plan that reads
incrementally instead - a real streaming source, not this one.

**Scala/Spark bridge**: this is the same slot Spark's `DataSourceV2` API
fills - implement a reader, hand back something that produces
`InternalRow`s, and the Catalyst optimizer treats your source like any
built-in one (pushing down projections and filters where your reader
reports it can accept them). The shape is identical: the extension point
is "produce rows, on request, lazily" - the planner doesn't care whether
those rows come from a Parquet file, a JDBC connection, or a `Vec` sitting
in a test fixture.

## A custom `AggregateUDF`: the multi-phase accumulator, not a one-shot function

**Claim**: a custom aggregate isn't a single function call over a column -
it's a *protocol* (`update_batch`, `state`, `merge_batch`, `evaluate`) that
DataFusion runs because the same aggregate must work whether it executes
over one partition or many in parallel.

**Code**: `amount_range(amount)` returns `MAX(amount) - MIN(amount)`.
`AmountRangeAccumulator` tracks two running values - `min` and `max` - which
is the point: a single-value accumulator (a custom `SUM`) would never need
`state()` or `merge_batch()` to do anything interesting, so it wouldn't
prove the multi-phase mechanism. Tracking two forces
`AmountRangeUdf::state_fields` to override the trait's one-field default,
and forces `merge_batch` to actually combine two partial `(min, max)`
pairs rather than just summing two numbers.

```console
$ cargo test -p pipeline amount_range_udaf_matches_independently_computed_max_minus_min -- --nocapture
```

The test's expected value - `99,999,999 - 1` unscaled - is computed by
hand from the fixture file, not derived from the accumulator under test.

**The part a single end-to-end SQL test can't prove**: the fixture's 8
rows fit in one Parquet row group, so a real query over it never actually
reaches `AggregateMode::Partial` - DataFusion only splits into
partial/final phases across multiple partitions. A second, lower-level
test closes that gap directly:

```console
$ cargo test -p pipeline amount_range_accumulator_merge_batch_combines_partial_states -- --nocapture
```

It builds two `AmountRangeAccumulator`s by hand (one fed `0.01..250.75`,
the other `45.50..999999.99`), serializes each with `state()`, merges both
into a fresh accumulator, and checks the merged min/max match the full
fixture's actual range - proving the combine logic that an 8-row fixture's
SQL plan would otherwise never exercise.

**Scala/Spark bridge**: this is exactly Spark's own
`Aggregator`/`UserDefinedAggregateFunction` contract -
`update`/`merge`/`evaluate`, with `merge` existing specifically because
Spark's own partial-aggregate-then-combine execution model needs it. The
difference worth noting: DataFusion's accumulator mutates `&mut self`
across calls (an Action, not a pure function - the trait's own docs call
out that calling `update_batch` twice on the same batch gives a wrong
answer), while `evaluate` reads the accumulated state back out without
consuming it. Same split Spark's own `Aggregator` API has, just made more
visible because Rust's ownership model forces every mutating call to say
`&mut self` explicitly.

## A custom `WindowUDF`: one calculation over a whole partition

**Claim**: a window function and an aggregate function solve different
shaped problems even when they're built from the same running total - a
window function returns one output row *per input row*, not one row per
group.

**Code**: `percent_of_total(amount) OVER ()` returns each row's share of
the whole partition's total. `PercentOfTotalEvaluator::evaluate_all` takes
the full column and row count in one call and returns a same-length output
array - no `OVER (PARTITION BY ...)` clause in the test, so the whole table
is one partition, and the window function's whole job is computing every
row's percentage of that one partition's sum in a single pass.

```console
$ cargo test -p pipeline percent_of_total_udwf_matches_independently_computed_percentages -- --nocapture
```

The test's expected percentages are computed by hand from the fixture's 8
raw unscaled amounts (summing to `100_154_174`), then checked two ways:
each row's percentage matches the hand-computed value, *and* all 8
percentages sum back to exactly 100.0 - the second check is what actually
proves `evaluate_all` saw the same partition total for every row, rather
than, say, a per-batch slice of it.

**Why `evaluate_all` and not the row-at-a-time `evaluate`**:
`PartitionEvaluator` offers both; `evaluate_all` computes the whole
partition in one call, which is the right trade for this file's 8-row
fixture. A partition too large to fit in memory would need the
row-at-a-time path plus an explicit window frame instead - not something
this fixture is large enough to force.

**Scala/Spark bridge**: the same distinction Spark SQL's own window
functions draw between `GROUP BY` (collapses rows) and `OVER (...)`
(keeps every row, attaches a computed value). `PartitionEvaluator` is
DataFusion's version of the per-partition buffer Spark's Catalyst window
exec holds while it computes `RANK()`, `LAG()`, or a running `SUM() OVER`
- same shape, same reason: the function needs to see the whole partition
(or a window of it) before it can answer for any single row.

## An async `ScalarUDF`: when producing one value needs an `.await`

**Claim**: DataFusion's scalar-function extension point isn't limited to
synchronous CPU work. A scalar function whose evaluation genuinely needs
to await something - a remote lookup, a cache call, another service - has
its own trait, and the physical planner handles the difference
transparently.

**Code**: `region_multiplier(id)` stands in for a per-id pricing-tier
lookup that - in a real system - would be a network call.
`RegionMultiplierUdf::lookup` is a fixed in-memory match statement here,
not a real request; what's proved is the *shape* (await something, then
return the same `ColumnarValue` contract a sync UDF returns), not real
network latency or partial failure.

```rust
async fn invoke_async_with_args(
    &self,
    args: ScalarFunctionArgs,
) -> Result<ColumnarValue, DataFusionError> {
    tokio::task::yield_now().await;
    let ids = /* ... */;
    let multipliers: Float64Array = ids.iter().map(|id| id.map(Self::lookup)).collect();
    Ok(ColumnarValue::Array(Arc::new(multipliers)))
}
```

The `tokio::task::yield_now().await` isn't incidental - it's there so a
passing test actually proves the physical plan awaited the function,
rather than happening to run it to completion synchronously and never
genuinely yielding.

```console
$ cargo test -p pipeline async_udf_region_multiplier_matches_independently_computed_lookup -- --nocapture
```

**The one thing worth noticing about the interface**: the sync path
(`ScalarUDFImpl::invoke_with_args`, used by this file's other scalar UDF,
`days_since_epoch_udf`) and the async path
(`AsyncScalarUDFImpl::invoke_async_with_args`) have the *same shape* - same
arguments in, same `ColumnarValue` out. The only thing the interface hides
is whether producing that value can run synchronously. DataFusion's
physical planner absorbs that difference itself: it inserts a special
execution node (`AsyncFuncExec`) only when a plan actually contains an
async function, so `region_multiplier`'s own SQL call site - plain
`SELECT id, region_multiplier(id) ... FROM orders` - never has to know or
care which path it took.

**Scala/Spark bridge**: there's no clean Spark analogue for *this specific
seam* - a Spark UDF is either a plain function or, if it needs async I/O,
something you'd typically wrap in a `mapPartitions` with a manually
managed thread pool or an async HTTP client, with none of that wrapping
visible to Catalyst. DataFusion's version makes the sync/async boundary a
first-class part of the UDF trait hierarchy instead of something you bolt
on around the side.

## A custom `OptimizerRule`: encoding a decision no generic rule could know

**Claim**: DataFusion's logical-plan optimizer isn't a fixed pipeline you
can only consume - you can register your own rewrite rule, and it runs
alongside the built-in ones (predicate pushdown, dead-filter elimination,
and the rest).

**Code**: `MaxRowsGuardrail` caps every top-level result set at a
configured `max_rows`, the way a platform team might guard a shared SQL
endpoint against an accidental unbounded scan. It's deliberately *not*
something a generic optimizer rule could express, because the right cap
for a given deployment isn't a general SQL simplification - it's a
decision this specific rule exists to encode.

```rust
fn rewrite(
    &self,
    plan: LogicalPlan,
    _config: &dyn OptimizerConfig,
) -> Result<Transformed<LogicalPlan>, DataFusionError> {
    if let LogicalPlan::Limit(Limit { skip, fetch, input }) = &plan {
        let already_capped = fetch.as_deref().is_some_and(|fetch_expr| {
            matches!(
                fetch_expr,
                Expr::Literal(ScalarValue::Int64(Some(n)), _) if *n <= self.max_rows
            )
        });
        if already_capped {
            return Ok(Transformed::no(plan));
        }
        return Ok(Transformed::yes(LogicalPlan::Limit(Limit {
            skip: skip.clone(),
            fetch: Some(Box::new(lit(self.max_rows))),
            input: Arc::clone(input),
        })));
    }
    Ok(Transformed::yes(LogicalPlan::Limit(Limit {
        skip: None,
        fetch: Some(Box::new(lit(self.max_rows))),
        input: Arc::new(plan),
    })))
}
```

One test proves both halves of the rule's actual contract - it caps an
unbounded query, and it leaves a query's own smaller `LIMIT` alone:

```console
$ cargo test -p pipeline optimizer_rule_max_rows_guardrail_caps_but_does_not_override_smaller_limit -- --nocapture
```

Against an 8-row fixture: `SELECT id FROM orders ORDER BY id` with the
guardrail set to 5 returns exactly 5 rows (not 8). `SELECT id FROM orders
ORDER BY id LIMIT 3` with the same guardrail still returns exactly 3 rows
- the rule caps, it never raises a query's own tighter limit.

**The trade-off, named explicitly**: this is a reliability trade, not a
free win. It buys protection against an unbounded-scan resource blowup at
the cost of *silently* truncating a legitimate large result rather than
erroring. A production version would need to surface the cap back to the
caller somehow (a warning, a response header) - this chapter proves only
the rewrite itself, not that follow-up.

**Scala/Spark bridge**: Spark doesn't have an equivalent plug-in point in
Catalyst's optimizer that's exposed this simply to end users -
`SparkSessionExtensions.injectOptimizerRule` exists and does the
analogous thing, but it's a less commonly reached-for API than this
chapter's one-rule, one-register pattern. Same underlying idea in both:
a custom rewrite registered once at session setup, applied to every query
afterward, invisible at the SQL call site.

## Standard SQL you already know, and one syntax you might not

Three more tests round out this file's SQL-surface claims - none require
a new extension point, all prove that DataFusion's SQL frontend parses and
executes exactly what the standard says it should, rather than assuming it
from the docs.

**CTEs are equivalent to the subquery they could be rewritten as**:

```console
$ cargo test -p pipeline cte_query_matches_equivalent_subquery -- --nocapture
```

```sql
WITH high_value AS (SELECT id FROM orders WHERE amount > 100)
SELECT id FROM high_value ORDER BY id
```

produces byte-identical `RecordBatch`es to the equivalent nested subquery
- 3 rows, ids 2/4/8, over the fixture's 8 orders.

**`EXCEPT` removes matching rows, set-difference style**:

```console
$ cargo test -p pipeline except_set_operation_removes_matching_rows -- --nocapture
```

```sql
SELECT id FROM orders WHERE amount > 40
EXCEPT
SELECT id FROM orders WHERE amount > 500
```

`amount > 40` keeps `{1,2,3,4,5,6,8}`; `amount > 500` keeps `{4,8}`;
`EXCEPT` removes the second set from the first, leaving `{1,2,3,5,6}` -
exactly what the test asserts.

**The pipe operator (`|>`) is accepted, and means what it reads like**:

```console
$ cargo test -p pipeline pipe_operator_syntax_matches_equivalent_standard_sql -- --nocapture
```

```sql
SELECT * FROM orders |> WHERE amount > 100 |> SELECT id |> ORDER BY id
```

matches `SELECT id FROM orders WHERE amount > 100 ORDER BY id` exactly,
row for row. This is BigQuery-originated syntax for reading a query
left-to-right in execution order instead of SQL's usual
`SELECT`-first order; DataFusion accepts it under its default `Generic`
SQL dialect.

**The trade-off worth naming once**: CTEs and `EXCEPT` are standard SQL,
portable across any engine that implements the standard. The pipe operator
is not - a query written with `|>` will not run unmodified against a
database that hasn't adopted this still-spreading syntax. That's the
actual trade this syntax buys: easier left-to-right reading, at the cost
of portability to anywhere else.

## Run the whole chapter live

```console
$ cargo test -p pipeline custom_table_provider -- --nocapture
$ cargo test -p pipeline amount_range -- --nocapture
$ cargo test -p pipeline percent_of_total_udwf -- --nocapture
$ cargo test -p pipeline async_udf_region_multiplier -- --nocapture
$ cargo test -p pipeline optimizer_rule_max_rows_guardrail -- --nocapture
$ cargo test -p pipeline cte_query_matches_equivalent_subquery except_set_operation_removes_matching_rows pipe_operator_syntax_matches_equivalent_standard_sql -- --nocapture
```
