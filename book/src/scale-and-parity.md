# Proving it at scale: 91GB, joins, and SQL parity against Spark

The previous chapter covered *how* Ballista and Comet push DataFusion past a
single process. This chapter covers the part that matters more: does any of
it hold up once the data stops being toy-sized, and does DataFusion actually
agree with Spark on what a query means? The historical tables retain previously
recorded results; this audit has not rerun the 91GB workloads. Reproduction needs
the original dataset, engine configuration and timing boundary. Projections and
limits of the surviving captures are identified separately below.

**The scale used throughout is 91GB / 6.6 billion rows, not 1TB.** An earlier
plan called for a literal terabyte; a real run turned out to need roughly
250–330GB of free disk against 229GB actually available on the machine doing
the work. Rather than fabricate a 1TB number, the row count was picked
honestly: a small 5-million-row sample measured 16.18 bytes/row on disk, and
`100GB / 16.18 bytes/row ≈ 6.6 billion rows` was the target. The real run
landed at 91GB - Parquet compressed slightly better at that row count than
the small sample predicted. Where a 1TB figure is still useful, it's labeled
as a linear *projection* from this real 91GB result, never presented as
something that was actually run - see the projection note near the end of
this chapter.

## The two-pass discipline: prove correctness small, then spend the time budget

Every scale run in this chapter follows the same shape: generate a small
dataset first (5 million rows), confirm every engine returns the *identical*
result, and only then run the same harness unchanged at full scale. This
caught a real bug before it wasted hours of runtime: Polars' `SUM` on a
`Decimal(10,2)` column stays at the input's own precision, while DataFusion
auto-widens the result type. At small scale the sum fit within 10 digits and
nothing broke; at 91GB the true sum needs 11 integer digits, and Polars
panicked (`decimal precision 10 can't fit values with 11 digits`). The fix -
explicitly casting `amount` to `Decimal(38, 2)` before `.sum()` - is one
line, but finding it *before* the ~100GB run started is what the small first
pass is for.

**Scala bridge**: this is the same discipline as running a Spark job against
a 1% sample before submitting it against the full partition. Here every
engine must agree with the independent oracle at each scale before its timings
are trusted. Different input sizes have different counts and sums; the sample
and full run are not expected to return the same result.

## The plain aggregate at 91GB: DataFusion, Ballista, Polars, Spark, Spark+Comet

Same query every prior chapter has used: `SELECT COUNT(*), SUM(amount) FROM
orders WHERE amount > 50.00`. Five engines, one 91GB/6.6-billion-row Parquet
dataset (8 partitions), two runs each, correctness checked against an
oracle computed independently from the dataset generator's own rule (not
from any engine's output):

| Engine | Mean elapsed | vs. Spark-vanilla |
|---|---|---|
| DataFusion (single-node) | 5.84s | ~7.75x |
| Ballista (2-executor local cluster) | 4.30s | ~10.5x |
| Comet-accelerated Spark | 13.94s | ~3.2x |
| Polars (Rust, lazy/streaming) | 16.65s | not compared |
| Spark (vanilla) | 45.25s | 1x (baseline) |

The original timing boundaries differ. DataFusion and Ballista start their
reported query timers after context creation and table registration. Spark's
`real` wraps the complete SQL process, including startup. The ratios above are
arithmetic comparisons of those captured boundaries; they do not establish matched
query throughput or a production migration speedup. Raw process and query timings
must remain labeled separately.

All five returned the exact same `order_count=3332996682,
total_amount=249991416386.72` on every run.

```sh
just m36-dataset-full     # generates benchmark/m3.6/orders, ~91GB
just m36-datafusion
just m36-ballista         # needs a live 2-executor Ballista cluster
just m36-polars
just m36-vanilla          # inside docker-compose.spark-comet.yml
just m36-accelerated      # Spark + the Comet plugin
```

**What this confirms and what it doesn't.** The recorded means yield
45.25 / 4.30 ≈ 10.5 and 45.25 / 5.84 ≈ 7.75, but the mixed timing boundaries
prevent interpreting those ratios as query-only throughput improvements. Results
matched the oracle at this scale. This is one filter/count/sum shape, one dataset,
two runs per engine and one host. Those samples cannot characterize p99 or predict
a multi-node deployment. The local two-executor Ballista topology also shares the
laptop's CPU, storage and memory with its scheduler.

### Keep engine comparisons separate from deployment claims

Polars and DataFusion run in-process on one machine here. Spark also runs locally
with `local[*]`; its distributed capability was not exercised across hosts. Ballista
uses two local executor processes. Listing captured times together does not make
these equivalent production deployments. Compare Polars with DataFusion only within
matched local query boundaries, and report Spark's process startup separately.
Neither this topology nor these query shapes justify a general engine ranking or
a prediction about a production multi-node platform.

### A sixth engine, a different scalability trade-off: DuckDB

Every engine measured so far either streams in memory (DataFusion,
Polars) or distributes across processes (Ballista, Spark). DuckDB takes a
third approach worth naming on its own: a vectorized, out-of-core
buffer-manager engine, designed from the start to run queries larger than
RAM on a single machine by spilling through its own buffer manager rather
than by distributing or by requiring everything to fit in memory. Proving
it on the exact same query, dataset, and scale as the DataFusion/Polars
numbers above is what turns "DuckDB is a different scalability trade-off"
from an assumed claim into a measured one - and this repo now carries a
`scale-aggregate-duckdb` bin, feature-gated behind `duckdb`, that runs the
identical `COUNT(*)`/`SUM(amount) WHERE amount > 50.00` query this
chapter's whole plain-aggregate table already uses:

```rust
let (order_count, total_amount): (i64, f64) = conn
    .query_row(
        "SELECT COUNT(*), CAST(SUM(amount) AS DECIMAL(38,2)) \
         FROM read_parquet(?) WHERE amount > 50.00",
        params![scan_path.to_string_lossy()],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .map_err(CliError::Query)?;
```

The explicit `CAST(... AS DECIMAL(38,2))` fixes the query's requested result
type. Its presence is not evidence that DuckDB's default decimal SUM retains input
precision or reproduces the separately observed Polars issue. This DuckDB leg still
needs its own measured scale result before any engine comparison is claimed.

```sh
cargo build -p pipeline --features duckdb
cargo run -p pipeline --features duckdb --bin scale-aggregate-duckdb -- --input benchmark/m3.6/orders
```

This bin follows the same house convention every other `scale-aggregate-*`
bin in this repo already follows: print the result and the elapsed time,
diff it externally against the other engines' output, don't assert
equality in-repo. It hasn't yet been run at the full 91GB scale this
chapter's table reports for the other five engines - until it has, treat
it as a proof that the comparison is runnable, not as a sixth row in the
table above.

## Joins at scale: the same shuffle-join proof, 2M rows and then 91GB/50GB

A `SUM`/`COUNT` over a single table never touches DataFusion's hash-join or
shuffle machinery. Proving those separately needed a second dataset and a
real join:

```sql
SELECT COUNT(*), SUM(orders.amount)
FROM orders JOIN shipments ON orders.id = shipments.order_id
WHERE orders.amount > 50.00
```

The first pass ran this at a small, checkable scale - 2,000,000 orders and
1,000,000 shipments (one shipment per even-numbered order id, plus one
deliberate duplicate to prove multi-shipment fan-out doesn't corrupt the
result) - against DataFusion, Spark vanilla, Spark+Comet, and Ballista.
Spark's `spark.sql.autoBroadcastJoinThreshold` was set to `-1` on both Spark
legs specifically to force a real shuffle join instead of Spark silently
broadcasting the smaller side, which would have skipped the shuffle path
entirely and defeated the point of the test.

```sh
just join-shuffle-dataset
just join-shuffle-datafusion
just join-shuffle-vanilla
just join-shuffle-accelerated
just join-shuffle-ballista     # needs a live 2-executor Ballista cluster
```

All four returned the same `500000 / 37500000.00`, matching an oracle
computed from the generator's own id-parity rule. Comet's query-only time
was consistently lower than vanilla's for this join (~0.94s vs. ~1.71s
mean, ~45% less) - the first evidence in datacrate that Comet's shuffle
manager, not just its scan/filter operators, measurably accelerates a real
query.

The second pass repeated the identical proof at 91GB orders / 50GB shipments
- `just m38-dataset-full`, `just m38-datafusion-join`, `just
m38-ballista-join` - and got the same byte-identical
`shipped_order_count=3332996680, shipped_total_amount=249991416171.22` from
both DataFusion and Ballista. Two real engine limitations had to be worked
around to get there, and they're worth knowing about if you ever run a
shuffle join at this size yourself:

- **DataFusion's default `HashJoinExec` can't spill its build side** in this
  DataFusion version, so a hash join whose build side doesn't fit in memory
  simply fails rather than spilling to disk. The workaround is
  `prefer_hash_join = false`, which forces the planner to pick the
  spill-capable `SortMergeJoinExec` instead - a real engine-configuration
  choice, not a claim that sort-merge is generally the better join strategy.
- Both DataFusion and Ballista needed a bounded `FairSpillPool` and an
  explicit `max_temp_directory_size` cap so spilling had somewhere safe to
  go instead of exhausting disk.

At this scale, single-node DataFusion (160.13s) actually beat the 2-executor
Ballista cluster (443.76s) - plausibly because Ballista pays network,
scheduler, and serialization overhead that a single process doesn't, and
that cost outweighs its parallelism advantage on this one host. This is
**not** a general "distributed is slower" claim; it's what happened on one
laptop with one join strategy, one run each (not yet repeated enough to
characterize variance).

## Group-by and filter shapes: where the "10x" story does and doesn't generalize

The plain aggregate favored Ballista, while the full-scale join favored
single-node DataFusion. Two additional query shapes were recorded at the same
91GB scale; they also favored DataFusion:

| Shape | DataFusion mean | Ballista mean | Ballista vs. Spark-vanilla |
|---|---|---|---|
| Plain aggregate (M3.6) | 5.84s | 4.30s | ~10.5x |
| High-cardinality `GROUP BY` (1000 buckets) | 12.97s | 19.52s | ~7.9x |
| Multi-predicate filter | 14.17s | 29.80s | ~3.4x |

```sh
just m38-datafusion-groupby   just m38-ballista-groupby   just m38-vanilla-groupby   just m38-accelerated-groupby
just m38-datafusion-filter    just m38-ballista-filter    just m38-vanilla-filter    just m38-accelerated-filter
```

All four engines matched exactly on both shapes (group-by spot-checked at
bucket 999: `order_count=6600000, total_amount=333299940.72`; filter:
`order_count=2856854299, total_amount=214278356945.44`).

These results are shape-dependent: Ballista's recorded query elapsed time is
lower on the plain aggregate and higher on the group-by, multi-predicate filter
and full-scale join. Scheduling, serialization and parallelism are possible causes,
but these captures do not isolate them. The ratios against Spark additionally
retain the mixed process/query timing boundary described above. None is a fixed
engine multiplier.

## Is DataFusion's speed actually predictable, or just fast on average?

A single elapsed-time number says nothing about how consistent that time is
run to run - which matters if you're demoing this live and need to know
whether a run might blow past its time budget. Both engines were run 10
times each against the 91GB dataset:

| Engine | mean | median | min | max | stddev | CV |
|---|---|---|---|---|---|---|
| DataFusion | 5.58s | 5.65s | 5.09s | 5.83s | 0.21s | 3.76% |
| Spark-vanilla | 40.80s | 40.20s | 39.17s | 45.03s | 1.58s | 3.86% |

DataFusion's absolute jitter (stddev, range) is roughly 7.5x smaller than
Spark's - which is what actually matters for keeping a live demo inside its
time budget. Their *relative* variability (coefficient of variation) is
about the same, 3.76% vs. 3.86% - so this is a claim about absolute
predictability, not about DataFusion having some second, independent
consistency advantage on top of being faster. One contaminated data point
(60.2s, caused by leftover contention from a just-killed prior command) was
excluded and rerun cleanly rather than left in or silently dropped.

## Checking the "30GB driver" line against the recorded container samples

A pitch line worth being honest about: "your Spark job needs a 30GB driver
and 15 minutes of GC tuning before it processes a single row." For the
workload datacrate actually runs - a single filter+aggregate query, no
joins, no `.collect()` of large results, `local[*]` mode with Spark's stock
default `spark.driver.memory=1g` and zero GC flags set anywhere in the
project - the measured reality was:

- Startup-only (`SELECT 1`, 3 runs): mean 4.36s.
- Highest observed container memory during the real 91GB aggregate query
  (sampled every 2s via `docker stats`): 1.446 GiB. This is a sampled container
  metric, not an isolated JVM heap/RSS measurement or a guaranteed peak. The
  Linux Docker CLI subtracts inactive file cache from reported usage; see the
  [Docker stats definition](https://docs.docker.com/reference/cli/docker/container/stats/).
  The local driver and executor shared one JVM inside a Docker Desktop VM capped
  at 7.748 GiB, distinct from the host's 64 GiB.

These samples are far below the rhetorical "30GB" and "15 minutes" figures,
but container memory and startup elapsed time do not measure driver heap need or
time spent tuning GC. For this specific workload: this doesn't refute the
30GB figure in general - no workload that plausibly needs that much driver
memory (a broadcast join landing in the driver, `.collect()`-ing a large
result set, a real multi-executor cluster's shuffle-metadata bookkeeping)
was attempted here. It shows the figure isn't *required* for this workload,
not that it's wrong for every workload.

## Does DataFusion actually agree with Spark on what a query means?

Throughput numbers are worthless if the two engines don't compute the same
answer. A single query was built specifically to exercise four areas of SQL
semantics that are notorious sources of silent, wrong-answer divergence
between engines:

```sql
SELECT id, amount, note,
  EXTRACT(DAY FROM placed_at) AS placed_day,
  SUM(amount) OVER (ORDER BY placed_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
  RANK() OVER (ORDER BY amount DESC) AS amount_rank
FROM orders
ORDER BY note NULLS FIRST, id
```

- `EXTRACT(DAY FROM ...)` - timestamp semantics.
- The running `SUM(...) OVER (...)` - window-frame semantics.
- `RANK() OVER (ORDER BY amount DESC)` - tie handling (two rows share
  `amount = 45.50` in the fixture).
- `ORDER BY note NULLS FIRST` - null-ordering semantics (two rows have a
  `NULL` note).

```sh
just parity-dataset
just parity-datafusion
just parity-spark
```

Run against an 8-row fixture designed to exercise all four at once, both
engines produced byte-identical output, row for row, column for column: the
two `NULL`-note rows sort first (not last) in both engines, both `45.50`
rows get `RANK` 6 (not a sequential 6/7 - proving tie handling matches, not
just presence of a rank), and the `EXTRACT` and running-total columns agree
exactly.

**What this does and doesn't cover.** This confirms DataFusion and Spark
agree on these four semantics for this repo's actual query shapes - real
confidence, not a guess. It's 8 rows on one host, so it says nothing about
behavior once a window function has to run across multiple partitions
(Spark's own planner warns "No Partition Defined for Window operation!
Moving all data to a single partition" for this exact query on both
engines - a `PARTITION BY` clause was never tested). Only `RANK()` and
`NULLS FIRST` were checked; `ROW_NUMBER()`, `DENSE_RANK()`, `LAG`/`LEAD`,
`RANGE BETWEEN` frames, and `NULLS LAST` were not. Decimal-arithmetic parity
and join-result parity were separately confirmed by the aggregate and join
comparisons earlier in this chapter - together, these are the semantics
categories checked so far, not an exhaustive SQL-compatibility claim.

## The earlier approximately 200x comparison used different clocks

The dated small aggregate captures report DataFusion query elapsed times of
27.17, 28.72 and 21.33 ms. Spark process elapsed times are 5.429, 5.512 and
5.497 seconds. Both return the same 2,500,000 matching rows and 188,750,000.00
sum, but the DataFusion timer starts after registration while Spark's shell timer
includes JVM/session startup and the SQL script. Dividing those values yields a
large ratio across different boundaries; it does not establish a roughly 200x
query-throughput improvement. The recorded report already distinguishes those
clocks. Keep the correctness result and the separate elapsed observations, without
promoting the ratio into a migration, cost-saving or productivity claim.

## The one number in this chapter that was never actually run: a 1TB projection

If a 1TB figure is ever quoted anywhere against this codebase, it should be
captioned exactly as it's captioned here: **projected from the measured 91GB
result, not run at 1TB.** The projection is a straight linear scale factor
(`1024GB / 91GB ≈ 11.25x`) applied to each of the five 91GB means above -
DataFusion ~65.7s, Ballista ~48.4s, Polars ~187.4s, Spark-vanilla ~509.2s,
Spark+Comet ~156.9s. A linear projection can't produce a different *ratio*
than the one already measured (the 10.5x and 2.85x figures fall out
unchanged) - it adds no new information about how the gap moves with scale.
The projection does not establish how either engine scales past the measured
point. Memory pressure, spill, parallelism, cache state and storage can change
both curves in either direction. It supplies no evidence that the true 1TB ratio
would be larger, smaller or unchanged.

## Proving Polars' lazy optimizer, not just citing it

The DataFusion-vs-Polars comparison earlier in this chapter treats Polars
as a black box that happens to be faster or slower. One claim behind
*why* Polars' lazy API is worth using at all - that `.filter()` and
`.select()` calls chained before `.collect()` get pushed down into the
scan itself, rather than running as separate passes after a full read -
deserves the same "prove, don't assert" treatment this chapter already
applies to every performance number: diff the actual query plan, don't
take the API's own marketing for it.

**Claim**: Polars' lazy query optimizer folds a `.filter()` predicate and
a `.select()` projection into the scan node itself when given the chance
- the eager equivalent has no such opportunity, because by the time
`.filter()` runs, the whole file has already been read.

**Code**: `orders_lazy()` builds the same scan/filter/select chain the
rest of this chapter's Polars numbers are built from, and `.explain(true)`
versus `.explain(false)` surfaces the optimizer's before/after directly as
plan text:

```rust
let optimized = lf.clone().explain(true).expect("optimized plan");
let unoptimized = lf.explain(false).expect("unoptimized plan");

assert!(optimized.contains("SELECTION: col(\"amount\") > 50.0"));
assert!(!optimized.contains("FILTER"));
assert!(unoptimized.contains("FILTER col(\"amount\") > 50.0"));
```

```console
$ cargo test -p pipeline --features polars polars_lazy_plan_pushes_predicate_below_scan -- --nocapture
$ cargo test -p pipeline --features polars polars_lazy_plan_pushes_projection_below_scan -- --nocapture
```

The optimized plan folds the predicate straight into the scan node's own
`SELECTION` clause and has no separate `FILTER` node left at all. The
unoptimized plan keeps `FILTER` as its own node sitting above the scan -
proving the two plans really are structurally different, not just
differently worded. The projection test makes the same point about column
pruning: the optimized plan's scan reads `PROJECT 2/4 COLUMNS` (only `id`
and `amount`), while the unoptimized plan reads all four and defers
column selection to a separate node afterward.

This is the same technique this repo's DataFusion chapters already use
(`EXPLAIN`-string parsing to prove pushdown happened, not just assume it
from a query running fast) applied to Polars' own plan representation -
the Data is the plan-string output, the Calculation is the marker-text
check against it, and `.explain()`/`.collect()` are the Actions that
produce real plans instead of hypothetical ones.

A third test closes the loop on correctness: the lazy path (scan, filter,
select, collect) and the eager path (read the whole file, then filter and
select afterward) must agree exactly, row for row:

```console
$ cargo test -p pipeline --features polars polars_eager_and_lazy_paths_produce_identical_results -- --nocapture
```

An optimizer that reorders work is only trustworthy if reordering it
never changes the answer - this is the same "byte-identical before any
speed claim" discipline the two-pass discipline section above applies to
every cross-engine comparison in this chapter, now applied to Polars
against itself.

## Compare physical layout before changing a writer default

Earlier, `compression_comparison` ran Snappy and default-level Zstd once over
one synthetic ingest batch. It checked values, but that could not establish a
workload-wide compression or row-group policy. The example now takes an explicit
candidate and bounded fixture settings. It compares the existing Snappy settings,
Zstd level 3 with the same row-group limit, and Snappy with 65,536-row groups.
The pinned default row-group limit is 1,048,576 rows. A batch write is not a promise
of one physical row group; the example inspects the resulting metadata.

```sh
cargo build --release -p pipeline --example compression_comparison --locked
./target/release/examples/compression_comparison --dataset orders --candidate baseline --rows 200000
./target/release/examples/compression_comparison --dataset orders --candidate zstd3 --rows 200000
./target/release/examples/compression_comparison --dataset orders --candidate small-groups --rows 200000
```

Use `--dataset ingest` for the separate three-column ingest schema. Optional
`--shuffled`, `--high-cardinality` and `--long-notes` flags vary physical workload
properties; ID-based logical values remain deterministic with shuffle seed 42.
Notes include nulls, Unicode, commas and newlines. Fixtures are capped at 250,000
rows, or 20,000 with long notes, because generation deliberately materializes this
bounded experimental input before timing. This is not the production ingest path.

Each run checks the complete Parquet schema, values and nulls outside timing,
including orders decimals and microsecond timestamps. Full-scan and narrow selective
queries additionally check counts and ID sums against an independent arithmetic
oracle. Reports include effective writer settings, physical row groups, columns
with min/max statistics, footer metadata size and native engine metrics. Tests
also cross a physical 65,536-row boundary and exercise all three candidates.

Write wall time includes file creation through Parquet close, excludes generation,
and does not include fsync. Scan wall time excludes planning but includes draining,
row counting and the client ID checksum. Scans follow the untimed full equality
decode, so these are warm-cache measurements. Native `bytes_scanned` counts scan
reader requested range lengths before reads; footer metadata can use a separate
fetch path. It is not total storage traffic, returned bytes or wire bytes.
Object-store request counts, wire bytes and per-stage CPU are reported unavailable.
An external process CPU/RSS measurement includes generation, verification, runtime
startup and teardown; it must not be attributed to one write or scan stage.

Interleave candidate order, warm up once and retain at least five raw samples
before comparing median and range. Include sorted/shuffled, repeated/high-cardinality
and short/long/null inputs with full-scan and write controls. Five samples do not
characterize tail latency. Small long-note fixtures that fit one row group are a
negative control for row-group splitting. These controls support a workload-specific
decision; they do not earn a production policy type, automatic tuner or default
migration on their own. In the local repeated comparison, smaller groups reduced
requested scan bytes for sorted IDs but increased them for shuffled IDs. Zstd
reduced file size while commonly exceeding the declared write or full-scan
regression guard. Long fixtures remained one row group under both Snappy settings.
Those trade-offs support retaining the production default; they establish no
cloud-traffic or application-throughput improvement. Snappy remains the production default.

## Try it yourself

Every command above assumes `benchmark/m3.6/orders` and `benchmark/m3.8/`
already exist at full scale - generating them (`just m36-dataset-full`,
`just m38-dataset-full`) writes tens of gigabytes to disk and takes real
wall-clock time. Start with the small first pass instead:

```sh
just m36-dataset          # 5,000,000 rows - correctness pass, seconds not minutes
just m36-datafusion
just m36-polars
```

Confirm all engines agree on the small dataset before spending the disk and
time budget on the full 91GB run.

The generators previously accepted zero batch sizes, which could leave the write
loop unable to advance. Their batch sizes now use `NonZeroU64`, so argument parsing
rejects zero before output creation. Timestamp multiplication previously happened
in i64 before division: a supported row count could still overflow that
intermediate product. It now happens in i128 before checked narrowing to i64. The orders
generator retains its total-row divisor; shipments retain the billion-order
divisor and delivery-day offsets. Shipments reject a requested range before
creating output if any emitted timestamp would exceed i64 microseconds. These
changes do not regenerate existing benchmark datasets or establish new timings.
