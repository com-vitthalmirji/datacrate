# Proving it at scale: 91GB, joins, and SQL parity against Spark

The previous chapter covered *how* Ballista and Comet push DataFusion past a
single process. This chapter covers the part that matters more: does any of
it hold up once the data stops being toy-sized, and does DataFusion actually
agree with Spark on what a query means? Every number below is measured, not
guessed — each one reproducible from the exact command shown, with the toolchain,
hardware, seed, and raw output recorded alongside it at the time it was run.

**The scale used throughout is 91GB / 6.6 billion rows, not 1TB.** An earlier
plan called for a literal terabyte; a real run turned out to need roughly
250–330GB of free disk against 229GB actually available on the machine doing
the work. Rather than fabricate a 1TB number, the row count was picked
honestly: a small 5-million-row sample measured 16.18 bytes/row on disk, and
`100GB / 16.18 bytes/row ≈ 6.6 billion rows` was the target. The real run
landed at 91GB — Parquet compressed slightly better at that row count than
the small sample predicted. Where a 1TB figure is still useful, it's labeled
as a linear *projection* from this real 91GB result, never presented as
something that was actually run — see the projection note near the end of
this chapter.

## The two-pass discipline: prove correctness small, then spend the time budget

Every scale run in this chapter follows the same shape: generate a small
dataset first (5 million rows), confirm every engine returns the *identical*
result, and only then run the same harness unchanged at full scale. This
caught a real bug before it wasted hours of runtime: Polars' `SUM` on a
`Decimal(10,2)` column stays at the input's own precision, while DataFusion
auto-widens the result type. At small scale the sum fit within 10 digits and
nothing broke; at 91GB the true sum needs 11 integer digits, and Polars
panicked (`decimal precision 10 can't fit values with 11 digits`). The fix —
explicitly casting `amount` to `Decimal(38, 2)` before `.sum()` — is one
line, but finding it *before* the ~100GB run started is what the small first
pass is for.

**Scala bridge**: this is the same discipline as running a Spark job against
a 1% sample before submitting it against the full partition — except here
the "sample" run also has to produce a byte-identical result to the full run
before the full run is trusted at all, not just "didn't crash."

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
| Polars (Rust, lazy/streaming) | 16.65s | ~2.7x |
| Spark (vanilla) | 45.25s | 1x (baseline) |

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

**What this confirms and what it doesn't.** The "roughly 10x" headline
number holds at this real scale (Ballista's 4.30s vs. Spark-vanilla's
45.25s), and single-node DataFusion alone already gets ~7.75x without
distributing anything. The DataFusion-vs-Polars comparison (~2.85x) stays
inside the Rust ecosystem, on purpose — see the note on scope below. What it
does *not* show: this is one query shape (filter + count/sum), one dataset,
2 runs per engine (enough to catch first-run warm-up noise, not enough to
characterize p50/p99 variance), and Ballista's 2-executor cluster runs on
the same laptop as everything else — not a real multi-node deployment.

### Why Polars only ever races DataFusion, never Spark

Every Polars number in datacrate is compared against DataFusion, never
against Spark. Polars and DataFusion are both single-machine, in-process
Rust query engines — comparing them measures which one is the better engine
for a workload that fits on one box. Spark is a distributed system whose
entire value proposition is coordinating work *across* machines; putting a
single-node Polars number next to a Spark cluster number would be comparing
different things and calling it a ranking. The same rule that keeps
Ballista, Comet, and DataFusion from being ranked against each other in the
previous chapter applies here to Polars too.

## Joins at scale: the same shuffle-join proof, 2M rows and then 91GB/50GB

A `SUM`/`COUNT` over a single table never touches DataFusion's hash-join or
shuffle machinery. Proving those separately needed a second dataset and a
real join:

```sql
SELECT COUNT(*), SUM(orders.amount)
FROM orders JOIN shipments ON orders.id = shipments.order_id
WHERE orders.amount > 50.00
```

The first pass ran this at a small, checkable scale — 2,000,000 orders and
1,000,000 shipments (one shipment per even-numbered order id, plus one
deliberate duplicate to prove multi-shipment fan-out doesn't corrupt the
result) — against DataFusion, Spark vanilla, Spark+Comet, and Ballista.
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
mean, ~45% less) — the first evidence in datacrate that Comet's shuffle
manager, not just its scan/filter operators, measurably accelerates a real
query.

The second pass repeated the identical proof at 91GB orders / 50GB shipments
— `just m38-dataset-full`, `just m38-datafusion-join`, `just
m38-ballista-join` — and got the same byte-identical
`shipped_order_count=3332996680, shipped_total_amount=249991416171.22` from
both DataFusion and Ballista. Two real engine limitations had to be worked
around to get there, and they're worth knowing about if you ever run a
shuffle join at this size yourself:

- **DataFusion's default `HashJoinExec` can't spill its build side** in this
  DataFusion version, so a hash join whose build side doesn't fit in memory
  simply fails rather than spilling to disk. The workaround is
  `prefer_hash_join = false`, which forces the planner to pick the
  spill-capable `SortMergeJoinExec` instead — a real engine-configuration
  choice, not a claim that sort-merge is generally the better join strategy.
- Both DataFusion and Ballista needed a bounded `FairSpillPool` and an
  explicit `max_temp_directory_size` cap so spilling had somewhere safe to
  go instead of exhausting disk.

At this scale, single-node DataFusion (160.13s) actually beat the 2-executor
Ballista cluster (443.76s) — plausibly because Ballista pays network,
scheduler, and serialization overhead that a single process doesn't, and
that cost outweighs its parallelism advantage on this one host. This is
**not** a general "distributed is slower" claim; it's what happened on one
laptop with one join strategy, one run each (not yet repeated enough to
characterize variance).

## Group-by and filter shapes: where the "10x" story does and doesn't generalize

The plain aggregate and the join both had Ballista beating single-node
DataFusion. Two more query shapes were run at the same 91GB scale to check
whether that holds generally — and it doesn't:

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

**The genuine reversal worth sitting with**: on both of these shapes,
Ballista is *slower* than single-node DataFusion, not faster — the opposite
of the plain-aggregate and join results. The likely reason is that these
shapes do less work per task, so Ballista's distributed
scheduling/coordination overhead becomes a bigger fraction of the total time
at a small 2-executor topology and never pays for itself. This wasn't
investigated further (it would need per-task profiling), but it's the kind
of result that has to be disclosed rather than quietly dropped if a "10x"
headline number gets used anywhere — the true ratio is shape-dependent,
ranging from ~3.4x to ~10.5x across the four shapes actually measured, not
a single fixed multiplier.

## Is DataFusion's speed actually predictable, or just fast on average?

A single elapsed-time number says nothing about how consistent that time is
run to run — which matters if you're demoing this live and need to know
whether a run might blow past its time budget. Both engines were run 10
times each against the 91GB dataset:

| Engine | mean | median | min | max | stddev | CV |
|---|---|---|---|---|---|---|
| DataFusion | 5.58s | 5.65s | 5.09s | 5.83s | 0.21s | 3.76% |
| Spark-vanilla | 40.80s | 40.20s | 39.17s | 45.03s | 1.58s | 3.86% |

DataFusion's absolute jitter (stddev, range) is roughly 7.5x smaller than
Spark's — which is what actually matters for keeping a live demo inside its
time budget. Their *relative* variability (coefficient of variation) is
about the same, 3.76% vs. 3.86% — so this is a claim about absolute
predictability, not about DataFusion having some second, independent
consistency advantage on top of being faster. One contaminated data point
(60.2s, caused by leftover contention from a just-killed prior command) was
excluded and rerun cleanly rather than left in or silently dropped.

## Checking the "30GB driver" line against an actual measurement

A pitch line worth being honest about: "your Spark job needs a 30GB driver
and 15 minutes of GC tuning before it processes a single row." For the
workload datacrate actually runs — a single filter+aggregate query, no
joins, no `.collect()` of large results, `local[*]` mode with Spark's stock
default `spark.driver.memory=1g` and zero GC flags set anywhere in the
project — the measured reality was:

- Startup-only (`SELECT 1`, 3 runs): mean 4.36s.
- Peak driver memory during the real 91GB aggregate query (sampled every 2s
  via `docker stats`): 1.446 GiB.

That's roughly 20x smaller than "30GB" and 200x smaller than "15 minutes,"
for this specific workload. The honest scoping: this doesn't refute the
30GB figure in general — no workload that plausibly needs that much driver
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

- `EXTRACT(DAY FROM ...)` — timestamp semantics.
- The running `SUM(...) OVER (...)` — window-frame semantics.
- `RANK() OVER (ORDER BY amount DESC)` — tie handling (two rows share
  `amount = 45.50` in the fixture).
- `ORDER BY note NULLS FIRST` — null-ordering semantics (two rows have a
  `NULL` note).

```sh
just parity-dataset
just parity-datafusion
just parity-spark
```

Run against an 8-row fixture designed to exercise all four at once, both
engines produced byte-identical output, row for row, column for column: the
two `NULL`-note rows sort first (not last) in both engines, both `45.50`
rows get `RANK` 6 (not a sequential 6/7 — proving tie handling matches, not
just presence of a rank), and the `EXTRACT` and running-total columns agree
exactly.

**What this does and doesn't cover.** This confirms DataFusion and Spark
agree on these four semantics for this repo's actual query shapes — real
confidence, not a guess. It's 8 rows on one host, so it says nothing about
behavior once a window function has to run across multiple partitions
(Spark's own planner warns "No Partition Defined for Window operation!
Moving all data to a single partition" for this exact query on both
engines — a `PARTITION BY` clause was never tested). Only `RANK()` and
`NULLS FIRST` were checked; `ROW_NUMBER()`, `DENSE_RANK()`, `LAG`/`LEAD`,
`RANGE BETWEEN` frames, and `NULLS LAST` were not. Decimal-arithmetic parity
and join-result parity were separately confirmed by the aggregate and join
comparisons earlier in this chapter — together, these are the semantics
categories checked so far, not an exhaustive SQL-compatibility claim.

## The one number in this chapter that was never actually run: a 1TB projection

If a 1TB figure is ever quoted anywhere against this codebase, it should be
captioned exactly as it's captioned here: **projected from the measured 91GB
result, not run at 1TB.** The projection is a straight linear scale factor
(`1024GB / 91GB ≈ 11.25x`) applied to each of the five 91GB means above —
DataFusion ~65.7s, Ballista ~48.4s, Polars ~187.4s, Spark-vanilla ~509.2s,
Spark+Comet ~156.9s. A linear projection can't produce a different *ratio*
than the one already measured (the 10.5x and 2.85x figures fall out
unchanged) — it adds no new information about how the gap moves with scale.
If anything, it's more likely to *understate* the real gap: Spark's GC
pauses are known to grow worse under memory pressure, so its true curve
past 64GB is plausibly super-linear, which would make the real 1TB gap
larger than 10.5x, not smaller. That reasoning is not itself a measurement,
which is exactly why the number stays labeled a projection instead of a
result.

## Try it yourself

Every command above assumes `benchmark/m3.6/orders` and `benchmark/m3.8/`
already exist at full scale — generating them (`just m36-dataset-full`,
`just m38-dataset-full`) writes tens of gigabytes to disk and takes real
wall-clock time. Start with the small first pass instead:

```sh
just m36-dataset          # 5,000,000 rows — correctness pass, seconds not minutes
just m36-datafusion
just m36-polars
```

Confirm all engines agree on the small dataset before spending the disk and
time budget on the full 91GB run.
