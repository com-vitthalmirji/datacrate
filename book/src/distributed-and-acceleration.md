# Beyond one process: distributing DataFusion, and accelerating Spark with it

Everything so far in datacrate runs on one machine, in one process. This
chapter covers the two ways this codebase pushes DataFusion past that
boundary, and - just as important - the one way it deliberately doesn't
compare them to each other.

**The framing, stated once, up front**: every benchmark and proof point here
measures against the same baseline - Apache Spark (JVM/Scala) - and answers
a different question about it:

| Proof point | Question it answers | Role |
|---|---|---|
| DataFusion (single-node) | Can one Rust process replace a single-node Spark job? | Spark *replacement* |
| Ballista | Can DataFusion, partitioned across a cluster, replace *distributed* Spark? | Spark *replacement*, distributed |
| Comet | Can DataFusion's execution engine make an *existing* Spark job faster without leaving Spark? | Spark *accelerant* |

These three never race each other. Ballista's job isn't to beat plain
DataFusion - a distributed system is never faster than a single node at data
small enough to fit on one machine; that's not what it's for. Comet's job
isn't to beat DataFusion standalone - it runs *inside* Spark's own JVM
driver and shuffle service, not instead of it. Each row of that table is its
own comparison against Spark, and the three rows are not comparable to each
other. If you catch yourself putting a Ballista elapsed time next to a Comet
elapsed time and calling one "faster," stop - that's not a comparison either
proof point was designed to answer.

## Ballista: the same query plan, partitioned across a cluster

`crates/pipeline/src/bin/ballista-aggregate.rs` runs the identical
aggregation query datacrate's DataFusion chapter already covers -
`SELECT COUNT(*), SUM(amount) FROM orders WHERE amount > 50.00` - against a
live Ballista cluster instead of a single-node `SessionContext`, specifically
so its result can be diffed against the already-proven single-node value.
Same query, same expected numbers, different execution substrate - Ballista
is not a new query engine, it's the same DataFusion query planner and
execution engine, with the physical plan's stages distributed across a
scheduler and a pool of executors instead of running in one process.

```console
$ just ballista-scheduler       # one terminal
$ just ballista-executor-1      # another terminal
$ just ballista-executor-2      # another terminal
$ cargo run --release -p pipeline --bin ballista-aggregate --features ballista
```

### Why the fixture is two files, not one

```rust
/// Splits `batch` roughly in half and writes each half as its own Parquet
/// file under `dir`, so a table registered over `dir` scans as (at least)
/// two partitions instead of one.
fn write_two_partition_orders(dir: &Path, batch: &RecordBatch) -> Result<(), CliError> {
```

(`ballista-aggregate.rs`, `write_two_partition_orders`)

The module doc comment names the exact failure mode this works around,
sourced from the Ballista Tuning Guide: a table backed by a single file has
exactly one partition and "will not be able to scale even if the cluster has
resource available." One file produces a single-task physical plan that
never leaves one executor - adding a second executor to the cluster buys you
nothing if the scheduler only ever had one task to hand out. Splitting the
fixture into two Parquet files gives the scan two partitions, which gives
the scheduler two tasks to place, which is what actually puts one task on
each registered executor. Distribution isn't automatic just because a
cluster exists - it follows the shape of the data, specifically its
partition count.

**Spark bridge**: this is the exact same constraint as a Spark job reading
one unsplittable file and getting a single-task stage regardless of how many
executors YARN or Kubernetes gave it - `spark.sql.files.maxPartitionBytes`
and coalesce/repartition exist to manage precisely this. Ballista inherits
the constraint because it inherits DataFusion's partition-per-file scan
behavior; the fix is the same shape in both systems, just applied by hand
here instead of via a Spark config knob.

```mermaid
flowchart TB
    Sched["Ballista Scheduler\n(owns the DAG, hands out tasks)"]
    E1["Executor 1"]
    E2["Executor 2"]
    F1["part-0.parquet"]
    F2["part-1.parquet"]
    Sched -->|task: partition 0| E1
    Sched -->|task: partition 1| E2
    E1 --> F1
    E2 --> F2
    E1 <-->|shuffle intermediate results| E2
```

Two Parquet files, two partitions, two tasks - one per executor. A single
file would collapse this to one task on one executor regardless of cluster
size.

### One scheduler, N executors - the topology, not a black box

`justfile`'s `ballista-scheduler`, `ballista-executor-1`, `ballista-executor-2`
recipes (and the further `ballista-executor-3` through `-12` recipes for
larger cluster runs) start real, separate OS processes communicating over
gRPC - a scheduler that accepts the logical plan and assigns tasks, and
executors that each run a share of those tasks and shuffle intermediate
results between each other. Nothing about this is simulated inside a single
process; it's a genuine distributed system, just small enough to run
entirely on one laptop for the purpose of proving the mechanism works.

**Spark bridge**: this is architecturally the same shape as a Spark
driver + a pool of executor JVMs talking over Spark's own RPC layer - a
scheduler that owns the DAG and hands out tasks, executors that run them and
exchange shuffle data. The proof point here isn't "distributed query
execution is a novel idea" - it's "DataFusion's plan and execution engine
can run under that same distributed shape without becoming a different
engine to reason about."

## From local processes to a measured real-cluster run

Every Ballista number earlier in this chapter, and every Spark number in the
[scale-and-parity chapter](scale-and-parity.md)'s join section, ran on one
physical host: DataFusion forced its shuffle through an in-process exchange
operator, and Spark ran in its default local mode inside a single container.
Neither crossed a real process/network boundary for the shuffle - a
single-process "cluster" still shares one heap, one scheduler thread, one
network loopback. The join-plus-shuffle query (`orders JOIN shipments ...`,
the same query and the same known-correct `500000` / `37500000.00` result
the scale chapter already proves) was rerun against a genuine multi-process
setup for each engine, still on one laptop, to see whether the local-process
numbers were hiding or flattering anything.

**Spark**: a second Compose file, alongside the existing single-container
setup used for the Comet benchmark, stands up an actual standalone cluster -
one `Master` container and two separate `Worker` containers, each registering
with the master over the Compose network instead of all running inside one
JVM:

```mermaid
flowchart TB
    M["spark-master container"]
    W1["spark-worker-1 container"]
    W2["spark-worker-2 container"]
    W1 -->|registers| M
    W2 -->|registers| M
```

Five runs against the master's URL; the first two were discarded as
cold-start/driver-registration outliers (consistent with every other
cold-start exclusion already made elsewhere in this book), leaving three
clean runs at roughly 7.2-7.3 seconds each - slower than the single-process
Spark number from the scale chapter's join section. That's expected, not a
regression: real network shuffle between two separate worker containers
costs more than an in-process local shuffle. This run establishes a more
honest floor, not an improvement.

**Ballista**: the same scheduler-plus-executors topology this chapter's
earlier Ballista section already describes, but actually exercised as a real
cluster for the first time - `ballista-scheduler` and two separate
`ballista-executor` OS processes, confirmed registered with the scheduler via
its own logs before the query runs, connecting over
`SessionContext::remote_with_state("df://localhost:50050", ...)`. Three runs
landed around 20-27 milliseconds - close to, and in this run slightly faster
than, the single-process DataFusion number from the same join. The most
likely reason: each of the two real executors only scans one of the join's
two split partition files, where a single process has to scan and shuffle
the whole dataset itself. This is a genuine distributed run, not a
reproduction of the single-process number, so it isn't a controlled
apples-to-apples comparison the way the single-process DataFusion-vs-Spark
numbers are - only the pairing within each cluster setup (Spark-cluster vs.
its own single-process baseline, Ballista-cluster vs. its own) is.

**What this still doesn't prove**: both clusters ran on one physical host.
Real network latency, a real scheduler choosing between machines, and
cross-host data skew are still untested. "Genuine multi-process, multi-port,
multi-container" is a materially stronger claim than "single process
pretending to be distributed" - it is not yet "multi-machine."

## Comet: DataFusion's engine, running inside Spark itself

Ballista replaces Spark. Comet does the opposite: it leaves an existing
Spark job's driver, scheduler, and shuffle service exactly where they are,
and swaps DataFusion's execution engine in underneath Spark's own physical
plan via `comet-spark-spark4.1_2.13-0.16.0.jar`
(`benchmark/spark-comet/comet-spark-spark4.1_2.13-0.16.0.jar`, loaded as a
Spark plugin). The SQL Spark runs doesn't change:

```sql
-- benchmark/spark-comet/query.sql
CREATE OR REPLACE TEMPORARY VIEW orders
USING parquet
OPTIONS (path '/spark-comet/orders.parquet');

SELECT COUNT(*) AS order_count, SUM(amount) AS total_amount
FROM orders
WHERE amount > 50.00;
```

What changes is which engine actually executes the scan, filter, and
aggregate operators underneath that plan - Comet substitutes DataFusion's
native operators for Spark's JVM ones where it can, inside the same Spark
process. A user running this query sees Spark; the acceleration is invisible
except in wall-clock time.

### One dataset, three ways of running the exact same query

```rust
//! M3.5 Comet benchmark fixture: generates a synthetic `orders`-schema
//! dataset deterministically ... Writes one Parquet file so DataFusion,
//! vanilla Spark, and Spark+Comet all read the exact same bytes.
```

(`crates/pipeline/examples/comet_benchmark_dataset.rs`)

```console
$ cargo run --release --package pipeline --example comet_benchmark_dataset -- \
    --output benchmark/spark-comet/orders.parquet
$ cargo run --release -p pipeline --bin comet-aggregate-datafusion -- \
    --input benchmark/spark-comet/orders.parquet
$ just spark-comet-up
$ # inside the spark container: run benchmark/spark-comet/query.sql
$ #   once against vanilla Spark, once with the Comet plugin enabled
$ just spark-comet-down
```

The dataset generator's comment states the deliberate design directly: one
Parquet file, one set of bytes, read by three different engines, so any
difference in wall-clock time reflects the execution engine and nothing
about the input differing between runs. Amounts cycle `1.00..=100.00` by
`(id % 100) + 1` - a seedless, deterministic pattern - specifically so
`order_count` and `total_amount` are computable independently of which
engine ran the query, giving every leg (DataFusion, vanilla Spark, Spark+Comet)
a correctness check as well as a timing number.

**Scala bridge**: think of Comet the way you'd think of swapping a JVM's
default garbage collector for a different one - the program you wrote, the
Spark SQL you submitted, is completely unchanged; a substitutable component
underneath the interface you already use got faster. That's a fundamentally
different kind of claim than "rewrite your Spark job in Rust," and this
codebase is careful to keep those two claims separate - Comet's benchmark
proves the substitution works and measures its effect against vanilla Spark,
nothing more, nothing about DataFusion-the-standalone-engine's own number
from a different benchmark leg.

```mermaid
flowchart LR
    subgraph JVM["Spark driver process - unchanged"]
        SQL["Same Spark SQL\n(query.sql, untouched)"]
        Plan["Spark physical plan\n(scan / filter / aggregate)"]
        Native["Comet plugin:\nDataFusion native operators\nrun underneath the plan"]
        SQL --> Plan --> Native
    end
```

The plan Spark builds doesn't change; what changes is which engine executes
the operators underneath it - Comet swaps in, Spark's driver and shuffle
service never leave the picture.

## From a manual query to a real application CI runs itself

Everything above runs Spark's `spark-sql` command-line tool against a
query file - useful for a quick comparison, but it's not how a real
Spark job is built, and until this codebase gained a second piece, no
CI job anywhere in this repository had ever started the Spark+Comet
Docker stack at all; the comparison only ever ran on a developer's own
machine.

`scala/comet-benchmark`'s `OrdersAggregateDriver` closes both gaps with
one real Scala application - a `SparkSession`, not a SQL text file piped
into a CLI:

```scala
val spark = SparkSession.builder().appName("comet-benchmark-orders-aggregate").getOrCreate()
val df = spark.read.parquet(args.output)
df.createOrReplaceTempView("orders")
val result = spark.sql(
  "SELECT COUNT(*) AS order_count, SUM(amount) AS total_amount FROM orders WHERE amount > 50.00"
)
```

The same compiled driver runs the comparison's two legs as two separate
`spark-submit` invocations against the same generated fixture - one
plain, one with the Comet jar and its shuffle manager on the classpath -
so nothing about the comparison depends on config state leaking between
runs inside one long-lived process. A plain Scala computation (no Spark
involved at all) checks the result independently, the same discipline
every other proof point in this book already follows.

A CI workflow now runs this driver's two invocations against a real,
ephemeral Spark+Comet container on every change to this code - the first
time any workflow in this repository has provisioned Docker, Spark, or
Comet, rather than leaving that step to a developer's own laptop.

### Reading the executed plan, not trusting the config flags

The claim earlier in this chapter - "Comet substitutes DataFusion's
native operators for Spark's JVM ones where it can" - is now backed by
something stronger than "the Comet jar was on the classpath and nothing
crashed." The accelerated run's own executed plan is inspected directly:

```text
CometHashAggregate [count#11L, sum#12, isEmpty#13], [Final], [count(1), sum(amount#1)]
+- CometExchange SinglePartition, ENSURE_REQUIREMENTS, CometNativeShuffle
   +- CometHashAggregate [amount#1], [Partial], [partial_count(1), partial_sum(amount#1)]
      +- CometFilter [amount#1], (isnotnull(amount#1) AND (amount#1 > 50.00))
         +- CometNativeScan parquet [amount#1] ...
```

Every operator in that plan - scan, filter, partial aggregate, the
exchange stage between partial and final aggregate, and the final
aggregate - carries a `Comet`-prefixed name. There is no row-conversion
operator anywhere in it. The plain (non-Comet) run's plan, over the exact
same query and fixture, contains zero `Comet`-prefixed operators. That
contrast - not a timing number - is the direct evidence that the
substitution actually happened for this query, rather than being
inferred from the presence of a configuration flag that might silently
have had no effect.

**The honest limit of this specific proof**: it runs at a small,
CI-appropriate row count on a shared CI runner, specifically to prove
the mechanism and keep the check fast and reliable - it is a correctness
and plan-shape proof, not a new performance number, and it does not
replace the wall-clock comparison already recorded earlier in this
chapter at realistic scale. The two are deliberately kept separate:
proving the substitution *happens* and measuring how much it's *worth*
are different claims, checked by different runs.

## Why the three never appear side by side as a ranking

Putting Ballista's elapsed time and Comet's elapsed time in one table,
ranked, would imply they're solving the same problem - they aren't. Ballista
answers "should this workload leave the JVM entirely, distributed?" Comet
answers "should this workload stay on Spark, just faster?" Those are
different decisions an engineering team makes for different reasons - data
gravity, operational maturity, existing job complexity, migration cost - and
collapsing them into one leaderboard erases exactly the information that
makes each proof point useful on its own. Every real number behind these
claims is reproducible - toolchain, hardware, seed, and elapsed time are
recorded alongside each measurement - and each one is labeled against its own
specific Spark baseline, not against each other.

## Try it yourself

```sh
just ballista-scheduler & just ballista-executor-1 & just ballista-executor-2 &
cargo run --release -p pipeline --bin ballista-aggregate --features ballista

cargo run --release --package pipeline --example comet_benchmark_dataset -- \
    --output benchmark/spark-comet/orders.parquet
cargo run --release -p pipeline --bin comet-aggregate-datafusion -- \
    --input benchmark/spark-comet/orders.parquet
```

Both proof points are runnable end to end on a single laptop - the cluster
in the Ballista case and the JVM in the Comet case are real, just scoped
small enough that "distributed" and "embedded in Spark" remain honest claims
without needing a real cluster to demonstrate them.

## A second, separate proof: the operator-replacement mechanism itself

The Comet benchmark above answers "does the substitution make a real query
faster" using the real `comet-spark-spark4.1_2.13-0.16.0.jar`. It does not
show *how* Comet decides which Spark physical operator a native DataFusion
operator is allowed to replace, because that decision lives inside Comet's
own closed-source rule set, not in this repo.

`scala/comet-interop` is a separate sbt/Scala project (not part of the
Cargo workspace, not wired into the Comet benchmark above) that proves that
mechanism in isolation: a `derives CometOperator` declaration generates a
`CometOperatorSpec` describing which Spark operator it replaces and under
what schema it's safe to do so, and a build-time sbt plugin
(`CometOpsScannerPlugin`) scans for every such declaration so the mapping is
checkable without running Spark at all. It uses stand-in `PlanNode`/
`ColumnarRule` types rather than extending Comet's or Spark's real classes,
because the real jars in this repo are compiled for Scala 2.13 and this
scaffold targets Scala 3 - the two are binary-incompatible, so there is no
real base class to extend even if the goal were to run it inside a live
Spark job. See `scala/comet-interop/README.md` for the full operator
coverage table.

In short: the benchmark proves the *substitution works and is fast*, on one
real query, with the real jar. `scala/comet-interop` proves the *rule that
decides what can be substituted* is declarable and checkable at build time,
on a wider set of operator categories, without needing Spark running at
all. Neither stands in for the other.
