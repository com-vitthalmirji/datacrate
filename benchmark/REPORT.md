# Benchmark report — 2026-09-21

Hardware: Apple M2 Max, 12 CPUs, 64GB RAM (macOS 26.6.2). Docker Desktop: 12 CPUs, 7.748GiB memory.
Toolchain: rustc 1.97.1. Spark: `apache/spark:4.1.3-java17`. Comet: `comet-spark-spark4.1_2.13-0.16.0.jar`.
Raw captures: `benchmark/reports/2026-09-21/*.txt` (local only, gitignored — 3 runs per leg unless noted).

## Parity (M3 gate — correctness, not speed)

`fixtures/m3/orders.csv` (8 rows) → window/null/ordering query, 3 runs each on DataFusion and Spark.
Byte-identical output across all 3 runs on both engines, and DataFusion matches Spark row-for-row
(ordering, `running_total`/`amount_rank`, NULL handling). No regression from 2026-09-17.

## Comet aggregate (5,000,000-row `orders.parquet`, `SUM(amount) WHERE amount > 50.00`)

| Leg | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| Spark vanilla | 5.429s | 5.512s | 5.497s |
| Spark + Comet | 5.946s | 5.858s | 5.741s |
| DataFusion | 27.17ms | 28.72ms | 21.33ms |

All three legs agree on the result (`2500000` rows, `188750000.00` total). Comet is slower than vanilla
Spark on this query/hardware. DataFusion's number times the query only (in-process `elapsed=`); the Spark
numbers wrap the whole `spark-sql` invocation including JVM startup, so only Comet vs. vanilla Spark is
apples-to-apples on magnitude — DataFusion's number just shows sub-30ms is achievable for this query shape.

## Join + shuffle (2,000,000 orders ⋈ 1,000,001 shipments, forced shuffle join)

| Leg | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| Spark vanilla | 6.639s | 6.285s | 6.227s |
| Spark + Comet | 6.919s | 6.606s | 6.878s |
| DataFusion | 64.58ms | 69.29ms | 61.37ms |

Same pattern: Comet doesn't beat vanilla Spark here either. All three legs agree on the result
(`500000` shipped orders, `37500000.00` total).

`join-shuffle-datafusion`'s first run this session took 7m 38s to compile (`lto=fat, codegen-units=1`,
cold on this checkout) — long enough to look like a hang before `ps aux` confirmed it was still in
`rustc`. Runs 2 and 3 compiled in under a second. Any `ci-release`-profile binary needs a warm-up build
before a timed loop, or a timeout that covers a cold compile.

## Operational gaps found this session

- `benchmark/join-shuffle/{orders_join,shipments_join}.parquet` and `benchmark/spark-comet/orders.parquet`
  are gitignored and don't exist on a fresh checkout. `just comet-dataset` and `just join-shuffle-dataset`
  must run first, or every leg fails with `PATH_NOT_FOUND`. Not documented anywhere yet.
- Cold `lto=fat, codegen-units=1` release builds take several minutes; a benchmark loop run right after
  a fresh checkout or `cargo clean` needs a longer first-run timeout.

## Deferred: full-scale (91GB/140GB) `m3.6`/`m3.8` reruns

Not run this session. `benchmark/m3.6` (91G) and `benchmark/m3.8` (140G) exist on disk from an earlier
session; regenerating them under Docker's current 7.748GiB memory allocation risks disk exhaustion and
Spark OOM/spill crashes, which would produce results that can't honestly be recorded as `demonstrated`.
Deferred pending a higher Docker Desktop memory allocation.
