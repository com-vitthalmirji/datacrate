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

## Row-vs-columnar micro-benchmarks — 2026-10-22

Hardware: Apple M2 Max, 12 CPUs, 64GB RAM (macOS 26.6.2). Toolchain: rustc 1.97.1.
In-process only, no Spark/JVM/Parquet I/O involved — these isolate one mechanism each
behind the Comet aggregate numbers above. System checked idle (`uptime`/`top`) before
each pass. 5 runs per leg, each its own process invocation (`--runs 1`, matching this
file's "whole invocation" convention) to avoid in-process warm-up bias.

### `row-vs-columnar` — filtered sum, 5,000,000 rows, no transpose involved

`SUM(amount) WHERE amount > 50.00` over a `Vec<OrderRow>` (AoS) vs. a single
`Float64Array` (Arrow SoA), both already in the layout the computation needs. Median of
5 runs: row (AoS) 7.27ms; Arrow `filter()+sum()` kernels 9.43ms (~30% *slower* than row —
`filter()` materializes a whole intermediate array before `sum()` runs); Arrow fused
single-pass scan 6.98ms (~4% faster than row). Conclusion: with no row/columnar
conversion involved, the layout-only effect on this hardware at this scale is small —
naive kernel composition can even cost more than it saves; it tests a
narrower mechanism than the one his benchmark below actually measures.

### `transpose-cost` — elementwise transform, 500,000 rows, with vs. without a row round trip

Reproduces the mechanism behind own 3.6x-at-100-columns benchmark: Comet read-only acceleration still
transposes Arrow batches to a row format before the write; Comet read+write never does.
Here: `cols` Arrow columns, transformed directly (no transpose) vs. via a round trip
through one flat row-major buffer (`UnsafeRow`'s actual shape — a `Vec<Vec<f64>>` first
attempt was discarded, see the binary's own doc comment, because per-row heap allocation
swamped the signal). Average of 5 runs (each its own process):

| Columns | Columnar (no transpose) | Row round trip | Ratio |
|---|---|---|---|
| 1 | 0.370ms | 1.250ms | 3.4x |
| 10 | 3.20ms | 17.19ms | 5.4x |
| 50 | 15.30ms | 217.98ms | 14.2x |
| 100 | 32.19ms | 388.36ms | 12.1x |

All four column counts agree on the result checksum across every run. The gap climbs
with column count (3.4x → 14.2x) through 50 columns, then plateaus/dips slightly at 100
— within the sample size here, not yet distinguishable from noise. Same direction as measured 3.6x at 100 columns: the win isn't "Rust is faster at arithmetic," it's
eliminating the row↔columnar conversion at a pipeline boundary, and that cost scales with
how wide the row is.

### `nested-transpose-cost` — `List<Float64>` column, with vs. without per-row materialization

A harder case than `transpose-cost`'s flat columns: a `List<Float64>` field, where each
row has a different length, so a row can only be materialized as its own owned `Vec<f64>`
— the per-row allocation a flat column can design away (see `transpose-cost.rs`'s own doc
comment on why it dropped a `Vec<Vec<f64>>` layout). Here: 200,000 rows, elementwise `+1.0`,
one pass over the flat values buffer (no per-row materialization) vs. walking each row,
collecting it into a `Vec<f64>`, transforming, rebuilding. Median of 3 runs (each its own
process):

| Max elements/row | Columnar (no materialization) | Row round trip (per-row `Vec`) | Ratio |
|---|---|---|---|
| 5 | 0.18ms | 11.58ms | 64.6x |
| 20 | 0.50ms | 11.39ms | 22.9x |
| 50 | 1.01ms | 17.12ms | 17.0x |

All three row lengths agree on the result checksum across every run. Unlike
`transpose-cost`'s flat-column gap (which climbed with column count), this gap *shrinks*
as rows get longer — the row-trip leg's per-row fixed overhead (one `Vec` allocation plus
the `ListBuilder`'s own per-row bookkeeping) dominates at short row lengths, so the
smallest case tested (5 elements/row) shows the largest gap, bigger than anything measured
for flat columns in this report. This supports, with a real measurement rather than just
citing the architecture, why native accelerators (Comet, Velox, Gluten alike) commonly
fall back to JVM/row execution specifically for nested-type operators: even a small
variable-length column pays a cost flat columns never have to.

**Type-safety follow-up**: the two layouts measured above (`cols` vs. the flat row-major
buffer) are now `ColumnarBatch`/`RowBatch` in `crates/pipeline/src/layout.rs` — two plain
structs, not interchangeable. Crossing between them only happens through a named call
(`to_row_major`/`to_columnar`), so a caller can no longer pass a row-major buffer where a
columnar one is required without `rustc` rejecting it. `tests/compile_fail/layout_type_mismatch.rs`
proves this: it asserts the real compiler error (`E0308: mismatched types ... expected
'&ColumnarBatch', found '&RowBatch'`) for exactly that mistake. This is a compile-time
guarantee claim, not a new performance number - the timing table above is unchanged.

## Zero-transpose end-to-end — 2026-10-22

Hardware: Apple M2 Max, 12 CPUs, 64GB RAM (macOS 26.6.2). Toolchain: rustc 1.97.1.
`zero-transpose-end-to-end` against the Comet benchmark's real 5,000,000-row
`benchmark/spark-comet/orders.parquet`: `SELECT * FROM orders WHERE amount > 50.00`,
concatenated, written to Parquet via `pipeline::write_parquet(&RecordBatch, ...)`.

3 runs (each its own process invocation): 204.54ms, 177.53ms, 180.20ms. All three
write the same 2,500,000 rows — matching the Comet aggregate benchmark's known
filtered row count above.

This is a structural claim, not a cost-quantification one: `write_parquet` takes
`&RecordBatch` (Arrow's own columnar type) directly, so nothing in this read →
filter → write path ever calls `to_row_major` — confirmed by reading the call chain,
not by this timing. The elapsed time here is a sanity check that the path actually
runs end-to-end on real data, not a benchmark result to compare against anything.
An earlier version of this binary also tried to quantify the transpose tax avoided
by running a second, forced-row-round-trip leg on one extracted column — that leg
skipped disk I/O and touched one column instead of four, so its timing wasn't
comparable to this leg's, and it was dropped rather than risk recording a misleading
number. The quantified tax (3.4x–14.2x) is already measured correctly, with
controlled column counts and no I/O confound, by the `transpose-cost` benchmark
above.

## Deferred: full-scale (91GB/140GB) `m3.6`/`m3.8` reruns

Not run this session. `benchmark/m3.6` (91G) and `benchmark/m3.8` (140G) exist on disk from an earlier
session; regenerating them under Docker's current 7.748GiB memory allocation risks disk exhaustion and
Spark OOM/spill crashes, which would produce results that can't honestly be recorded as `demonstrated`.
Deferred pending a higher Docker Desktop memory allocation.
