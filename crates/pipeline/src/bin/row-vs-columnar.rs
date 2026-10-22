//! Isolates the row-vs-columnar effect behind the headline number in
//! `benchmark/REPORT.md` (DataFusion ~27ms vs. Spark/Comet ~5.5-5.9s on the
//! same `SUM(amount) WHERE amount > 50.00` shape). That number proves the
//! whole-pipeline claim; this binary times just the one mechanism behind it
//! — scanning one column out of a struct-of-rows (`AoS`, modeling a
//! JVM-style row) versus scanning a single contiguous `Float64Array`
//! (`SoA`/Arrow columnar) — on identical in-process data, no Spark/JVM
//! involved. `amount` is `f64` here, not `Decimal128` as in the real
//! pipeline schema: this isolates the layout effect using Arrow's ready-made
//! `cmp`/`filter`/`sum` kernels on a type they support directly, rather than
//! re-deriving decimal arithmetic for an illustrative microbenchmark.

use std::hint::black_box;
use std::process::ExitCode;
use std::time::Instant;

use arrow::array::Float64Array;
use arrow::compute::kernels::cmp::gt;
use arrow::compute::{filter, sum};
use clap::Parser;

#[derive(Parser)]
#[command(
    name = "row-vs-columnar",
    about = "Time a filtered sum over row-oriented (AoS) vs. columnar (Arrow) layouts"
)]
struct Args {
    /// Row count. Defaults to the same scale as the Comet aggregate benchmark.
    #[arg(long, default_value_t = 5_000_000)]
    rows: usize,

    /// Timed repetitions per layout.
    #[arg(long, default_value_t = 3)]
    runs: usize,
}

/// One row the way a JVM object would lay it out: fields interleaved in a
/// single allocation, `note` adding the kind of per-row heap overhead a real
/// `Order`/`Row` object carries alongside the one column the query needs.
struct OrderRow {
    id: i64,
    amount: f64,
    placed_at: i64,
    note: String,
}

fn generate_rows(rows: usize) -> Vec<OrderRow> {
    (0..rows)
        .map(|i| OrderRow {
            id: i as i64,
            amount: (i % 100) as f64 + 0.5,
            placed_at: 1_700_000_000 + i as i64,
            note: format!("order-{i}"),
        })
        .collect()
}

fn generate_amount_column(rows: &[OrderRow]) -> Float64Array {
    Float64Array::from_iter_values(rows.iter().map(|row| row.amount))
}

/// Row-oriented filtered sum: a scalar loop over `Vec<OrderRow>`, touching
/// each row's full stride (`id`/`placed_at`/`note` included) to read the one
/// field the query needs — the cache-locality cost Jordan's talk attributes
/// to row-oriented JVM execution.
fn sum_amount_over_threshold_rows(rows: &[OrderRow], threshold: f64) -> f64 {
    let mut total = 0.0_f64;
    for row in rows {
        black_box(row.id);
        black_box(row.placed_at);
        black_box(&row.note);
        if row.amount > threshold {
            total += row.amount;
        }
    }
    total
}

/// Columnar filtered sum via Arrow's `cmp`/`filter`/`sum` kernels. `filter`
/// materializes a whole new `Float64Array` for the matching rows before
/// `sum` scans it — a real allocation-and-copy cost, not just arithmetic.
fn sum_amount_over_threshold_columnar(amounts: &Float64Array, threshold: f64) -> f64 {
    let threshold_array = Float64Array::from_value(threshold, amounts.len());
    let mask = gt(amounts, &threshold_array).expect("gt kernel should run on equal-length arrays");
    let filtered = filter(amounts, &mask).expect("filter kernel should run on a boolean mask");
    let filtered = filtered
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("filter preserves the Float64 array type");
    sum(filtered).unwrap_or(0.0)
}

/// Columnar filtered sum, fused: one pass over the contiguous slice with no
/// intermediate array, the same fusion the row-oriented loop already gets
/// for free. Isolates the layout effect from `filter()`'s materialization
/// cost above.
fn sum_amount_over_threshold_columnar_fused(amounts: &Float64Array, threshold: f64) -> f64 {
    amounts
        .values()
        .iter()
        .filter(|&&amount| amount > threshold)
        .sum()
}

fn time_runs<F: Fn() -> f64>(label: &str, runs: usize, f: F) -> f64 {
    let mut last_result = 0.0_f64;
    let mut durations = Vec::with_capacity(runs);
    for _ in 0..runs {
        let start = Instant::now();
        last_result = f();
        durations.push(start.elapsed());
    }
    let formatted: Vec<String> = durations.iter().map(|d| format!("{d:?}")).collect();
    println!("{label}: {} (result={last_result})", formatted.join(", "));
    last_result
}

fn main() -> ExitCode {
    let args = Args::parse();
    let threshold = 50.0_f64;

    let rows = generate_rows(args.rows);
    let amounts = generate_amount_column(&rows);

    let row_result = time_runs("row (AoS)", args.runs, || {
        sum_amount_over_threshold_rows(&rows, threshold)
    });
    let columnar_result = time_runs(
        "columnar (Arrow SoA, filter+sum kernels)",
        args.runs,
        || sum_amount_over_threshold_columnar(&amounts, threshold),
    );
    let columnar_fused_result = time_runs("columnar (Arrow SoA, fused scan)", args.runs, || {
        sum_amount_over_threshold_columnar_fused(&amounts, threshold)
    });

    if (row_result - columnar_result).abs() > 1e-6
        || (row_result - columnar_fused_result).abs() > 1e-6
    {
        eprintln!(
            "row-vs-columnar: result mismatch, row={row_result} columnar={columnar_result} columnar_fused={columnar_fused_result}"
        );
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}
