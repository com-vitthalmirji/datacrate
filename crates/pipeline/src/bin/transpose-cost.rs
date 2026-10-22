//! Isolates the mechanism behind the dramatic number in Jordan Epstein's own
//! Comet benchmark (`docs/internals/datafusion-comet-story-2.md`, the
//! three-way read/write-Parquet comparison near the end): Comet
//! read-only-acceleration still transposes Arrow batches to Spark's internal
//! row format before the write, because the write path demands rows; Comet
//! read+write never transposes at all. He measures a 3.6x gap at 100 columns
//! that shrinks to a small-but-real gap at 1 column — the gap tracks column
//! count because a transpose touches every column per row, so a wider row
//! costs more to build and tear down.
//!
//! This binary reproduces that shape without Parquet I/O (file I/O would
//! dominate wall time at this row count and hide the transpose signal the
//! way it already hid once in `row-vs-columnar`'s filtered-sum case, which
//! had no transpose step at all): generate `cols` Arrow columns, then time
//! applying the same elementwise transform two ways — directly on the
//! columns (no transpose, modeling Comet read+write), and via a round trip
//! through a flat row-major buffer (modeling Comet read-only, which still
//! owes the write path rows, laid out the way Spark's `UnsafeRow` is: one
//! contiguous buffer, not nested per-row allocations). Both paths must
//! produce identical output; this is checked before exit.

use std::process::ExitCode;
use std::time::Instant;

use arrow::array::Float64Array;
use clap::Parser;

#[derive(Parser)]
#[command(
    name = "transpose-cost",
    about = "Time an elementwise transform with vs. without a row-major round trip"
)]
struct Args {
    /// Row count.
    #[arg(long, default_value_t = 500_000)]
    rows: usize,

    /// Column count — the dimension the transpose cost is expected to scale with.
    #[arg(long, default_value_t = 50)]
    cols: usize,

    /// Timed repetitions per path.
    #[arg(long, default_value_t = 3)]
    runs: usize,
}

fn generate_columns(rows: usize, cols: usize) -> Vec<Float64Array> {
    (0..cols)
        .map(|c| Float64Array::from_iter_values((0..rows).map(|r| (r + c) as f64)))
        .collect()
}

/// No transpose: each column transformed in place, Comet read+write's shape.
fn transform_columnar(columns: &[Float64Array]) -> Vec<Float64Array> {
    columns
        .iter()
        .map(|column| Float64Array::from_iter_values(column.values().iter().map(|v| v + 1.0)))
        .collect()
}

/// Arrow columns -> one flat row-major buffer (row `r`, column `c` at
/// `r * cols + c`) — a single contiguous allocation, the same shape as
/// Spark's `UnsafeRow`. A `Vec<Vec<f64>>` was tried first and discarded: it
/// forces one heap allocation per row regardless of column count, which
/// swamps the signal this binary is trying to isolate (see `docs/internals`
/// for the discarded numbers). The gather itself still touches `cols`
/// separate source arrays per row, which is the real transpose cost.
fn to_row_major(columns: &[Float64Array], rows: usize, cols: usize) -> Vec<f64> {
    let mut buf = vec![0.0_f64; rows * cols];
    for r in 0..rows {
        for c in 0..cols {
            buf[r * cols + c] = columns[c].value(r);
        }
    }
    buf
}

fn transform_rows(buf: &[f64]) -> Vec<f64> {
    buf.iter().map(|v| v + 1.0).collect()
}

/// Flat row-major buffer -> Arrow columns, the transpose back before the
/// result can be used as columns again.
fn to_columnar(buf: &[f64], rows: usize, cols: usize) -> Vec<Float64Array> {
    (0..cols)
        .map(|c| Float64Array::from_iter_values((0..rows).map(|r| buf[r * cols + c])))
        .collect()
}

fn sum_all(columns: &[Float64Array]) -> f64 {
    columns.iter().map(|c| c.values().iter().sum::<f64>()).sum()
}

fn time_runs<F: Fn() -> Vec<Float64Array>>(label: &str, runs: usize, f: F) -> f64 {
    let mut last_sum = 0.0_f64;
    let mut durations = Vec::with_capacity(runs);
    for _ in 0..runs {
        let start = Instant::now();
        let result = f();
        durations.push(start.elapsed());
        last_sum = sum_all(&result);
    }
    let formatted: Vec<String> = durations.iter().map(|d| format!("{d:?}")).collect();
    println!("{label}: {} (checksum={last_sum})", formatted.join(", "));
    last_sum
}

fn main() -> ExitCode {
    let args = Args::parse();
    let columns = generate_columns(args.rows, args.cols);

    let columnar_sum = time_runs("columnar (no transpose)", args.runs, || {
        transform_columnar(&columns)
    });
    let row_trip_sum = time_runs(
        "row round trip (transpose, transform, transpose back)",
        args.runs,
        || {
            let rows = to_row_major(&columns, args.rows, args.cols);
            let transformed = transform_rows(&rows);
            to_columnar(&transformed, args.rows, args.cols)
        },
    );

    if (columnar_sum - row_trip_sum).abs() > 1e-6 {
        eprintln!(
            "transpose-cost: result mismatch, columnar={columnar_sum} row_trip={row_trip_sum}"
        );
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}
