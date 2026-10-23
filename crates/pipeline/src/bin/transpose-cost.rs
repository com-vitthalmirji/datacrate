//! Isolates the mechanism behind the dramatic number in a public Comet
//! benchmark (`docs/internals/datafusion-comet-story-2.md`, the
//! three-way read/write-Parquet comparison near the end): Comet
//! read-only-acceleration still transposes Arrow batches to Spark's internal
//! row format before the write, because the write path demands rows; Comet
//! read+write never transposes at all. That benchmark measures a 3.6x gap at
//! 100 columns that shrinks to a small-but-real gap at 1 column — the gap
//! tracks column count because a transpose touches every column per row, so
//! a wider row costs more to build and tear down.
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
//!
//! The round trip itself goes through [`pipeline::layout::ColumnarBatch`] and
//! [`pipeline::layout::RowBatch`], not a private copy of the transpose: that
//! module exists so this exact operation is a named, visible call at every
//! site that performs it, instead of something a benchmark or an operator
//! can reimplement silently inside its own body.

use std::process::ExitCode;
use std::time::Instant;

use arrow::array::Float64Array;
use clap::Parser;
use pipeline::layout::ColumnarBatch;

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
    let batch = ColumnarBatch::try_new(columns, args.rows)
        .expect("generate_columns always produces columns of length rows");
    let row_trip_sum = time_runs(
        "row round trip (transpose, transform, transpose back)",
        args.runs,
        || {
            let row_major = batch.to_row_major();
            let transformed: Vec<f64> = row_major.buffer().iter().map(|v| v + 1.0).collect();
            pipeline::layout::RowBatch::try_new(transformed, args.rows, args.cols)
                .expect("transformed buffer keeps the same rows*cols length")
                .to_columnar()
                .columns()
                .to_vec()
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
