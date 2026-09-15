//! M3.5 Comet benchmark fixture: generates a synthetic `orders`-schema
//! dataset deterministically (sequential `id`, modulo-cycled `amount`/`note`,
//! no RNG — same seedless pattern as `compression_comparison.rs`'s
//! `synthetic_batch()`), large enough that Spark-vs-Comet wall-clock
//! separates from JVM/Docker startup noise. Writes one Parquet file so
//! DataFusion, vanilla Spark, and Spark+Comet all read the exact same bytes.
//!
//! Run: `cargo run --release --package pipeline --example comet_benchmark_dataset -- \
//!   --output benchmark/spark-comet/orders.parquet`

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use arrow::array::{
    Decimal128Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use clap::Parser;
use parquet::basic::Compression;
use pipeline::datafusion_query::orders_schema;
use pipeline::write_parquet;

const ROW_COUNT: usize = 5_000_000;
const MICROS_PER_DAY: i64 = 86_400_000_000;

#[derive(Parser)]
#[command(
    name = "comet-benchmark-dataset",
    about = "Generate a synthetic orders Parquet fixture for the Spark/Comet/DataFusion benchmark"
)]
struct Args {
    /// Path to write the generated Parquet file to.
    #[arg(long)]
    output: PathBuf,
}

/// Amounts cycle 1.00..=100.00 by `(id % 100) + 1`, so exactly 1/2 of rows
/// have `amount > 50.00` — the filter predicate benchmarked downstream — and
/// the expected `order_count`/`total_amount` are computable independently of
/// any engine under test.
fn synthetic_orders_batch() -> RecordBatch {
    let notes = ["first order", "rush", "bulk", "backfill", ""];

    let ids: Int64Array = (0..ROW_COUNT as i64).collect();
    let amounts = Decimal128Array::from_iter_values(
        (0..ROW_COUNT as i64).map(|id| (id % 100 + 1) as i128 * 100),
    )
    .with_precision_and_scale(10, 2)
    .expect("unscaled amounts in 100..=10000 fit Decimal128(10, 2)");
    let placed_ats = TimestampMicrosecondArray::from_iter_values(
        (0..ROW_COUNT as i64).map(|id| id * MICROS_PER_DAY / ROW_COUNT as i64),
    );
    let note_array: StringArray = (0..ROW_COUNT)
        .map(|i| match notes[i % notes.len()] {
            "" => None,
            note => Some(note),
        })
        .collect();

    RecordBatch::try_new(
        orders_schema(),
        vec![
            Arc::new(ids),
            Arc::new(amounts),
            Arc::new(placed_ats),
            Arc::new(note_array),
        ],
    )
    .expect("synthetic columns match orders_schema")
}

fn main() -> ExitCode {
    let args = Args::parse();
    let batch = synthetic_orders_batch();
    let rows = batch.num_rows();
    match write_parquet(&batch, &args.output, Compression::SNAPPY) {
        Ok(()) => {
            println!("wrote {rows} row(s) to {}", args.output.display());
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("comet-benchmark-dataset: {err}");
            ExitCode::FAILURE
        }
    }
}
