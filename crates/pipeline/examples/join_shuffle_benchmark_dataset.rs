//! M3.7 join/shuffle benchmark fixture: generates two synthetic Parquet
//! files - `orders` (same shape as [`comet_benchmark_dataset`]'s fixture) and
//! `shipments`, related by `shipments.order_id = orders.id` - deterministically
//! (sequential ids, modulo-cycled fields, no RNG), so the join's
//! `shipped_order_count`/`shipped_total_amount` are computable independently
//! of any engine under test. Only even-numbered orders get a shipment (half
//! the rows are dropped by the join, not just decorated), and order 0 gets a
//! second shipment row to prove duplicate-shipment fan-out doesn't corrupt
//! the filtered aggregate (its amount, 1.00, never passes `amount > 50.00`).
//!
//! Run: `cargo run --release --package pipeline --example join_shuffle_benchmark_dataset -- \
//!   --output-dir benchmark/join-shuffle`

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use arrow::array::{
    Decimal128Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use clap::Parser;
use parquet::basic::Compression;
use pipeline::datafusion_query::{orders_schema, shipments_schema};
use pipeline::write_parquet;

const ORDER_COUNT: usize = 2_000_000;
const MICROS_PER_DAY: i64 = 86_400_000_000;
const CARRIERS: [&str; 4] = ["DHL", "UPS", "FedEx", "USPS"];

#[derive(Parser)]
#[command(
    name = "join-shuffle-benchmark-dataset",
    about = "Generate synthetic orders/shipments Parquet fixtures for the M3.7 join/shuffle benchmark"
)]
struct Args {
    /// Directory to write orders_join.parquet and shipments_join.parquet into.
    #[arg(long)]
    output_dir: PathBuf,
}

/// Same generation rule as `comet_benchmark_dataset`'s `synthetic_orders_batch`:
/// `amount = (id % 100) + 1`, so exactly half of rows have `amount > 50.00`.
fn synthetic_orders_batch() -> RecordBatch {
    let notes = ["first order", "rush", "bulk", "backfill", ""];

    let ids: Int64Array = (0..ORDER_COUNT as i64).collect();
    let amounts = Decimal128Array::from_iter_values(
        (0..ORDER_COUNT as i64).map(|id| (id % 100 + 1) as i128 * 100),
    )
    .with_precision_and_scale(10, 2)
    .expect("unscaled amounts in 100..=10000 fit Decimal128(10, 2)");
    let placed_ats = TimestampMicrosecondArray::from_iter_values(
        (0..ORDER_COUNT as i64).map(|id| id * MICROS_PER_DAY / ORDER_COUNT as i64),
    );
    let note_array: StringArray = (0..ORDER_COUNT)
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

/// One shipment per even-numbered order id, plus one extra duplicate for
/// order 0. Odd-numbered orders get none, so the join drops them.
fn synthetic_shipments_batch() -> RecordBatch {
    let mut order_ids: Vec<i64> = (0..ORDER_COUNT as i64).step_by(2).collect();
    order_ids.push(0);

    let carriers: StringArray = order_ids
        .iter()
        .enumerate()
        .map(|(i, _)| Some(CARRIERS[i % CARRIERS.len()]))
        .collect();
    let shipped_ats = TimestampMicrosecondArray::from_iter_values(
        order_ids
            .iter()
            .map(|&id| id * MICROS_PER_DAY / ORDER_COUNT as i64),
    );
    let order_id_array: Int64Array = order_ids.into_iter().collect();

    RecordBatch::try_new(
        shipments_schema(),
        vec![
            Arc::new(order_id_array),
            Arc::new(carriers),
            Arc::new(shipped_ats),
        ],
    )
    .expect("synthetic columns match shipments_schema")
}

fn main() -> ExitCode {
    let args = Args::parse();
    if let Err(err) = std::fs::create_dir_all(&args.output_dir) {
        eprintln!(
            "join-shuffle-benchmark-dataset: creating {}: {err}",
            args.output_dir.display()
        );
        return ExitCode::FAILURE;
    }

    let orders = synthetic_orders_batch();
    let orders_path = args.output_dir.join("orders_join.parquet");
    if let Err(err) = write_parquet(&orders, &orders_path, Compression::SNAPPY) {
        eprintln!("join-shuffle-benchmark-dataset: {err}");
        return ExitCode::FAILURE;
    }
    println!(
        "wrote {} row(s) to {}",
        orders.num_rows(),
        orders_path.display()
    );

    let shipments = synthetic_shipments_batch();
    let shipments_path = args.output_dir.join("shipments_join.parquet");
    if let Err(err) = write_parquet(&shipments, &shipments_path, Compression::SNAPPY) {
        eprintln!("join-shuffle-benchmark-dataset: {err}");
        return ExitCode::FAILURE;
    }
    println!(
        "wrote {} row(s) to {}",
        shipments.num_rows(),
        shipments_path.display()
    );

    ExitCode::SUCCESS
}
