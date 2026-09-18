//! M3.8 shipments-at-scale generator: writes a `shipments`-schema Parquet
//! file (or, with `--partitions N > 1`, a directory of N Parquet files)
//! covering order ids `0..order_rows`, streaming row-group by row-group like
//! `scale_benchmark_dataset.rs` so the total size isn't bounded by RAM.
//!
//! Deterministic, no RNG, same convention as the orders generator: each
//! order id's shipment count is `id % 3` (0, 1, or 2 shipments), matching
//! `fixtures/m3/shipments.csv`'s established 0/1/2-shipments-per-order mix
//! so `join_aggregate_query_sql`'s inner join has the same shape at scale
//! as it does at small-fixture scale, just with a real row count behind it.
//!
//! Run: `cargo run --release --package pipeline --example scale_benchmark_shipments -- \
//!   --output benchmark/m3.8/shipments.parquet --order-rows 5000000`
//! Partitioned: `... --output benchmark/m3.8/shipments --order-rows 5000000 --partitions 4`

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use clap::Parser;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use pipeline::datafusion_query::shipments_schema;

const MICROS_PER_DAY: i64 = 86_400_000_000;

#[derive(Parser)]
#[command(
    name = "scale-benchmark-shipments",
    about = "Generate a shipments-schema Parquet file (or partitioned directory) covering 0..order-rows order ids for the M3.8 join-at-scale comparison"
)]
struct Args {
    /// Path to write to: a single Parquet file when `--partitions 1`
    /// (default), or a directory of `part-NNNNN.parquet` files otherwise.
    #[arg(long)]
    output: PathBuf,
    /// Number of order ids to generate shipments for (0..order_rows) —
    /// should match the orders table's `--rows` for a well-formed join.
    #[arg(long)]
    order_rows: u64,
    /// Order ids per streamed batch/row-group (bounds peak memory; does not
    /// affect the generated data).
    #[arg(long, default_value_t = 2_000_000)]
    batch_size: u64,
    /// Number of output Parquet files; the order-id range is split evenly
    /// across them.
    #[arg(long, default_value_t = 1)]
    partitions: u64,
}

#[derive(Debug)]
enum CliError {
    OpenOutput {
        path: PathBuf,
        source: std::io::Error,
    },
    WriteParquet(parquet::errors::ParquetError),
    OrderRowsExceedI64Max {
        order_rows: u64,
    },
    ZeroPartitions,
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OpenOutput { path, source } => {
                write!(f, "creating {}: {source}", path.display())
            }
            Self::WriteParquet(source) => write!(f, "writing parquet: {source}"),
            Self::OrderRowsExceedI64Max { order_rows } => {
                write!(
                    f,
                    "--order-rows {order_rows} exceeds i64::MAX ({}); order_id and timestamp columns are i64",
                    i64::MAX
                )
            }
            Self::ZeroPartitions => write!(f, "--partitions must be at least 1"),
        }
    }
}

/// Shipment rows for a single order id: `id % 3 == 0` ships zero times,
/// `== 1` ships once, `== 2` ships twice — same mix
/// `fixtures/m3/shipments.csv` already established at small scale.
fn shipment_rows_for_order(id: u64) -> Vec<(i64, String, i64)> {
    let placed_at = (id as i64) * MICROS_PER_DAY / 1_000_000_000;
    match id % 3 {
        0 => Vec::new(),
        1 => vec![(
            id as i64,
            format!("carrier-{}", id % 5),
            placed_at + MICROS_PER_DAY,
        )],
        _ => vec![
            (
                id as i64,
                format!("carrier-{}", id % 5),
                placed_at + MICROS_PER_DAY,
            ),
            (
                id as i64,
                format!("carrier-{}", (id + 1) % 5),
                placed_at + 2 * MICROS_PER_DAY,
            ),
        ],
    }
}

fn synthetic_batch(start_id: u64, len: u64) -> RecordBatch {
    let mut order_ids = Vec::new();
    let mut carriers = Vec::new();
    let mut shipped_ats = Vec::new();
    for id in start_id..start_id + len {
        for (order_id, carrier, shipped_at) in shipment_rows_for_order(id) {
            order_ids.push(order_id);
            carriers.push(carrier);
            shipped_ats.push(shipped_at);
        }
    }

    let order_id_array: Int64Array = order_ids.into_iter().collect();
    let carrier_array: StringArray = carriers.into_iter().map(Some).collect();
    let shipped_at_array: TimestampMicrosecondArray = shipped_ats.into_iter().map(Some).collect();

    RecordBatch::try_new(
        shipments_schema(),
        vec![
            Arc::new(order_id_array),
            Arc::new(carrier_array),
            Arc::new(shipped_at_array),
        ],
    )
    .expect("synthetic columns match shipments_schema")
}

/// Splits `order_rows` into `partitions` near-equal, contiguous
/// `(start_id, len)` order-id ranges; the first `order_rows % partitions`
/// ranges absorb the remainder.
fn partition_ranges(order_rows: u64, partitions: u64) -> Vec<(u64, u64)> {
    let base = order_rows / partitions;
    let remainder = order_rows % partitions;
    let mut ranges = Vec::with_capacity(partitions as usize);
    let mut start = 0u64;
    for i in 0..partitions {
        let len = base + u64::from(i < remainder);
        ranges.push((start, len));
        start += len;
    }
    ranges
}

fn partition_path(output: &Path, partitions: u64, index: usize) -> PathBuf {
    if partitions == 1 {
        output.to_path_buf()
    } else {
        output.join(format!("part-{index:05}.parquet"))
    }
}

/// Streams shipment rows for order ids `start_id..start_id+len` into one
/// Parquet file, calling `on_batch` with the cumulative order ids processed
/// in this partition after each row group. Returns the file's on-disk byte
/// size (0 if unreadable — only used for the summary line, not correctness).
fn write_partition(
    path: &Path,
    start_id: u64,
    len: u64,
    batch_size: u64,
    mut on_batch: impl FnMut(u64),
) -> Result<u64, CliError> {
    let file = std::fs::File::create(path).map_err(|source| CliError::OpenOutput {
        path: path.to_path_buf(),
        source,
    })?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, shipments_schema(), Some(props))
        .map_err(CliError::WriteParquet)?;

    let mut processed = 0u64;
    while processed < len {
        let batch_len = batch_size.min(len - processed);
        let batch = synthetic_batch(start_id + processed, batch_len);
        writer.write(&batch).map_err(CliError::WriteParquet)?;
        processed += batch_len;
        on_batch(processed);
    }
    writer.close().map_err(CliError::WriteParquet)?;

    Ok(std::fs::metadata(path).map(|meta| meta.len()).unwrap_or(0))
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("scale-benchmark-shipments: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), CliError> {
    let args = Args::parse();
    if args.order_rows > i64::MAX as u64 {
        return Err(CliError::OrderRowsExceedI64Max {
            order_rows: args.order_rows,
        });
    }
    if args.partitions == 0 {
        return Err(CliError::ZeroPartitions);
    }
    if args.partitions > 1 {
        std::fs::create_dir_all(&args.output).map_err(|source| CliError::OpenOutput {
            path: args.output.clone(),
            source,
        })?;
    }

    let start = Instant::now();
    let ranges = partition_ranges(args.order_rows, args.partitions);

    let mut processed_total = 0u64;
    let mut total_bytes = 0u64;
    for (index, (start_id, len)) in ranges.into_iter().enumerate() {
        let path = partition_path(&args.output, args.partitions, index);
        let base_processed = processed_total;
        total_bytes += write_partition(
            &path,
            start_id,
            len,
            args.batch_size,
            |partition_processed| {
                eprintln!(
                    "processed {}/{} order ids ({:.1?} elapsed)",
                    base_processed + partition_processed,
                    args.order_rows,
                    start.elapsed()
                );
            },
        )?;
        processed_total += len;
    }

    println!(
        "wrote shipments for {} order id(s) across {} partition(s) to {} ({total_bytes} bytes, {:.1?} elapsed)",
        args.order_rows,
        args.partitions,
        args.output.display(),
        start.elapsed()
    );

    Ok(())
}
