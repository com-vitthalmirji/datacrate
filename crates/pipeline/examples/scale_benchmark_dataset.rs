//! M3.6 dataset generator: writes an `orders`-schema Parquet file (or, with
//! `--partitions N > 1`, a directory of N Parquet files) at whatever row
//! count is requested, streaming row-group by row-group so the total size
//! isn't bounded by RAM (the M3.5/M3.7 generators build one in-memory batch,
//! which doesn't scale to ~100GB targets).
//! Partitioning lets every M3.6 engine (DataFusion, Ballista, Polars, Spark)
//! register the output as one table without a post-hoc file-splitting step.
//!
//! Deterministic, no RNG (same convention as `comet_benchmark_dataset.rs`
//! and `compression_comparison.rs`'s `synthetic_batch()`): `amount` is a
//! multiplicative hash of `id` folded into cents 100..=10000 (~9,901 distinct
//! values, ~50% pass `amount > 50.00` - the filter every other M3 leg's
//! query also uses), and `note` is `note-<id>` for most rows (near-unique,
//! so it doesn't dictionary-compress away to nothing the way M3.5's 5-value
//! `note` column does) with roughly one in seven rows `NULL`. This mix is
//! deliberately higher-cardinality than the M3.5/M3.7 generators so the
//! on-disk Parquet size is a realistic few-x compression ratio, not an
//! unrealistic dictionary-compression ratio - matching the reasoning
//! `decisions.md` already used to estimate `~100GB` from a "1TB logical,
//! ~3-4x compressed" starting point.
//!
//! Run: `cargo run --release --package pipeline --example scale_benchmark_dataset -- \
//!   --output benchmark/m3.6/orders.parquet --rows 5000000`
//! Partitioned: `... --output benchmark/m3.6/orders --rows 5000000 --partitions 4`

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    Decimal128Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use clap::Parser;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use pipeline::datafusion_query::orders_schema;

const MICROS_PER_DAY: i64 = 86_400_000_000;

#[derive(Parser)]
#[command(
    name = "scale-benchmark-dataset",
    about = "Generate an orders-schema Parquet file (or partitioned directory) of arbitrary row count for the M3.6 scale comparison"
)]
struct Args {
    /// Path to write to: a single Parquet file when `--partitions 1`
    /// (default), or a directory of `part-NNNNN.parquet` files otherwise.
    #[arg(long)]
    output: PathBuf,
    /// Total number of rows to generate.
    #[arg(long)]
    rows: u64,
    /// Rows per streamed batch/row-group (bounds peak memory; does not
    /// affect the generated data).
    #[arg(long, default_value_t = 2_000_000)]
    batch_size: u64,
    /// Number of output Parquet files; `--rows` is split evenly across them.
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
    RowsExceedI64Max {
        rows: u64,
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
            Self::RowsExceedI64Max { rows } => {
                write!(
                    f,
                    "--rows {rows} exceeds i64::MAX ({}); id and timestamp columns are i64",
                    i64::MAX
                )
            }
            Self::ZeroPartitions => write!(f, "--partitions must be at least 1"),
        }
    }
}

/// Multiplicative hash (Knuth's constant) folded into cents `100..=10000`
/// (i.e. `$1.00..=$100.00`, ~9,901 distinct values), decorrelated from `id`'s
/// sequential order so the column doesn't just delta-compress away.
fn amount_cents(id: u64) -> i128 {
    let hashed = id.wrapping_mul(2_654_435_761) >> 16;
    (hashed % 9_901 + 100) as i128
}

/// `start_id + len <= total_rows` and `total_rows <= i64::MAX` (checked once
/// by `run()` before the write loop), so every `id as i64` / `total_rows as
/// i64` below is a lossless cast, not a truncation.
fn synthetic_batch(start_id: u64, len: u64, total_rows: u64) -> RecordBatch {
    let ids: Int64Array = (start_id..start_id + len).map(|id| id as i64).collect();
    let amounts = Decimal128Array::from_iter_values((start_id..start_id + len).map(amount_cents))
        .with_precision_and_scale(10, 2)
        .expect("cents in 100..=10000 fit Decimal128(10, 2)");
    let placed_ats = TimestampMicrosecondArray::from_iter_values(
        (start_id..start_id + len).map(|id| (id as i64) * MICROS_PER_DAY / total_rows as i64),
    );
    let note_array: StringArray = (start_id..start_id + len)
        .map(|id| {
            if id % 7 == 0 {
                None
            } else {
                Some(format!("note-{id}"))
            }
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

/// Splits `rows` into `partitions` near-equal, contiguous `(start_id, len)`
/// ranges; the first `rows % partitions` ranges absorb the remainder.
fn partition_ranges(rows: u64, partitions: u64) -> Vec<(u64, u64)> {
    let base = rows / partitions;
    let remainder = rows % partitions;
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

/// Streams `len` rows starting at `start_id` into one Parquet file, calling
/// `on_batch` with the cumulative row count written to this partition after
/// each row group. Returns the file's on-disk byte size (0 if unreadable —
/// only used for the summary line, not correctness).
fn write_partition(
    path: &Path,
    start_id: u64,
    len: u64,
    total_rows: u64,
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
    let mut writer =
        ArrowWriter::try_new(file, orders_schema(), Some(props)).map_err(CliError::WriteParquet)?;

    let mut written = 0u64;
    while written < len {
        let batch_len = batch_size.min(len - written);
        let batch = synthetic_batch(start_id + written, batch_len, total_rows);
        writer.write(&batch).map_err(CliError::WriteParquet)?;
        written += batch_len;
        on_batch(written);
    }
    writer.close().map_err(CliError::WriteParquet)?;

    Ok(std::fs::metadata(path).map(|meta| meta.len()).unwrap_or(0))
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("scale-benchmark-dataset: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), CliError> {
    let args = Args::parse();
    if args.rows > i64::MAX as u64 {
        return Err(CliError::RowsExceedI64Max { rows: args.rows });
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
    let ranges = partition_ranges(args.rows, args.partitions);

    let mut written_total = 0u64;
    let mut total_bytes = 0u64;
    for (index, (start_id, len)) in ranges.into_iter().enumerate() {
        let path = partition_path(&args.output, args.partitions, index);
        let base_written = written_total;
        total_bytes += write_partition(
            &path,
            start_id,
            len,
            args.rows,
            args.batch_size,
            |partition_written| {
                eprintln!(
                    "wrote {}/{} rows ({:.1?} elapsed)",
                    base_written + partition_written,
                    args.rows,
                    start.elapsed()
                );
            },
        )?;
        written_total += len;
    }

    println!(
        "wrote {} row(s) across {} partition(s) to {} ({total_bytes} bytes, {:.2} bytes/row, {:.1?} elapsed)",
        args.rows,
        args.partitions,
        args.output.display(),
        total_bytes as f64 / args.rows as f64,
        start.elapsed()
    );

    Ok(())
}
