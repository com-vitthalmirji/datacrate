//! M3.6 dataset generator: writes an `orders`-schema Parquet file at whatever
//! row count is requested, streaming row-group by row-group so the total
//! size isn't bounded by RAM (the M3.5/M3.7 generators build one in-memory
//! batch, which doesn't scale to the ~100GB candidate M3.6 targets - see
//! `docs/internals/notes/decisions.md`, 2026-09-15 "M3.6 scoped" entry).
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

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Decimal128Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use clap::Parser;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use pipeline::datafusion_query::orders_schema;

const MICROS_PER_DAY: i64 = 86_400_000_000;

#[derive(Parser)]
#[command(
    name = "scale-benchmark-dataset",
    about = "Generate an orders-schema Parquet file of arbitrary row count for the M3.6 scale comparison"
)]
struct Args {
    /// Path to write the Parquet file to.
    #[arg(long)]
    output: PathBuf,
    /// Total number of rows to generate.
    #[arg(long)]
    rows: u64,
    /// Rows per streamed batch/row-group (bounds peak memory; does not
    /// affect the generated data).
    #[arg(long, default_value_t = 2_000_000)]
    batch_size: u64,
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

    let file = std::fs::File::create(&args.output).map_err(|source| CliError::OpenOutput {
        path: args.output.clone(),
        source,
    })?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, orders_schema(), Some(props))
        .map_err(CliError::WriteParquet)?;

    let start = Instant::now();
    let mut written = 0u64;
    while written < args.rows {
        let len = args.batch_size.min(args.rows - written);
        let batch = synthetic_batch(written, len, args.rows);
        writer.write(&batch).map_err(CliError::WriteParquet)?;
        written += len;
        eprintln!(
            "wrote {written}/{} rows ({:.1?} elapsed)",
            args.rows,
            start.elapsed()
        );
    }
    writer.close().map_err(CliError::WriteParquet)?;

    let bytes = std::fs::metadata(&args.output)
        .map(|meta| meta.len())
        .unwrap_or(0);
    println!(
        "wrote {} row(s) to {} ({bytes} bytes, {:.2} bytes/row, {:.1?} elapsed)",
        args.rows,
        args.output.display(),
        bytes as f64 / args.rows as f64,
        start.elapsed()
    );

    Ok(())
}
