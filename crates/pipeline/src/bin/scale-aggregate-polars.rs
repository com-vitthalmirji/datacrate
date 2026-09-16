//! M3.6 scale comparison, Polars leg: runs the same aggregation as
//! [`pipeline::datafusion_query::aggregate_query_sql`]
//! (`COUNT(*)`, `SUM(amount) WHERE amount > 50.00`) against the Parquet file
//! or partitioned directory produced by the `scale_benchmark_dataset`
//! example, via Polars' lazy/streaming engine instead of DataFusion. Output
//! is diffed against `scale-aggregate-datafusion`'s result before any
//! wall-clock number is trusted — this is a Rust-ecosystem-internal
//! comparison (DataFusion vs. Polars), never run against Spark.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

use clap::Parser;
use polars::prelude::*;

#[derive(Parser)]
#[command(
    name = "scale-aggregate-polars",
    about = "Run the M3.6 scale aggregation against a Parquet file or directory via Polars"
)]
struct Args {
    /// Path to the orders Parquet file or directory to aggregate. A
    /// directory is expanded to a `part-*.parquet` glob so Polars scans
    /// every partition written by `scale_benchmark_dataset --partitions N`.
    #[arg(long)]
    input: PathBuf,
}

#[derive(Debug)]
enum CliError {
    Scan(PolarsError),
    Collect(PolarsError),
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scan(source) => write!(f, "scanning orders parquet: {source}"),
            Self::Collect(source) => write!(f, "running aggregation query: {source}"),
        }
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("scale-aggregate-polars: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), CliError> {
    let args = Args::parse();
    let scan_path = if args.input.is_dir() {
        args.input.join("part-*.parquet")
    } else {
        args.input
    };

    let lf = LazyFrame::scan_parquet(
        PlPath::new(scan_path.to_string_lossy().as_ref()),
        ScanArgsParquet::default(),
    )
    .map_err(CliError::Scan)?
    .filter(col("amount").gt(lit(50.00)))
    .select([
        len().alias("order_count"),
        // Widen before summing: Polars keeps SUM(decimal) at the input
        // column's own precision (10), which overflows once the aggregate
        // exceeds 8 integer digits; DataFusion widens automatically.
        col("amount")
            .cast(DataType::Decimal(Some(38), Some(2)))
            .sum()
            .alias("total_amount"),
    ]);

    let start = Instant::now();
    let result = lf.collect().map_err(CliError::Collect)?;
    let elapsed = start.elapsed();

    println!("{result}");
    println!("elapsed={elapsed:?}");
    Ok(())
}
