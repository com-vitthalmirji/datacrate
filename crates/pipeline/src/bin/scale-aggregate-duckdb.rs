//! M3.6 scale comparison, DuckDB leg: runs the same aggregation as
//! [`pipeline::datafusion_query::aggregate_query_sql`]
//! (`COUNT(*)`, `SUM(amount) WHERE amount > 50.00`) against the Parquet file
//! or partitioned directory produced by the `scale_benchmark_dataset`
//! example, via DuckDB's vectorized, out-of-core buffer-manager engine
//! instead of DataFusion's in-memory streaming default. Output is diffed
//! against `scale-aggregate-datafusion`'s result before any wall-clock
//! number is trusted — a different scalability trade-off on the same
//! fixture and scale, not a speed claim on its own
//! (<https://iceberglakehouse.com/posts/2026-05-23-single-node-data-engineering-duckdb-datafusion-polars-lakesail/>).

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

use clap::Parser;
use duckdb::{Connection, params};

#[derive(Parser)]
#[command(
    name = "scale-aggregate-duckdb",
    about = "Run the M3.6 scale aggregation against a Parquet file or directory via DuckDB"
)]
struct Args {
    /// Path to the orders Parquet file or directory to aggregate. A
    /// directory is expanded to a `part-*.parquet` glob so DuckDB's
    /// `read_parquet` scans every partition written by
    /// `scale_benchmark_dataset --partitions N`.
    #[arg(long)]
    input: PathBuf,
}

#[derive(Debug)]
enum CliError {
    Connect(duckdb::Error),
    Query(duckdb::Error),
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(source) => write!(f, "opening in-memory duckdb connection: {source}"),
            Self::Query(source) => write!(f, "running aggregation query: {source}"),
        }
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("scale-aggregate-duckdb: {err}");
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

    let conn = Connection::open_in_memory().map_err(CliError::Connect)?;

    let start = Instant::now();
    let (order_count, total_amount): (i64, f64) = conn
        .query_row(
            "SELECT COUNT(*), CAST(SUM(amount) AS DECIMAL(38,2)) \
             FROM read_parquet(?) WHERE amount > 50.00",
            params![scan_path.to_string_lossy()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(CliError::Query)?;
    let elapsed = start.elapsed();

    println!("order_count={order_count} total_amount={total_amount:.2}");
    println!("elapsed={elapsed:?}");
    Ok(())
}
