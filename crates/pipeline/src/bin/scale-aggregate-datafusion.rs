//! M3.6 scale comparison, DataFusion leg: runs the same aggregation as
//! [`pipeline::datafusion_query::aggregate_query_sql`]
//! (`SELECT COUNT(*), SUM(amount) FROM orders WHERE amount > 50.00`) against
//! the Parquet file or partitioned directory produced by the
//! `scale_benchmark_dataset` example, timed with `std::time::Instant`. This
//! is the correctness oracle every other M3.6 leg (Ballista, Polars, Spark,
//! Spark+Comet) is diffed against before any wall-clock number is trusted.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

use clap::Parser;
use datafusion::prelude::SessionContext;
use pipeline::datafusion_query::{aggregate_query_sql, register_orders};

#[derive(Parser)]
#[command(
    name = "scale-aggregate-datafusion",
    about = "Run the M3.6 scale aggregation against a Parquet file or directory via DataFusion"
)]
struct Args {
    /// Path to the orders Parquet file or partitioned directory to aggregate.
    #[arg(long)]
    input: PathBuf,
}

#[derive(Debug)]
enum CliError {
    Register(datafusion::error::DataFusionError),
    Query(datafusion::error::DataFusionError),
    FormatResult(arrow::error::ArrowError),
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Register(source) => write!(f, "registering orders table: {source}"),
            Self::Query(source) => write!(f, "running aggregation query: {source}"),
            Self::FormatResult(source) => write!(f, "formatting result: {source}"),
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("scale-aggregate-datafusion: {err}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), CliError> {
    let args = Args::parse();
    let ctx = SessionContext::new();
    register_orders(&ctx, &args.input)
        .await
        .map_err(CliError::Register)?;

    let start = Instant::now();
    let batches = aggregate_query_sql(&ctx).await.map_err(CliError::Query)?;
    let elapsed = start.elapsed();

    println!(
        "{}",
        arrow::util::pretty::pretty_format_batches(&batches).map_err(CliError::FormatResult)?
    );
    println!("elapsed={elapsed:?}");
    Ok(())
}
