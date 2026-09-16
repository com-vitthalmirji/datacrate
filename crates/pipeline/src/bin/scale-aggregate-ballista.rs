//! M3.6 scale comparison, Ballista leg: runs the same aggregation as
//! [`pipeline::datafusion_query::aggregate_query_sql`] against a Parquet
//! directory produced by the `scale_benchmark_dataset` example, over a live
//! Ballista cluster instead of a single-node `SessionContext`. Output is
//! diffed against `scale-aggregate-datafusion`'s result before any
//! wall-clock number is trusted (same oracle discipline as M3.5).
//!
//! Requires a scheduler and at least two executors already running (see
//! `just ballista-scheduler`, `just ballista-executor-1`, `just
//! ballista-executor-2`), and an `--input` directory with more than one
//! Parquet file — per the Ballista Tuning Guide, a table backed by a single
//! file has exactly one partition and won't scale across executors
//! (<https://datafusion.apache.org/ballista/user-guide/tuning-guide.html>).
//! `scale_benchmark_dataset --partitions N` (N > 1) produces such a directory.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

use ballista::datafusion::execution::SessionStateBuilder;
use ballista::datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use ballista::prelude::*;
use clap::Parser;

#[derive(Parser)]
#[command(
    name = "scale-aggregate-ballista",
    about = "Run the M3.6 scale aggregation against a partitioned Parquet directory over Ballista"
)]
struct Args {
    /// Path to the orders Parquet directory to aggregate.
    #[arg(long)]
    input: PathBuf,
}

#[derive(Debug)]
enum CliError {
    Ballista(datafusion::error::DataFusionError),
    FormatResult(arrow::error::ArrowError),
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ballista(source) => write!(f, "running query against ballista: {source}"),
            Self::FormatResult(source) => write!(f, "formatting result: {source}"),
        }
    }
}

impl From<datafusion::error::DataFusionError> for CliError {
    fn from(source: datafusion::error::DataFusionError) -> Self {
        Self::Ballista(source)
    }
}

impl From<arrow::error::ArrowError> for CliError {
    fn from(source: arrow::error::ArrowError) -> Self {
        Self::FormatResult(source)
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("scale-aggregate-ballista: {err}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), CliError> {
    let args = Args::parse();

    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(4)
        .with_ballista_job_name("M3.6 scale aggregate");
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    let ctx = SessionContext::remote_with_state("df://localhost:50050", state).await?;

    ctx.register_parquet(
        "orders",
        args.input.to_string_lossy().as_ref(),
        ParquetReadOptions::default(),
    )
    .await?;

    let start = Instant::now();
    let batches = ctx
        .sql("SELECT COUNT(*) AS order_count, SUM(amount) AS total_amount FROM orders WHERE amount > 50.00")
        .await?
        .collect()
        .await?;
    let elapsed = start.elapsed();

    println!("{}", arrow::util::pretty::pretty_format_batches(&batches)?);
    println!("elapsed={elapsed:?}");
    Ok(())
}
