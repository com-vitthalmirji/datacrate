//! M3.8 join-at-scale benchmark, Ballista leg: runs
//! [`pipeline::datafusion_query::join_aggregate_query_sql`] against a live
//! Ballista cluster over pre-partitioned `orders`/`shipments` Parquet
//! directories (`scale_benchmark_dataset --partitions N` /
//! `scale_benchmark_shipments --partitions N`, N > 1).
//!
//! Unlike `join-shuffle-ballista.rs` (M3.7's small-fixture leg, which reads a
//! single input file fully into memory and re-splits it in-process), this
//! binary registers `--orders`/`--shipments` directories directly — required
//! at M3.8's 91GB scale, where reading either side into one `RecordBatch`
//! would defeat the point of a distributed join. The M3.7 binary is left
//! unchanged since it's still M3.7's proven small-scale evidence path.
//!
//! Requires a scheduler and at least two executors already running (see
//! `just ballista-scheduler`, `just ballista-executor-1`, `just
//! ballista-executor-2`).

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

use ballista::datafusion::execution::SessionStateBuilder;
use ballista::datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use ballista::prelude::*;
use clap::Parser;
use pipeline::datafusion_query::join_aggregate_query_sql;

#[derive(Parser)]
#[command(
    name = "join-shuffle-ballista-scale",
    about = "Run the M3.8 join-at-scale benchmark against partitioned Parquet directories over Ballista"
)]
struct Args {
    /// Path to the orders Parquet directory.
    #[arg(long)]
    orders: PathBuf,
    /// Path to the shipments Parquet directory.
    #[arg(long)]
    shipments: PathBuf,
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
            eprintln!("join-shuffle-ballista-scale: {err}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), CliError> {
    let args = Args::parse();

    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(12)
        .with_ballista_job_name("M3.8 join-at-scale");
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    let ctx = SessionContext::remote_with_state("df://localhost:50050", state).await?;

    ctx.register_parquet(
        "orders",
        args.orders.to_string_lossy().as_ref(),
        ParquetReadOptions::default(),
    )
    .await?;
    ctx.register_parquet(
        "shipments",
        args.shipments.to_string_lossy().as_ref(),
        ParquetReadOptions::default(),
    )
    .await?;

    let start = Instant::now();
    let batches = join_aggregate_query_sql(&ctx).await?;
    let elapsed = start.elapsed();

    println!("{}", arrow::util::pretty::pretty_format_batches(&batches)?);
    println!("elapsed={elapsed:?}");
    Ok(())
}
