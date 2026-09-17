//! M3.6/M3.8 scale comparison, Ballista leg: runs one of three query shapes
//! against a Parquet directory produced by the `scale_benchmark_dataset`
//! example, over a live Ballista cluster instead of a single-node
//! `SessionContext`. Output is diffed against `scale-aggregate-datafusion`'s
//! result before any wall-clock number is trusted (same correctness-reference
//! discipline as M3.5). `--query` defaults to `aggregate` (M3.6's original shape);
//! `group-by-bucket` and `multi-predicate` are the M3.8 additions, SQL text
//! matching [`pipeline::datafusion_query::group_by_bucket_query_sql`] and
//! [`pipeline::datafusion_query::multi_predicate_query_sql`] exactly.
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
use clap::{Parser, ValueEnum};

#[derive(Clone, Copy, ValueEnum)]
enum QueryShape {
    Aggregate,
    GroupByBucket,
    MultiPredicate,
}

impl QueryShape {
    fn sql(self) -> &'static str {
        match self {
            Self::Aggregate => {
                "SELECT COUNT(*) AS order_count, SUM(amount) AS total_amount \
                 FROM orders WHERE amount > 50.00"
            }
            Self::GroupByBucket => {
                "SELECT id % 1000 AS bucket, COUNT(*) AS order_count, SUM(amount) AS total_amount \
                 FROM orders GROUP BY bucket ORDER BY bucket"
            }
            Self::MultiPredicate => {
                "SELECT COUNT(*) AS order_count, SUM(amount) AS total_amount \
                 FROM orders WHERE amount > 50.00 AND note IS NOT NULL"
            }
        }
    }
}

#[derive(Parser)]
#[command(
    name = "scale-aggregate-ballista",
    about = "Run an M3.6/M3.8 scale query against a partitioned Parquet directory over Ballista"
)]
struct Args {
    /// Path to the orders Parquet directory to query.
    #[arg(long)]
    input: PathBuf,
    /// Which query shape to run.
    #[arg(long, value_enum, default_value = "aggregate")]
    query: QueryShape,
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
    let batches = ctx.sql(args.query.sql()).await?.collect().await?;
    let elapsed = start.elapsed();

    println!("{}", arrow::util::pretty::pretty_format_batches(&batches)?);
    println!("elapsed={elapsed:?}");
    Ok(())
}
