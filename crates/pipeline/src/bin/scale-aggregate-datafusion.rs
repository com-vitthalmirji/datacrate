//! M3.6/M3.8 scale comparison, DataFusion leg: runs one of three aggregation
//! shapes over the Parquet file or partitioned directory produced by the
//! `scale_benchmark_dataset` example, timed with `std::time::Instant`. This
//! is the correctness reference every other leg (Ballista, Polars, Spark,
//! Spark+Comet) is diffed against before any wall-clock number is trusted.
//! `--query` defaults to `aggregate` (M3.6's original single-shape leg);
//! `group-by-bucket` and `multi-predicate` are later additions — same table,
//! same binary, since all three are variations of one benchmark leg rather
//! than independently-evolving legs.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

use clap::{Parser, ValueEnum};
use datafusion::prelude::SessionContext;
use pipeline::datafusion_query::{
    aggregate_query_sql, group_by_bucket_query_sql, multi_predicate_query_sql, register_orders,
};

#[derive(Clone, Copy, ValueEnum)]
enum QueryShape {
    Aggregate,
    GroupByBucket,
    MultiPredicate,
}

#[derive(Parser)]
#[command(
    name = "scale-aggregate-datafusion",
    about = "Run an M3.6/M3.8 scale query against a Parquet file or directory via DataFusion"
)]
struct Args {
    /// Path to the orders Parquet file or partitioned directory to query.
    #[arg(long)]
    input: PathBuf,
    /// Which query shape to run.
    #[arg(long, value_enum, default_value = "aggregate")]
    query: QueryShape,
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
    let batches = match args.query {
        QueryShape::Aggregate => aggregate_query_sql(&ctx).await,
        QueryShape::GroupByBucket => group_by_bucket_query_sql(&ctx).await,
        QueryShape::MultiPredicate => multi_predicate_query_sql(&ctx).await,
    }
    .map_err(CliError::Query)?;
    let elapsed = start.elapsed();

    println!(
        "{}",
        arrow::util::pretty::pretty_format_batches(&batches).map_err(CliError::FormatResult)?
    );
    println!("elapsed={elapsed:?}");
    Ok(())
}
