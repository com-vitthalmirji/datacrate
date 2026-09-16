//! M3.5 Comet benchmark, DataFusion leg: runs the same aggregation as
//! [`pipeline::datafusion_query::aggregate_query_sql`]
//! (`SELECT COUNT(*), SUM(amount) FROM orders WHERE amount > 50.00`) against
//! the Parquet fixture produced by the `comet_benchmark_dataset` example,
//! timed with `std::time::Instant`. This is the correctness oracle and
//! DataFusion timing leg of the three-way Spark / Spark+Comet / DataFusion
//! comparison — `benchmark/spark-comet/query.sql` runs the Spark-SQL
//! equivalent over the same file.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

use clap::Parser;
use datafusion::prelude::SessionContext;
use pipeline::datafusion_query::{aggregate_query_sql, register_orders};

#[derive(Parser)]
#[command(
    name = "comet-aggregate-datafusion",
    about = "Run the Comet benchmark aggregation against a Parquet file via DataFusion"
)]
struct Args {
    /// Path to the orders Parquet file to aggregate.
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
            eprintln!("comet-aggregate-datafusion: {err}");
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
