//! M3.7 join/shuffle benchmark, DataFusion leg: runs
//! [`pipeline::datafusion_query::join_aggregate_query_sql`] (`SELECT
//! COUNT(*), SUM(amount) FROM orders JOIN shipments ON id = order_id WHERE
//! amount > 50.00`) against the Parquet fixtures produced by the
//! `join_shuffle_benchmark_dataset` example, timed with `std::time::Instant`.
//! This is the correctness oracle and DataFusion timing leg of the three-way
//! Spark / Spark+Comet / DataFusion comparison -
//! `benchmark/join-shuffle/query.sql` runs the Spark-SQL equivalent over the
//! same files.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

use clap::Parser;
use datafusion::prelude::SessionContext;
use pipeline::datafusion_query::{join_aggregate_query_sql, register_orders, register_shipments};

#[derive(Parser)]
#[command(
    name = "join-shuffle-datafusion",
    about = "Run the join/shuffle benchmark aggregation against Parquet files via DataFusion"
)]
struct Args {
    /// Path to the orders Parquet file.
    #[arg(long)]
    orders: PathBuf,
    /// Path to the shipments Parquet file.
    #[arg(long)]
    shipments: PathBuf,
}

#[derive(Debug)]
enum CliError {
    RegisterOrders(datafusion::error::DataFusionError),
    RegisterShipments(datafusion::error::DataFusionError),
    Query(datafusion::error::DataFusionError),
    FormatResult(arrow::error::ArrowError),
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RegisterOrders(source) => write!(f, "registering orders table: {source}"),
            Self::RegisterShipments(source) => write!(f, "registering shipments table: {source}"),
            Self::Query(source) => write!(f, "running join aggregation query: {source}"),
            Self::FormatResult(source) => write!(f, "formatting result: {source}"),
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("join-shuffle-datafusion: {err}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), CliError> {
    let args = Args::parse();
    let ctx = SessionContext::new();
    register_orders(&ctx, &args.orders)
        .await
        .map_err(CliError::RegisterOrders)?;
    register_shipments(&ctx, &args.shipments)
        .await
        .map_err(CliError::RegisterShipments)?;

    let start = Instant::now();
    let batches = join_aggregate_query_sql(&ctx)
        .await
        .map_err(CliError::Query)?;
    let elapsed = start.elapsed();

    println!(
        "{}",
        arrow::util::pretty::pretty_format_batches(&batches).map_err(CliError::FormatResult)?
    );
    println!("elapsed={elapsed:?}");
    Ok(())
}
