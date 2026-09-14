//! M3.7 join/shuffle benchmark, DataFusion leg: runs
//! [`pipeline::datafusion_query::join_aggregate_query_sql`] (`SELECT
//! COUNT(*), SUM(amount) FROM orders JOIN shipments ON id = order_id WHERE
//! amount > 50.00`) against the Parquet fixtures produced by the
//! `join_shuffle_benchmark_dataset` example, timed with `std::time::Instant`.
//! This is the correctness reference and DataFusion timing leg of the three-way
//! Spark / Spark+Comet / DataFusion comparison -
//! `benchmark/join-shuffle/query.sql` runs the Spark-SQL equivalent over the
//! same files.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

use clap::Parser;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use pipeline::datafusion_query::{join_aggregate_query_sql, register_orders, register_shipments};

/// Bounds the in-memory `FairSpillPool` so the join spills to disk instead of
/// growing unbounded and swap-thrashing the OS — the default `RuntimeEnv` has
/// no memory limit at all. See docs/adr/0007-datafusion-resource-control.md.
const MEMORY_POOL_SIZE_BYTES: usize = 40 * 1024 * 1024 * 1024;
/// Disk quota for spilled data, matching the raised quota already used by
/// `ballista-executor-scale` for the same 91GB dataset.
const MAX_TEMP_DIRECTORY_SIZE_BYTES: u64 = 150 * 1024 * 1024 * 1024;

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
    Runtime(datafusion::error::DataFusionError),
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RegisterOrders(source) => write!(f, "registering orders table: {source}"),
            Self::RegisterShipments(source) => write!(f, "registering shipments table: {source}"),
            Self::Query(source) => write!(f, "running join aggregation query: {source}"),
            Self::FormatResult(source) => write!(f, "formatting result: {source}"),
            Self::Runtime(source) => write!(f, "building runtime env: {source}"),
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
    let target_partitions = std::thread::available_parallelism()
        .map(std::num::NonZero::get)
        .unwrap_or(1);
    // HashJoinExec's build side has no disk-spill fallback (collects the
    // whole build side or fails); SortMergeJoinExec does spill, so force it
    // for a build side too large to fit in the memory pool.
    let config = SessionConfig::new()
        .with_target_partitions(target_partitions)
        .set_bool("datafusion.optimizer.prefer_hash_join", false);
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(std::sync::Arc::new(FairSpillPool::new(
            MEMORY_POOL_SIZE_BYTES,
        )))
        .with_max_temp_directory_size(MAX_TEMP_DIRECTORY_SIZE_BYTES)
        .build_arc()
        .map_err(CliError::Runtime)?;
    let ctx = SessionContext::new_with_config_rt(config, runtime);
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
