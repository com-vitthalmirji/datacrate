//! M3.7 join/shuffle benchmark, Ballista leg: runs
//! [`pipeline::datafusion_query::join_aggregate_query_sql`] against a live
//! Ballista cluster over the same `orders`/`shipments` Parquet fixtures the
//! DataFusion and Spark legs already use
//! (`join_shuffle_benchmark_dataset` example output), so the result can be
//! diffed against the already-proven single-node value
//! (shipped_order_count=500000, shipped_total_amount=3750000000 unscaled).
//!
//! Both input files are re-split into two Parquet files each before
//! registering, for the same reason `ballista-aggregate.rs` does it: per the
//! Ballista Tuning Guide, a table backed by a single file has exactly one
//! partition and "will not be able to scale even if the cluster has resource
//! available" (<https://datafusion.apache.org/ballista/user-guide/tuning-guide.html>).
//! Two files per side gives the scheduler two tasks per side to place across
//! the two registered executors, so the join's build/probe actually shuffles
//! data between them instead of running the whole plan on one executor.
//!
//! Requires a scheduler and at least two executors already running (see
//! `just ballista-scheduler`, `just ballista-executor-1`, `just
//! ballista-executor-2`).

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Instant;

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use ballista::datafusion::execution::SessionStateBuilder;
use ballista::datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use ballista::prelude::*;
use clap::Parser;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use pipeline::datafusion_query::{join_aggregate_query_sql, orders_schema, shipments_schema};
use pipeline::write_parquet;

#[derive(Parser)]
#[command(
    name = "join-shuffle-ballista",
    about = "Run the M3.7 join/shuffle benchmark aggregation against a live Ballista cluster"
)]
struct Args {
    /// Path to the orders Parquet file (single file; re-split here into two).
    #[arg(long)]
    orders: PathBuf,
    /// Path to the shipments Parquet file (single file; re-split here into two).
    #[arg(long)]
    shipments: PathBuf,
}

#[derive(Debug)]
enum CliError {
    OpenInput {
        path: PathBuf,
        source: std::io::Error,
    },
    OpenParquet(parquet::errors::ParquetError),
    ReadParquet(arrow::error::ArrowError),
    ConcatBatches(arrow::error::ArrowError),
    CreateDir {
        path: PathBuf,
        source: std::io::Error,
    },
    WriteParquet(pipeline::PipelineIoError),
    Ballista(datafusion::error::DataFusionError),
    FormatOutput(arrow::error::ArrowError),
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OpenInput { path, source } => {
                write!(f, "opening {}: {source}", path.display())
            }
            Self::OpenParquet(source) => write!(f, "opening parquet reader: {source}"),
            Self::ReadParquet(source) => write!(f, "reading parquet: {source}"),
            Self::ConcatBatches(source) => write!(f, "concatenating row groups: {source}"),
            Self::CreateDir { path, source } => {
                write!(f, "creating directory {}: {source}", path.display())
            }
            Self::WriteParquet(source) => write!(f, "writing partition file: {source}"),
            Self::Ballista(source) => write!(f, "running query against ballista: {source}"),
            Self::FormatOutput(source) => write!(f, "formatting output: {source}"),
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
        Self::FormatOutput(source)
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("join-shuffle-ballista: {err}");
            ExitCode::FAILURE
        }
    }
}

/// Reads every row group of `path` and concatenates them into one batch
/// against `schema`, mirroring `pipeline::read_parquet` but parameterized
/// over schema since orders and shipments differ.
fn read_parquet_batch(path: &Path, schema: &SchemaRef) -> Result<RecordBatch, CliError> {
    let file = File::open(path).map_err(|source| CliError::OpenInput {
        path: path.to_path_buf(),
        source,
    })?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(CliError::OpenParquet)?
        .build()
        .map_err(CliError::OpenParquet)?;
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<_, _>>()
        .map_err(CliError::ReadParquet)?;
    concat_batches(schema, &batches).map_err(CliError::ConcatBatches)
}

/// Splits `batch` roughly in half and writes each half as its own Parquet
/// file under `dir`, so a table registered over `dir` scans as two
/// partitions instead of one.
fn write_two_partition_table(dir: &Path, batch: &RecordBatch) -> Result<(), CliError> {
    std::fs::create_dir_all(dir).map_err(|source| CliError::CreateDir {
        path: dir.to_path_buf(),
        source,
    })?;

    let midpoint = batch.num_rows() / 2;
    let first_half = batch.slice(0, midpoint);
    let second_half = batch.slice(midpoint, batch.num_rows() - midpoint);

    write_parquet(
        &first_half,
        &dir.join("part-0.parquet"),
        Compression::SNAPPY,
    )
    .map_err(CliError::WriteParquet)?;
    write_parquet(
        &second_half,
        &dir.join("part-1.parquet"),
        Compression::SNAPPY,
    )
    .map_err(CliError::WriteParquet)?;
    Ok(())
}

async fn run() -> Result<(), CliError> {
    let args = Args::parse();

    let orders_batch = read_parquet_batch(&args.orders, &orders_schema())?;
    let shipments_batch = read_parquet_batch(&args.shipments, &shipments_schema())?;

    let orders_dir = std::env::temp_dir().join("ballista-join-shuffle-orders");
    let shipments_dir = std::env::temp_dir().join("ballista-join-shuffle-shipments");
    write_two_partition_table(&orders_dir, &orders_batch)?;
    write_two_partition_table(&shipments_dir, &shipments_batch)?;

    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(4)
        .with_ballista_job_name("M3.7 join/shuffle leg (2 partitions per side)");
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    let ctx = SessionContext::remote_with_state("df://localhost:50050", state).await?;

    ctx.register_parquet(
        "orders",
        orders_dir.to_string_lossy().as_ref(),
        ParquetReadOptions::default(),
    )
    .await?;
    ctx.register_parquet(
        "shipments",
        shipments_dir.to_string_lossy().as_ref(),
        ParquetReadOptions::default(),
    )
    .await?;

    let plan = ctx
        .sql(
            "EXPLAIN SELECT COUNT(*) AS shipped_order_count, SUM(orders.amount) AS shipped_total_amount \
             FROM orders JOIN shipments ON orders.id = shipments.order_id \
             WHERE orders.amount > 50.00",
        )
        .await?
        .collect()
        .await?;
    println!("{}", arrow::util::pretty::pretty_format_batches(&plan)?);

    let start = Instant::now();
    let batches = join_aggregate_query_sql(&ctx).await?;
    let elapsed = start.elapsed();

    println!("{}", arrow::util::pretty::pretty_format_batches(&batches)?);
    println!("elapsed={elapsed:?}");

    Ok(())
}
