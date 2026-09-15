//! M3.5 Ballista proof: runs the same aggregation query as
//! [`pipeline::datafusion_query::aggregate_query_sql`]
//! (`SELECT COUNT(*), SUM(amount) FROM orders WHERE amount > 50.00` over
//! `fixtures/m3/orders.csv`) against a live Ballista cluster instead of a
//! single-node `SessionContext`, so the result can be diffed against the
//! already-proven single-node value (order_count=5, total_amount=100145073
//! unscaled, per `aggregate_query_sums_and_counts_filtered_rows`).
//!
//! The fixture is written as two Parquet files in one directory, not one
//! file: per the Ballista Tuning Guide, a table backed by a single file has
//! exactly one partition and "will not be able to scale even if the cluster
//! has resource available"
//! (<https://datafusion.apache.org/ballista/user-guide/tuning-guide.html>).
//! One file produced a single-task plan that never left one executor; two
//! files give the scan two partitions, so the scheduler has two tasks to
//! place and can put one on each registered executor.
//!
//! Requires a scheduler and at least two executors already running (see
//! `just ballista-scheduler`, `just ballista-executor-1`, `just
//! ballista-executor-2`).

use std::path::Path;
use std::process::ExitCode;

use arrow::array::RecordBatch;
use ballista::datafusion::execution::SessionStateBuilder;
use ballista::datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use ballista::prelude::*;
use parquet::basic::Compression;
use pipeline::datafusion_query::fixture_to_orders_batch;
use pipeline::write_parquet;

#[derive(Debug)]
enum CliError {
    Fixture(pipeline::PipelineIoError),
    CreateDir {
        path: std::path::PathBuf,
        source: std::io::Error,
    },
    Ballista(datafusion::error::DataFusionError),
    FormatPlan(arrow::error::ArrowError),
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fixture(source) => write!(f, "reading orders fixture: {source}"),
            Self::CreateDir { path, source } => {
                write!(f, "creating directory {}: {source}", path.display())
            }
            Self::Ballista(source) => write!(f, "running query against ballista: {source}"),
            Self::FormatPlan(source) => write!(f, "formatting EXPLAIN output: {source}"),
        }
    }
}

impl From<pipeline::PipelineIoError> for CliError {
    fn from(source: pipeline::PipelineIoError) -> Self {
        Self::Fixture(source)
    }
}

impl From<datafusion::error::DataFusionError> for CliError {
    fn from(source: datafusion::error::DataFusionError) -> Self {
        Self::Ballista(source)
    }
}

impl From<arrow::error::ArrowError> for CliError {
    fn from(source: arrow::error::ArrowError) -> Self {
        Self::FormatPlan(source)
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("ballista-aggregate: {err}");
            ExitCode::FAILURE
        }
    }
}

/// Splits `batch` roughly in half and writes each half as its own Parquet
/// file under `dir`, so a table registered over `dir` scans as (at least)
/// two partitions instead of one.
fn write_two_partition_orders(dir: &Path, batch: &RecordBatch) -> Result<(), CliError> {
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
    )?;
    write_parquet(
        &second_half,
        &dir.join("part-1.parquet"),
        Compression::SNAPPY,
    )?;
    Ok(())
}

async fn run() -> Result<(), CliError> {
    let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/m3/orders.csv");
    let batch = fixture_to_orders_batch(&fixture_path)?;

    let orders_dir = std::env::temp_dir().join("ballista-aggregate-orders");
    write_two_partition_orders(&orders_dir, &batch)?;

    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(4)
        .with_ballista_job_name("M3.5 aggregate proof (2 partitions)");
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

    let plan = ctx
        .sql(
            "EXPLAIN SELECT COUNT(*) AS order_count, SUM(amount) AS total_amount \
             FROM orders WHERE amount > 50.00",
        )
        .await?
        .collect()
        .await?;
    println!("{}", arrow::util::pretty::pretty_format_batches(&plan)?);

    let df = ctx
        .sql("SELECT COUNT(*) AS order_count, SUM(amount) AS total_amount FROM orders WHERE amount > 50.00")
        .await?;
    df.show().await?;

    Ok(())
}
