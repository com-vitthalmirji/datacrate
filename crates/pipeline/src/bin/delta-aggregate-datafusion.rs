//! Delta-table leg of the Comet benchmark's aggregation query: opens the
//! Delta table written by `delta-dataset` and runs the same
//! [`pipeline::datafusion_query::aggregate_query_sql`] query DataFusion runs
//! directly against raw Parquet in `comet-aggregate-datafusion`. The
//! difference between the two numbers isolates the cost (if any) of Delta's
//! transaction-log layer on top of an Arrow-native, zero-transpose read —
//! not a Spark/JVM comparison, which `benchmark/spark-comet` already covers.
//!
//! Run: `cargo run --release --package pipeline --bin delta-aggregate-datafusion -- \
//!   --table benchmark/delta/orders`

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

use clap::Parser;
use datafusion::prelude::SessionContext;
use pipeline::datafusion_query::aggregate_query_sql;
use url::Url;

#[derive(Parser)]
#[command(
    name = "delta-aggregate-datafusion",
    about = "Run the Comet benchmark aggregation against a Delta table via DataFusion"
)]
struct Args {
    /// Path to the Delta table directory written by `delta-dataset`.
    #[arg(long)]
    table: PathBuf,
}

#[derive(Debug)]
enum CliError {
    TableUrl(std::io::Error),
    OpenTable(deltalake::errors::DeltaTableError),
    Provider(datafusion::error::DataFusionError),
    RegisterSession(deltalake::errors::DeltaTableError),
    RegisterTable(datafusion::error::DataFusionError),
    Query(datafusion::error::DataFusionError),
    FormatResult(datafusion::arrow::error::ArrowError),
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TableUrl(source) => write!(f, "resolving Delta table path: {source}"),
            Self::OpenTable(source) => write!(f, "opening Delta table: {source}"),
            Self::Provider(source) => write!(f, "building Delta table provider: {source}"),
            Self::RegisterSession(source) => {
                write!(f, "registering Delta table's object store: {source}")
            }
            Self::RegisterTable(source) => write!(f, "registering orders table: {source}"),
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
            eprintln!("delta-aggregate-datafusion: {err}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), CliError> {
    let args = Args::parse();
    let table_path = args.table.canonicalize().map_err(CliError::TableUrl)?;
    let table_url = Url::from_directory_path(&table_path)
        .map_err(|()| CliError::TableUrl(std::io::Error::other("not an absolute directory path")))?;

    let table = deltalake::open_table(table_url)
        .await
        .map_err(CliError::OpenTable)?;

    let ctx = SessionContext::new();
    table
        .update_datafusion_session(&ctx.state())
        .map_err(CliError::RegisterSession)?;
    let provider = table.table_provider().await.map_err(CliError::Provider)?;
    ctx.register_table("orders", provider)
        .map_err(CliError::RegisterTable)?;

    let start = Instant::now();
    let batches = aggregate_query_sql(&ctx).await.map_err(CliError::Query)?;
    let elapsed = start.elapsed();

    println!(
        "{}",
        datafusion::arrow::util::pretty::pretty_format_batches(&batches)
            .map_err(CliError::FormatResult)?
    );
    println!("elapsed={elapsed:?}");
    Ok(())
}
