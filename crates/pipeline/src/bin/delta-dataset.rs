//! Converts the existing Comet-benchmark `orders.parquet` fixture
//! (`benchmark/spark-comet/orders.parquet`, 5,000,000 rows) into a Delta
//! table, written with the native Rust `delta-rs` writer. No new data is
//! generated: this binary reads the exact bytes the Comet/DataFusion/Spark
//! three-way comparison already uses, so a Delta-table benchmark leg stays
//! apples-to-apples with `benchmark/REPORT.md`'s existing numbers rather than
//! introducing a second, subtly different dataset.
//!
//! Run: `cargo run --release --package pipeline --bin delta-dataset -- \
//!   --input benchmark/spark-comet/orders.parquet --output benchmark/delta/orders`

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use datafusion::prelude::SessionContext;
use deltalake::DeltaTable;
use deltalake::protocol::SaveMode;
use pipeline::datafusion_query::register_orders;
use url::Url;

#[derive(Parser)]
#[command(
    name = "delta-dataset",
    about = "Write the orders Parquet fixture as a Delta table"
)]
struct Args {
    /// Path to the source orders Parquet file.
    #[arg(long)]
    input: PathBuf,

    /// Directory to write the Delta table to (created if absent).
    #[arg(long)]
    output: PathBuf,
}

#[derive(Debug)]
enum CliError {
    Register(datafusion::error::DataFusionError),
    Collect(datafusion::error::DataFusionError),
    TableUrl(url::ParseError),
    OpenTable(deltalake::errors::DeltaTableError),
    Write(deltalake::errors::DeltaTableError),
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Register(source) => write!(f, "registering source orders table: {source}"),
            Self::Collect(source) => write!(f, "reading source orders rows: {source}"),
            Self::TableUrl(source) => write!(f, "building Delta table URL: {source}"),
            Self::OpenTable(source) => write!(f, "opening Delta table location: {source}"),
            Self::Write(source) => write!(f, "writing Delta table: {source}"),
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(rows) => {
            println!("wrote {rows} row(s) to a Delta table");
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("delta-dataset: {err}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<usize, CliError> {
    let args = Args::parse();

    let ctx = SessionContext::new();
    register_orders(&ctx, &args.input)
        .await
        .map_err(CliError::Register)?;
    let batches = ctx
        .sql("SELECT * FROM orders")
        .await
        .map_err(CliError::Collect)?
        .collect()
        .await
        .map_err(CliError::Collect)?;
    let row_count = batches
        .iter()
        .map(datafusion::arrow::array::RecordBatch::num_rows)
        .sum();

    std::fs::create_dir_all(&args.output).map_err(|source| {
        CliError::OpenTable(deltalake::errors::DeltaTableError::generic(format!(
            "creating {}: {source}",
            args.output.display()
        )))
    })?;
    let table_url = Url::from_directory_path(args.output.canonicalize().map_err(|source| {
        CliError::OpenTable(deltalake::errors::DeltaTableError::generic(format!(
            "resolving {}: {source}",
            args.output.display()
        )))
    })?)
    .map_err(|()| CliError::TableUrl(url::ParseError::RelativeUrlWithoutBase))?;

    let table = DeltaTable::try_from_url(table_url)
        .await
        .map_err(CliError::OpenTable)?;
    table
        .write(batches)
        .with_save_mode(SaveMode::Overwrite)
        .await
        .map_err(CliError::Write)?;

    Ok(row_count)
}
