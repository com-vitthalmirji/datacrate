//! M3-gate Spark-vs-DataFusion parity probe fixture: converts the existing
//! `fixtures/m3/orders.csv` (8 rows, already covers nulls, tied decimals,
//! and a timestamp spread) to Parquet so DataFusion and Spark can both read
//! the exact same bytes for `benchmark/parity/query.sql`.
//!
//! Run: `cargo run --release --package pipeline --example parity_fixture -- \
//!   --input fixtures/m3/orders.csv --output benchmark/parity/orders.parquet`

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use parquet::basic::Compression;
use pipeline::datafusion_query::fixture_to_orders_batch;
use pipeline::write_parquet;

#[derive(Parser)]
#[command(
    name = "parity-fixture",
    about = "Convert the M3 orders CSV fixture to Parquet for the Spark/DataFusion parity probe"
)]
struct Args {
    /// Path to the `id,amount,placed_at,note` CSV fixture.
    #[arg(long)]
    input: PathBuf,

    /// Path to write the converted Parquet file to.
    #[arg(long)]
    output: PathBuf,
}

fn main() -> ExitCode {
    let args = Args::parse();
    let batch = match fixture_to_orders_batch(&args.input) {
        Ok(batch) => batch,
        Err(err) => {
            eprintln!("parity-fixture: {err}");
            return ExitCode::FAILURE;
        }
    };
    let rows = batch.num_rows();
    match write_parquet(&batch, &args.output, Compression::SNAPPY) {
        Ok(()) => {
            println!("wrote {rows} row(s) to {}", args.output.display());
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("parity-fixture: {err}");
            ExitCode::FAILURE
        }
    }
}
