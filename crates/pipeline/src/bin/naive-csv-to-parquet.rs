//! Naive baseline for the M2 memory-envelope claim: loads the entire input
//! CSV into a `Vec<Row>` (via [`pipeline::fixture_to_record_batch`]) before
//! building one giant `RecordBatch` and writing it as Parquet in a single
//! shot — no batching, no streaming. Local files only, no object store, so
//! the comparison against `bounded-csv-to-parquet` isolates CSV-parse/batch
//! memory behaviour rather than S3 I/O (already fixed on both sides). See
//! docs/adr/0005.1-stream-object-store-io-in-chunks.md.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use parquet::basic::Compression;
use pipeline::{fixture_to_record_batch, write_parquet};

#[derive(Parser)]
#[command(
    name = "naive-csv-to-parquet",
    about = "Load an entire CSV into memory, then write it as Parquet in one shot"
)]
struct Args {
    /// Path to the input CSV file.
    #[arg(long)]
    input: PathBuf,

    /// Path to write the output Parquet file to.
    #[arg(long)]
    output: PathBuf,
}

fn main() -> ExitCode {
    let args = Args::parse();
    match run(&args) {
        Ok(rows) => {
            println!("wrote {rows} row(s) to {}", args.output.display());
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("naive-csv-to-parquet: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &Args) -> Result<usize, pipeline::PipelineIoError> {
    let batch = fixture_to_record_batch(&args.input)?;
    let rows = batch.num_rows();
    write_parquet(&batch, &args.output, Compression::SNAPPY)?;
    Ok(rows)
}
