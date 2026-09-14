//! Local-file counterpart to `naive-csv-to-parquet`: runs the existing
//! batched, bounded-memory [`pipeline::bounded::run_bounded_pipeline`]
//! directly against local files (no object store), so its peak memory can be
//! compared against the naive whole-file-in-a-`Vec` approach on identical
//! input, with S3 I/O excluded from both sides. See
//! `docs/internals/notes/decisions.md`'s 2026-09-14 streaming follow-up.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use parquet::basic::Compression;
use pipeline::bounded::{CancellationToken, PipelineConfig, run_bounded_pipeline};

#[derive(Parser)]
#[command(
    name = "bounded-csv-to-parquet",
    about = "Run the batched, bounded-memory CSV -> Parquet pipeline against local files"
)]
struct Args {
    /// Path to the input CSV file.
    #[arg(long)]
    input: PathBuf,

    /// Path to write the output Parquet file to.
    #[arg(long)]
    output: PathBuf,

    /// Rows per `RecordBatch`.
    #[arg(long, default_value_t = 1_000)]
    batch_size: usize,

    /// Capacity of the channel between the reader and writer threads.
    #[arg(long, default_value_t = 4)]
    channel_capacity: usize,
}

fn main() -> ExitCode {
    let args = Args::parse();
    let config = PipelineConfig {
        batch_size: args.batch_size,
        channel_capacity: args.channel_capacity,
        compression: Compression::SNAPPY,
    };

    match run_bounded_pipeline(
        &args.input,
        &args.output,
        &config,
        &CancellationToken::new(),
    ) {
        Ok(report) => {
            println!(
                "wrote {} batch(es), {} row(s) to {}",
                report.batches_written,
                report.rows_written,
                args.output.display()
            );
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("bounded-csv-to-parquet: {err}");
            ExitCode::FAILURE
        }
    }
}
