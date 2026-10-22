//! Proves the "fully solved" case named in the Comet talk narrative: a pure
//! DataFusion pipeline has no JVM boundary, so a read -> filter -> write
//! round trip over the Comet benchmark's `orders.parquet` fixture never
//! needs to leave Arrow's columnar layout. [`pipeline::write_parquet`] takes
//! `&RecordBatch` - Arrow's own columnar type - directly, so there is no
//! row-shaped intermediate anywhere in this call chain. This is distinct
//! from Comet's own Iceberg-write gap (`docs/internals/datafusion-comet-story-2.md`):
//! that gap exists because Java's Iceberg writer demands row format at a
//! JVM boundary this pipeline doesn't have.
//!
//! This binary only proves the structural claim (the write path here never
//! transposes) and reports wall-clock time for the real fixture as a sanity
//! check, not as a quantified cost. An earlier version of this binary also
//! ran a "forced row round trip" leg on one extracted column to try to
//! quantify the tax avoided, but that leg skipped disk I/O and operated on
//! a single column while this leg writes four columns to Parquet - the two
//! numbers weren't comparable and were dropped rather than risk recording a
//! misleading benchmark. `transpose-cost.rs` already measures that tax
//! correctly (controlled column counts, no I/O confound) - see its results
//! in `benchmark/REPORT.md`'s "Row-vs-columnar micro-benchmarks" section.
//!
//! Run: `cargo run --release --package pipeline --bin zero-transpose-end-to-end -- \
//!   --input benchmark/spark-comet/orders.parquet --output /tmp/orders-filtered.parquet`

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

use arrow::compute::concat_batches;
use clap::Parser;
use datafusion::prelude::SessionContext;
use parquet::basic::Compression;
use pipeline::datafusion_query::register_orders;
use pipeline::write_parquet;

#[derive(Parser)]
#[command(
    name = "zero-transpose-end-to-end",
    about = "Read, filter, and write the orders fixture with zero row transpose"
)]
struct Args {
    /// Path to the orders Parquet file to read and filter.
    #[arg(long)]
    input: PathBuf,

    /// Path to write the filtered result to.
    #[arg(long)]
    output: PathBuf,
}

#[derive(Debug)]
enum CliError {
    Register(datafusion::error::DataFusionError),
    Query(datafusion::error::DataFusionError),
    Concat(arrow::error::ArrowError),
    Write(pipeline::PipelineIoError),
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Register(source) => write!(f, "registering orders table: {source}"),
            Self::Query(source) => write!(f, "running filter query: {source}"),
            Self::Concat(source) => write!(f, "concatenating filtered batches: {source}"),
            Self::Write(source) => write!(f, "writing filtered parquet: {source}"),
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("zero-transpose-end-to-end: {err}");
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

    // The fully-solved path. Filter, then write the result straight from
    // the RecordBatches DataFusion already produced - no row format ever
    // enters this call chain.
    let start = Instant::now();
    let filtered = ctx
        .sql("SELECT * FROM orders WHERE amount > 50.00")
        .await
        .map_err(CliError::Query)?
        .collect()
        .await
        .map_err(CliError::Query)?;
    let schema = filtered[0].schema();
    let combined = concat_batches(&schema, &filtered).map_err(CliError::Concat)?;
    write_parquet(&combined, &args.output, Compression::SNAPPY).map_err(CliError::Write)?;
    let elapsed = start.elapsed();
    let rows_written = combined.num_rows();

    println!("rows_written={rows_written}");
    println!("zero_transpose_elapsed={elapsed:?}");
    Ok(())
}
