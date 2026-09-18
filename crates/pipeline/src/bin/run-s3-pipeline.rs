//! M2 smoke-test CLI: runs [`pipeline::bounded::run_bounded_pipeline_s3`]
//! against a real (or MinIO-compatible) S3 bucket. Credentials and endpoint
//! come from the standard AWS environment variables
//! (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`,
//! `AWS_REGION`, `AWS_ALLOW_HTTP`), picked up by
//! [`object_store::aws::AmazonS3Builder::from_env`] — no MinIO-specific flag
//! plumbing needed.

use std::process::ExitCode;

use clap::Parser;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use parquet::basic::Compression;
use pipeline::bounded::{CancellationToken, PipelineConfig, run_bounded_pipeline_s3};

#[derive(Parser)]
#[command(
    name = "run-s3-pipeline",
    about = "Run the bounded CSV -> Parquet pipeline against an S3-compatible bucket"
)]
struct Args {
    /// S3 bucket name.
    #[arg(long)]
    bucket: String,

    /// Key of the input CSV object within the bucket.
    #[arg(long)]
    input_key: String,

    /// Key to write the output Parquet object to within the bucket.
    #[arg(long)]
    output_key: String,

    /// Rows per `RecordBatch`.
    #[arg(long, default_value_t = 1_000)]
    batch_size: usize,

    /// Capacity of the channel between the reader and writer threads.
    #[arg(long, default_value_t = 4)]
    channel_capacity: usize,
}

#[derive(Debug)]
enum CliError {
    BuildStore {
        source: object_store::Error,
    },
    Pipeline {
        source: pipeline::bounded::PipelineError,
    },
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CliError::BuildStore { source } => write!(f, "failed to configure S3 client: {source}"),
            CliError::Pipeline { source } => write!(f, "pipeline failed: {source}"),
        }
    }
}

impl std::error::Error for CliError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CliError::BuildStore { source } => Some(source),
            CliError::Pipeline { source } => Some(source),
        }
    }
}

fn main() -> ExitCode {
    let args = Args::parse();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("run-s3-pipeline: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &Args) -> Result<(), CliError> {
    let store = AmazonS3Builder::from_env()
        .with_bucket_name(&args.bucket)
        .build()
        .map_err(|source| CliError::BuildStore { source })?;

    let config = PipelineConfig {
        batch_size: args.batch_size,
        channel_capacity: args.channel_capacity,
        compression: Compression::SNAPPY,
    };

    let report = run_bounded_pipeline_s3(
        &store,
        &ObjectPath::from(args.input_key.as_str()),
        &ObjectPath::from(args.output_key.as_str()),
        &config,
        &CancellationToken::new(),
    )
    .map_err(|source| CliError::Pipeline { source })?;

    println!(
        "wrote {} batch(es), {} row(s) to s3://{}/{}",
        report.batches_written, report.rows_written, args.bucket, args.output_key
    );
    Ok(())
}
