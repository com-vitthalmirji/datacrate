//! Prints a `#[derive(Contract, Connector)]` struct matching an existing
//! Parquet file's schema, as a starting point for a pipeline's data model.
//! The compile-time feasibility check against the real file on every build
//! is a separate mechanism (a pipeline crate's own `build.rs`), not this CLI.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use schema_codegen::SchemaCodegenError;

#[derive(Parser)]
#[command(
    name = "scaffold-cli",
    about = "Generate a Contract/Connector struct from a Parquet file's schema"
)]
struct Args {
    /// Path to the Parquet file whose schema to read.
    from_parquet: PathBuf,

    /// Name of the generated struct.
    #[arg(long)]
    name: String,

    /// Write the generated struct here instead of stdout.
    #[arg(long)]
    out: Option<PathBuf>,
}

#[derive(Debug)]
enum CliError {
    Schema {
        source: SchemaCodegenError,
    },
    WriteOutput {
        path: PathBuf,
        source: std::io::Error,
    },
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CliError::Schema { source } => write!(f, "{source}"),
            CliError::WriteOutput { path, source } => {
                write!(f, "failed to write '{}': {source}", path.display())
            }
        }
    }
}

impl std::error::Error for CliError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CliError::Schema { source } => Some(source),
            CliError::WriteOutput { source, .. } => Some(source),
        }
    }
}

fn main() -> ExitCode {
    let args = Args::parse();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("scaffold-cli: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &Args) -> Result<(), CliError> {
    let schema = schema_codegen::read_parquet_schema(&args.from_parquet)
        .map_err(|source| CliError::Schema { source })?;
    let source = schema_codegen::struct_source(&args.name, &schema)
        .map_err(|source| CliError::Schema { source })?;

    match &args.out {
        Some(path) => std::fs::write(path, &source).map_err(|source| CliError::WriteOutput {
            path: path.clone(),
            source,
        }),
        None => {
            print!("{source}");
            Ok(())
        }
    }
}
