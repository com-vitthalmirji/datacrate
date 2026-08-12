//! M1 CLI contract: see `workshop/M1.md`.
//!
//! Current limitation: output is written record-by-record as the input is
//! streamed, so a malformed record partway through the file can leave
//! already-selected rows on stdout before the error is detected and the
//! process exits non-zero. Buffering the whole output to avoid this would
//! defeat the point of streaming, so it is accepted rather than fixed.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

#[derive(Parser)]
#[command(name = "csv-select", about = "Select one column from a CSV file")]
struct Args {
    /// Path to the input CSV file.
    input: PathBuf,

    /// Zero-based index of the column to select.
    #[arg(long)]
    column: usize,

    /// Treat the first record as data instead of headers.
    #[arg(long)]
    no_headers: bool,
}

#[derive(Debug)]
enum CliError {
    OpenInput { path: PathBuf, source: csv::Error },
    ColumnOutOfRange { column: usize, available: usize },
    ReadRecord { source: csv::Error },
    WriteOutput { source: csv::Error },
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CliError::OpenInput { path, source } => {
                write!(f, "cannot read '{}': {source}", path.display())
            }
            CliError::ColumnOutOfRange { column, available } => {
                write!(
                    f,
                    "column {column} is out of range: only {available} column(s) present"
                )
            }
            CliError::ReadRecord { source } => write!(f, "malformed CSV: {source}"),
            CliError::WriteOutput { source } => write!(f, "failed to write output: {source}"),
        }
    }
}

impl std::error::Error for CliError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CliError::OpenInput { source, .. }
            | CliError::ReadRecord { source }
            | CliError::WriteOutput { source } => Some(source),
            CliError::ColumnOutOfRange { .. } => None,
        }
    }
}

fn main() -> ExitCode {
    let args = Args::parse();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("csv-select: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &Args) -> Result<(), CliError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(!args.no_headers)
        .from_path(&args.input)
        .map_err(|source| CliError::OpenInput {
            path: args.input.clone(),
            source,
        })?;

    let mut writer = csv::WriterBuilder::new().from_writer(std::io::stdout());

    if args.no_headers {
        write_no_headers(&mut reader, &mut writer, args.column)?;
    } else {
        write_with_headers(&mut reader, &mut writer, args.column)?;
    }

    writer.flush().map_err(|source| CliError::WriteOutput {
        source: csv::Error::from(source),
    })
}

fn write_with_headers<R: std::io::Read, W: std::io::Write>(
    reader: &mut csv::Reader<R>,
    writer: &mut csv::Writer<W>,
    column: usize,
) -> Result<(), CliError> {
    // `reader.headers()` returns `&StringRecord` borrowed from `reader`. That
    // borrow must end before `reader.records()` is called below (same `&mut
    // reader`), so the header row is cloned here rather than held by reference.
    let headers = reader
        .headers()
        .map_err(|source| CliError::ReadRecord { source })?
        .clone();
    let selected_header = headers.get(column).ok_or(CliError::ColumnOutOfRange {
        column,
        available: headers.len(),
    })?;

    writer
        .write_record([selected_header])
        .map_err(|source| CliError::WriteOutput { source })?;

    for record in reader.records() {
        let record = record.map_err(|source| CliError::ReadRecord { source })?;
        let field = record.get(column).ok_or(CliError::ColumnOutOfRange {
            column,
            available: record.len(),
        })?;
        writer
            .write_record([field])
            .map_err(|source| CliError::WriteOutput { source })?;
    }
    Ok(())
}

fn write_no_headers<R: std::io::Read, W: std::io::Write>(
    reader: &mut csv::Reader<R>,
    writer: &mut csv::Writer<W>,
    column: usize,
) -> Result<(), CliError> {
    let mut records = reader.records();

    let Some(first) = records.next() else {
        return Ok(());
    };
    let first = first.map_err(|source| CliError::ReadRecord { source })?;
    let field = first.get(column).ok_or(CliError::ColumnOutOfRange {
        column,
        available: first.len(),
    })?;
    writer
        .write_record([field])
        .map_err(|source| CliError::WriteOutput { source })?;

    for record in records {
        let record = record.map_err(|source| CliError::ReadRecord { source })?;
        let field = record.get(column).ok_or(CliError::ColumnOutOfRange {
            column,
            available: record.len(),
        })?;
        writer
            .write_record([field])
            .map_err(|source| CliError::WriteOutput { source })?;
    }
    Ok(())
}
