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

    write_selected_column(&mut reader, &mut writer, args.column, !args.no_headers)?;

    writer.flush().map_err(|source| CliError::WriteOutput {
        source: csv::Error::from(source),
    })
}

/// Looks up `column` in `record`, reporting [`CliError::ColumnOutOfRange`] if
/// it's missing.
fn select_field(record: &csv::StringRecord, column: usize) -> Result<&str, CliError> {
    record.get(column).ok_or(CliError::ColumnOutOfRange {
        column,
        available: record.len(),
    })
}

/// Looks up `column` in `record` via [`select_field`], then writes it as a
/// one-column record.
fn select_and_write<W: std::io::Write>(
    writer: &mut csv::Writer<W>,
    record: &csv::StringRecord,
    column: usize,
) -> Result<(), CliError> {
    let field = select_field(record, column)?;
    writer
        .write_record([field])
        .map_err(|source| CliError::WriteOutput { source })
}

fn write_selected_column<R: std::io::Read, W: std::io::Write>(
    reader: &mut csv::Reader<R>,
    writer: &mut csv::Writer<W>,
    column: usize,
    has_headers: bool,
) -> Result<(), CliError> {
    if has_headers {
        // `reader.headers()` returns `&StringRecord` borrowed from `reader`. That
        // borrow must end before `reader.records()` is called below (same `&mut
        // reader`), so the header row is cloned here rather than held by reference.
        let headers = reader
            .headers()
            .map_err(|source| CliError::ReadRecord { source })?
            .clone();
        select_and_write(writer, &headers, column)?;
    }

    for record in reader.records() {
        let record = record.map_err(|source| CliError::ReadRecord { source })?;
        select_and_write(writer, &record, column)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_field_returns_correct_field_for_valid_index() {
        let record = csv::StringRecord::from(vec!["a", "b", "c"]);

        assert_eq!(select_field(&record, 1).expect("index 1 exists"), "b");
    }

    #[test]
    fn select_field_errors_on_out_of_range_column() {
        let record = csv::StringRecord::from(vec!["a", "b"]);

        let err = select_field(&record, 5).expect_err("index 5 is out of range");

        assert!(matches!(
            err,
            CliError::ColumnOutOfRange {
                column: 5,
                available: 2
            }
        ));
    }
}
