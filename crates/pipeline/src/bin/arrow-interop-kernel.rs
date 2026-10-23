//! Phase E1 (`docs/internals/experiment.md` section 13): the Rust side of
//! the Scala-to-Rust subprocess boundary. Reads an Arrow IPC file written
//! by the Scala driver (`scala/arrow-interop`'s
//! `ObjectMaterializationDriver`), runs
//! [`pipeline::simd_add::wrapping_add_simd`] on its single `Int32` `value`
//! column, writes the result as a new Arrow IPC file, and exits.
//!
//! The boundary is deliberately the plainest one available: files on
//! disk, read with the ordinary (not mmap-backed)
//! `arrow::ipc::reader::FileReader`. This is intentional, not an
//! oversight — Decision 1 forbids a custom JNI bridge, and Phase E2 is
//! the dedicated Arrow-native C Data Interface investigation; using
//! Phase D's zero-copy mmap path here as well would blur which effect a
//! measured E1 number is actually attributable to (the plan's own
//! "Comparison rule": don't let one investigation's mechanism leak into
//! another's measurement).

use std::fs::File;
use std::process::ExitCode;
use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch};
use arrow::ipc::reader::FileReader as IpcFileReader;
use arrow::ipc::writer::FileWriter as IpcFileWriter;
use clap::Parser;
use pipeline::simd_add::wrapping_add_simd;

#[derive(Parser)]
#[command(
    name = "arrow-interop-kernel",
    about = "Reads an IPC file's `value` column, runs wrapping_add_simd, writes the result as a new IPC file"
)]
struct Args {
    /// Path to the input Arrow IPC file, written by the Scala driver.
    #[arg(long)]
    input: String,

    /// Path to write the result Arrow IPC file to.
    #[arg(long)]
    output: String,

    /// The constant added to every element.
    #[arg(long, default_value_t = 7)]
    k: i32,
}

fn main() -> ExitCode {
    let args = Args::parse();

    let input_file = File::open(&args.input).expect("open input IPC file");
    let mut reader = IpcFileReader::try_new(input_file, None).expect("construct IPC reader");
    let batch = reader
        .next()
        .expect("input IPC file has one batch")
        .expect("decode input IPC batch");
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("input column is Int32");

    let result = match wrapping_add_simd(values, args.k) {
        Ok(result) => result,
        Err(error) => {
            eprintln!("arrow-interop-kernel: {error}");
            return ExitCode::FAILURE;
        }
    };

    let output_schema = batch.schema();
    let output_batch = RecordBatch::try_new(output_schema.clone(), vec![Arc::new(result)])
        .expect("kernel output matches the input schema shape");

    let output_file = File::create(&args.output).expect("create output IPC file");
    let mut writer =
        IpcFileWriter::try_new(output_file, &output_schema).expect("construct IPC writer");
    writer.write(&output_batch).expect("write output IPC batch");
    writer.finish().expect("finish output IPC file");

    ExitCode::SUCCESS
}
