//! Phase E3 (`docs/internals/experiment.md` section 13, "separate-process
//! alternative"): the same Scala-producer -> Rust-consumer shape as
//! `arrow-interop-kernel` (Phase E1), but reading the published IPC file
//! via the zero-copy mmap path (`pipeline::ipc_mmap`) instead of the
//! plain `arrow::ipc::reader::FileReader`.
//!
//! This is the deliberate, single difference from Phase E1's kernel: E1
//! intentionally avoided Phase D's mmap path so its own numbers would not
//! be attributable to two investigations at once; E3 is specifically the
//! "Scala producer -> completed immutable Arrow IPC artifact -> Rust
//! memory-mapped reader -> controlled handler" shape the plan names, so
//! reusing that exact path here is the point, not a conflation.
//!
//! Immutability between the Scala producer finishing its write and this
//! process mapping the file is assumed, not enforced, for the same reason
//! recorded in `pipeline::ipc_mmap`'s own doc comment: this experiment
//! does not build a cross-process reference-counting protocol. Here the
//! assumption holds because `ObjectMaterializationDriver.scala` only
//! invokes this binary (as a subprocess, waiting for it to exit) after
//! its own IPC write has already fully completed, not concurrently with
//! it — the same Scala driver serves both Phase E1 and E3, differing
//! only in which kernel binary path it is pointed at.

use std::fs::File;
use std::process::ExitCode;
use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch};
use arrow::ipc::writer::FileWriter as IpcFileWriter;
use clap::Parser;
use pipeline::ipc_mmap::mmap_decode_single_i32_column;
use pipeline::simd_add::wrapping_add_simd;

#[derive(Parser)]
#[command(
    name = "arrow-interop-kernel-mmap",
    about = "Reads an IPC file's `value` column via mmap, runs wrapping_add_simd, writes the result as a new IPC file"
)]
struct Args {
    /// Path to the input Arrow IPC file, written and published by the
    /// Scala driver.
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

    let (batch, in_mapped_region) =
        mmap_decode_single_i32_column(std::path::Path::new(&args.input));
    eprintln!(
        "arrow-interop-kernel-mmap: decoded value buffer pointer inside the mapped region (no copy occurred): {in_mapped_region}"
    );

    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("input column is Int32");

    let result = match wrapping_add_simd(values, args.k) {
        Ok(result) => result,
        Err(error) => {
            eprintln!("arrow-interop-kernel-mmap: {error}");
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
