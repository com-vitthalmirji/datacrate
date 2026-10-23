//! Phase D (`docs/internals/experiment.md` section 12): memory-mapped IPC
//! feeding `pipeline::simd_add`'s handler, under an explicit lifetime and
//! publication contract.
//!
//! ```text
//! Completed immutable uncompressed IPC file
//!     -> read-only mapping
//!     -> compatible IPC decoder (arrow_ipc::reader::FileDecoder)
//!     -> validated arrays
//!     -> handler (wrapping_add_simd)
//!     -> scalar result
//! ```
//!
//! Uses `arrow_ipc::reader::FileDecoder` directly against a `Buffer` backed
//! by the mapped bytes, mirroring arrow-rs's own documented
//! `zero_copy_ipc` pattern (see `FileDecoder`'s own doc example in
//! `arrow-ipc`), rather than `arrow::ipc::reader::FileReader`, which reads
//! through `Read + Seek` into owned, copied buffers and would not be
//! zero-copy at all.
//!
//! Decoding validation is left enabled throughout — `FileDecoder`'s
//! `with_skip_validation` is never called. Alignment is left at the
//! decoder's default (`require_alignment: false`): if the mapped array
//! data is misaligned, the decoder transparently copies into a new aligned
//! buffer instead of erroring; this binary checks, rather than assumes,
//! which of the two actually happened by testing whether the decoded
//! array's buffer pointer falls inside the mapped region.
//!
//! **Immutability is assumed, not enforced.** Nothing in this binary takes
//! a lock or otherwise prevents the mapped file from being modified by
//! another process between `Mmap::map` and the decoder finishing its read.
//! `memmap2::Mmap::map` is itself `unsafe` specifically because of this —
//! the caller must independently guarantee the file is not concurrently
//! modified. Here that guarantee holds only because this binary owns the
//! whole file's lifecycle (it wrote it moments earlier, in-process, and
//! nothing else touches it) — a real multi-process deployment would need
//! its own independent mechanism (e.g. a content-addressed, write-once
//! path) which this binary does not attempt to build (plan: "do not build
//! a cross-process reference-counting protocol").
//!
//! Two measured legs, kept separate per the plan:
//! 1. Querying an already-published artifact: one file is written and
//!    published once, untimed, then the mmap-decode-kernel path is timed
//!    repeatedly against that same already-durable file.
//! 2. Producer write/publication through consumer result: a fresh file is
//!    written, published, mapped, decoded, and run through the kernel, as
//!    one combined timed span, once per run (a shared file across runs
//!    would reuse leg 1's already-published state instead of measuring
//!    fresh publication).
//!
//! Publication mirrors `pipeline::staging::LocalStagedOutput`'s sequence
//! (sync file, rename into place, sync parent directory) rather than
//! inventing a new protocol, per the plan's "reuse existing publication
//! mechanisms where appropriate." That type is `pub(crate)` to the
//! `pipeline` library and not reachable from this separate binary crate,
//! so the three steps are reproduced directly here with the same
//! primitives (`tempfile::Builder`, `NamedTempFile::persist`, a parent-
//! directory `File::sync_all`), not re-exported or made public merely for
//! this benchmark.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::ptr::NonNull;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int32Array, RecordBatch};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::convert::fb_to_schema;
use arrow::ipc::reader::{FileDecoder, read_footer_length};
use arrow::ipc::root_as_footer;
use arrow::ipc::writer::FileWriter as IpcFileWriter;
use clap::Parser;
use memmap2::Mmap;
use pipeline::simd_add::{wrapping_add_reference, wrapping_add_simd};
use tempfile::tempdir;

#[derive(Parser)]
#[command(
    name = "mmap-ipc-handoff",
    about = "Time mmap-backed zero-copy IPC decoding feeding the SIMD handler, split into 'already-published' vs. 'write-through-publication' legs"
)]
struct Args {
    /// Element count for the single `value` column.
    #[arg(long, default_value_t = 5_000_000)]
    len: usize,

    /// Timed repetitions per leg.
    #[arg(long, default_value_t = 5)]
    runs: usize,
}

const K: i32 = 7;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]))
}

/// Not timed: fixture generation is not part of either publication or the
/// mmap/decode/kernel path.
fn generate_batch(len: usize) -> RecordBatch {
    let values = Int32Array::from(
        (0..len as i64)
            .map(|i| (i % 1_000_000) as i32)
            .collect::<Vec<_>>(),
    );
    RecordBatch::try_new(schema(), vec![Arc::new(values)]).expect("one column, matching schema")
}

/// Writes `batch` as an IPC file into owned temporary storage, then
/// durably publishes it to `output`, mirroring
/// `pipeline::staging::LocalStagedOutput`'s three-phase sequence (sync the
/// owned file, rename it into place, sync the destination directory) —
/// not calling that type directly since it is `pub(crate)`.
fn write_and_publish(batch: &RecordBatch, output: &Path) {
    let parent = output.parent().expect("output has a parent directory");
    let mut staged = tempfile::Builder::new()
        .prefix(".mmap-ipc-handoff-")
        .tempfile_in(parent)
        .expect("create owned staging file");

    {
        let file = staged
            .reopen()
            .expect("reopen staged file for the IPC writer");
        let mut writer =
            IpcFileWriter::try_new(file, &batch.schema()).expect("construct IPC writer");
        writer.write(batch).expect("write IPC batch");
        writer.finish().expect("finish IPC file");
    }

    staged
        .as_file_mut()
        .sync_all()
        .expect("sync staged file before rename");
    let persisted = staged
        .persist(output)
        .expect("rename staged file into place");
    persisted.sync_all().expect("sync persisted file");
    File::open(parent)
        .expect("open parent directory")
        .sync_all()
        .expect("sync parent directory after rename");
}

/// Maps `path` read-only and decodes its single record batch with
/// `FileDecoder`, mirroring `FileDecoder`'s own documented zero-copy
/// example. Returns the decoded batch plus whether its one data buffer's
/// pointer falls inside the mapped region (a real no-copy signal, not
/// assumed).
///
/// # Safety
///
/// `memmap2::Mmap::map` requires the caller to guarantee the file is not
/// concurrently modified for the mapping's lifetime. That guarantee holds
/// here because this process wrote the file moments earlier and nothing
/// else references it — see the module-level doc comment.
fn mmap_decode(path: &Path) -> (RecordBatch, bool) {
    let file = File::open(path).expect("open published IPC file");
    let mmap = unsafe { Mmap::map(&file).expect("map published IPC file read-only") };
    let base_ptr = mmap.as_ptr();
    let base_len = mmap.len();

    // SAFETY: `owner` (the `Arc<Mmap>` below) is kept alive for at least as
    // long as any `Buffer` built from it, because every such `Buffer`
    // stores a clone of that `Arc` as its `Deallocation::Custom` owner.
    // `base_ptr`/`base_len` describe exactly the memory `mmap` owns.
    let owner: Arc<dyn arrow::alloc::Allocation> = Arc::new(mmap);
    let buffer = unsafe {
        Buffer::from_custom_allocation(
            NonNull::new(base_ptr as *mut u8).expect("mmap base pointer is non-null"),
            base_len,
            owner,
        )
    };

    // Mirrors `FileDecoder`'s own doc example exactly (arrow_ipc::reader),
    // substituting this mmap-backed `Buffer` for its `Buffer::from_vec`.
    let trailer_start = buffer.len() - 10;
    let footer_len = read_footer_length(
        buffer[trailer_start..]
            .try_into()
            .expect("IPC file has a 10-byte trailer"),
    )
    .expect("valid IPC footer length");
    let footer = root_as_footer(&buffer[trailer_start - footer_len..trailer_start])
        .expect("valid IPC footer");
    let decoded_schema = Arc::new(fb_to_schema(
        footer.schema().expect("schema present in footer"),
    ));
    let mut decoder = FileDecoder::new(decoded_schema, footer.version());

    for block in footer.dictionaries().iter().flatten() {
        let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
        let data = buffer.slice_with_length(block.offset() as _, block_len);
        decoder
            .read_dictionary(block, &data)
            .expect("read dictionary block");
    }

    let batches = footer
        .recordBatches()
        .expect("record batches present in footer");
    assert_eq!(batches.len(), 1, "exactly one batch was written");
    let block = batches.get(0);
    let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
    let data = buffer.slice_with_length(block.offset() as _, block_len);
    let decoded = decoder
        .read_record_batch(block, &data)
        .expect("decode record batch")
        .expect("record batch is present, not a dictionary-only block");

    let values = decoded
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("value column is Int32");
    let value_ptr = values.values().as_ptr() as usize;
    let in_mapped_region =
        value_ptr >= base_ptr as usize && value_ptr < base_ptr as usize + base_len;

    (decoded, in_mapped_region)
}

fn run_kernel(decoded: &RecordBatch) -> Int32Array {
    let values = decoded
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("value column is Int32");
    wrapping_add_simd(values, K).expect("decoded column has no nulls")
}

fn main() -> ExitCode {
    let args = Args::parse();
    if args.runs == 0 {
        eprintln!("mmap-ipc-handoff: --runs must be at least 1");
        return ExitCode::FAILURE;
    }

    let fixture = generate_batch(args.len);
    let fixture_values = fixture
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("fixture value column is Int32");
    let oracle = wrapping_add_reference(fixture_values, K).expect("fixture has no nulls");

    let dir = tempdir().expect("create temp directory for published artifacts");

    // Leg 1: querying an already-published artifact. Publication happens
    // once, untimed; only the mmap-decode-kernel path is timed, repeated.
    let already_published_path = dir.path().join("already-published.arrow");
    write_and_publish(&fixture, &already_published_path);

    let mut leg1_durations = Vec::with_capacity(args.runs);
    let mut leg1_result = None;
    let mut leg1_in_mapped_region = false;
    for _ in 0..args.runs {
        let start = Instant::now();
        let (decoded, in_mapped_region) = mmap_decode(&already_published_path);
        let result = run_kernel(&decoded);
        leg1_durations.push(start.elapsed());
        leg1_in_mapped_region = in_mapped_region;
        leg1_result = Some(result);
    }
    let leg1_result = leg1_result.expect("runs is always > 0");

    // Leg 2: producer write/publication through consumer result. One fresh
    // file per run, all phases included in the one timed span.
    let mut leg2_durations = Vec::with_capacity(args.runs);
    let mut leg2_result = None;
    let mut leg2_in_mapped_region = false;
    for run in 0..args.runs {
        let path: PathBuf = dir
            .path()
            .join(format!("write-through-publication-{run}.arrow"));
        let start = Instant::now();
        write_and_publish(&fixture, &path);
        let (decoded, in_mapped_region) = mmap_decode(&path);
        let result = run_kernel(&decoded);
        leg2_durations.push(start.elapsed());
        leg2_in_mapped_region = in_mapped_region;
        leg2_result = Some(result);
    }
    let leg2_result = leg2_result.expect("runs is always > 0");

    if leg1_result.values() != oracle.values() || leg2_result.values() != oracle.values() {
        eprintln!("mmap-ipc-handoff: result mismatch against the independent oracle");
        return ExitCode::FAILURE;
    }

    let formatted = |durations: &[Duration]| -> String {
        durations
            .iter()
            .map(|d| format!("{d:?}"))
            .collect::<Vec<_>>()
            .join(", ")
    };

    println!(
        "leg 1 (query an already-published artifact, mmap+decode+kernel only): {}",
        formatted(&leg1_durations)
    );
    println!(
        "  decoded value buffer pointer inside the mapped region (no copy occurred): {leg1_in_mapped_region}"
    );
    println!(
        "leg 2 (write+publish through consumer result, one fresh file per run): {}",
        formatted(&leg2_durations)
    );
    println!(
        "  decoded value buffer pointer inside the mapped region (no copy occurred): {leg2_in_mapped_region}"
    );
    println!();
    println!(
        "mmap-ipc-handoff: len={} runs={} both legs match the independent oracle; \
         immutability is assumed (this process owns the whole file lifecycle), not \
         cross-process enforced; decoding validation was never skipped \
         (see docs/internals/experiment.md section 12)",
        args.len, args.runs
    );

    ExitCode::SUCCESS
}
