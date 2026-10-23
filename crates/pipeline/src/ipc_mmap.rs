//! Zero-copy mmap-backed IPC decoding for a single-`Int32`-column record
//! batch, shared by Phase D's same-process benchmark
//! (`bin/mmap-ipc-handoff.rs`) and Phase E3's separate-process,
//! Scala-produced leg (`bin/arrow-interop-kernel-mmap.rs`,
//! `docs/internals/experiment.md` section 13). Extracted into the library
//! once a second real call site needed this exact logic, not written
//! speculatively ahead of need.
//!
//! Mirrors `arrow_ipc::reader::FileDecoder`'s own documented zero-copy
//! example, substituting an `memmap2::Mmap`-backed [`arrow::buffer::Buffer`]
//! for its `Buffer::from_vec`, rather than
//! `arrow::ipc::reader::FileReader` (which reads through `Read + Seek`
//! into owned, copied buffers and would not be zero-copy at all).
//!
//! Decoding validation is left enabled throughout —
//! `FileDecoder::with_skip_validation` is never called.

use std::fs::File;
use std::path::Path;
use std::ptr::NonNull;
use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch};
use arrow::buffer::Buffer;
use arrow::ipc::convert::fb_to_schema;
use arrow::ipc::reader::{FileDecoder, read_footer_length};
use arrow::ipc::root_as_footer;
use memmap2::Mmap;

/// Maps `path` read-only and decodes its single record batch, whose only
/// column must be `Int32` (this crate's `wrapping_add_simd` domain).
/// Returns the decoded batch plus whether that column's buffer pointer
/// falls inside the mapped region — a real no-copy signal, checked rather
/// than assumed (alignment is left at `FileDecoder`'s documented default:
/// auto-copy on misalignment, zero-copy otherwise).
///
/// # Safety
///
/// `memmap2::Mmap::map` requires the caller to guarantee the file is not
/// concurrently modified for the mapping's lifetime. Callers of this
/// function must hold that guarantee independently — this function does
/// not itself enforce it (plan: "state whether the experiment assumes
/// externally guaranteed immutability or enforces it").
///
/// # Panics
///
/// Panics if `path` cannot be opened or mapped, if its contents are not a
/// valid Arrow IPC file containing exactly one record batch, or if that
/// batch's only column is not `Int32` — all are experiment-harness
/// preconditions, not recoverable runtime states for this function's
/// callers (all of which are experiment binaries, not user-facing code).
pub fn mmap_decode_single_i32_column(path: &Path) -> (RecordBatch, bool) {
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
