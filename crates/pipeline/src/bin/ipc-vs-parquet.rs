//! Phase C (`docs/internals/experiment.md` section 11): measures
//! persistence trade-offs between Arrow IPC and Parquet without confusing
//! storage format with CPU effects — write duration, read-to-result
//! duration, and file size are all recorded separately, for both a full
//! scan and a narrow single-column projection out of the same four-column
//! schema.
//!
//! Three variants, all built from one identical in-memory `RecordBatch`:
//! uncompressed Arrow IPC (file format, not streaming), uncompressed
//! Parquet, and Zstandard-compressed Parquet (already a pinned dependency
//! feature in this crate).
//!
//! Explicit limitations, stated rather than silently assumed:
//! - **Not a cold-cache benchmark.** Files are read back immediately after
//!   being written, in the same process, on whatever filesystem the OS
//!   gives this process — page-cache state is not controlled or reset.
//! - **Ordinary buffered reads, not memory-mapped.** `arrow::ipc::reader`
//!   here opens a plain `File`/`BufReader`; this binary makes no mmap
//!   claim at all (Phase D's subject, not this one).
//! - **Memory metrics are not measured in this slice.** Recording a crude,
//!   unreliable number would misstate the plan's own "memory metrics and
//!   their scope" requirement worse than stating plainly that this binary
//!   does not attempt it.
//! - **No winner is claimed.** This binary prints raw durations and sizes;
//!   it does not rank the three variants against each other.

use std::fs::File;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::compute::kernels::cmp::gt;
use arrow::compute::{filter, sum};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::reader::FileReader as IpcFileReader;
use arrow::ipc::writer::FileWriter as IpcFileWriter;
use clap::Parser;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use pipeline::write_parquet;
use tempfile::tempdir;

#[derive(Parser)]
#[command(
    name = "ipc-vs-parquet",
    about = "Time write/read-to-result for Arrow IPC vs. uncompressed/zstd Parquet, full scan and narrow projection"
)]
struct Args {
    /// Row count. One batch, one Parquet row group — documented, not hidden.
    #[arg(long, default_value_t = 5_000_000)]
    rows: usize,
}

const THRESHOLD: f64 = 50.0;
/// Column index of `amount` in the four-column schema below, used for both
/// the IPC reader's `projection: Option<Vec<usize>>` and the Parquet
/// `ProjectionMask::leaves` call.
const AMOUNT_COLUMN_INDEX: usize = 1;

/// Four columns: `id`/`amount`/`placed_at`/`note`, matching this repo's
/// established orders-fixture shape (`datafusion_query::orders_schema`),
/// except `amount` is `Float64` here rather than `Decimal128` — the same
/// choice `row-vs-columnar.rs` already made, to use Arrow's ready-made
/// `cmp`/`filter`/`sum` kernels on a type they support directly rather than
/// re-deriving decimal arithmetic for an illustrative microbenchmark.
fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new(
            "placed_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("note", DataType::Utf8, false),
    ]))
}

/// Not timed: fixture generation is not part of any measured write or read.
/// Deterministic by row index, no RNG and no seed to record; no nulls in
/// any column.
fn generate_batch(rows: usize) -> RecordBatch {
    let ids: Int64Array = (0..rows as i64).collect();
    let amounts: Float64Array = (0..rows).map(|i| (i % 100) as f64 + 0.5).collect();
    let placed_at = TimestampMicrosecondArray::from_iter_values(
        (0..rows).map(|i| 1_700_000_000_000_000 + i as i64),
    );
    let notes = StringArray::from_iter_values((0..rows).map(|i| format!("order-{i}")));
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(ids),
            Arc::new(amounts),
            Arc::new(placed_at),
            Arc::new(notes),
        ],
    )
    .expect("four equal-length, schema-matching columns")
}

fn sum_amount_over_threshold(amounts: &Float64Array) -> f64 {
    let threshold = Float64Array::from_value(THRESHOLD, amounts.len());
    let mask = gt(amounts, &threshold).expect("gt kernel should run on equal-length arrays");
    let filtered = filter(amounts, &mask).expect("filter kernel should run on a boolean mask");
    let filtered = filtered
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("filter preserves the Float64 array type");
    sum(filtered).unwrap_or(0.0)
}

/// `f64` addition is not associative (rust-skills: never compare floats
/// with `==`), so two sums over the same logical values can legitimately
/// differ in the last few bits depending on reduction order. Used both
/// by `main`'s own cross-format check and by the regression test below.
fn approx_eq(a: f64, b: f64) -> bool {
    (a - b).abs() <= a.abs().max(b.abs()).max(1.0) * 1e-9
}

fn write_ipc(batch: &RecordBatch, path: &Path) -> Duration {
    let start = Instant::now();
    let file = File::create(path).expect("create IPC file");
    let mut writer = IpcFileWriter::try_new(file, &batch.schema()).expect("construct IPC writer");
    writer.write(batch).expect("write IPC batch");
    writer.finish().expect("finish IPC file");
    start.elapsed()
}

/// Full scan: reads every column back, computes the same filtered sum.
fn read_ipc_full_scan(path: &Path) -> (Duration, f64) {
    let start = Instant::now();
    let file = File::open(path).expect("open IPC file");
    let mut reader = IpcFileReader::try_new(file, None).expect("construct IPC reader");
    let batch = reader
        .next()
        .expect("one batch was written")
        .expect("decode IPC batch");
    let amounts = batch
        .column(AMOUNT_COLUMN_INDEX)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("amount column is Float64");
    let result = sum_amount_over_threshold(amounts);
    (start.elapsed(), result)
}

/// Narrow projection: the IPC reader's own `projection` parameter decodes
/// only the requested column, not all four.
fn read_ipc_narrow_projection(path: &Path) -> (Duration, f64) {
    let start = Instant::now();
    let file = File::open(path).expect("open IPC file");
    let mut reader = IpcFileReader::try_new(file, Some(vec![AMOUNT_COLUMN_INDEX]))
        .expect("construct projected IPC reader");
    let batch = reader
        .next()
        .expect("one batch was written")
        .expect("decode IPC batch");
    let amounts = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("projected column is Float64");
    let result = sum_amount_over_threshold(amounts);
    (start.elapsed(), result)
}

fn write_parquet_variant(batch: &RecordBatch, path: &Path, compression: Compression) -> Duration {
    let start = Instant::now();
    write_parquet(batch, path, compression).expect("write Parquet file");
    start.elapsed()
}

/// `pipeline::read_parquet` is *not* a generic Parquet reader — it
/// validates every batch against this crate's own fixed internal
/// `schema()` (`id`/`name`/`note`, matching `fixtures/m3/orders.csv`),
/// which is a different shape than this binary's own four-column
/// fixture. The first real CI execution of this binary (never run before
/// — only compiled — until `ee7db0a` added an execution step) caught
/// this: `read_parquet` panicked with a schema-mismatch error reporting
/// *its* expected schema, not this file's. Fixed by reading the full
/// batch directly here, against this binary's own known schema, instead
/// of reusing a helper that was never actually generic.
fn read_full_batch(path: &Path) -> RecordBatch {
    let file = File::open(path).expect("open Parquet file");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("construct Parquet reader builder")
        .build()
        .expect("build Parquet reader");
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<_, _>>()
        .expect("decode Parquet batches");
    arrow::compute::concat_batches(&schema(), &batches).expect("concatenate batches")
}

fn read_parquet_full_scan(path: &Path) -> (Duration, f64) {
    let start = Instant::now();
    let batch = read_full_batch(path);
    let amounts = batch
        .column(AMOUNT_COLUMN_INDEX)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("amount column is Float64");
    let result = sum_amount_over_threshold(amounts);
    (start.elapsed(), result)
}

/// `write_parquet` writes `batch` as a single *row group*, but
/// `ParquetRecordBatchReaderBuilder::build()`'s decoder still splits that
/// one row group back into multiple `RecordBatch`es of its own default
/// size (independent of how many rows the row group holds) — so a single
/// `reader.next()` call here silently returned only the first chunk,
/// undercounting everything after it. The IPC legs' own single
/// `reader.next()` call happens to be safe for a different reason
/// (`write_ipc` always round-trips one `write()` call as exactly one
/// message, with no internal chunking), which is exactly why this bug
/// surfaced only here and only on real CI execution — `cargo test`/clippy
/// have no way to catch a result that is merely *numerically* wrong.
/// Fixed by summing across every batch the reader actually produces.
fn read_parquet_narrow_projection(path: &Path) -> (Duration, f64) {
    let start = Instant::now();
    let file = File::open(path).expect("open Parquet file");
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).expect("construct Parquet reader builder");
    let projection = ProjectionMask::leaves(builder.parquet_schema(), [AMOUNT_COLUMN_INDEX]);
    let reader = builder
        .with_projection(projection)
        .build()
        .expect("build projected Parquet reader");

    let mut result = 0.0;
    for batch in reader {
        let batch = batch.expect("decode Parquet batch");
        let amounts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("projected column is Float64");
        result += sum_amount_over_threshold(amounts);
    }
    (start.elapsed(), result)
}

fn report_variant(
    label: &str,
    path: &Path,
    write_duration: Duration,
    full_scan: (Duration, f64),
    narrow_projection: (Duration, f64),
) {
    let file_size = std::fs::metadata(path)
        .expect("file exists after write")
        .len();
    println!("{label}:");
    println!("  file size: {file_size} bytes");
    println!("  write duration: {write_duration:?}");
    println!(
        "  read-to-result (full scan, all 4 columns decoded): {:?} (result={})",
        full_scan.0, full_scan.1
    );
    println!(
        "  read-to-result (narrow projection, amount only):   {:?} (result={})",
        narrow_projection.0, narrow_projection.1
    );
}

fn main() -> ExitCode {
    let args = Args::parse();
    let dir = tempdir().expect("create temp directory for the three fixture files");

    let batch = generate_batch(args.rows);

    let ipc_path = dir.path().join("orders.arrow");
    let ipc_write_duration = write_ipc(&batch, &ipc_path);
    let ipc_full_scan = read_ipc_full_scan(&ipc_path);
    let ipc_narrow = read_ipc_narrow_projection(&ipc_path);

    let parquet_uncompressed_path = dir.path().join("orders_uncompressed.parquet");
    let parquet_uncompressed_write_duration = write_parquet_variant(
        &batch,
        &parquet_uncompressed_path,
        Compression::UNCOMPRESSED,
    );
    let parquet_uncompressed_full_scan = read_parquet_full_scan(&parquet_uncompressed_path);
    let parquet_uncompressed_narrow = read_parquet_narrow_projection(&parquet_uncompressed_path);

    let parquet_zstd_path = dir.path().join("orders_zstd.parquet");
    let zstd_compression = Compression::ZSTD(ZstdLevel::default());
    let parquet_zstd_write_duration =
        write_parquet_variant(&batch, &parquet_zstd_path, zstd_compression);
    let parquet_zstd_full_scan = read_parquet_full_scan(&parquet_zstd_path);
    let parquet_zstd_narrow = read_parquet_narrow_projection(&parquet_zstd_path);

    // `f64` summation is not associative (rust-skills: never compare floats
    // with `==`) — full-scan and narrow-projection reads can decode the
    // same logical values into differently-chunked buffers, so Arrow's
    // `sum()` kernel's internal SIMD reduction can legitimately add them in
    // a different order and land on a result that differs in the last few
    // bits, despite being logically correct. An exact `!=` chain here
    // genuinely failed on real CI execution (the first time this binary was
    // ever run) for exactly this reason. Fixed with a relative tolerance,
    // and every value is printed before the check runs — a silent early
    // return on mismatch previously meant a failure produced no diagnostic
    // output at all, costing a second full CI round trip just to see the
    // numbers.
    let results = [
        ("ipc full scan", ipc_full_scan.1),
        ("ipc narrow", ipc_narrow.1),
        (
            "parquet uncompressed full scan",
            parquet_uncompressed_full_scan.1,
        ),
        ("parquet uncompressed narrow", parquet_uncompressed_narrow.1),
        ("parquet zstd full scan", parquet_zstd_full_scan.1),
        ("parquet zstd narrow", parquet_zstd_narrow.1),
    ];
    for (label, value) in &results {
        println!("result ({label}): {value}");
    }

    let reference = ipc_full_scan.1;
    if results
        .iter()
        .any(|(_, value)| !approx_eq(*value, reference))
    {
        eprintln!(
            "ipc-vs-parquet: result mismatch between formats or scan modes (see values printed above)"
        );
        return ExitCode::FAILURE;
    }

    println!(
        "ipc-vs-parquet: rows={}, 1 batch, 1 row group, no nulls, deterministic (no RNG seed)",
        args.rows
    );
    println!();
    report_variant(
        "Arrow IPC (uncompressed, file format)",
        &ipc_path,
        ipc_write_duration,
        ipc_full_scan,
        ipc_narrow,
    );
    println!();
    report_variant(
        "Parquet (uncompressed)",
        &parquet_uncompressed_path,
        parquet_uncompressed_write_duration,
        parquet_uncompressed_full_scan,
        parquet_uncompressed_narrow,
    );
    println!();
    report_variant(
        "Parquet (Zstandard)",
        &parquet_zstd_path,
        parquet_zstd_write_duration,
        parquet_zstd_full_scan,
        parquet_zstd_narrow,
    );
    println!();
    println!(
        "ipc-vs-parquet: all variants agree on the filtered-sum result; \
         not a cold-cache benchmark (read immediately follows write in the \
         same process); ordinary buffered reads only, no mmap claim; \
         memory metrics not measured in this slice; no format is claimed \
         as a winner (see docs/internals/experiment.md section 11)"
    );

    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::arrow::arrow_reader::DEFAULT_BATCH_SIZE;

    /// Regression test for a real bug found only by actually running this
    /// binary in CI (compiling and linting it caught nothing):
    /// `read_parquet_narrow_projection` called the projected reader's
    /// `.next()` exactly once, silently discarding every batch after the
    /// first. `ParquetRecordBatchReaderBuilder::build()` decodes a row
    /// group back into multiple `RecordBatch`es of its own default size
    /// (`DEFAULT_BATCH_SIZE = 1024`) regardless of how many rows the row
    /// group holds, so any row count past that threshold already exposed
    /// the bug — this test uses 2,500 rows specifically so the fix is
    /// checked by a fast unit test instead of only by the slow, full
    /// binary execution this bug first surfaced in.
    #[test]
    fn narrow_projection_sums_every_decoded_batch_not_just_the_first() {
        let rows = 2_500;
        let batch = generate_batch(rows);
        let dir = tempdir().expect("create temp directory");
        let path = dir.path().join("regression.parquet");
        write_parquet(&batch, &path, Compression::UNCOMPRESSED).expect("write parquet file");

        // Confirms the fixture actually forces multiple decoded batches —
        // without this, the test would pass even with the original bug,
        // since a single-batch file never exercises the discarded-batches
        // code path at all.
        let probe_file = File::open(&path).expect("open parquet file for the batch-count probe");
        let probe_builder = ParquetRecordBatchReaderBuilder::try_new(probe_file)
            .expect("construct probe reader builder");
        let probe_reader = probe_builder.build().expect("build probe reader");
        let decoded_batch_count = probe_reader.count();
        assert!(
            decoded_batch_count > 1,
            "test fixture of {rows} rows must decode into more than one \
             batch at the default batch size ({DEFAULT_BATCH_SIZE}) for \
             this regression to mean anything; got {decoded_batch_count}"
        );

        let (_, narrow_result) = read_parquet_narrow_projection(&path);
        let amounts = batch
            .column(AMOUNT_COLUMN_INDEX)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("amount column is Float64");
        let expected = sum_amount_over_threshold(amounts);

        assert!(
            approx_eq(narrow_result, expected),
            "narrow projection summed {narrow_result}, expected {expected} \
             (within tolerance) — a result this far off means batches \
             after the first were silently dropped again"
        );
    }
}
