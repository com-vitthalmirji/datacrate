//! Week 5 build-contract evidence: compares two Parquet compression
//! configurations (Snappy vs Zstd) on a synthetic batch, validating that
//! both round-trip to identical data before reporting size/time.
//!
//! Run: `cargo run --release --package pipeline --example compression_comparison`

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use parquet::basic::Compression;
use pipeline::{read_parquet, schema, write_parquet};

const ROW_COUNT: usize = 200_000;

fn synthetic_batch() -> RecordBatch {
    let names = ["Ada", "Grace", "Linus", "Barbara", "Katherine"];
    let notes = [
        "streaming ingest",
        "batch backfill",
        "schema drift detected",
        "",
        "nightly compaction",
    ];

    let ids: Int64Array = (0..ROW_COUNT as i64).collect();
    let name_array: StringArray = (0..ROW_COUNT)
        .map(|i| Some(names[i % names.len()]))
        .collect();
    let note_array: StringArray = (0..ROW_COUNT)
        .map(|i| match notes[i % notes.len()] {
            "" => None,
            note => Some(note),
        })
        .collect();

    RecordBatch::try_new(
        schema(),
        vec![Arc::new(ids), Arc::new(name_array), Arc::new(note_array)],
    )
    .expect("synthetic columns match the fixed schema")
}

fn run_once(batch: &RecordBatch, compression: Compression, label: &str) {
    let dir = tempfile::tempdir().expect("temp dir should be creatable");
    let path = dir.path().join("bench.parquet");

    let write_start = Instant::now();
    write_parquet(batch, &path, compression).expect("batch should write cleanly");
    let write_elapsed = write_start.elapsed();

    let file_size = std::fs::metadata(&path)
        .expect("written file should have metadata")
        .len();

    let read_start = Instant::now();
    let round_tripped = read_parquet(&path).expect("batch should read back cleanly");
    let read_elapsed = read_start.elapsed();

    assert_eq!(round_tripped.num_rows(), batch.num_rows());
    for column in 0..batch.num_columns() {
        assert_eq!(
            round_tripped.column(column).as_ref(),
            batch.column(column).as_ref()
        );
    }

    println!(
        "{label}: write={write_elapsed:?} read={read_elapsed:?} file_bytes={file_size} rows={}",
        batch.num_rows()
    );
}

fn main() {
    let batch = synthetic_batch();
    run_once(&batch, Compression::SNAPPY, "snappy");
    run_once(&batch, Compression::ZSTD(Default::default()), "zstd");
}
