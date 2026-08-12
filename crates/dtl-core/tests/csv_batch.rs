// INTEGRATION TESTS — TDD style
//
// TDD means: write the test first, watch it fail (RED),
// then write the minimum code to make it pass (GREEN).
//
// These tests are in a separate file because they test the public
// behaviour of the library — just like a real user would use it.
// They don't care about internals.

use dtl_core::csv_zero_copy::CsvBatch;

// ── TEST 1: Basic row count ───────────────────────────────────────────────────
// The simplest possible check: give it 3 rows, get back 3 rows.
// This is always the first test to write — if count is wrong, nothing else matters.
#[test]
fn parses_three_records() {
    let raw = b"alice,30\nbob,25\ncarol,35"; // b"..." means raw bytes, not a String
    let batch = CsvBatch::parse(raw, b'\n');
    assert_eq!(batch.record_count(), 3);
}

// ── TEST 2: Empty input ───────────────────────────────────────────────────────
// In DE pipelines, empty partitions happen. We should return 0, not panic.
#[test]
fn empty_input_gives_zero_records() {
    let raw = b"";
    let batch = CsvBatch::parse(raw, b'\n');
    assert_eq!(batch.record_count(), 0);
}

// TEST 3: THE KEY DE TEST — zero-copy proof
// This test proves that CsvBatch does NOT copy the data.
// Each record slice points INTO the original buffer — same memory address.

// If you have a 500MB Parquet file in memory & you split it into rows,
// you do NOT want 500MB of extra copies. This is the same motivation behind
// Arrow's RecordBatch, but Arrow gets there differently: its columns are
// Arc<dyn Array> (shared, refcounted ownership) with their own Buffer
// slicing semantics, not a borrowed &'a [u8] into a caller-owned buffer.
// CsvBatch is a borrowed-slice lifetime exercise, not an Arrow RecordBatch.
#[test]
fn records_are_slices_into_original_buffer_not_copies() {
    let raw = b"alice,30\nbob,25";
    let batch = CsvBatch::parse(raw, b'\n');

    let first_record: &[u8] = batch.records[0];

    // Check the content is correct
    assert_eq!(first_record, b"alice,30");

    // Now check the MEMORY ADDRESS.
    // If it's a copy, the address would be different from raw.
    // If it's a zero-copy slice, the address will be INSIDE raw's range.
    let raw_start = raw.as_ptr() as usize;
    let raw_end = raw_start + raw.len();
    let rec_start = first_record.as_ptr() as usize;

    assert!(
        rec_start >= raw_start && rec_start < raw_end,
        "Expected a zero-copy slice into the buffer, but got a copy at a different address"
    );
}

// ── TEST 4: Fetch a specific row by index ─────────────────────────────────────
// `record_at` should return the right row as a readable string.
// In a pipeline, you use this to inspect or route a specific record.
#[test]
fn record_at_returns_correct_content() {
    let raw = b"alice,30\nbob,25\ncarol,35";
    let batch = CsvBatch::parse(raw, b'\n');

    assert_eq!(batch.record_at(0), Some("alice,30"));
    assert_eq!(batch.record_at(1), Some("bob,25"));
    assert_eq!(batch.record_at(2), Some("carol,35"));
}

// ── TEST 5: Out-of-bounds access returns None, not a crash ───────────────────
// In a real pipeline, index-out-of-bounds should never crash the process.
// Rust forces us to return Option — so we handle this safely by design.
#[test]
fn record_at_out_of_bounds_returns_none() {
    let raw = b"alice,30";
    let batch = CsvBatch::parse(raw, b'\n');
    assert_eq!(batch.record_at(99), None); // 99 doesn't exist — None, not panic
}

// ── TEST 6: Trailing newline is not an empty record ───────────────────────────
// Almost every real file (CSV from S3, output of spark write, etc.)
// ends with a newline character. We should NOT count that as a blank row.
#[test]
fn trailing_newline_does_not_create_empty_record() {
    let raw = b"alice,30\nbob,25\n"; // note the trailing \n
    let batch = CsvBatch::parse(raw, b'\n');
    assert_eq!(batch.record_count(), 2); // still 2, not 3
}

// ── TEST 7: Custom delimiter ──────────────────────────────────────────────────
// Real DE data comes in many formats: CSV, TSV, PSV (pipe-separated).
// The delimiter should be configurable — not hardcoded to comma or newline.
#[test]
fn pipe_separated_values_parse_correctly() {
    let raw = b"alice|engineer|berlin";
    let batch = CsvBatch::parse(raw, b'|'); // use pipe as delimiter
    assert_eq!(batch.record_count(), 3);
    assert_eq!(batch.record_at(1), Some("engineer"));
}
