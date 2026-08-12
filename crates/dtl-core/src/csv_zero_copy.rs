// WHY THIS FILE EXISTS:
// Rust does not allow a struct to hold data AND a pointer into that same data.
// If you move the struct, the pointer would point to the wrong place — a bug.
// So Rust says: "I won't allow this at all."
//
// The FIX Rust nudges you toward (the "intuitive option"):
// Let the CALLER own the data (the raw bytes).
// This struct just BORROWS a view into it — zero extra copies.
//
// This is NOT how Arrow's RecordBatch works — Arrow columns are
// Arc<dyn Array> (shared ownership, refcounted), and Arrow's own Buffer
// type has its own shared-slicing semantics, not a borrowed &'a [u8].
// CsvBatch is a borrowed-byte-segments exercise in lifetimes, not a
// miniature Arrow RecordBatch. Any real Arrow comparison is M2+ scope.

// The <'a> is called a "lifetime".
// It just means: "the slices inside CsvBatch must not outlive
// the buffer they point into."
// The compiler checks this for you — no runtime cost.
pub struct CsvBatch<'a> {
    // A list of rows. // No copy. No new allocation per row.
    pub records: Vec<&'a [u8]>,
}
impl<'a> CsvBatch<'a> {
    // file read into memory & a delimiter byte.
    // Returns a batch of zero-copy row slices.
    // `raw: &'a [u8]` means: "I borrow raw bytes, slices will
    // live as long as those bytes do."
    //
    // NOT a real CSV parser: this is a naive split on a single delimiter
    // byte, with no handling of quoted fields or delimiters embedded inside
    // quotes. It's a lifetime/borrowing exercise, not something to reuse for
    // actual CSV input (see crates/csv-cli, which uses the `csv` crate).
    pub fn parse(raw: &'a [u8], delimiter: u8) -> Self {
        let records = raw
            .split(|&b| b == delimiter) // split on delimiter (e.g. b'\n')
            .filter(|line| !line.is_empty()) // skip empty lines (trailing \n)
            .collect();
        CsvBatch { records }
    }

    // Get one row by index, as a readable string.
    // Returns Option - no panic on bad input.
    pub fn record_at(&self, index: usize) -> Option<&str> {
        self.records
            .get(index)
            .and_then(|r| std::str::from_utf8(r).ok())
    }
    pub fn record_count(&self) -> usize {
        self.records.len()
    }
}
