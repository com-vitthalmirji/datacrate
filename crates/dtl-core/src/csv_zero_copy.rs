//! A zero-copy CSV row splitter, and the lifetime bound that makes it safe.
//!
//! Rust does not allow a struct to hold data AND a pointer into that same
//! data. If you moved the struct, the pointer would point to the wrong
//! place — a bug. So Rust says: "I won't allow this at all."
//!
//! The fix Rust nudges you toward: let the caller own the data (the raw
//! bytes). [`CsvBatch`] just borrows a view into it — zero extra copies. The
//! `'a` lifetime means "the slices inside `CsvBatch` must not outlive the
//! buffer they point into," and the compiler checks this for you at no
//! runtime cost.
//!
//! This is not how Arrow's `RecordBatch` works — Arrow columns are
//! `Arc<dyn Array>` (shared ownership, refcounted), and Arrow's own `Buffer`
//! type has its own shared-slicing semantics, not a borrowed `&'a [u8]`.
//! `CsvBatch` is a borrowed-byte-segments exercise in lifetimes, not a
//! miniature Arrow `RecordBatch`. Any real Arrow comparison is M2+ scope.

/// A batch of CSV rows, each borrowed as a byte slice from the same buffer.
///
/// # Examples
///
/// ```
/// use dtl_core::csv_zero_copy::CsvBatch;
///
/// let raw = b"a,b\nc,d";
/// let batch = CsvBatch::parse(raw, b'\n');
/// assert_eq!(batch.record_count(), 2);
/// assert_eq!(batch.record_at(0), Some("a,b"));
/// ```
pub struct CsvBatch<'a> {
    /// The rows, in input order. No copy, no new allocation per row.
    pub records: Vec<&'a [u8]>,
}

impl<'a> CsvBatch<'a> {
    /// Splits `raw` into rows on every `delimiter` byte, skipping empty rows.
    ///
    /// `raw: &'a [u8]` means the returned batch borrows `raw`, so its row
    /// slices live as long as `raw` does.
    ///
    /// This is not a real CSV parser: it's a naive split on a single
    /// delimiter byte, with no handling of quoted fields or delimiters
    /// embedded inside quotes. It's a lifetime/borrowing exercise, not
    /// something to reuse for actual CSV input (see `crates/csv-cli`, which
    /// uses the `csv` crate).
    ///
    /// # Examples
    ///
    /// ```
    /// use dtl_core::csv_zero_copy::CsvBatch;
    ///
    /// let batch = CsvBatch::parse(b"1,2\n3,4\n", b'\n');
    /// assert_eq!(batch.record_count(), 2);
    /// ```
    pub fn parse(raw: &'a [u8], delimiter: u8) -> Self {
        let records = raw
            .split(|&b| b == delimiter) // split on delimiter (e.g. b'\n')
            .filter(|line| !line.is_empty()) // skip empty lines (trailing \n)
            .collect();
        CsvBatch { records }
    }

    /// Returns the row at `index` as a UTF-8 string, or `None` if `index` is
    /// out of range or the row is not valid UTF-8.
    ///
    /// # Examples
    ///
    /// ```
    /// use dtl_core::csv_zero_copy::CsvBatch;
    ///
    /// let batch = CsvBatch::parse(b"a,b\n", b'\n');
    /// assert_eq!(batch.record_at(0), Some("a,b"));
    /// assert_eq!(batch.record_at(1), None);
    /// ```
    pub fn record_at(&self, index: usize) -> Option<&str> {
        self.records
            .get(index)
            .and_then(|r| std::str::from_utf8(r).ok())
    }

    /// Returns the number of rows in the batch.
    pub fn record_count(&self) -> usize {
        self.records.len()
    }
}
