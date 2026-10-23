//! Row-major and columnar batches as distinct types, so a hidden
//! row<->columnar transpose — the exact mechanism `transpose-cost.rs`
//! benchmarks, and the one behind Comet's read-only-acceleration slowdown
//! documented in `benchmark/REPORT.md` — becomes a named, visible call
//! (`to_row_major`/`to_columnar`) instead of something an operator can do
//! silently inside its own body.
//!
//! This is the same move the Scala.IO 2025 "Catalytic Compilation" talk makes
//! for Catalyst's SQL analyzer: a check that already exists (there, the
//! column resolver; here, Rust's own type checker) gets surfaced at the call
//! site instead of deferred. No phantom-type machinery is needed — two plain
//! structs are enough, because `ColumnarBatch` and `RowBatch` are already
//! incompatible types to the compiler. See `layout_type_mismatch` in
//! `tests/compile_fail/` for the proof: a function that takes `&ColumnarBatch`
//! rejects a `&RowBatch` argument at `cargo build` time.

use arrow::array::Float64Array;

/// A failure to construct a [`ColumnarBatch`] or [`RowBatch`] whose declared
/// shape doesn't match the data actually supplied.
#[derive(Debug)]
pub enum LayoutError {
    /// A column's length didn't match the batch's declared row count.
    ColumnLengthMismatch {
        /// Zero-based index of the offending column.
        column: usize,
        /// The batch's declared row count.
        expected: usize,
        /// The offending column's actual length.
        found: usize,
    },
    /// A row-major buffer's length didn't match `rows * cols`.
    BufferLengthMismatch {
        /// `rows * cols`.
        expected: usize,
        /// The buffer's actual length.
        found: usize,
    },
}

impl std::fmt::Display for LayoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ColumnLengthMismatch {
                column,
                expected,
                found,
            } => write!(
                f,
                "column {column} has length {found}, expected {expected} to match the batch's row count"
            ),
            Self::BufferLengthMismatch { expected, found } => write!(
                f,
                "row-major buffer has length {found}, expected {expected} (rows * cols)"
            ),
        }
    }
}

impl std::error::Error for LayoutError {}

/// A batch of equal-length `f64` columns (Arrow's native, zero-transpose
/// layout). Rows live are addressed as `columns[c].value(r)`.
#[derive(Debug, Clone)]
pub struct ColumnarBatch {
    columns: Vec<Float64Array>,
    rows: usize,
}

impl ColumnarBatch {
    /// Builds a batch from `columns`, each required to have length `rows`.
    /// `rows` is taken explicitly, not inferred from `columns[0]`, so a
    /// zero-column batch has an unambiguous row count instead of an
    /// undefined one.
    ///
    /// # Errors
    ///
    /// Returns [`LayoutError::ColumnLengthMismatch`] if any column's length
    /// differs from `rows`.
    pub fn try_new(columns: Vec<Float64Array>, rows: usize) -> Result<Self, LayoutError> {
        for (column, array) in columns.iter().enumerate() {
            if array.len() != rows {
                return Err(LayoutError::ColumnLengthMismatch {
                    column,
                    expected: rows,
                    found: array.len(),
                });
            }
        }
        Ok(Self { columns, rows })
    }

    /// The batch's row count.
    #[must_use]
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// The batch's column count.
    #[must_use]
    pub fn cols(&self) -> usize {
        self.columns.len()
    }

    /// Borrows the underlying columns.
    #[must_use]
    pub fn columns(&self) -> &[Float64Array] {
        &self.columns
    }

    /// Transposes this batch into row-major layout, gathering `cols` source
    /// arrays per row. This is the operation `transpose-cost.rs` measures the
    /// cost of; calling it is now the only way to obtain a [`RowBatch`] from
    /// columnar data; see the module docs for why that matters. A
    /// `Vec<Vec<f64>>` buffer was tried first and discarded: it forces one
    /// heap allocation per row regardless of column count, which swamped the
    /// transpose signal `transpose-cost.rs` is measuring.
    ///
    /// Takes `&self`, not `self`: the transpose only reads column values, so
    /// borrowing avoids forcing every caller — including a benchmark timing
    /// this exact call — to clone the source columns first just to satisfy
    /// ownership.
    #[must_use]
    pub fn to_row_major(&self) -> RowBatch {
        let cols = self.cols();
        let rows = self.rows;
        let mut buffer = vec![0.0_f64; rows * cols];
        for r in 0..rows {
            for (c, column) in self.columns.iter().enumerate() {
                buffer[r * cols + c] = column.value(r);
            }
        }
        RowBatch { buffer, rows, cols }
    }
}

/// A batch stored as one flat row-major buffer (row `r`, column `c` at
/// `r * cols + c`) — the same contiguous-allocation shape as Spark's
/// `UnsafeRow`.
#[derive(Debug, Clone)]
pub struct RowBatch {
    buffer: Vec<f64>,
    rows: usize,
    cols: usize,
}

impl RowBatch {
    /// Builds a batch from a flat `buffer`, required to have length
    /// `rows * cols`.
    ///
    /// # Errors
    ///
    /// Returns [`LayoutError::BufferLengthMismatch`] if `buffer.len() != rows * cols`.
    pub fn try_new(buffer: Vec<f64>, rows: usize, cols: usize) -> Result<Self, LayoutError> {
        let expected = rows * cols;
        if buffer.len() != expected {
            return Err(LayoutError::BufferLengthMismatch {
                expected,
                found: buffer.len(),
            });
        }
        Ok(Self { buffer, rows, cols })
    }

    /// The batch's row count.
    #[must_use]
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// The batch's column count.
    #[must_use]
    pub fn cols(&self) -> usize {
        self.cols
    }

    /// Borrows the underlying flat row-major buffer.
    #[must_use]
    pub fn buffer(&self) -> &[f64] {
        &self.buffer
    }

    /// Transposes this batch back into columnar layout. Calling this after
    /// [`ColumnarBatch::to_row_major`] is a round trip; the whole point of
    /// these two types is that such a round trip is now visible at both call
    /// sites instead of hidden inside an operator. Takes `&self` for the same
    /// reason as `to_row_major`: the transpose only reads the buffer.
    #[must_use]
    pub fn to_columnar(&self) -> ColumnarBatch {
        let rows = self.rows;
        let cols = self.cols;
        let columns = (0..cols)
            .map(|c| Float64Array::from_iter_values((0..rows).map(|r| self.buffer[r * cols + c])))
            .collect();
        ColumnarBatch { columns, rows }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_columns(rows: usize, cols: usize) -> Vec<Float64Array> {
        (0..cols)
            .map(|c| Float64Array::from_iter_values((0..rows).map(|r| (r + c) as f64)))
            .collect()
    }

    #[test]
    fn round_trip_through_row_major_preserves_values() {
        let original = sample_columns(5, 3);
        let batch = ColumnarBatch::try_new(original.clone(), 5).expect("equal-length columns");

        let round_tripped = batch.to_row_major().to_columnar();

        assert_eq!(round_tripped.rows(), 5);
        assert_eq!(round_tripped.cols(), 3);
        for (c, expected) in original.iter().enumerate() {
            assert_eq!(round_tripped.columns()[c].values(), expected.values());
        }
    }

    #[test]
    fn single_column_round_trips() {
        let original = sample_columns(4, 1);
        let batch = ColumnarBatch::try_new(original.clone(), 4).expect("equal-length columns");

        let round_tripped = batch.to_row_major().to_columnar();

        assert_eq!(round_tripped.columns()[0].values(), original[0].values());
    }

    #[test]
    fn zero_rows_round_trips_to_an_empty_buffer() {
        let batch = ColumnarBatch::try_new(sample_columns(0, 3), 0).expect("equal-length columns");

        let row_major = batch.to_row_major();
        assert_eq!(row_major.buffer().len(), 0);

        let round_tripped = row_major.to_columnar();
        assert_eq!(round_tripped.rows(), 0);
        assert_eq!(round_tripped.cols(), 3);
    }

    #[test]
    fn zero_columns_round_trips_to_an_empty_buffer() {
        let batch = ColumnarBatch::try_new(Vec::new(), 5).expect("no columns to mismatch");

        let row_major = batch.to_row_major();
        assert_eq!(row_major.buffer().len(), 0);
        assert_eq!(row_major.rows(), 5);

        let round_tripped = row_major.to_columnar();
        assert_eq!(round_tripped.rows(), 5);
        assert_eq!(round_tripped.cols(), 0);
    }

    #[test]
    fn try_new_rejects_a_column_shorter_than_the_declared_row_count() {
        let columns = vec![Float64Array::from_iter_values([1.0, 2.0])];
        let err = ColumnarBatch::try_new(columns, 3).expect_err("length 2 column vs rows=3");
        assert!(matches!(
            err,
            LayoutError::ColumnLengthMismatch {
                column: 0,
                expected: 3,
                found: 2
            }
        ));
    }

    #[test]
    fn try_new_rejects_a_buffer_whose_length_is_not_rows_times_cols() {
        let err =
            RowBatch::try_new(vec![1.0, 2.0, 3.0], 2, 2).expect_err("length 3 vs rows*cols=4");
        assert!(matches!(
            err,
            LayoutError::BufferLengthMismatch {
                expected: 4,
                found: 3
            }
        ));
    }

    #[test]
    fn gather_order_matches_row_major_addressing() {
        // columns[c].value(r) must land at buffer[r * cols + c] — assert the
        // exact addressing, not just a round trip, since a round trip alone
        // can't distinguish a correct transpose from a consistently wrong one.
        let columns = vec![
            Float64Array::from_iter_values([10.0, 11.0]),
            Float64Array::from_iter_values([20.0, 21.0]),
        ];
        let batch = ColumnarBatch::try_new(columns, 2).expect("equal-length columns");

        let row_major = batch.to_row_major();
        assert_eq!(row_major.buffer(), &[10.0, 20.0, 11.0, 21.0]);
    }
}
