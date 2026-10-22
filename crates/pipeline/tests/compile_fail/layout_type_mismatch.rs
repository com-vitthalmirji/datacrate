use pipeline::layout::{ColumnarBatch, RowBatch};

fn requires_columnar(_batch: &ColumnarBatch) {}

fn main() {
    let row_batch = RowBatch::try_new(vec![1.0, 2.0], 1, 2).unwrap();
    requires_columnar(&row_batch);
}
