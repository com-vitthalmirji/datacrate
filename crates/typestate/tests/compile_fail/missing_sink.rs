use std::path::PathBuf;

use typestate::PipelineBuilder;

fn identity(batch: arrow::array::RecordBatch) -> arrow::array::RecordBatch {
    batch
}

fn main() {
    let _ = PipelineBuilder::new()
        .source(PathBuf::from("fixtures/m1/headers.csv"))
        .transform(identity)
        .build();
}
