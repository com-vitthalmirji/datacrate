use typestate::PipelineBuilder;

fn identity(batch: arrow::array::RecordBatch) -> arrow::array::RecordBatch {
    batch
}

fn main() {
    let _ = PipelineBuilder::new()
        .transform(identity)
        .sink(identity)
        .build();
}
