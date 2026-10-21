use typestate::PipelineBuilder;

fn identity(batch: arrow::array::RecordBatch) -> Result<arrow::array::RecordBatch, pipeline::PipelineIoError> {
    Ok(batch)
}

fn main() {
    let _ = PipelineBuilder::new()
        .transform(identity)
        .sink(identity)
        .build();
}
