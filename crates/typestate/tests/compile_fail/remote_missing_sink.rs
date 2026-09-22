use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use pipeline::bounded::PipelineConfig;
use typestate::RemotePipelineBuilder;

fn identity(batch: arrow::array::RecordBatch) -> arrow::array::RecordBatch {
    batch
}

fn main() {
    let _ = RemotePipelineBuilder::new(Arc::new(InMemory::new()), PipelineConfig::default())
        .source(ObjectPath::from("input.csv"))
        .transform(identity)
        .build(&pipeline::bounded::CancellationToken::new());
}
