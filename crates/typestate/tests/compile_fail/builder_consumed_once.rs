use typestate::PipelineBuilder;
fn main() {
    let builder = PipelineBuilder::new()
        .source("unused.csv".into())
        .transform(|batch| batch)
        .sink(|batch| batch);
    let _ = builder.build();
    let _ = builder.build();
}
