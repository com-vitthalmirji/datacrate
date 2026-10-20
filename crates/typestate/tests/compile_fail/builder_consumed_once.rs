use typestate::PipelineBuilder;
fn main() {
    let builder = PipelineBuilder::new()
        .source("unused.csv".into())
        .transform(Ok)
        .sink(Ok);
    let _ = builder.build();
    let _ = builder.build();
}
