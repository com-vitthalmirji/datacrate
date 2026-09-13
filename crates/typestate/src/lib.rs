//! Typestate pipeline builder.
//!
//! `build()` only exists once a source, a transform, and a sink have all
//! been supplied — an incomplete pipeline is a compile error, not a runtime
//! panic. Each required stage is tracked by its own type parameter (rather
//! than one combined "pipeline state" type), so adding a stage narrows that
//! parameter from [`Missing`] to [`Present`] independently of the others.

#![warn(missing_docs)]

use std::path::PathBuf;

use arrow::array::RecordBatch;

/// Marks a builder stage that has not been supplied yet.
#[derive(Debug, Default)]
pub struct Missing;

/// Marks a builder stage that has been supplied.
#[derive(Debug)]
pub struct Present<T>(T);

/// A batch-to-batch transform applied after the source is read.
///
/// Boxed rather than a bare `fn` pointer so a caller can supply a capturing
/// closure (e.g. a transform parameterized by a column name or threshold
/// read from config) — a plain `fn(RecordBatch) -> RecordBatch` can only
/// ever be a free function, which is too narrow for any transform that
/// needs state from its call site.
pub type Transform = Box<dyn Fn(RecordBatch) -> RecordBatch>;

/// A batch-to-batch step applied last, before the pipeline returns.
///
/// Boxed for the same reason as [`Transform`]: capturing closures, not just
/// free functions, need to be usable as a sink.
pub type Sink = Box<dyn Fn(RecordBatch) -> RecordBatch>;

/// Builds a pipeline that reads a CSV fixture, applies a transform, then a
/// sink. `Src`, `Xf`, and `Snk` each track whether their stage has been
/// supplied; `build()` is only defined for `PipelineBuilder<Present<_>,
/// Present<_>, Present<_>>`.
#[derive(Debug, Default)]
pub struct PipelineBuilder<Src, Xf, Snk> {
    source: Src,
    transform: Xf,
    sink: Snk,
}

impl PipelineBuilder<Missing, Missing, Missing> {
    /// Starts a new, empty pipeline builder.
    #[must_use]
    pub fn new() -> Self {
        PipelineBuilder {
            source: Missing,
            transform: Missing,
            sink: Missing,
        }
    }
}

impl<Xf, Snk> PipelineBuilder<Missing, Xf, Snk> {
    /// Supplies the CSV fixture path this pipeline reads from.
    #[must_use]
    pub fn source(self, path: PathBuf) -> PipelineBuilder<Present<PathBuf>, Xf, Snk> {
        PipelineBuilder {
            source: Present(path),
            transform: self.transform,
            sink: self.sink,
        }
    }
}

impl<Src, Snk> PipelineBuilder<Src, Missing, Snk> {
    /// Supplies the transform applied to the batch read from the source.
    ///
    /// Accepts any `Fn(RecordBatch) -> RecordBatch`, not just free
    /// functions — a closure that captures its call site's state (e.g. a
    /// column name) works here too.
    #[must_use]
    pub fn transform<F>(self, f: F) -> PipelineBuilder<Src, Present<Transform>, Snk>
    where
        F: Fn(RecordBatch) -> RecordBatch + 'static,
    {
        PipelineBuilder {
            source: self.source,
            transform: Present(Box::new(f)),
            sink: self.sink,
        }
    }
}

impl<Src, Xf> PipelineBuilder<Src, Xf, Missing> {
    /// Supplies the sink applied to the transformed batch before it's returned.
    ///
    /// Accepts any `Fn(RecordBatch) -> RecordBatch`, not just free
    /// functions — see [`PipelineBuilder::transform`].
    #[must_use]
    pub fn sink<F>(self, f: F) -> PipelineBuilder<Src, Xf, Present<Sink>>
    where
        F: Fn(RecordBatch) -> RecordBatch + 'static,
    {
        PipelineBuilder {
            source: self.source,
            transform: self.transform,
            sink: Present(Box::new(f)),
        }
    }
}

impl PipelineBuilder<Present<PathBuf>, Present<Transform>, Present<Sink>> {
    /// Runs the pipeline: reads the CSV fixture, applies the transform, then
    /// the sink.
    ///
    /// # Errors
    ///
    /// Returns [`pipeline::PipelineIoError`] if the fixture cannot be read or
    /// parsed into a `RecordBatch`.
    pub fn build(self) -> Result<RecordBatch, pipeline::PipelineIoError> {
        let batch = pipeline::fixture_to_record_batch(&self.source.0)?;
        let batch = (self.transform.0)(batch);
        Ok((self.sink.0)(batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_path(name: &str) -> PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/m1")
            .join(name)
    }

    fn identity(batch: RecordBatch) -> RecordBatch {
        batch
    }

    #[test]
    fn runs_a_complete_pipeline_over_a_fixture() {
        let batch = PipelineBuilder::new()
            .source(fixture_path("headers.csv"))
            .transform(identity)
            .sink(identity)
            .build()
            .expect("fixture should convert cleanly");

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 3);
    }

    #[test]
    fn stages_can_be_supplied_in_any_order() {
        let batch = PipelineBuilder::new()
            .transform(identity)
            .sink(identity)
            .source(fixture_path("headers.csv"))
            .build()
            .expect("fixture should convert cleanly");

        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn transform_accepts_a_capturing_closure() {
        // Regression test: Transform/Sink used to be bare `fn` pointers,
        // which reject any closure that captures its call site's state.
        let expected_rows = 3usize;
        let batch = PipelineBuilder::new()
            .source(fixture_path("headers.csv"))
            .transform(move |batch| {
                assert_eq!(batch.num_rows(), expected_rows);
                batch
            })
            .sink(identity)
            .build()
            .expect("fixture should convert cleanly");

        assert_eq!(batch.num_rows(), expected_rows);
    }

    #[test]
    fn compile_fail_incomplete_builders_have_no_build_method() {
        let t = trybuild::TestCases::new();
        t.compile_fail("tests/compile_fail/*.rs");
    }
}
