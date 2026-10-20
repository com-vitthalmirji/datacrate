use std::cell::Cell;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use arrow::array::RecordBatch;
use pipeline::PipelineIoError;
use typestate::PipelineBuilder;

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/m1/headers.csv")
}

#[test]
fn callback_error_preserves_local_transform_source() {
    let result = PipelineBuilder::new()
        .source(fixture())
        .transform(|_| {
            Err(PipelineIoError::InvalidId {
                row: 7,
                value: "bad".into(),
            })
        })
        .sink(Ok)
        .build();
    let error = result.expect_err("transform error");
    assert!(
        error.to_string().contains("transform callback at batch 0"),
        "{error}"
    );
    assert!(
        std::error::Error::source(&error)
            .expect("original source")
            .to_string()
            .contains("row 7")
    );
}

#[test]
fn callback_error_preserves_local_sink_source() {
    let result = PipelineBuilder::new()
        .source(fixture())
        .transform(Ok)
        .sink(|_| {
            Err(PipelineIoError::InvalidId {
                row: 9,
                value: "bad sink".into(),
            })
        })
        .build();
    let error = result.expect_err("sink error");
    assert!(
        error.to_string().contains("sink callback at batch 0"),
        "{error}"
    );
    assert!(
        std::error::Error::source(&error)
            .expect("original source")
            .to_string()
            .contains("row 9")
    );
}

#[test]
fn invalid_transform_output_never_reaches_local_sink() {
    let called = Rc::new(Cell::new(false));
    let observed = Rc::clone(&called);
    let result = PipelineBuilder::new()
        .source(fixture())
        .transform(|_| {
            Ok(RecordBatch::new_empty(std::sync::Arc::new(
                arrow::datatypes::Schema::empty(),
            )))
        })
        .sink(move |batch| {
            observed.set(true);
            Ok(batch)
        })
        .build();
    assert!(result.is_err());
    assert!(!called.get());
}

#[test]
fn invalid_sink_output_is_not_returned_as_success() {
    let result = PipelineBuilder::new()
        .source(fixture())
        .transform(Ok)
        .sink(|_| {
            Ok(RecordBatch::new_empty(std::sync::Arc::new(
                arrow::datatypes::Schema::empty(),
            )))
        })
        .build();
    assert!(matches!(result, Err(PipelineIoError::InvalidBatch { .. })));
}

#[test]
fn remote_callback_error_is_reported_without_publishing_output() {
    use object_store::{ObjectStoreExt, memory::InMemory, path::Path as ObjectPath};
    use pipeline::{
        CallbackStage,
        bounded::{CancellationToken, PipelineConfig, PipelineError},
    };
    use std::{num::NonZeroUsize, sync::Arc};
    use typestate::RemotePipelineBuilder;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let store = Arc::new(InMemory::new());
    let input = ObjectPath::from("input.csv");
    runtime
        .block_on(store.put(&input, std::fs::read(fixture()).expect("fixture").into()))
        .expect("seed");
    let config = PipelineConfig {
        batch_size: NonZeroUsize::new(1).expect("positive"),
        ..PipelineConfig::default()
    };
    let calls = Cell::new(0);
    let result = RemotePipelineBuilder::new(store.clone(), config)
        .source(input)
        .transform(move |batch| {
            let index = calls.get();
            calls.set(index + 1);
            if index == 1 {
                Err(PipelineIoError::InvalidId {
                    row: 17,
                    value: "remote callback".into(),
                })
            } else {
                Ok(batch)
            }
        })
        .sink(ObjectPath::from("output.parquet"))
        .build(&CancellationToken::new());
    assert!(matches!(
        result,
        Err(PipelineError::Io(PipelineIoError::Callback {
            stage: CallbackStage::Transform,
            batch_index: 1,
            ..
        }))
    ));
    assert!(
        runtime
            .block_on(store.head(&ObjectPath::from("output.parquet")))
            .is_err()
    );
    assert!(
        runtime
            .block_on(store.head(&ObjectPath::from("output.parquet.manifest.json")))
            .is_err()
    );
}
