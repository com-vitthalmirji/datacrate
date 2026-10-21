use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use pipeline::bounded::{CancellationToken, PipelineConfig, run_bounded_pipeline};

#[test]
fn runtime_contract_rejects_renamed_fields_before_publication() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let output = dir.path().join("output.parquet");
    std::fs::write(&output, b"previous output").expect("previous output");
    let transform = |batch: RecordBatch| {
        let schema = Schema::new(vec![
            Field::new("other_id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("note", DataType::Utf8, true),
        ]);
        Ok(
            RecordBatch::try_new(Arc::new(schema), batch.columns().to_vec())
                .expect("renamed batch"),
        )
    };
    let result = run_bounded_pipeline(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/m1/headers.csv"),
        &output,
        &PipelineConfig::default(),
        Some(&transform),
        &CancellationToken::new(),
    );
    let error = result.expect_err("renamed output must be rejected");
    assert!(
        error.to_string().contains("invalid ingest batch"),
        "{error}"
    );
    assert_eq!(
        std::fs::read(output).expect("old output"),
        b"previous output"
    );
}

#[test]
fn runtime_contract_arrow_rejects_nulls_in_nonnullable_columns() {
    let result = RecordBatch::try_new(
        pipeline::schema(),
        vec![
            Arc::new(Int64Array::from(vec![None])),
            Arc::new(StringArray::from(vec![Some("name")])),
            Arc::new(StringArray::from(vec![None::<&str>])),
        ],
    );
    assert!(
        result.is_err(),
        "safe Arrow construction enforces nonnullable arrays"
    );
}

fn valid_batch() -> RecordBatch {
    RecordBatch::try_new(
        pipeline::schema(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec![Some("name")])),
            Arc::new(StringArray::from(vec![None::<&str>])),
        ],
    )
    .expect("valid batch")
}

#[test]
fn runtime_contract_rejects_column_count_and_order() {
    use pipeline::{BatchViolation, PipelineIoError, validate_ingest_batch};
    let batch = valid_batch();
    assert!(matches!(
        validate_ingest_batch(&batch.project(&[0, 1]).expect("two columns")),
        Err(PipelineIoError::InvalidBatch {
            violation: BatchViolation::ColumnCount {
                expected: 3,
                found: 2
            }
        })
    ));
    assert!(matches!(
        validate_ingest_batch(&batch.project(&[1, 0, 2]).expect("reordered")),
        Err(PipelineIoError::InvalidBatch {
            violation: BatchViolation::FieldName { index: 0, .. }
        })
    ));
}

#[test]
fn runtime_contract_rejects_wrong_logical_type() {
    use arrow::array::UInt64Array;
    use pipeline::{BatchViolation, PipelineIoError, validate_ingest_batch};
    let batch = valid_batch();
    let schema = Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("note", DataType::Utf8, true),
    ]);
    let wrong = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt64Array::from(vec![1])),
            Arc::clone(batch.column(1)),
            Arc::clone(batch.column(2)),
        ],
    )
    .expect("wrong type batch");
    assert!(matches!(
        validate_ingest_batch(&wrong),
        Err(PipelineIoError::InvalidBatch {
            violation: BatchViolation::DataType { index: 0, .. }
        })
    ));
}

#[test]
fn runtime_contract_rejects_different_declared_nullability() {
    use pipeline::{BatchViolation, PipelineIoError, validate_ingest_batch};
    let batch = valid_batch();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, false),
        Field::new("note", DataType::Utf8, true),
    ]);
    let wrong =
        RecordBatch::try_new(Arc::new(schema), batch.columns().to_vec()).expect("nullable id");
    assert!(matches!(
        validate_ingest_batch(&wrong),
        Err(PipelineIoError::InvalidBatch {
            violation: BatchViolation::Nullability {
                index: 0,
                expected: false,
                found: true
            }
        })
    ));
}

#[test]
fn runtime_contract_accepts_empty_batches_and_ignores_uninterpreted_metadata() {
    use pipeline::validate_ingest_batch;
    validate_ingest_batch(&RecordBatch::new_empty(pipeline::schema())).expect("empty batch");
    let batch = valid_batch();
    let schema = batch.schema().as_ref().clone().with_metadata(
        [("source".to_owned(), "fixture".to_owned())]
            .into_iter()
            .collect(),
    );
    let with_metadata =
        RecordBatch::try_new(Arc::new(schema), batch.columns().to_vec()).expect("metadata");
    validate_ingest_batch(&with_metadata).expect("metadata is not part of ingest contract");
}

#[test]
fn callback_error_releases_waiting_producer_and_preserves_context() {
    use pipeline::bounded::PipelineError;
    use pipeline::{CallbackStage, PipelineIoError};
    use std::num::NonZeroUsize;
    use std::sync::mpsc;
    use std::time::Duration;
    for capacity in [0, 1] {
        let dir = tempfile::tempdir().expect("temporary directory");
        let output = dir.path().join("output.parquet");
        std::fs::write(&output, b"previous output").expect("previous output");
        let input = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/m1/headers.csv");
        let destination = output.clone();
        let (sent, received) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let calls = std::cell::Cell::new(0);
            let transform = |batch| {
                let index = calls.get();
                calls.set(index + 1);
                if index == 1 {
                    Err(PipelineIoError::InvalidId {
                        row: 7,
                        value: "callback input".into(),
                    })
                } else {
                    Ok(batch)
                }
            };
            let config = PipelineConfig {
                batch_size: NonZeroUsize::new(1).expect("positive"),
                channel_capacity: capacity,
                ..PipelineConfig::default()
            };
            let result = run_bounded_pipeline(
                &input,
                &destination,
                &config,
                Some(&transform),
                &CancellationToken::new(),
            );
            sent.send(result).expect("result receiver");
        });
        let result = received
            .recv_timeout(Duration::from_secs(5))
            .expect("producer must terminate after consumer error");
        worker.join().expect("pipeline thread");
        match result {
            Err(PipelineError::Io(PipelineIoError::Callback {
                stage: CallbackStage::Transform,
                batch_index: 1,
                source,
            })) => {
                assert!(matches!(*source, PipelineIoError::InvalidId { row: 7, .. }));
            }
            other => panic!("unexpected result: {other:?}"),
        }
        assert_eq!(
            std::fs::read(&output).expect("old output"),
            b"previous output"
        );
        assert_eq!(std::fs::read_dir(dir.path()).expect("outputs").count(), 1);
    }
}
