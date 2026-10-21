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

#[test]
fn byte_admission_pipeline_preserves_results_and_reports_retained_charge() {
    use std::num::NonZeroUsize;
    for capacity in [0, 1, 4] {
        let dir = tempfile::tempdir().expect("temporary directory");
        let input = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/m1/headers.csv");
        let output = dir.path().join("output.parquet");
        let config = PipelineConfig {
            batch_size: NonZeroUsize::new(1).expect("positive"),
            channel_capacity: capacity,
            max_admitted_bytes: NonZeroUsize::new(16_384),
            ..PipelineConfig::default()
        };
        let report =
            run_bounded_pipeline(&input, &output, &config, None, &CancellationToken::new())
                .expect("bounded budget");
        let actual = pipeline::read_parquet(&output).expect("published output");
        assert_eq!(
            actual,
            pipeline::fixture_to_record_batch(&input).expect("reference")
        );
        assert_eq!(report.rows_written, 3);
        assert_eq!(report.admitted_bytes, Some(0));
        assert!(
            report
                .peak_admitted_bytes
                .is_some_and(|bytes| bytes > 0 && bytes <= 16_384)
        );
    }
}

#[test]
fn byte_admission_producer_error_cannot_publish_partial_output() {
    use pipeline::bounded::PipelineError;
    let dir = tempfile::tempdir().expect("temporary directory");
    let output = dir.path().join("output.parquet");
    std::fs::write(&output, b"previous").expect("old output");
    let config = PipelineConfig {
        max_admitted_bytes: std::num::NonZeroUsize::new(1),
        ..PipelineConfig::default()
    };
    let input = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/m1/headers.csv");
    assert!(matches!(
        run_bounded_pipeline(&input, &output, &config, None, &CancellationToken::new()),
        Err(PipelineError::BatchTooLarge { .. })
    ));
    assert_eq!(std::fs::read(output).expect("old output"), b"previous");
    assert_eq!(
        std::fs::read_dir(dir.path())
            .expect("output directory")
            .count(),
        1
    );
}

#[test]
fn byte_admission_late_producer_error_discards_already_processed_rows() {
    use pipeline::bounded::PipelineError;
    let dir = tempfile::tempdir().expect("temporary directory");
    let input = dir.path().join("input.csv");
    std::fs::write(
        &input,
        format!("id,name,note\n1,small,\n2,{},\n", "x".repeat(50_000)),
    )
    .expect("input");
    let output = dir.path().join("output.parquet");
    std::fs::write(&output, b"previous").expect("old output");
    let config = PipelineConfig {
        batch_size: std::num::NonZeroUsize::new(1).expect("positive"),
        channel_capacity: 0,
        max_admitted_bytes: std::num::NonZeroUsize::new(4096),
        ..PipelineConfig::default()
    };
    let observed = std::cell::Cell::new(0);
    let transform = |batch| {
        observed.set(observed.get() + 1);
        Ok(batch)
    };
    let result = run_bounded_pipeline(
        &input,
        &output,
        &config,
        Some(&transform),
        &CancellationToken::new(),
    );
    assert!(matches!(result, Err(PipelineError::BatchTooLarge { .. })));
    assert_eq!(
        observed.get(),
        1,
        "the first admitted batch reached consumer processing"
    );
    assert_eq!(std::fs::read(output).expect("old output"), b"previous");
    assert_eq!(std::fs::read_dir(dir.path()).expect("files").count(), 2);
}

#[test]
fn byte_admission_callback_error_releases_budget_and_keeps_source() {
    use pipeline::bounded::PipelineError;
    use pipeline::{CallbackStage, PipelineIoError};
    for capacity in [0, 1] {
        let dir = tempfile::tempdir().expect("temporary directory");
        let config = PipelineConfig {
            channel_capacity: capacity,
            max_admitted_bytes: std::num::NonZeroUsize::new(16_384),
            ..PipelineConfig::default()
        };
        let input = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/m1/headers.csv");
        let transform = |_| {
            Err(PipelineIoError::InvalidId {
                row: 7,
                value: "callback".into(),
            })
        };
        assert!(matches!(
            run_bounded_pipeline(
                &input,
                &dir.path().join("output"),
                &config,
                Some(&transform),
                &CancellationToken::new()
            ),
            Err(PipelineError::Io(PipelineIoError::Callback {
                stage: CallbackStage::Transform,
                batch_index: 0,
                ..
            }))
        ));
        assert_eq!(
            std::fs::read_dir(dir.path())
                .expect("output directory")
                .count(),
            0
        );
    }
}

#[test]
fn runtime_contract_rejects_malformed_callback_shapes_with_typed_errors() {
    use arrow::array::UInt64Array;
    use pipeline::bounded::PipelineError;
    use pipeline::{BatchViolation, PipelineIoError};
    for shape in 0..3 {
        for budget in [None, std::num::NonZeroUsize::new(16_384)] {
            let dir = tempfile::tempdir().expect("directory");
            let output = dir.path().join("output");
            std::fs::write(&output, b"old").expect("old output");
            let transform = |batch: RecordBatch| {
                if shape == 0 {
                    return Ok(batch.project(&[0, 1]).expect("two columns"));
                }
                let fields = vec![
                    Field::new(
                        "id",
                        if shape == 1 {
                            DataType::UInt64
                        } else {
                            DataType::Int64
                        },
                        shape == 2,
                    ),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("note", DataType::Utf8, true),
                ];
                let mut columns = batch.columns().to_vec();
                if shape == 1 {
                    columns[0] = Arc::new(UInt64Array::from(vec![1; batch.num_rows()]));
                }
                Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                    .expect("safely constructible drift"))
            };
            let input = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/m1/headers.csv");
            let config = PipelineConfig {
                max_admitted_bytes: budget,
                ..PipelineConfig::default()
            };
            let error = run_bounded_pipeline(
                &input,
                &output,
                &config,
                Some(&transform),
                &CancellationToken::new(),
            )
            .expect_err("reject callback drift");
            match (shape, error) {
                (
                    0,
                    PipelineError::Io(PipelineIoError::InvalidBatch {
                        violation: BatchViolation::ColumnCount { .. },
                    }),
                )
                | (
                    1,
                    PipelineError::Io(PipelineIoError::InvalidBatch {
                        violation: BatchViolation::DataType { index: 0, .. },
                    }),
                )
                | (
                    2,
                    PipelineError::Io(PipelineIoError::InvalidBatch {
                        violation: BatchViolation::Nullability { index: 0, .. },
                    }),
                ) => (),
                (_, error) => {
                    panic!("expected contract rejection before writer/digest, got {error:?}")
                }
            }
            assert_eq!(std::fs::read(output).expect("previous output"), b"old");
            assert_eq!(std::fs::read_dir(dir.path()).expect("directory").count(), 1);
        }
    }
}
