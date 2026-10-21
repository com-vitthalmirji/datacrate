#![allow(dead_code)]
use arrow::array::{Int64Array, RecordBatch};
use contracts::{Contract, arrow::schema};
use std::sync::Arc;
use typestate::contract_pipeline::{CallbackError, ContractPipeline};
#[derive(Contract)]
struct Bronze {
    id: i64,
}
#[derive(Contract)]
struct Silver {
    id: i64,
}
fn batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(schema::<Bronze>().expect("schema")),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .expect("batch")
}
#[test]
fn matching_nominally_distinct_stages_run_in_order() {
    let mut seen = 0;
    let plan = ContractPipeline::source::<Bronze, _>(|| Ok(std::iter::once(Ok(batch()))))
        .transform::<Silver, Silver>(Ok)
        .sink::<Bronze>(|batch| {
            seen += batch.num_rows();
            Ok(())
        })
        .build()
        .expect("prepare");
    let report = plan.run().expect("run");
    assert_eq!(seen, 2);
    assert_eq!(report.rows_processed, 2);
    assert_eq!(report.batches_processed, 1);
}

use std::{
    cell::{Cell, RefCell},
    error::Error,
    rc::Rc,
};
use typestate::contract_pipeline::{Failure, Stage};
#[derive(Contract)]
struct Gold {
    value: String,
}
#[derive(Contract)]
struct Unsupported {
    value: char,
}
fn problem() -> CallbackError {
    Box::new(std::io::Error::other("original failure"))
}
#[test]
fn schema_changing_chain_and_source_are_lazy_and_ordered() {
    let events = Rc::new(RefCell::new(Vec::new()));
    let source = events.clone();
    let first = events.clone();
    let second = events.clone();
    let sink = events.clone();
    let plan = ContractPipeline::source::<Bronze, _>(move || {
        source.borrow_mut().push("source");
        Ok(std::iter::once(Ok(batch())))
    })
    .transform::<Silver, Gold>(move |_| {
        first.borrow_mut().push("first");
        Ok(RecordBatch::try_new(
            Arc::new(schema::<Gold>()?),
            vec![Arc::new(arrow::array::StringArray::from(vec!["a", "b"]))],
        )?)
    })
    .transform::<Gold, Gold>(move |b| {
        second.borrow_mut().push("second");
        Ok(b.slice(1, 1))
    })
    .sink::<Gold>(move |b| {
        sink.borrow_mut().push("sink");
        assert_eq!(b.schema().field(0).name(), "value");
        assert_eq!(b.num_rows(), 1);
        Ok(())
    })
    .build()
    .expect("prepare");
    assert!(events.borrow().is_empty());
    let report = plan.run().expect("run");
    assert_eq!(*events.borrow(), ["source", "first", "second", "sink"]);
    assert_eq!(report.rows_processed, 1);
}
#[test]
fn empty_input_and_empty_batch_are_distinct_successes() {
    let empty = ContractPipeline::source::<Bronze, _>(|| Ok(std::iter::empty()))
        .sink::<Silver>(|_| panic!("no batch"))
        .build()
        .expect("prepare")
        .run()
        .expect("run");
    assert_eq!(empty.batches_processed, 0);
    let empty_batch = RecordBatch::new_empty(Arc::new(schema::<Bronze>().expect("schema")));
    let report = ContractPipeline::source::<Bronze, _>(|| Ok(std::iter::once(Ok(empty_batch))))
        .sink::<Silver>(|b| {
            assert_eq!(b.num_rows(), 0);
            Ok(())
        })
        .build()
        .expect("prepare")
        .run()
        .expect("run");
    assert_eq!((report.batches_processed, report.rows_processed), (1, 0));
}
#[test]
fn all_mappings_are_preflighted_before_source_io() {
    let opened = Cell::new(false);
    let result = ContractPipeline::source::<Unsupported, _>(|| {
        opened.set(true);
        Ok(std::iter::empty())
    })
    .sink::<Unsupported>(|_| Ok(()))
    .build();
    let e = result.err().expect("unsupported source");
    assert_eq!(e.stage, Stage::Source);
    assert_eq!(e.batch_index, None);
    assert!(matches!(e.failure, Failure::Mapping(_)));
    assert!(!opened.get());
    let result = ContractPipeline::source::<Bronze, _>(|| {
        opened.set(true);
        Ok(std::iter::empty())
    })
    .transform::<Silver, Unsupported>(|_| panic!("preflight"))
    .sink::<Unsupported>(|_| Ok(()))
    .build();
    let e = result.err().expect("unsupported transform");
    assert_eq!(e.stage, Stage::Transform(0));
    assert!(!opened.get());
}
#[test]
fn lying_source_and_transform_never_reach_downstream() {
    let calls = Cell::new(0);
    let wrong = RecordBatch::new_empty(Arc::new(schema::<Gold>().expect("schema")));
    let e = ContractPipeline::source::<Bronze, _>(|| Ok(std::iter::once(Ok(wrong))))
        .transform::<Silver, Silver>(|b| {
            calls.set(calls.get() + 1);
            Ok(b)
        })
        .sink::<Bronze>(|_| {
            calls.set(calls.get() + 1);
            Ok(())
        })
        .build()
        .expect("prepare")
        .run()
        .expect_err("lying source");
    assert_eq!((e.stage, e.batch_index), (Stage::Source, Some(0)));
    assert_eq!(calls.get(), 0);
    assert!(matches!(e.failure, Failure::Validation(_)));
    let e = ContractPipeline::source::<Bronze, _>(|| Ok(std::iter::once(Ok(batch()))))
        .transform::<Silver, Gold>(Ok)
        .transform::<Gold, Gold>(|b| {
            calls.set(calls.get() + 1);
            Ok(b)
        })
        .sink::<Gold>(|_| {
            calls.set(calls.get() + 1);
            Ok(())
        })
        .build()
        .expect("prepare")
        .run()
        .expect_err("lying transform");
    assert_eq!((e.stage, e.batch_index), (Stage::Transform(0), Some(0)));
    assert_eq!(calls.get(), 0);
    assert!(matches!(e.failure, Failure::Validation(_)));
}
#[test]
fn preserves_original_factory_iterator_transform_and_sink_errors() {
    for failing in [
        Stage::SourceFactory,
        Stage::Source,
        Stage::Transform(0),
        Stage::Sink,
    ] {
        let downstream = Cell::new(0);
        let e = ContractPipeline::source::<Bronze, _>(|| {
            if failing == Stage::SourceFactory {
                return Err(problem());
            }
            Ok(std::iter::once(if failing == Stage::Source {
                Err(problem())
            } else {
                Ok(batch())
            }))
        })
        .transform::<Silver, Silver>(|b| {
            if failing == Stage::Transform(0) {
                return Err(problem());
            }
            Ok(b)
        })
        .sink::<Bronze>(|_| {
            if failing == Stage::Sink {
                return Err(problem());
            }
            downstream.set(downstream.get() + 1);
            Ok(())
        })
        .build()
        .expect("prepare")
        .run()
        .expect_err("stage error");
        assert_eq!(e.stage, failing);
        assert_eq!(
            e.batch_index,
            if failing == Stage::SourceFactory {
                None
            } else {
                Some(0)
            }
        );
        assert!(
            e.source()
                .expect("source")
                .downcast_ref::<std::io::Error>()
                .is_some()
        );
        assert_eq!(e.source().expect("source").to_string(), "original failure");
        assert_eq!(downstream.get(), 0);
    }
}
struct TrackedInput {
    index: usize,
    dropped: Rc<Cell<bool>>,
    reads: Rc<Cell<usize>>,
    sinks: Rc<Cell<usize>>,
}
impl Iterator for TrackedInput {
    type Item = Result<RecordBatch, CallbackError>;
    fn next(&mut self) -> Option<Self::Item> {
        assert_eq!(
            self.reads.get(),
            self.sinks.get(),
            "source must not be collected ahead of sink"
        );
        self.reads.set(self.reads.get() + 1);
        self.index += 1;
        Some(Ok(if self.index == 1 {
            batch()
        } else {
            RecordBatch::new_empty(Arc::new(schema::<Gold>().expect("schema")))
        }))
    }
}
impl Drop for TrackedInput {
    fn drop(&mut self) {
        self.dropped.set(true);
    }
}
#[test]
fn later_invalid_batch_stops_reads_and_drops_iterator_without_undoing_prior_sink() {
    let dropped = Rc::new(Cell::new(false));
    let reads = Rc::new(Cell::new(0));
    let sinks = Rc::new(Cell::new(0));
    let input = TrackedInput {
        index: 0,
        dropped: dropped.clone(),
        reads: reads.clone(),
        sinks: sinks.clone(),
    };
    let e = ContractPipeline::source::<Bronze, _>(|| Ok(input))
        .sink::<Silver>(|_| {
            sinks.set(sinks.get() + 1);
            Ok(())
        })
        .build()
        .expect("prepare")
        .run()
        .expect_err("second batch");
    assert_eq!((e.stage, e.batch_index), (Stage::Source, Some(1)));
    assert_eq!(sinks.get(), 1);
    assert_eq!(reads.get(), 2);
    assert!(dropped.get());
}
#[test]
fn later_invalid_transform_stops_before_the_next_sink_call() {
    let calls = Cell::new(0);
    let sinks = Cell::new(0);
    let e =
        ContractPipeline::source::<Bronze, _>(|| Ok(vec![Ok(batch()), Ok(batch())].into_iter()))
            .transform::<Silver, Silver>(|batch| {
                calls.set(calls.get() + 1);
                if calls.get() == 2 {
                    Ok(RecordBatch::new_empty(Arc::new(
                        schema::<Gold>().expect("schema"),
                    )))
                } else {
                    Ok(batch)
                }
            })
            .sink::<Bronze>(|_| {
                sinks.set(sinks.get() + 1);
                Ok(())
            })
            .build()
            .expect("prepare")
            .run()
            .expect_err("second transform output");
    assert_eq!((e.stage, e.batch_index), (Stage::Transform(0), Some(1)));
    assert!(matches!(e.failure, Failure::Validation(_)));
    assert_eq!(calls.get(), 2);
    assert_eq!(sinks.get(), 1);
}
#[derive(Contract)]
struct Empty {}
#[test]
fn report_overflow_fails_before_the_next_sink_action() {
    use arrow::array::RecordBatchOptions;
    let make = |rows| {
        RecordBatch::try_new_with_options(
            Arc::new(schema::<Empty>().expect("schema")),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .expect("zero-column batch")
    };
    let calls = Cell::new(0);
    let e = ContractPipeline::source::<Empty, _>(|| {
        Ok(vec![Ok(make(usize::MAX)), Ok(make(1))].into_iter())
    })
    .sink::<Empty>(|_| {
        calls.set(calls.get() + 1);
        Ok(())
    })
    .build()
    .expect("prepare")
    .run()
    .expect_err("counter overflow");
    assert!(matches!(e.failure, Failure::CountOverflow));
    assert_eq!((e.stage, e.batch_index), (Stage::Sink, Some(1)));
    assert_eq!(calls.get(), 1);
}
