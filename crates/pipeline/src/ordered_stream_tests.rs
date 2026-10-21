//! Ordered streams may spill before returning output; consumer pacing is separate.
use crate::datafusion_query::{context_with_memory_limit, orders_schema, row_query_plan};
use arrow::array::{
    Decimal128Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use datafusion::datasource::MemTable;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures_util::TryStreamExt;
use std::sync::Arc;
use std::time::Duration;

fn spill_count(plan: &Arc<dyn ExecutionPlan>) -> usize {
    plan.metrics()
        .and_then(|metrics| metrics.spill_count())
        .unwrap_or(0)
        + plan.children().into_iter().map(spill_count).sum::<usize>()
}

#[tokio::test]
async fn streamed_row_query_forced_sort_spill_releases_files_after_early_drop() {
    let directory = tempfile::tempdir().expect("spill directory");
    let base = context_with_memory_limit(2 * 1024 * 1024, directory.path()).expect("runtime");
    let mut config = base.copied_config().with_batch_size(256);
    config.options_mut().execution.sort_spill_reservation_bytes = 64 * 1024;
    config.options_mut().execution.sort_in_place_threshold_bytes = 64 * 1024;
    let ctx = SessionContext::new_with_config_rt(config, base.runtime_env());
    let total = 65_536i64;
    let batches = (0..64)
        .map(|chunk| {
            let start = chunk * 1024i64;
            let ids = start..start + 1024;
            RecordBatch::try_new(
                orders_schema(),
                vec![
                    Arc::new(Int64Array::from_iter_values(ids.clone())),
                    Arc::new(
                        Decimal128Array::from_iter_values(ids.clone().map(|_| 6000))
                            .with_precision_and_scale(10, 2)
                            .expect("decimal"),
                    ),
                    Arc::new(TimestampMicrosecondArray::from_iter_values(
                        ids.clone().map(|id| total - id),
                    )),
                    Arc::new(StringArray::from_iter_values(
                        ids.map(|id| format!("{id:06}-{}", "n".repeat(128))),
                    )),
                ],
            )
            .expect("orders batch")
        })
        .collect::<Vec<_>>();
    ctx.register_table(
        "orders",
        Arc::new(MemTable::try_new(orders_schema(), vec![batches]).expect("bounded test fixture")),
    )
    .expect("table");
    let plan = row_query_plan(&ctx)
        .await
        .expect("row query")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let mut stream = datafusion::physical_plan::execute_stream(Arc::clone(&plan), ctx.task_ctx())
        .expect("stream");
    let first = tokio::time::timeout(Duration::from_secs(10), stream.try_next())
        .await
        .expect("first batch deadline")
        .expect("first batch")
        .expect("nonempty");
    assert_eq!(first.num_rows(), 256);
    assert_eq!(
        first
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ids")
            .value(0),
        total - 1
    );
    assert!(
        spill_count(&plan) > 0,
        "the ordered row query must actually spill"
    );
    assert!(
        ctx.runtime_env().spilling_progress().active_files_count > 0,
        "early drop must exercise live spill ownership"
    );
    drop(first);
    drop(stream);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if ctx.runtime_env().memory_pool.reserved() == 0
                && ctx.runtime_env().spilling_progress().active_files_count == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("stream drop releases spill files and memory");
    // DiskManager retains its empty namespace directory until runtime teardown.
    for entry in directory.path().read_dir().expect("spill directory") {
        let entry = entry.expect("namespace");
        assert!(entry.file_type().expect("type").is_dir());
        assert_eq!(
            entry
                .path()
                .read_dir()
                .expect("namespace directory")
                .count(),
            0
        );
    }
    drop(plan);
    drop(ctx);
    drop(base);
    assert_eq!(
        directory
            .path()
            .read_dir()
            .expect("spill directory")
            .count(),
        0
    );
}

#[derive(Debug)]
struct CountedOrders {
    schema: arrow::datatypes::SchemaRef,
    batch: RecordBatch,
    yielded: Arc<std::sync::atomic::AtomicUsize>,
}
impl datafusion::physical_plan::streaming::PartitionStream for CountedOrders {
    fn schema(&self) -> &arrow::datatypes::SchemaRef {
        &self.schema
    }
    fn execute(
        &self,
        _: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::physical_plan::SendableRecordBatchStream {
        use futures_util::StreamExt;
        let yielded = Arc::clone(&self.yielded);
        let batch = self.batch.clone();
        let input = futures_util::stream::iter((0..32).map(move |_| Ok(batch.clone()))).inspect(
            move |_| {
                yielded.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            },
        );
        Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                input,
            ),
        )
    }
}

#[tokio::test]
async fn streamed_row_query_slow_consumer_pauses_output_after_sort_consumes_input() {
    use datafusion::catalog::streaming::StreamingTable;
    use datafusion::prelude::SessionConfig;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let fixture =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/m3/orders.csv");
    let batch = crate::datafusion_query::fixture_to_orders_batch(&fixture).expect("fixture");
    let yielded = Arc::new(AtomicUsize::new(0));
    let ctx = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(1)
            .with_batch_size(1),
    );
    ctx.register_table(
        "orders",
        Arc::new(
            StreamingTable::try_new(
                orders_schema(),
                vec![Arc::new(CountedOrders {
                    schema: orders_schema(),
                    batch,
                    yielded: Arc::clone(&yielded),
                })],
            )
            .expect("streaming table"),
        ),
    )
    .expect("register");
    let mut stream = row_query_plan(&ctx)
        .await
        .expect("plan")
        .execute_stream()
        .await
        .expect("stream");
    let first = stream.try_next().await.expect("first").expect("output");
    assert_eq!(first.num_rows(), 1);
    assert_eq!(
        yielded.load(Ordering::SeqCst),
        32,
        "global ordering consumes every source batch before first output"
    );
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let released = Arc::clone(&gate);
    let delivered = Arc::new(AtomicUsize::new(1));
    let observed = Arc::clone(&delivered);
    let (paused, waiting) = tokio::sync::oneshot::channel();
    let consumer = tokio::spawn(async move {
        let permit = released.acquire();
        tokio::pin!(permit);
        let mut paused = Some(paused);
        // Signal only after the sink's permit has actually returned Pending.
        futures_util::future::poll_fn(|cx| {
            let poll = std::future::Future::poll(permit.as_mut(), cx);
            if poll.is_pending()
                && let Some(signal) = paused.take()
            {
                signal.send(()).expect("pause observer");
            }
            poll
        })
        .await
        .expect("sink released")
        .forget();
        let mut rows = first.num_rows();
        drop(first);
        while let Some(batch) = stream.try_next().await.expect("remaining output") {
            rows += batch.num_rows();
            observed.fetch_add(1, Ordering::SeqCst);
        }
        rows
    });
    tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("pause deadline")
        .expect("paused");
    assert_eq!(delivered.load(Ordering::SeqCst), 1);
    assert!(!consumer.is_finished());
    gate.add_permits(1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), consumer)
            .await
            .expect("resume deadline")
            .expect("consumer"),
        32 * 5
    );
    assert_eq!(delivered.load(Ordering::SeqCst), 32 * 5);
}
