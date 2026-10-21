//! Logical ObjectStore observations, not network or billing measurements.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use arrow::array::{
    ArrayRef, Decimal128Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use futures_util::{StreamExt, stream::BoxStream};
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

use crate::datafusion_query::{register_parquet_table_from_object_store, row_query_plan};
use datafusion::prelude::{SessionConfig, SessionContext};

#[derive(Debug, Default)]
struct Observations {
    attempted_heads: AtomicU64,
    attempted_gets: AtomicU64,
    successful_gets: AtomicU64,
    bounded_requested_bytes: AtomicU64,
    other_gets: AtomicU64,
    returned_bytes: AtomicU64,
}

#[derive(Debug, Default)]
struct ObservedStore {
    inner: InMemory,
    observations: Arc<Observations>,
    fail_reads: AtomicBool,
}

impl std::fmt::Display for ObservedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("observed query test store")
    }
}

#[async_trait::async_trait]
impl ObjectStore for ObservedStore {
    async fn put_opts(
        &self,
        key: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(key, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        key: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(key, opts).await
    }
    async fn get_opts(
        &self,
        key: &ObjectPath,
        opts: GetOptions,
    ) -> object_store::Result<GetResult> {
        let observations = &self.observations;
        let head = opts.head;
        if head {
            observations.attempted_heads.fetch_add(1, Ordering::SeqCst);
        } else {
            observations.attempted_gets.fetch_add(1, Ordering::SeqCst);
            match &opts.range {
                Some(object_store::GetRange::Bounded(range)) => {
                    observations
                        .bounded_requested_bytes
                        .fetch_add(range.end - range.start, Ordering::SeqCst);
                }
                _ => {
                    observations.other_gets.fetch_add(1, Ordering::SeqCst);
                }
            }
            if self.fail_reads.load(Ordering::SeqCst) {
                return Err(object_store::Error::Generic {
                    store: "observed query test store",
                    source: "injected query read failure".into(),
                });
            }
        }
        // Forward all GetOptions without changing conditional/range semantics.
        let result = self.inner.get_opts(key, opts).await?;
        if !head {
            observations.successful_gets.fetch_add(1, Ordering::SeqCst);
        }
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let observations = Arc::clone(&self.observations);
        let stream = result.into_stream().map(move |chunk| {
            if let Ok(bytes) = &chunk {
                observations
                    .returned_bytes
                    .fetch_add(bytes.len() as u64, Ordering::SeqCst);
            }
            chunk
        });
        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(stream)),
            meta,
            range,
            attributes,
        })
    }
    fn delete_stream(
        &self,
        keys: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(keys)
    }
    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        opts: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

fn parquet_input(statistics: bool) -> Vec<u8> {
    const ROWS: usize = 4096;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(10, 2), true),
        Field::new(
            "placed_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("note", DataType::Utf8, true),
    ]));
    let ids: Int64Array = (0..ROWS as i64).collect();
    let amounts = Decimal128Array::from(
        (0..ROWS)
            .map(|row| (row % 17 != 0).then_some(row as i128 * 100))
            .collect::<Vec<_>>(),
    )
    .with_precision_and_scale(10, 2)
    .expect("precision");
    let times = TimestampMicrosecondArray::from_iter_values(0..ROWS as i64);
    let notes: StringArray = (0..ROWS)
        .map(|row| (row % 3 != 0).then(|| format!("row {row}: {}", "payload".repeat(20))))
        .collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(ids),
        Arc::new(amounts),
        Arc::new(times),
        Arc::new(notes),
    ];
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).expect("input");
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(64))
        .set_statistics_enabled(if statistics {
            EnabledStatistics::Chunk
        } else {
            EnabledStatistics::None
        })
        .build();
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, Some(properties)).expect("writer");
    writer.write(&batch).expect("write");
    writer.into_inner().expect("footer")
}

async fn context(bytes: &[u8], pruning: bool) -> (SessionContext, Arc<ObservedStore>) {
    let store = Arc::new(ObservedStore::default());
    store
        .inner
        .put(&ObjectPath::from("orders.parquet"), bytes.to_vec().into())
        .await
        .expect("seed");
    let mut config = SessionConfig::new().with_target_partitions(1);
    config.options_mut().execution.parquet.pruning = pruning;
    config.options_mut().execution.parquet.pushdown_filters = false;
    config.options_mut().execution.parquet.enable_page_index = false;
    let ctx = SessionContext::new_with_config(config);
    register_parquet_table_from_object_store(
        &ctx,
        "orders",
        store.clone(),
        "query-observations",
        "orders.parquet",
    )
    .await
    .expect("register observed store on query runtime");
    (ctx, store)
}

async fn selected_ids(ctx: &SessionContext) -> Vec<i64> {
    let batches = ctx
        .sql("SELECT id FROM orders WHERE amount >= 4000.00 ORDER BY id")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("query");
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("ids")
                .values()
                .to_vec()
        })
        .collect()
}

#[tokio::test]
async fn pruning_unknown_statistics_retain_null_boundary_and_matching_rows() {
    let bytes = parquet_input(false);
    {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        use std::io::Write;
        let mut file = tempfile::NamedTempFile::new().expect("metadata fixture");
        file.write_all(&bytes).expect("fixture bytes");
        let reader =
            SerializedFileReader::new(file.reopen().expect("fixture file")).expect("metadata");
        assert_eq!(reader.metadata().num_row_groups(), 64);
        assert!(reader.metadata().row_groups().iter().all(|group| {
            group
                .columns()
                .iter()
                .all(|column| column.statistics().is_none())
        }));
    }
    let expected = (4000..4096_i64)
        .filter(|id| id % 17 != 0)
        .collect::<Vec<_>>();
    for pruning in [true, false] {
        let (ctx, _) = context(&bytes, pruning).await;
        assert_eq!(selected_ids(&ctx).await, expected);
    }
}

#[tokio::test]
async fn query_read_observations_count_successful_payloads_and_requested_ranges() {
    let bytes = parquet_input(true);
    let (ctx, store) = context(&bytes, true).await;
    assert_eq!(
        selected_ids(&ctx).await,
        (4000..4096_i64)
            .filter(|id| id % 17 != 0)
            .collect::<Vec<_>>()
    );
    let observed = &store.observations;
    assert!(observed.attempted_gets.load(Ordering::SeqCst) > 0);
    assert!(observed.bounded_requested_bytes.load(Ordering::SeqCst) > 0);
    assert!(observed.returned_bytes.load(Ordering::SeqCst) > 0);
    assert_eq!(
        observed.successful_gets.load(Ordering::SeqCst),
        observed.attempted_gets.load(Ordering::SeqCst)
    );
}

#[tokio::test]
async fn query_read_observations_pruning_reduces_logical_payload_after_result_parity() {
    let bytes = parquet_input(true);
    let mut totals = Vec::new();
    let expected = (4000..4096_i64)
        .filter(|id| id % 17 != 0)
        .collect::<Vec<_>>();
    for pruning in [true, false] {
        let (ctx, store) = context(&bytes, pruning).await;
        assert_eq!(selected_ids(&ctx).await, expected);
        let observed = &store.observations;
        let returned = observed.returned_bytes.load(Ordering::SeqCst);
        let attempted = observed.attempted_gets.load(Ordering::SeqCst);
        let requested = observed.bounded_requested_bytes.load(Ordering::SeqCst);
        println!(
            "pruning={pruning} registration+query: GET attempts={attempted}, bounded requested bytes={requested}, returned payload bytes={returned}, non-bounded GETs={}",
            observed.other_gets.load(Ordering::SeqCst)
        );
        totals.push(returned);
    }
    assert!(
        totals[0] < totals[1],
        "selective fixture must avoid payload work: {totals:?}"
    );
}

#[tokio::test]
async fn streamed_row_query_propagates_controlled_backend_read_failure() {
    use futures_util::TryStreamExt;
    let (ctx, store) = context(&parquet_input(true), true).await;
    let plan = row_query_plan(&ctx).await.expect("logical plan");
    store.fail_reads.store(true, Ordering::SeqCst);
    let error = match plan.execute_stream().await {
        Err(error) => error,
        Ok(mut stream) => loop {
            match stream.try_next().await {
                Err(error) => break error,
                Ok(Some(_)) => {}
                Ok(None) => panic!("read failure was hidden"),
            }
        },
    };
    assert!(
        error.to_string().contains("injected query read failure"),
        "{error}"
    );
    assert!(
        store.observations.attempted_gets.load(Ordering::SeqCst)
            > store.observations.successful_gets.load(Ordering::SeqCst)
    );
}

#[tokio::test]
async fn query_read_observations_preserve_conditions_and_count_failed_attempts() {
    let store = ObservedStore::default();
    let key = ObjectPath::from("input");
    store
        .inner
        .put(&key, vec![1u8, 2, 3].into())
        .await
        .expect("seed");
    let meta = store.head(&key).await.expect("observed HEAD");
    let error = store
        .get_opts(
            &key,
            GetOptions {
                range: Some((0..1).into()),
                if_match: Some("wrong-etag".to_owned()),
                ..Default::default()
            },
        )
        .await
        .expect_err("preserved condition");
    assert!(matches!(error, object_store::Error::Precondition { .. }));
    let observations = &store.observations;
    assert_eq!(observations.attempted_heads.load(Ordering::SeqCst), 1);
    assert_eq!(observations.attempted_gets.load(Ordering::SeqCst), 1);
    assert_eq!(observations.successful_gets.load(Ordering::SeqCst), 0);
    assert_eq!(observations.returned_bytes.load(Ordering::SeqCst), 0);
    let response = store
        .get_opts(
            &key,
            GetOptions {
                range: Some((0..1).into()),
                if_match: meta.e_tag,
                ..Default::default()
            },
        )
        .await
        .expect("matching ETag");
    assert_eq!(response.range, 0..1);
    assert_eq!(response.bytes().await.expect("body").as_ref(), &[1]);
    assert_eq!(observations.attempted_gets.load(Ordering::SeqCst), 2);
    assert_eq!(observations.successful_gets.load(Ordering::SeqCst), 1);
    assert_eq!(
        observations.bounded_requested_bytes.load(Ordering::SeqCst),
        2
    );
    assert_eq!(observations.returned_bytes.load(Ordering::SeqCst), 1);
}
