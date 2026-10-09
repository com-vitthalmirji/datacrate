//! M3 Week 7: register a Parquet file as a DataFusion table and run the same
//! filter/projection/ordering query and the same aggregation through both the
//! SQL and DataFrame APIs.
//!
//! Week 8 added plan reading (`EXPLAIN ANALYZE`), filter/projection
//! pushdown, a scalar UDF, and a CAST-divergence characterization. Week 9
//! added memory-limited contexts (`context_with_memory_limit`), streaming
//! execution via `DataFrame::execute_stream`, aggregate spilling, the
//! hash-join build-side failure path, and stream-drop cleanup - all proven
//! directly against the pinned `datafusion = "55.1.0"` source, not assumed
//! from docs. See docs/adr/0006-datafusion-query-parity-and-pushdown-proof.md
//! and docs/adr/0007-datafusion-resource-control.md.

use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

#[cfg(test)]
use arrow::array::{Array, Float64Array};
use arrow::array::{
    ArrayRef, Decimal128Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use chrono::NaiveDateTime;
#[cfg(test)]
use datafusion::catalog::{Session, TableProvider};
#[cfg(test)]
use datafusion::common::ScalarValue;
#[cfg(test)]
use datafusion::common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion::error::DataFusionError;
#[cfg(test)]
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{FairSpillPool, TrackConsumersPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::{ColumnarValue, JoinType, Volatility, col, create_udf};
#[cfg(test)]
use datafusion::logical_expr::{Expr, Limit, LogicalPlan, TableType};
#[cfg(test)]
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
#[cfg(test)]
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
#[cfg(test)]
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
#[cfg(test)]
use datafusion::physical_plan::memory::MemoryStream;
#[cfg(test)]
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream,
};
use datafusion::prelude::{SessionConfig, SessionContext, lit};
use object_store::ObjectStore;
use url::Url;

use crate::{PipelineIoError, parse_id, read_csv_rows};

/// The fixed four-column schema (`id: Int64`, `amount: Decimal128(10, 2)`,
/// `placed_at: Timestamp(Microsecond)`, `note: Utf8`, nullable) every
/// [`fixture_to_orders_batch`] batch shares.
pub fn orders_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(10, 2), false),
        Field::new(
            "placed_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("note", DataType::Utf8, true),
    ]))
}

/// An order's `id` (`orders_schema`'s primary key) and a shipment's
/// `order_id` (`shipments_schema`'s foreign key) are the same domain value —
/// this newtype keeps that value distinct from every other bare `i64` in
/// `OrderRow`/`ShipmentRow` (`amount`, `placed_at`, `shipped_at`) so a future
/// call site can't pass one where another is expected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OrderId(i64);

/// One parsed `id,amount,placed_at,note` row. `amount` is stored as an
/// unscaled `i128` (matching `Decimal128(10, 2)`'s scale of 2), `placed_at`
/// as microseconds since the Unix epoch.
struct OrderRow {
    id: OrderId,
    amount: i128,
    placed_at: i64,
    note: Option<String>,
}

/// Parses a fixed-scale-2 decimal string (e.g. `"100.00"`, `"0.01"`) into its
/// unscaled `i128` representation (e.g. `10000`, `1`), matching
/// `Decimal128(_, 2)`.
fn parse_amount(row: usize, value: &str) -> Result<i128, PipelineIoError> {
    let invalid = || PipelineIoError::InvalidAmount {
        row,
        value: value.to_string(),
    };
    let (whole, frac) = value.split_once('.').ok_or_else(invalid)?;
    if frac.len() != 2 {
        return Err(invalid());
    }
    let whole: i128 = whole.parse().map_err(|_| invalid())?;
    let frac: i128 = frac.parse().map_err(|_| invalid())?;
    let sign = if value.starts_with('-') { -1 } else { 1 };
    whole
        .checked_mul(100)
        .and_then(|scaled| scaled.checked_add(sign * frac))
        .ok_or_else(invalid)
}

fn parse_timestamp(row: usize, value: &str) -> Result<i64, PipelineIoError> {
    let naive = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S").map_err(|_| {
        PipelineIoError::InvalidTimestamp {
            row,
            value: value.to_string(),
        }
    })?;
    Ok(naive.and_utc().timestamp_micros())
}

fn parse_order_row(row: usize, record: &csv::StringRecord) -> Result<OrderRow, PipelineIoError> {
    let id = parse_id(row, record)?;
    let amount = parse_amount(row, record.get(1).unwrap_or_default())?;
    let placed_at = parse_timestamp(row, record.get(2).unwrap_or_default())?;
    let note = match record.get(3).unwrap_or_default() {
        "" => None,
        note => Some(note.to_string()),
    };
    Ok(OrderRow {
        id: OrderId(id),
        amount,
        placed_at,
        note,
    })
}

/// Reads a headered CSV file with an `id,amount,placed_at,note` schema and
/// converts it into a single typed Arrow [`RecordBatch`] matching
/// [`orders_schema`].
///
/// # Errors
///
/// Returns [`PipelineIoError`] if the file cannot be opened, a record cannot
/// be read, `id` is not a valid integer, `amount` is not a fixed-scale-2
/// decimal, `placed_at` is not a valid `YYYY-MM-DDTHH:MM:SS` timestamp, or
/// the resulting arrays cannot be assembled into a `RecordBatch`.
pub fn fixture_to_orders_batch(path: &Path) -> Result<RecordBatch, PipelineIoError> {
    rows_to_orders_batch(read_csv_rows(path, parse_order_row)?)
}

fn rows_to_orders_batch(rows: Vec<OrderRow>) -> Result<RecordBatch, PipelineIoError> {
    let ids: Int64Array = rows.iter().map(|r| r.id.0).collect();
    let amounts = Decimal128Array::from_iter_values(rows.iter().map(|r| r.amount))
        .with_precision_and_scale(10, 2)
        .map_err(|source| PipelineIoError::BuildBatch { source })?;
    let placed_ats = TimestampMicrosecondArray::from_iter_values(rows.iter().map(|r| r.placed_at));
    let notes: StringArray = rows.iter().map(|r| r.note.as_deref()).collect();

    RecordBatch::try_new(
        orders_schema(),
        vec![
            Arc::new(ids),
            Arc::new(amounts),
            Arc::new(placed_ats),
            Arc::new(notes),
        ],
    )
    .map_err(|source| PipelineIoError::BuildBatch { source })
}

/// Registers `path` as a DataFusion table named `orders` on `ctx`.
///
/// # Errors
///
/// Returns a [`DataFusionError`] if the Parquet file cannot be registered.
pub async fn register_orders(ctx: &SessionContext, path: &Path) -> Result<(), DataFusionError> {
    ctx.register_parquet(
        "orders",
        path.to_string_lossy().as_ref(),
        datafusion::prelude::ParquetReadOptions::default(),
    )
    .await
}

/// The fixed three-column schema (`order_id: Int64`, `carrier: Utf8`,
/// `shipped_at: Timestamp(Microsecond)`) every [`fixture_to_shipments_batch`]
/// batch shares. `order_id` is a foreign key into [`orders_schema`]'s `id`,
/// not a primary key — an order may have zero or more shipments.
pub fn shipments_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("carrier", DataType::Utf8, false),
        Field::new(
            "shipped_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

struct ShipmentRow {
    order_id: OrderId,
    carrier: String,
    shipped_at: i64,
}

fn parse_shipment_row(
    row: usize,
    record: &csv::StringRecord,
) -> Result<ShipmentRow, PipelineIoError> {
    let order_id = parse_id(row, record)?;
    let carrier = record.get(1).unwrap_or_default().to_string();
    let shipped_at = parse_timestamp(row, record.get(2).unwrap_or_default())?;
    Ok(ShipmentRow {
        order_id: OrderId(order_id),
        carrier,
        shipped_at,
    })
}

/// Reads a headered CSV file with an `order_id,carrier,shipped_at` schema and
/// converts it into a single typed Arrow [`RecordBatch`] matching
/// [`shipments_schema`].
///
/// # Errors
///
/// Returns [`PipelineIoError`] if the file cannot be opened, a record cannot
/// be read, `order_id` is not a valid integer, `shipped_at` is not a valid
/// `YYYY-MM-DDTHH:MM:SS` timestamp, or the resulting arrays cannot be
/// assembled into a `RecordBatch`.
pub fn fixture_to_shipments_batch(path: &Path) -> Result<RecordBatch, PipelineIoError> {
    rows_to_shipments_batch(read_csv_rows(path, parse_shipment_row)?)
}

fn rows_to_shipments_batch(rows: Vec<ShipmentRow>) -> Result<RecordBatch, PipelineIoError> {
    let order_ids: Int64Array = rows.iter().map(|r| r.order_id.0).collect();
    let carriers: StringArray = rows.iter().map(|r| Some(r.carrier.as_str())).collect();
    let shipped_ats =
        TimestampMicrosecondArray::from_iter_values(rows.iter().map(|r| r.shipped_at));

    RecordBatch::try_new(
        shipments_schema(),
        vec![
            Arc::new(order_ids),
            Arc::new(carriers),
            Arc::new(shipped_ats),
        ],
    )
    .map_err(|source| PipelineIoError::BuildBatch { source })
}

/// Registers `path` as a DataFusion table named `shipments` on `ctx`.
///
/// # Errors
///
/// Returns a [`DataFusionError`] if the Parquet file cannot be registered.
pub async fn register_shipments(ctx: &SessionContext, path: &Path) -> Result<(), DataFusionError> {
    ctx.register_parquet(
        "shipments",
        path.to_string_lossy().as_ref(),
        datafusion::prelude::ParquetReadOptions::default(),
    )
    .await
}

/// Registers `store` on `ctx`'s `RuntimeEnv` for the `s3://<bucket>` scheme
/// and host, then registers the Parquet object at `s3://<bucket>/<key>` as a
/// table named `table_name`. `store`'s own bucket configuration (set via
/// `AmazonS3Builder::with_bucket_name`) and `bucket` must agree, since
/// DataFusion dispatches every query against this table to `store` purely by
/// matching the URL's scheme and host — it never inspects `store` itself.
/// This lets `ctx.sql(...)`/`ctx.table(...)` read Parquet directly from
/// object storage, instead of the `download_to_temp`-then-register-local-path
/// pattern [`object_store_io`](crate::object_store_io) uses for the bounded
/// pipeline.
///
/// # Errors
///
/// Returns a [`DataFusionError`] if `bucket` cannot be parsed into a
/// `s3://<bucket>` URL, or if the Parquet object cannot be registered.
pub async fn register_parquet_table_from_object_store(
    ctx: &SessionContext,
    table_name: &str,
    store: Arc<dyn ObjectStore>,
    bucket: &str,
    key: &str,
) -> Result<(), DataFusionError> {
    let bucket_url = Url::parse(&format!("s3://{bucket}")).map_err(|source| {
        DataFusionError::Configuration(format!("invalid bucket {bucket:?}: {source}"))
    })?;
    ctx.runtime_env().register_object_store(&bucket_url, store);

    ctx.register_parquet(
        table_name,
        &format!("s3://{bucket}/{key}"),
        datafusion::prelude::ParquetReadOptions::default(),
    )
    .await
}

/// Runs a left join of `orders` to `shipments` on `orders.id = shipments.order_id`
/// via the SQL API. Orders with no shipment row (e.g. id 7 in the fixture)
/// keep their row with `carrier`/`shipped_at` set to `NULL`; orders with more
/// than one shipment (e.g. id 3) appear once per shipment.
///
/// # Errors
///
/// Returns a [`DataFusionError`] if the query cannot be planned or executed.
pub async fn join_query_sql(ctx: &SessionContext) -> Result<Vec<RecordBatch>, DataFusionError> {
    ctx.sql(
        "SELECT orders.id, orders.amount, shipments.carrier, shipments.shipped_at \
         FROM orders LEFT JOIN shipments ON orders.id = shipments.order_id \
         ORDER BY orders.id, shipments.carrier",
    )
    .await?
    .collect()
    .await
}

/// Runs the same left join as [`join_query_sql`] via the DataFrame API.
///
/// # Errors
///
/// Returns a [`DataFusionError`] if the query cannot be planned or executed.
pub async fn join_query_dataframe(
    ctx: &SessionContext,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let orders = ctx.table("orders").await?;
    let shipments = ctx.table("shipments").await?;
    orders
        .join(shipments, JoinType::Left, &["id"], &["order_id"], None)?
        .select(vec![
            col("id"),
            col("amount"),
            col("carrier"),
            col("shipped_at"),
        ])?
        .sort(vec![
            col("id").sort(true, false),
            col("carrier").sort(true, false),
        ])?
        .collect()
        .await
}

/// Runs a ranking window (`RANK() OVER (ORDER BY amount DESC)`) over `orders`
/// via the SQL API.
///
/// # Errors
///
/// Returns a [`DataFusionError`] if the query cannot be planned or executed.
pub async fn window_query_sql(ctx: &SessionContext) -> Result<Vec<RecordBatch>, DataFusionError> {
    ctx.sql(
        "SELECT id, amount, RANK() OVER (ORDER BY amount DESC) AS amount_rank \
         FROM orders ORDER BY amount_rank, id",
    )
    .await?
    .collect()
    .await
}

/// Runs the same ranking window as [`window_query_sql`] via the DataFrame API.
///
/// # Errors
///
/// Returns a [`DataFusionError`] if the query cannot be planned or executed.
pub async fn window_query_dataframe(
    ctx: &SessionContext,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    use datafusion::functions_window::expr_fn::rank;
    use datafusion::logical_expr::ExprFunctionExt;

    let rank_expr = rank()
        .order_by(vec![col("amount").sort(false, false)])
        .build()?
        .alias("amount_rank");

    ctx.table("orders")
        .await?
        .window(vec![rank_expr])?
        .select(vec![col("id"), col("amount"), col("amount_rank")])?
        .sort(vec![
            col("amount_rank").sort(true, false),
            col("id").sort(true, false),
        ])?
        .collect()
        .await
}

const MICROS_PER_DAY: i64 = 86_400_000_000;

/// A single-column scalar UDF (`days_since_epoch(placed_at) -> BIGINT`)
/// compared, in [`explain_days_since_epoch_udf_vs_builtin`], against the
/// equivalent built-in expression `CAST(placed_at AS BIGINT) / 86400000000`
/// to prove native Rust scalar UDFs get the same
/// constant-folding/simplification treatment as built-ins, not a
/// row-by-row performance penalty.
///
/// # Panics
///
/// The returned UDF's implementation panics if DataFusion invokes it with an
/// argument that isn't a `Timestamp(Microsecond)` array — this cannot happen
/// in practice because `create_udf` below fixes the signature to exactly
/// that type.
pub fn days_since_epoch_udf() -> datafusion::logical_expr::ScalarUDF {
    let implementation = std::sync::Arc::new(|args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let timestamps = args[0]
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("days_since_epoch is registered with a Timestamp(Microsecond) signature");
        let days: Int64Array = timestamps
            .iter()
            .map(|micros| micros.map(|m| m.div_euclid(MICROS_PER_DAY)))
            .collect();
        Ok(ColumnarValue::from(std::sync::Arc::new(days) as ArrayRef))
    });

    create_udf(
        "days_since_epoch",
        vec![DataType::Timestamp(TimeUnit::Microsecond, None)],
        DataType::Int64,
        Volatility::Immutable,
        implementation,
    )
}

/// Runs `EXPLAIN` (plan-only; the comparison only needs to prove optimizer
/// treatment, not runtime metrics) for both [`days_since_epoch_udf`] applied
/// to a constant timestamp literal and the equivalent built-in cast/divide
/// expression on the same literal, returning `(udf_plan, builtin_plan)`.
/// Both plans should fold the constant input down to a `Literal` in
/// `logical_plan after simplify_expressions`, confirming the UDF (declared
/// `Volatility::Immutable`) participates in constant folding exactly like
/// the built-in `CAST`/`/` expression does.
///
/// # Errors
///
/// Returns a [`DataFusionError`] if either query cannot be planned.
pub async fn explain_days_since_epoch_udf_vs_builtin(
    ctx: &SessionContext,
) -> Result<(Vec<RecordBatch>, Vec<RecordBatch>), DataFusionError> {
    ctx.register_udf(days_since_epoch_udf());
    let literal = "CAST(TIMESTAMP '2026-01-03T00:00:00' AS TIMESTAMP(6))";
    let udf_plan = ctx
        .sql(&format!(
            "EXPLAIN SELECT days_since_epoch({literal}) AS days"
        ))
        .await?
        .collect()
        .await?;
    let builtin_plan = ctx
        .sql(&format!(
            "EXPLAIN SELECT CAST({literal} AS BIGINT) / {MICROS_PER_DAY} AS days"
        ))
        .await?
        .collect()
        .await?;
    Ok((udf_plan, builtin_plan))
}

/// Runs `EXPLAIN ANALYZE <sql>` and returns the resulting plan+metrics
/// batches. Callers extract runtime metrics (e.g. `bytes_scanned`,
/// `pushdown_rows_pruned`) from the formatted plan text in these batches —
/// `EXPLAIN` alone (without `ANALYZE`) only shows optimizer intent, not
/// confirmed runtime behaviour.
///
/// # Errors
///
/// Returns a [`DataFusionError`] if the query cannot be planned or executed.
pub async fn explain_analyze(
    ctx: &SessionContext,
    sql: &str,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    ctx.sql(&format!("EXPLAIN ANALYZE {sql}"))
        .await?
        .collect()
        .await
}

/// Builds a [`SessionContext`] whose `SessionConfig` has row-level Parquet
/// filter pushdown explicitly enabled (`ParquetOptions::pushdown_filters`,
/// off by default in DataFusion because of past performance regressions).
///
/// This only assembles configuration — no Parquet file is read, and no
/// pushdown actually happens, until a query is planned and executed against
/// the returned context.
pub fn context_with_filter_pushdown() -> SessionContext {
    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = true;
    SessionContext::new_with_config(config)
}

/// Builds a [`SessionContext`] whose `RuntimeEnv` enforces a `max_bytes`
/// memory limit and spills to `spill_dir`.
///
/// Uses [`FairSpillPool`] rather than the `with_memory_limit` builder
/// method's default `GreedyMemoryPool`, and pins `target_partitions` to `1`
/// rather than leaving it at the host's CPU count. Both are required,
/// together, for the memory-limited aggregate to spill deterministically:
/// `FairSpillPool` divides `max_bytes` evenly across however many
/// spillable streams are running concurrently, so at 2+ partitions on a
/// tight budget each stream's share can be smaller than the few hundred
/// bytes one post-spill aggregate batch needs - not host-CPU-count flaky,
/// but partition-count flaky in the same way. Pinning to 1 partition gives
/// the single aggregate stream the whole budget. See
/// docs/adr/0007-datafusion-resource-control.md.
///
/// `with_temp_file_path(spill_dir)` is set even though the default
/// `DiskManager` already spills to the OS temp directory - it exists here
/// purely so tests can point at a known, inspectable directory rather than
/// the shared OS temp dir.
///
/// Like [`context_with_filter_pushdown`], this only assembles configuration —
/// no memory is reserved and no spilling occurs until a query runs against
/// the returned context.
///
/// # Errors
///
/// Returns a [`DataFusionError`] if the `RuntimeEnv` cannot be built.
#[tracing::instrument(fields(spill_dir = %spill_dir.display()))]
pub fn context_with_memory_limit(
    max_bytes: usize,
    spill_dir: &Path,
) -> Result<SessionContext, DataFusionError> {
    const TRACKED_CONSUMERS: NonZeroUsize = NonZeroUsize::new(5).expect("5 is nonzero");

    tracing::debug!("building memory-limited SessionContext");
    let pool = TrackConsumersPool::new(FairSpillPool::new(max_bytes), TRACKED_CONSUMERS);
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(pool))
        .with_temp_file_path(spill_dir)
        .build_arc()?;
    let config = SessionConfig::new().with_target_partitions(1);
    Ok(SessionContext::new_with_config_rt(config, runtime))
}

/// Runs the row-level filter/projection/ordering query (`amount > 50.00`,
/// `id, amount, placed_at, note`, ordered by `placed_at`) via the SQL API.
///
/// # Errors
///
/// Returns a [`DataFusionError`] if the query cannot be planned or executed.
pub async fn row_query_sql(ctx: &SessionContext) -> Result<Vec<RecordBatch>, DataFusionError> {
    ctx.sql(
        "SELECT id, amount, placed_at, note FROM orders \
         WHERE amount > 50.00 ORDER BY placed_at ASC",
    )
    .await?
    .collect()
    .await
}

/// Runs the same row-level filter/projection/ordering query as
/// [`row_query_sql`] via the DataFrame API.
///
/// # Errors
///
/// Returns a [`DataFusionError`] if the query cannot be planned or executed.
pub async fn row_query_dataframe(
    ctx: &SessionContext,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    ctx.table("orders")
        .await?
        .filter(col("amount").gt(lit(50.0_f64)))?
        .select(vec![
            col("id"),
            col("amount"),
            col("placed_at"),
            col("note"),
        ])?
        .sort(vec![col("placed_at").sort(true, false)])?
        .collect()
        .await
}

/// Runs the aggregation query (`COUNT(*)`, `SUM(amount)` over `amount >
/// 50.00`) via the SQL API.
///
/// # Errors
///
/// Returns a [`DataFusionError`] if the query cannot be planned or executed.
pub async fn aggregate_query_sql(
    ctx: &SessionContext,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    ctx.sql(
        "SELECT COUNT(*) AS order_count, SUM(amount) AS total_amount \
         FROM orders WHERE amount > 50.00",
    )
    .await?
    .collect()
    .await
}

/// Runs an inner join of `orders` to `shipments` on `orders.id =
/// shipments.order_id`, filtered to `amount > 50.00`, collapsed to a single
/// `COUNT(*)`/`SUM(amount)` row. Unlike [`join_query_sql`]'s LEFT JOIN (which
/// preserves every order), this INNER JOIN drops unshipped orders and
/// duplicates multi-shipment orders' contribution to the sum - the M3.7
/// join/shuffle comparison benchmarks this shape because it is the one whose
/// physical plan requires a real hash-join build/probe (and, distributed, a
/// shuffle) rather than a single-table scan.
///
/// # Errors
///
/// Returns a [`DataFusionError`] if the query cannot be planned or executed.
pub async fn join_aggregate_query_sql(
    ctx: &SessionContext,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    ctx.sql(
        "SELECT COUNT(*) AS shipped_order_count, SUM(orders.amount) AS shipped_total_amount \
         FROM orders JOIN shipments ON orders.id = shipments.order_id \
         WHERE orders.amount > 50.00",
    )
    .await?
    .collect()
    .await
}

/// Runs the same aggregation query as [`aggregate_query_sql`] via the
/// DataFrame API.
///
/// # Errors
///
/// Returns a [`DataFusionError`] if the query cannot be planned or executed.
pub async fn aggregate_query_dataframe(
    ctx: &SessionContext,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    use datafusion::functions_aggregate::count::count;
    use datafusion::functions_aggregate::sum::sum;

    ctx.table("orders")
        .await?
        .filter(col("amount").gt(lit(50.0_f64)))?
        .aggregate(
            vec![],
            vec![
                count(lit(1)).alias("order_count"),
                sum(col("amount")).alias("total_amount"),
            ],
        )?
        .collect()
        .await
}

/// Runs a high-cardinality grouped aggregation (`id % 1000` buckets,
/// `COUNT(*)`/`SUM(amount)` per bucket) over `orders`, via the SQL API. Unlike
/// [`aggregate_query_sql`]'s single-row reduction, this exercises a
/// grouped-aggregation physical plan (M3.8's group-by-shape leg).
///
/// # Errors
///
/// Returns a [`DataFusionError`] if the query cannot be planned or executed.
pub async fn group_by_bucket_query_sql(
    ctx: &SessionContext,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    ctx.sql(
        "SELECT id % 1000 AS bucket, COUNT(*) AS order_count, SUM(amount) AS total_amount \
         FROM orders GROUP BY bucket ORDER BY bucket",
    )
    .await?
    .collect()
    .await
}

/// Runs the same grouped aggregation as [`group_by_bucket_query_sql`] via the
/// DataFrame API.
///
/// # Errors
///
/// Returns a [`DataFusionError`] if the query cannot be planned or executed.
pub async fn group_by_bucket_query_dataframe(
    ctx: &SessionContext,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    use datafusion::functions_aggregate::count::count;
    use datafusion::functions_aggregate::sum::sum;

    ctx.table("orders")
        .await?
        .with_column("bucket", col("id") % lit(1000_i64))?
        .aggregate(
            vec![col("bucket")],
            vec![
                count(lit(1)).alias("order_count"),
                sum(col("amount")).alias("total_amount"),
            ],
        )?
        .sort(vec![col("bucket").sort(true, false)])?
        .collect()
        .await
}

/// Runs a multi-predicate filter (`amount > 50.00 AND note IS NOT NULL`) over
/// `orders`, collapsed to a single `COUNT(*)`/`SUM(amount)` row, via the SQL
/// API. Exercises the fixture's `note` column (nullable, ~1-in-7 rows null at
/// M3.8 scale) alongside the existing `amount` filter, testing
/// predicate-pushdown/selectivity with two conditions instead of one.
///
/// # Errors
///
/// Returns a [`DataFusionError`] if the query cannot be planned or executed.
pub async fn multi_predicate_query_sql(
    ctx: &SessionContext,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    ctx.sql(
        "SELECT COUNT(*) AS order_count, SUM(amount) AS total_amount \
         FROM orders WHERE amount > 50.00 AND note IS NOT NULL",
    )
    .await?
    .collect()
    .await
}

/// Runs the same multi-predicate filter as [`multi_predicate_query_sql`] via
/// the DataFrame API.
///
/// # Errors
///
/// Returns a [`DataFusionError`] if the query cannot be planned or executed.
pub async fn multi_predicate_query_dataframe(
    ctx: &SessionContext,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    use datafusion::functions_aggregate::count::count;
    use datafusion::functions_aggregate::sum::sum;

    ctx.table("orders")
        .await?
        .filter(
            col("amount")
                .gt(lit(50.0_f64))
                .and(col("note").is_not_null()),
        )?
        .aggregate(
            vec![],
            vec![
                count(lit(1)).alias("order_count"),
                sum(col("amount")).alias("total_amount"),
            ],
        )?
        .collect()
        .await
}

/// A hand-rolled [`TableProvider`] over `orders` batches already held in
/// memory, proving DataFusion's custom-data-source extension point rather
/// than asserting it from the docs.
///
/// Trade-off named explicitly (DDIA ch04/ch07 lens): this provider
/// materializes every batch up front and keeps them all resident for the
/// table's lifetime - the same choice `register_orders` makes for Parquet
/// via `fixture_to_orders_batch`. That's the right trade for this crate's
/// fixture-sized data; a provider over a table larger than memory would need
/// to replace the `Vec<RecordBatch>` field with a chunked/streaming scan
/// instead, at the cost of a more complex `scan()`.
#[cfg(test)]
#[derive(Debug)]
struct OrdersMemoryTableProvider {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

#[cfg(test)]
impl OrdersMemoryTableProvider {
    fn new(batches: Vec<RecordBatch>) -> Self {
        Self {
            schema: orders_schema(),
            batches,
        }
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl TableProvider for OrdersMemoryTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// # Errors
    ///
    /// Returns a [`DataFusionError`] if `projection` names a column index
    /// outside `self.schema`.
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let projection = projection.cloned();
        let projected_schema = match &projection {
            Some(indices) => Arc::new(self.schema.project(indices)?),
            None => Arc::clone(&self.schema),
        };
        Ok(Arc::new(OrdersMemoryExec::new(
            self.batches.clone(),
            projected_schema,
            projection,
        )))
    }
}

/// The [`ExecutionPlan`] [`OrdersMemoryTableProvider::scan`] returns: a
/// single-partition leaf node that re-plays the batches it was handed,
/// applying `projection` via [`MemoryStream`] - the same stream DataFusion's
/// own `MemTable` uses, reused here rather than hand-rolled (Ousterhout:
/// define errors/special cases out of existence by reusing a mechanism
/// DataFusion already validated).
#[cfg(test)]
#[derive(Debug)]
struct OrdersMemoryExec {
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    properties: Arc<PlanProperties>,
}

#[cfg(test)]
impl OrdersMemoryExec {
    fn new(batches: Vec<RecordBatch>, schema: SchemaRef, projection: Option<Vec<usize>>) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            batches,
            schema,
            projection,
            properties,
        }
    }
}

#[cfg(test)]
impl DisplayAs for OrdersMemoryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OrdersMemoryExec: partitions=1")
    }
}

#[cfg(test)]
impl ExecutionPlan for OrdersMemoryExec {
    fn name(&self) -> &'static str {
        "OrdersMemoryExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion, DataFusionError>,
    ) -> Result<TreeNodeRecursion, DataFusionError> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(self)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let stream = MemoryStream::try_new(
            self.batches.clone(),
            Arc::clone(&self.schema),
            self.projection.clone(),
        )?;
        Ok(Box::pin(stream))
    }
}

/// A hand-rolled [`AggregateUDF`] - `amount_range(amount)` returns
/// `MAX(amount) - MIN(amount)` over `orders.amount` - proving the
/// multi-phase accumulator mechanism (`update_batch`/`state`/`merge_batch`)
/// rather than asserting it from the docs. Deliberately tracks two running
/// values (min and max), not one, so [`AmountRangeUdf::state_fields`] has to
/// override the trait's one-field default and [`AmountRangeAccumulator::state`]/
/// [`AmountRangeAccumulator::merge_batch`] exercise the actual partial-state
/// combine DataFusion uses for parallel partitions - a single-value
/// accumulator (e.g. a custom SUM) would never need to.
///
/// Grokking Simplicity lens: [`AmountRangeAccumulator`]'s two `Option<i128>`
/// fields are Data; `update_batch`/`merge_batch` are Actions (they mutate
/// that data across calls, so calling them twice on the same batch gives a
/// wrong answer - the trait's own doc comment on `Accumulator::evaluate`
/// calls this out as a correctness rule, not a style note); `evaluate`
/// reads the data back out without consuming it, closer to a Calculation
/// despite the `&mut self` the trait signature requires for internal
/// buffer reuse.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AmountRangeUdf {
    signature: datafusion::logical_expr::Signature,
}

#[cfg(test)]
impl AmountRangeUdf {
    fn new() -> Self {
        Self {
            signature: datafusion::logical_expr::Signature::exact(
                vec![DataType::Decimal128(10, 2)],
                Volatility::Immutable,
            ),
        }
    }
}

#[cfg(test)]
impl datafusion::logical_expr::AggregateUDFImpl for AmountRangeUdf {
    fn name(&self) -> &str {
        "amount_range"
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Decimal128(10, 2))
    }

    fn accumulator(
        &self,
        _acc_args: datafusion::logical_expr::function::AccumulatorArgs,
    ) -> Result<Box<dyn datafusion::logical_expr::Accumulator>, DataFusionError> {
        Ok(Box::new(AmountRangeAccumulator::default()))
    }

    fn state_fields(
        &self,
        args: datafusion::logical_expr::function::StateFieldsArgs,
    ) -> Result<Vec<arrow::datatypes::FieldRef>, DataFusionError> {
        let decimal = DataType::Decimal128(10, 2);
        Ok(vec![
            Arc::new(Field::new(
                datafusion::logical_expr::utils::format_state_name(args.name, "min"),
                decimal.clone(),
                true,
            )),
            Arc::new(Field::new(
                datafusion::logical_expr::utils::format_state_name(args.name, "max"),
                decimal,
                true,
            )),
        ])
    }
}

/// [`AmountRangeUdf::accumulator`]'s per-group running state: the smallest
/// and largest unscaled `i128` seen so far (matching `Decimal128(10, 2)`'s
/// representation), `None` until the first non-null value arrives.
#[cfg(test)]
#[derive(Debug, Default)]
struct AmountRangeAccumulator {
    min: Option<i128>,
    max: Option<i128>,
}

#[cfg(test)]
impl datafusion::logical_expr::Accumulator for AmountRangeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), DataFusionError> {
        let amounts = values[0]
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("amount_range is registered with a Decimal128(10, 2) signature");
        for value in amounts.iter().flatten() {
            self.min = Some(self.min.map_or(value, |min| min.min(value)));
            self.max = Some(self.max.map_or(value, |max| max.max(value)));
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<datafusion::common::ScalarValue, DataFusionError> {
        let range = match (self.min, self.max) {
            (Some(min), Some(max)) => Some(max - min),
            _ => None,
        };
        Ok(datafusion::common::ScalarValue::Decimal128(range, 10, 2))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<datafusion::common::ScalarValue>, DataFusionError> {
        Ok(vec![
            datafusion::common::ScalarValue::Decimal128(self.min, 10, 2),
            datafusion::common::ScalarValue::Decimal128(self.max, 10, 2),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<(), DataFusionError> {
        let mins = states[0]
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("amount_range's first state field is the running min");
        let maxes = states[1]
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("amount_range's second state field is the running max");
        for min in mins.iter().flatten() {
            self.min = Some(self.min.map_or(min, |current| current.min(min)));
        }
        for max in maxes.iter().flatten() {
            self.max = Some(self.max.map_or(max, |current| current.max(max)));
        }
        Ok(())
    }
}

/// A custom [`datafusion::logical_expr::WindowUDFImpl`], the fourth and
/// last DataFusion extension point this file proves end-to-end alongside
/// `TableProvider`/`ExecutionPlan` (a custom data source), `ScalarUDF`
/// (`days_since_epoch`), and `AggregateUDF` (`amount_range`): a window
/// function, `percent_of_total(amount) OVER ()`, that rewrites each row's
/// amount as a percentage of the sum of every row in its partition.
///
/// Software Design Depth Doctrine lens: a window function's one piece of
/// hidden complexity that a `ScalarUDF` cannot hide is that its result for
/// one row depends on every other row in the same partition.
/// [`PercentOfTotalEvaluator::evaluate_all`] is where that dependency is
/// absorbed - it reads the whole `values` array once, sums it, then maps
/// every row against that single sum. The caller never sees this; the SQL
/// reads like calling any built-in window function.
///
/// DDIA lens: `evaluate_all` trades memory for simplicity the same way a
/// non-streaming batch step does - the whole partition must be
/// materialized before the first output row exists, unlike
/// [`AmountRangeAccumulator`]'s running min/max, which processes one row
/// (or one spilled partial state) at a time. That is the right trade-off
/// for this file's 8-row fixture; a partition too large to fit in memory
/// would need the row-at-a-time `evaluate` + window-frame path instead -
/// not proven here, since nothing in this fixture forces that choice.
#[cfg(test)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct PercentOfTotalUdf {
    signature: datafusion::logical_expr::Signature,
}

#[cfg(test)]
impl PercentOfTotalUdf {
    fn new() -> Self {
        Self {
            signature: datafusion::logical_expr::Signature::exact(
                vec![DataType::Decimal128(10, 2)],
                Volatility::Immutable,
            ),
        }
    }
}

#[cfg(test)]
impl datafusion::logical_expr::WindowUDFImpl for PercentOfTotalUdf {
    fn name(&self) -> &str {
        "percent_of_total"
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _partition_evaluator_args: datafusion::logical_expr::function::PartitionEvaluatorArgs,
    ) -> Result<Box<dyn datafusion::logical_expr::PartitionEvaluator>, DataFusionError> {
        Ok(Box::new(PercentOfTotalEvaluator))
    }

    fn field(
        &self,
        field_args: datafusion::logical_expr::function::WindowUDFFieldArgs,
    ) -> Result<arrow::datatypes::FieldRef, DataFusionError> {
        Ok(Arc::new(Field::new(
            field_args.name(),
            DataType::Float64,
            true,
        )))
    }
}

/// [`PercentOfTotalUdf::partition_evaluator`]'s implementation.
///
/// Grokking Simplicity lens: `evaluate_all` is a Calculation - same
/// `values`/`num_rows` in, same `ArrayRef` out, no state carried between
/// calls - unlike [`AmountRangeAccumulator`]'s `update_batch`/`merge_batch`,
/// which are Actions precisely because they accumulate across calls.
#[cfg(test)]
#[derive(Debug)]
struct PercentOfTotalEvaluator;

#[cfg(test)]
impl datafusion::logical_expr::PartitionEvaluator for PercentOfTotalEvaluator {
    fn evaluate_all(
        &mut self,
        values: &[ArrayRef],
        num_rows: usize,
    ) -> Result<ArrayRef, DataFusionError> {
        let amounts = values[0]
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("percent_of_total is registered with a Decimal128(10, 2) signature");
        let total: i128 = amounts.iter().flatten().sum();
        let percentages: Float64Array = (0..num_rows)
            .map(|row| {
                amounts
                    .is_valid(row)
                    .then(|| amounts.value(row) as f64 / total as f64 * 100.0)
            })
            .collect();
        Ok(Arc::new(percentages))
    }
}

/// A custom [`datafusion::logical_expr::async_udf::AsyncScalarUDFImpl`], the
/// fifth and final DataFusion extension point this file proves end-to-end
/// alongside `TableProvider`/`ExecutionPlan`, `ScalarUDF` (`days_since_epoch`),
/// `AggregateUDF` (`amount_range`), and `WindowUDF` (`percent_of_total`): a
/// scalar function whose per-batch evaluation is itself an `async fn`, for
/// the case where producing a result genuinely requires awaiting something
/// (a remote lookup, a cache, another service) rather than pure CPU work.
///
/// Software Design Depth Doctrine lens: [`ScalarUDFImpl::invoke_with_args`]
/// (the sync path, used by `days_since_epoch_udf`) and
/// `AsyncScalarUDFImpl::invoke_async_with_args` have the same shape - same
/// `ScalarFunctionArgs` in, same `ColumnarValue` out. The interface hides
/// exactly one thing: whether producing that value can be computed
/// synchronously. DataFusion's physical planner is what absorbs that
/// difference - it inserts an `AsyncFuncExec` node only when a plan
/// actually contains an async function - so `region_multiplier`'s own call
/// site (plain SQL, same as every other UDF in this file) never has to
/// know which path it took.
///
/// DDIA lens: this models an external dependency the way a remote
/// pricing-tier lookup would work - [`RegionMultiplierUdf::lookup`] is a
/// fixed, in-memory stand-in, not a real network call. What *is* proven
/// here is the shape (await a lookup keyed by row, return the same
/// `ColumnarValue` contract as a sync UDF); real network latency,
/// timeouts, or partial failure are not exercised by this fixture and are
/// not claimed as demonstrated.
#[cfg(test)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct RegionMultiplierUdf {
    signature: datafusion::logical_expr::Signature,
}

#[cfg(test)]
impl RegionMultiplierUdf {
    fn new() -> Self {
        Self {
            signature: datafusion::logical_expr::Signature::exact(
                vec![DataType::Int64],
                Volatility::Volatile,
            ),
        }
    }

    /// The fixed, in-memory stand-in for a remote per-id pricing-tier
    /// lookup - unknown ids default to `1.0`, same as a real lookup
    /// service would for an id outside its known set.
    fn lookup(id: i64) -> f64 {
        match id {
            1 | 4 | 8 => 1.5,
            5 | 6 => 0.8,
            _ => 1.0,
        }
    }
}

#[cfg(test)]
impl datafusion::logical_expr::ScalarUDFImpl for RegionMultiplierUdf {
    fn name(&self) -> &str {
        "region_multiplier"
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(
        &self,
        _args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue, DataFusionError> {
        Err(DataFusionError::Internal(
            "region_multiplier is async-only; the physical planner must route calls \
             through AsyncFuncExec, not invoke_with_args directly"
                .to_string(),
        ))
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl datafusion::logical_expr::async_udf::AsyncScalarUDFImpl for RegionMultiplierUdf {
    async fn invoke_async_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue, DataFusionError> {
        // Yield once before computing, so a passing test proves the
        // physical plan actually awaited this function rather than
        // happening to run it to completion synchronously.
        tokio::task::yield_now().await;
        let ids = match &args.args[0] {
            ColumnarValue::Array(array) => Arc::clone(array),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(1)?,
        };
        let ids = ids
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("region_multiplier is registered with an Int64 signature");
        let multipliers: Float64Array = ids.iter().map(|id| id.map(Self::lookup)).collect();
        Ok(ColumnarValue::Array(Arc::new(multipliers)))
    }
}

/// A custom [`OptimizerRule`], the sixth extension point this file proves
/// end-to-end: a logical-plan rewrite that caps every top-level result set
/// at `max_rows`, the way a platform team would guard a shared SQL endpoint
/// against an accidental unbounded scan. Unlike the generic simplifications
/// already built into `datafusion-optimizer` (e.g. duplicate-predicate or
/// dead-filter elimination), this rule encodes a decision no generic rule
/// could know - the right cap for *this* deployment - so it genuinely needs
/// to be a custom rule rather than a request to change upstream.
///
/// DDIA lens: this is a deliberate reliability/correctness trade-off, not a
/// free win. It buys protection against unbounded-scan resource exhaustion
/// at the cost of silently truncating a legitimate large result instead of
/// erroring. A production version of this guardrail would surface the cap
/// back to the caller (a warning, a response header); this demo proves only
/// the rewrite itself, not that follow-up.
///
/// Software Design Depth Doctrine lens ("define errors out of existence"):
/// without this rule, "forgetting a LIMIT" is a runtime risk every query
/// author must remember to avoid. With it registered, that risk is removed
/// from the plan shape itself - every top-level plan that reaches execution
/// already has an acceptable `fetch`, so there is no separate missing-limit
/// error path to write or forget.
///
/// Grokking Simplicity lens: [`OptimizerRule::rewrite`] is a Calculation -
/// same [`LogicalPlan`] in, same [`Transformed<LogicalPlan>`] out, no I/O,
/// no shared mutable state. The Action/Calculation split lives one layer up:
/// `SessionContext::add_optimizer_rule` (an Action, called once at setup) is
/// what wires this pure rewrite into the optimizer's call sequence; the SQL
/// call site that triggers it never has to know the rule exists.
#[cfg(test)]
#[derive(Debug)]
struct MaxRowsGuardrail {
    max_rows: i64,
}

#[cfg(test)]
impl MaxRowsGuardrail {
    fn new(max_rows: i64) -> Self {
        Self { max_rows }
    }
}

#[cfg(test)]
impl OptimizerRule for MaxRowsGuardrail {
    fn name(&self) -> &str {
        "max_rows_guardrail"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        if let LogicalPlan::Limit(Limit { skip, fetch, input }) = &plan {
            let already_capped = fetch.as_deref().is_some_and(|fetch_expr| {
                matches!(
                    fetch_expr,
                    Expr::Literal(ScalarValue::Int64(Some(n)), _) if *n <= self.max_rows
                )
            });
            if already_capped {
                return Ok(Transformed::no(plan));
            }
            return Ok(Transformed::yes(LogicalPlan::Limit(Limit {
                skip: skip.clone(),
                fetch: Some(Box::new(lit(self.max_rows))),
                input: Arc::clone(input),
            })));
        }

        Ok(Transformed::yes(LogicalPlan::Limit(Limit {
            skip: None,
            fetch: Some(Box::new(lit(self.max_rows))),
            input: Arc::new(plan),
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write_parquet;
    use arrow::array::{Array, StringViewArray, UInt64Array};
    use parquet::basic::Compression;
    use tempfile::NamedTempFile;

    fn fixture_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/m3/orders.csv")
    }

    fn shipments_fixture_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/m3/shipments.csv")
    }

    #[test]
    fn rows_to_orders_batch_builds_correct_schema_and_array_lengths() {
        let rows = vec![
            OrderRow {
                id: OrderId(1),
                amount: 10000,
                placed_at: 0,
                note: Some("first".to_string()),
            },
            OrderRow {
                id: OrderId(2),
                amount: 250,
                placed_at: 1,
                note: None,
            },
        ];

        let batch = rows_to_orders_batch(rows).expect("rows should build a batch");

        assert_eq!(batch.schema(), orders_schema());
        assert_eq!(batch.num_rows(), 2);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column is Int64");
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
        let notes = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("note column is String");
        assert_eq!(notes.value(0), "first");
        assert!(notes.is_null(1));
    }

    #[test]
    fn rows_to_shipments_batch_builds_correct_schema_and_array_lengths() {
        let rows = vec![ShipmentRow {
            order_id: OrderId(1),
            carrier: "ups".to_string(),
            shipped_at: 5,
        }];

        let batch = rows_to_shipments_batch(rows).expect("rows should build a batch");

        assert_eq!(batch.schema(), shipments_schema());
        assert_eq!(batch.num_rows(), 1);
        let order_ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("order_id column is Int64");
        assert_eq!(order_ids.value(0), 1);
        let carriers = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("carrier column is String");
        assert_eq!(carriers.value(0), "ups");
    }

    async fn context_over_fixture() -> (SessionContext, NamedTempFile) {
        context_over_fixture_with(SessionContext::new()).await
    }

    async fn context_over_fixture_with(ctx: SessionContext) -> (SessionContext, NamedTempFile) {
        let batch = fixture_to_orders_batch(&fixture_path()).expect("fixture parses");
        let parquet = tempfile::Builder::new()
            .suffix(".parquet")
            .tempfile()
            .expect("temp file");
        write_parquet(&batch, parquet.path(), Compression::SNAPPY).expect("write parquet");

        register_orders(&ctx, parquet.path())
            .await
            .expect("register table");
        (ctx, parquet)
    }

    async fn context_over_orders_and_shipments() -> (SessionContext, NamedTempFile, NamedTempFile) {
        context_over_orders_and_shipments_with(SessionContext::new()).await
    }

    async fn context_over_orders_and_shipments_with(
        ctx: SessionContext,
    ) -> (SessionContext, NamedTempFile, NamedTempFile) {
        let (ctx, orders_parquet) = context_over_fixture_with(ctx).await;

        let batch = fixture_to_shipments_batch(&shipments_fixture_path()).expect("fixture parses");
        let parquet = tempfile::Builder::new()
            .suffix(".parquet")
            .tempfile()
            .expect("temp file");
        write_parquet(&batch, parquet.path(), Compression::SNAPPY).expect("write parquet");
        register_shipments(&ctx, parquet.path())
            .await
            .expect("register table");

        (ctx, orders_parquet, parquet)
    }

    #[tokio::test]
    async fn registers_and_queries_a_parquet_table_via_object_store() {
        use object_store::ObjectStoreExt;
        use object_store::PutPayload;
        use object_store::memory::InMemory;

        let batch = fixture_to_orders_batch(&fixture_path()).expect("fixture parses");
        let parquet = tempfile::Builder::new()
            .suffix(".parquet")
            .tempfile()
            .expect("temp file");
        write_parquet(&batch, parquet.path(), Compression::SNAPPY).expect("write parquet");
        let bytes = std::fs::read(parquet.path()).expect("read parquet bytes");

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(
                &object_store::path::Path::from("orders.parquet"),
                PutPayload::from(bytes),
            )
            .await
            .expect("seed object store");

        let ctx = SessionContext::new();
        register_parquet_table_from_object_store(
            &ctx,
            "orders",
            store,
            "test-bucket",
            "orders.parquet",
        )
        .await
        .expect("register table from object store");

        let expected = context_over_fixture().await;
        let local_batches = row_query_sql(&expected.0).await.expect("local query");
        let object_store_batches = row_query_sql(&ctx).await.expect("object-store query");

        assert_eq!(local_batches, object_store_batches);
    }

    #[tokio::test]
    async fn row_query_sql_and_dataframe_paths_agree() {
        let (ctx, _parquet) = context_over_fixture().await;

        let sql_batches = row_query_sql(&ctx).await.expect("sql query");
        let df_batches = row_query_dataframe(&ctx).await.expect("dataframe query");

        assert_eq!(sql_batches, df_batches);
    }

    #[tokio::test]
    async fn aggregate_query_sql_and_dataframe_paths_agree() {
        let (ctx, _parquet) = context_over_fixture().await;

        let sql_batches = aggregate_query_sql(&ctx).await.expect("sql query");
        let df_batches = aggregate_query_dataframe(&ctx)
            .await
            .expect("dataframe query");

        assert_eq!(sql_batches, df_batches);
    }

    #[tokio::test]
    async fn group_by_bucket_query_sql_and_dataframe_paths_agree() {
        let (ctx, _parquet) = context_over_fixture().await;

        let sql_batches = group_by_bucket_query_sql(&ctx).await.expect("sql query");
        let df_batches = group_by_bucket_query_dataframe(&ctx)
            .await
            .expect("dataframe query");

        assert_eq!(sql_batches, df_batches);
    }

    #[tokio::test]
    async fn multi_predicate_query_sql_and_dataframe_paths_agree() {
        let (ctx, _parquet) = context_over_fixture().await;

        let sql_batches = multi_predicate_query_sql(&ctx).await.expect("sql query");
        let df_batches = multi_predicate_query_dataframe(&ctx)
            .await
            .expect("dataframe query");

        assert_eq!(sql_batches, df_batches);
    }

    #[tokio::test]
    async fn row_query_preserves_nulls_decimals_timestamps_and_ordering() {
        let (ctx, _parquet) = context_over_fixture().await;
        let batches = row_query_sql(&ctx).await.expect("sql query");
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];

        // amount > 50.00 keeps ids 1, 2, 3, 4, 8 (5 rows); the other three
        // (45.50 x2, 0.01) are filtered out.
        assert_eq!(batch.num_rows(), 5);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column is Int64");
        // ORDER BY placed_at ASC: id 4 (2026-01-01) < id 2 (2026-01-03)
        // < id 1 (2026-01-05) < id 8 (2026-01-06) < id 3 (2026-01-07).
        assert_eq!(ids.values(), &[4, 2, 1, 8, 3]);

        let amounts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("amount column is Decimal128");
        assert_eq!(
            amounts.values(),
            &[100_000, 25_075, 10_000, 99_999_999, 9_999]
        );

        let notes = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("note column is Utf8View (DataFusion's schema_force_view_types default)");
        assert!(
            notes.is_null(0),
            "id 4's note is empty in the fixture, so it must be null"
        );
        assert!(
            notes.is_null(1),
            "id 2's note is empty in the fixture, so it must be null"
        );
        assert_eq!(notes.value(2), "First order");
        assert_eq!(notes.value(3), "Largest");
        assert_eq!(notes.value(4), "Rush");
    }

    #[tokio::test]
    async fn aggregate_query_sums_and_counts_filtered_rows() {
        let (ctx, _parquet) = context_over_fixture().await;
        let batches = aggregate_query_sql(&ctx).await.expect("sql query");
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);

        let counts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("order_count column is Int64");
        assert_eq!(counts.value(0), 5);

        let totals = batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("total_amount column is Decimal128");
        // 100.00 + 250.75 + 99.99 + 1000.00 + 999999.99 = 1,001,450.73
        assert_eq!(totals.value(0), 100_145_073);
    }

    #[tokio::test]
    async fn join_query_sql_and_dataframe_paths_agree() {
        let (ctx, _orders_parquet, _shipments_parquet) = context_over_orders_and_shipments().await;

        let sql_batches = join_query_sql(&ctx).await.expect("sql query");
        let df_batches = join_query_dataframe(&ctx).await.expect("dataframe query");

        assert_eq!(sql_batches, df_batches);
    }

    #[tokio::test]
    async fn join_keeps_unshipped_orders_and_duplicates_multi_shipment_orders() {
        let (ctx, _orders_parquet, _shipments_parquet) = context_over_orders_and_shipments().await;
        let batches = join_query_sql(&ctx).await.expect("sql query");
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];

        // 8 orders, minus id 3 (two shipments, +1 row), minus ids 5,6,7
        // (no shipment, but LEFT JOIN keeps them as a single null row each):
        // 8 + 1 = 9 rows total.
        assert_eq!(batch.num_rows(), 9);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column is Int64");
        let carriers = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("carrier column is Utf8View");

        let unshipped: Vec<i64> = (0..batch.num_rows())
            .filter(|&i| carriers.is_null(i))
            .map(|i| ids.value(i))
            .collect();
        assert_eq!(unshipped, vec![5, 6, 7]);

        let order_3_carrier_count = (0..batch.num_rows()).filter(|&i| ids.value(i) == 3).count();
        assert_eq!(order_3_carrier_count, 2);
    }

    #[tokio::test]
    async fn join_aggregate_counts_and_sums_shipped_orders_over_fifty() {
        let (ctx, _orders_parquet, _shipments_parquet) = context_over_orders_and_shipments().await;
        let batches = join_aggregate_query_sql(&ctx).await.expect("sql query");
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);

        let counts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("shipped_order_count column is Int64");
        // Orders with amount > 50.00: ids 1, 2, 3, 4, 8 (5 orders). Every one
        // of those has at least one shipment (order 3 has two: DHL, FedEx),
        // so the inner join keeps 6 rows.
        assert_eq!(counts.value(0), 6);

        let totals = batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("shipped_total_amount column is Decimal128");
        // order 3's amount is 99.99, not order 8's 999999.99 (id and
        // placed_at order don't coincide in the fixture): 100.00 + 250.75 +
        // 99.99*2 (order 3's two shipments) + 1000.00 + 999999.99 =
        // 1,001,550.72.
        assert_eq!(totals.value(0), 100_155_072);
    }

    #[tokio::test]
    async fn window_query_sql_and_dataframe_paths_agree() {
        let (ctx, _parquet) = context_over_fixture().await;

        let sql_batches = window_query_sql(&ctx).await.expect("sql query");
        let df_batches = window_query_dataframe(&ctx).await.expect("dataframe query");

        assert_eq!(sql_batches, df_batches);
    }

    #[tokio::test]
    async fn window_ranks_orders_by_amount_descending() {
        let (ctx, _parquet) = context_over_fixture().await;
        let batches = window_query_sql(&ctx).await.expect("sql query");
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 8);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column is Int64");
        // Amounts descending: id 8 (999999.99) ranks first.
        assert_eq!(ids.values()[0], 8);
        let ranks = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("amount_rank column is UInt64 (RANK()'s native output type)");
        assert_eq!(ranks.value(0), 1);
    }

    #[tokio::test]
    async fn filter_pushdown_reduces_bytes_scanned_for_selective_predicate() {
        let (default_ctx, parquet) = context_over_fixture().await;
        let pushdown_ctx = context_with_filter_pushdown();
        register_orders(&pushdown_ctx, parquet.path())
            .await
            .expect("register table");

        let sql = "SELECT id FROM orders WHERE amount > 999999.00";
        let default_batches = explain_analyze(&default_ctx, sql)
            .await
            .expect("explain analyze");
        let pushdown_batches = explain_analyze(&pushdown_ctx, sql)
            .await
            .expect("explain analyze");

        let default_metrics = format!("{default_batches:?}");
        let pushdown_metrics = format!("{pushdown_batches:?}");

        // Row-level pushdown_filters=true evaluates the predicate while
        // decoding and skips non-matching rows before they ever reach the
        // FilterExec; that shows up as a nonzero pushdown_rows_pruned count.
        // With pushdown disabled, DataFusion still row-group-prunes via
        // min/max statistics (this fixture is one row group), so
        // pushdown_rows_pruned stays present but 0 - it is the *value*, not
        // mere presence of the metric key, that proves the setting worked.
        assert_eq!(
            extract_metric(&default_metrics, "pushdown_rows_pruned"),
            0,
            "default context has pushdown disabled, so pushdown_rows_pruned must stay 0, got: \
             {default_metrics}"
        );
        assert!(
            extract_metric(&pushdown_metrics, "pushdown_rows_pruned") > 0,
            "pushdown-enabled context should report pushdown_rows_pruned > 0, got: \
             {pushdown_metrics}"
        );
    }

    #[tokio::test]
    async fn aggregate_query_explain_analyze_reports_output_rows() {
        let (ctx, _parquet) = context_over_fixture().await;
        let batches = explain_analyze(
            &ctx,
            "SELECT COUNT(*) AS order_count, SUM(amount) AS total_amount \
             FROM orders WHERE amount > 50.00",
        )
        .await
        .expect("explain analyze");
        let plan = format!("{batches:?}");

        assert!(
            plan.contains("AggregateExec"),
            "aggregate plan must show an AggregateExec operator, got: {plan}"
        );
        assert_eq!(
            extract_metric(&plan, "output_rows"),
            1,
            "COUNT/SUM with no GROUP BY collapses to a single row, got: {plan}"
        );
    }

    #[tokio::test]
    async fn join_query_explain_analyze_reports_output_rows() {
        let (ctx, _orders_parquet, _shipments_parquet) = context_over_orders_and_shipments().await;
        let batches = explain_analyze(
            &ctx,
            "SELECT orders.id, orders.amount, shipments.carrier, shipments.shipped_at \
             FROM orders LEFT JOIN shipments ON orders.id = shipments.order_id \
             ORDER BY orders.id, shipments.carrier",
        )
        .await
        .expect("explain analyze");
        let plan = format!("{batches:?}");

        assert!(
            plan.contains("HashJoinExec"),
            "join plan must show a HashJoinExec operator, got: {plan}"
        );
        assert_eq!(
            extract_metric(&plan, "output_rows"),
            9,
            "left join keeps all 8 orders plus one duplicate for order 3's two shipments, \
             got: {plan}"
        );
    }

    #[tokio::test]
    async fn window_query_explain_analyze_reports_output_rows() {
        let (ctx, _parquet) = context_over_fixture().await;
        let batches = explain_analyze(
            &ctx,
            "SELECT id, amount, RANK() OVER (ORDER BY amount DESC) AS amount_rank \
             FROM orders ORDER BY amount_rank, id",
        )
        .await
        .expect("explain analyze");
        let plan = format!("{batches:?}");

        assert!(
            plan.contains("BoundedWindowAggExec"),
            "window plan must show a BoundedWindowAggExec operator, got: {plan}"
        );
        assert_eq!(
            extract_metric(&plan, "output_rows"),
            8,
            "a ranking window over all 8 orders keeps every row, got: {plan}"
        );
    }

    fn extract_metric(explain_output: &str, key: &str) -> u64 {
        let marker = format!("{key}=");
        let start = explain_output
            .find(&marker)
            .unwrap_or_else(|| panic!("EXPLAIN ANALYZE output must report {key}"))
            + marker.len();
        let rest = &explain_output[start..];
        let end = rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(rest.len());
        rest[..end].parse().expect("metric value is numeric")
    }

    #[tokio::test]
    async fn projection_pushdown_scans_fewer_bytes_for_narrow_select() {
        let (ctx, _parquet) = context_over_fixture().await;

        let narrow = explain_analyze(&ctx, "SELECT id FROM orders")
            .await
            .expect("explain analyze");
        let wide = explain_analyze(&ctx, "SELECT * FROM orders")
            .await
            .expect("explain analyze");

        let narrow_metrics = format!("{narrow:?}");
        let wide_metrics = format!("{wide:?}");
        let narrow_bytes = extract_metric(&narrow_metrics, "bytes_scanned");
        let wide_bytes = extract_metric(&wide_metrics, "bytes_scanned");

        assert!(
            narrow_bytes < wide_bytes,
            "narrow SELECT id ({narrow_bytes} bytes) should scan fewer bytes than \
             SELECT * ({wide_bytes} bytes) under projection pushdown"
        );
    }

    #[tokio::test]
    async fn days_since_epoch_udf_computes_expected_value_on_real_data() {
        let (ctx, _parquet) = context_over_fixture().await;
        ctx.register_udf(days_since_epoch_udf());

        let batches = ctx
            .sql("SELECT id, days_since_epoch(placed_at) AS days FROM orders ORDER BY id")
            .await
            .expect("plan query")
            .collect()
            .await
            .expect("execute query");
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];

        let days = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("days column is Int64");
        // id 4 is placed 2026-01-01T00:00:00 - exactly the epoch offset for
        // that date, so its days_since_epoch is a fixed, independently
        // computable number of days since 1970-01-01.
        let expected_days_for_2026_01_01 =
            NaiveDateTime::parse_from_str("2026-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S")
                .expect("2026-01-01T00:00:00 is a valid literal")
                .and_utc()
                .timestamp()
                / 86_400;
        assert_eq!(days.value(3), expected_days_for_2026_01_01);
    }

    #[tokio::test]
    async fn days_since_epoch_udf_gets_same_constant_folding_as_builtin_equivalent() {
        let ctx = SessionContext::new();
        let (udf_plan, builtin_plan) = explain_days_since_epoch_udf_vs_builtin(&ctx)
            .await
            .expect("explain both plans");

        let udf_text = format!("{udf_plan:?}");
        let builtin_text = format!("{builtin_plan:?}");

        // Both must fold to the same literal value at plan time - the UDF's
        // presence must not suppress constant folding relative to the
        // built-in CAST/divide expression.
        assert!(
            udf_text.contains("20456") && builtin_text.contains("20456"),
            "expected both plans to constant-fold days_since_epoch('2026-01-03') to 20456; \
             udf plan: {udf_text}\nbuiltin plan: {builtin_text}"
        );
    }

    #[tokio::test]
    async fn amount_range_udaf_matches_independently_computed_max_minus_min() {
        let (ctx, _parquet) = context_over_fixture().await;
        ctx.register_udaf(datafusion::logical_expr::AggregateUDF::from(
            AmountRangeUdf::new(),
        ));

        let batches = ctx
            .sql("SELECT amount_range(amount) AS range FROM orders")
            .await
            .expect("plan query")
            .collect()
            .await
            .expect("execute query");
        assert_eq!(batches.len(), 1);

        let range = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("range column is Decimal128(10, 2)");
        // fixtures/m3/orders.csv: max 999999.99, min 0.01 - computed by hand
        // from the fixture file, not derived from the accumulator under test.
        let expected_range_unscaled = 99_999_999i128 - 1i128;
        assert_eq!(range.value(0), expected_range_unscaled);
    }

    #[tokio::test]
    async fn percent_of_total_udwf_matches_independently_computed_percentages() {
        let (ctx, _parquet) = context_over_fixture().await;
        ctx.register_udwf(datafusion::logical_expr::WindowUDF::from(
            PercentOfTotalUdf::new(),
        ));

        let batches = ctx
            .sql("SELECT id, percent_of_total(amount) OVER () AS pct FROM orders ORDER BY id")
            .await
            .expect("plan query")
            .collect()
            .await
            .expect("execute query");
        assert_eq!(batches.len(), 1);

        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column is Int64");
        let percentages = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("pct column is Float64");

        // fixtures/m3/orders.csv's 8 unscaled amounts (cents) sum to
        // 100_154_174 - summed by hand from the raw file, not derived from
        // the window function under test. Each row's expected share is
        // amount / total * 100.
        let total_unscaled = 100_154_174_f64;
        let expected_unscaled_by_id = [
            (1i64, 10_000_f64),
            (2, 25_075.0),
            (3, 9_999.0),
            (4, 100_000.0),
            (5, 4_550.0),
            (6, 4_550.0),
            (7, 1.0),
            (8, 99_999_999.0),
        ];
        assert_eq!(ids.len(), expected_unscaled_by_id.len());

        let mut percentage_sum = 0.0_f64;
        for (row, &(expected_id, unscaled)) in expected_unscaled_by_id.iter().enumerate() {
            assert_eq!(ids.value(row), expected_id);
            let expected_pct = unscaled / total_unscaled * 100.0;
            let actual_pct = percentages.value(row);
            assert!(
                (actual_pct - expected_pct).abs() < 1e-9,
                "row {row} (id={expected_id}): expected {expected_pct}, got {actual_pct}"
            );
            percentage_sum += actual_pct;
        }
        // Every row's share of the single (whole-table) partition must sum
        // back to 100%, proving evaluate_all saw the same partition total
        // for every row, not a per-batch or per-row-group slice of it.
        assert!(
            (percentage_sum - 100.0).abs() < 1e-9,
            "percentages must sum to 100.0, got {percentage_sum}"
        );
    }

    #[tokio::test]
    async fn async_udf_region_multiplier_matches_independently_computed_lookup() {
        let (ctx, _parquet) = context_over_fixture().await;
        ctx.register_udf(
            datafusion::logical_expr::async_udf::AsyncScalarUDF::new(Arc::new(
                RegionMultiplierUdf::new(),
            ))
            .into_scalar_udf(),
        );

        let batches = ctx
            .sql("SELECT id, region_multiplier(id) AS mult FROM orders ORDER BY id")
            .await
            .expect("plan query")
            .collect()
            .await
            .expect("execute query");
        assert_eq!(batches.len(), 1);

        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column is Int64");
        let multipliers = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("mult column is Float64");

        // Independently computed from RegionMultiplierUdf::lookup's own
        // match arms, not derived by calling the UDF under test.
        let expected_by_id = [
            (1i64, 1.5),
            (2, 1.0),
            (3, 1.0),
            (4, 1.5),
            (5, 0.8),
            (6, 0.8),
            (7, 1.0),
            (8, 1.5),
        ];
        assert_eq!(ids.len(), expected_by_id.len());
        for (row, &(expected_id, expected_mult)) in expected_by_id.iter().enumerate() {
            assert_eq!(ids.value(row), expected_id);
            let actual_mult = multipliers.value(row);
            assert!(
                (actual_mult - expected_mult).abs() < 1e-9,
                "row {row} (id={expected_id}): expected {expected_mult}, got {actual_mult}"
            );
        }
    }

    #[tokio::test]
    async fn optimizer_rule_max_rows_guardrail_caps_but_does_not_override_smaller_limit() {
        let (ctx, _parquet) = context_over_fixture().await;
        ctx.add_optimizer_rule(Arc::new(MaxRowsGuardrail::new(5)));

        let uncapped = ctx
            .sql("SELECT id FROM orders ORDER BY id")
            .await
            .expect("plan unbounded query")
            .collect()
            .await
            .expect("execute unbounded query");
        let uncapped_rows: usize = uncapped.iter().map(RecordBatch::num_rows).sum();
        // The fixture has 8 rows; without the guardrail this would be 8.
        assert_eq!(uncapped_rows, 5);

        let already_limited = ctx
            .sql("SELECT id FROM orders ORDER BY id LIMIT 3")
            .await
            .expect("plan already-limited query")
            .collect()
            .await
            .expect("execute already-limited query");
        let already_limited_rows: usize = already_limited.iter().map(RecordBatch::num_rows).sum();
        // The user's own smaller LIMIT must survive, not be raised to 5.
        assert_eq!(already_limited_rows, 3);
    }

    #[test]
    fn amount_range_accumulator_merge_batch_combines_partial_states() {
        use datafusion::logical_expr::Accumulator;

        // The fixture's 8 rows fit in a single Parquet row group, so a real
        // SQL plan over it never reaches AggregateMode::Partial - DataFusion
        // only splits into Partial/Final phases across multiple partitions.
        // Exercising AmountRangeAccumulator::state/merge_batch directly is
        // the precise way to prove that combine logic: build two partition
        // accumulators (split 0.01..250.75 and 45.50..999999.99), serialize
        // each with `state()`, merge both into a fresh accumulator, and
        // check the merged min/max match the full fixture's range.
        let decimal = |v: &str| -> i128 {
            let (whole, frac) = v
                .split_once('.')
                .expect("fixture literal has a decimal point");
            format!("{whole}{frac}")
                .parse()
                .expect("fixture literal parses")
        };

        let mut left = AmountRangeAccumulator::default();
        left.update_batch(&[Arc::new(
            Decimal128Array::from(vec![decimal("100.00"), decimal("250.75"), decimal("0.01")])
                .with_precision_and_scale(10, 2)
                .expect("valid Decimal128(10, 2)"),
        )])
        .expect("update_batch on left partition");

        let mut right = AmountRangeAccumulator::default();
        right
            .update_batch(&[Arc::new(
                Decimal128Array::from(vec![
                    decimal("99.99"),
                    decimal("1000.00"),
                    decimal("45.50"),
                    decimal("45.50"),
                    decimal("999999.99"),
                ])
                .with_precision_and_scale(10, 2)
                .expect("valid Decimal128(10, 2)"),
            )])
            .expect("update_batch on right partition");

        let left_state = left.state().expect("serialize left partial state");
        let right_state = right.state().expect("serialize right partial state");

        let mut merged = AmountRangeAccumulator::default();
        for state in [left_state, right_state] {
            let mins =
                datafusion::common::ScalarValue::iter_to_array(std::iter::once(state[0].clone()))
                    .expect("build mins array");
            let maxes =
                datafusion::common::ScalarValue::iter_to_array(std::iter::once(state[1].clone()))
                    .expect("build maxes array");
            merged
                .merge_batch(&[mins, maxes])
                .expect("merge_batch combines a partial state");
        }

        let range = merged.evaluate().expect("evaluate merged accumulator");
        assert_eq!(
            range,
            datafusion::common::ScalarValue::Decimal128(
                Some(decimal("999999.99") - decimal("0.01")),
                10,
                2
            ),
            "merging two partitions' partial state must reproduce the fixture's full \
             max-minus-min, proving merge_batch - not just update_batch - is correct"
        );
    }

    #[tokio::test]
    async fn cast_string_to_decimal_diverges_from_spark_on_malformed_input() {
        let ctx = SessionContext::new();

        // Re-verified 2026-09-14 against datafusion 55.1.0 (this crate's
        // pinned version): DataFusion no longer returns NULL for malformed
        // string-to-decimal casts, diverging from DataFusion Comet's older
        // Spark-parity tracking. Only a well-formed integer-looking string
        // ("0") succeeds; every other malformed case errors out of the
        // `simplify_expressions` optimizer rule instead of producing a NULL
        // or a 0.0.
        let well_formed = ctx
            .sql("SELECT CAST('0' AS DECIMAL(10,2)) AS d")
            .await
            .expect("plan cast query")
            .collect()
            .await
            .expect("execute cast query");
        let decimals = well_formed[0]
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("d is Decimal128");
        assert!(!decimals.is_null(0));
        assert_eq!(decimals.value(0), 0, "CAST('0' AS DECIMAL) must be 0");

        for malformed in ["'4e7'", "''", "'.'", "'-'", "'+'"] {
            let sql = format!("SELECT CAST({malformed} AS DECIMAL(10,2)) AS d");
            let result = ctx
                .sql(&sql)
                .await
                .expect("plan cast query")
                .collect()
                .await;
            assert!(
                result.is_err(),
                "CAST({malformed} AS DECIMAL) must error under datafusion 55.1.0, got: \
                 {result:?}"
            );
        }
    }

    const AGGREGATE_SPILL_MAX_BYTES: usize = 1200;
    const GROUP_BY_NOTE_SQL: &str =
        "SELECT note, COUNT(*) AS cnt, SUM(amount) AS total FROM orders GROUP BY note";

    /// Installs a `tracing` subscriber so `context_with_memory_limit`'s span
    /// is visible under `RUST_LOG=pipeline=debug cargo test -- --nocapture`
    /// during the Week 9 resource-control lab. `try_init` is idempotent
    /// across the three tests below that call it.
    fn init_lab_tracing() {
        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();
    }

    #[tokio::test]
    async fn memory_limited_aggregate_spills_and_still_completes() {
        init_lab_tracing();
        let spill_dir = tempfile::tempdir().expect("temp spill dir");
        let ctx = context_with_memory_limit(AGGREGATE_SPILL_MAX_BYTES, spill_dir.path())
            .expect("build memory-limited context");
        let (ctx, _parquet) = context_over_fixture_with(ctx).await;

        let batches = ctx
            .sql(GROUP_BY_NOTE_SQL)
            .await
            .expect("plan query")
            .collect()
            .await
            .expect("aggregate query must succeed under a memory limit by spilling");

        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(
            total_rows, 7,
            "6 distinct non-null notes plus 1 NULL group in the fixture"
        );

        let progress = ctx.runtime_env().spilling_progress();
        assert!(
            progress.active_files_count > 0
                || spill_dir.path().read_dir().expect("read spill dir").count() > 0,
            "a memory-constrained GROUP BY must materialize at least one spill file, got \
             spilling_progress={progress:?}"
        );
    }

    #[tokio::test]
    async fn memory_limited_join_fails_with_resources_exhausted() {
        init_lab_tracing();
        let spill_dir = tempfile::tempdir().expect("temp spill dir");
        let ctx = context_with_memory_limit(AGGREGATE_SPILL_MAX_BYTES, spill_dir.path())
            .expect("build memory-limited context");
        let (ctx, _orders_parquet, _shipments_parquet) =
            context_over_orders_and_shipments_with(ctx).await;

        let result = join_query_sql(&ctx).await;

        // The join's ORDER BY adds a sort after the hash join, so DataFusion
        // may wrap the underlying ResourcesExhausted in a Context(..) error
        // (e.g. from the external sort merge) - find_root() unwraps that to
        // the actual cause.
        let err = result.expect_err("query must fail under this memory limit");
        assert!(
            matches!(err.find_root(), DataFusionError::ResourcesExhausted(_)),
            "hash join build side has no spill fallback in datafusion 55.1.0 \
             (see docs/adr/0007.1-hash-join-build-side-spill-gap.md) - \
             expected ResourcesExhausted at the root, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn dropping_stream_early_cleans_up_spill_files() {
        use futures_util::StreamExt;

        init_lab_tracing();
        let spill_dir = tempfile::tempdir().expect("temp spill dir");
        let ctx = context_with_memory_limit(AGGREGATE_SPILL_MAX_BYTES, spill_dir.path())
            .expect("build memory-limited context");
        let (ctx, _parquet) = context_over_fixture_with(ctx).await;

        let mut stream = ctx
            .sql(GROUP_BY_NOTE_SQL)
            .await
            .expect("plan query")
            .execute_stream()
            .await
            .expect("start streaming execution");
        stream.next().await;
        drop(stream);

        // The aggregation's spilling runs on a spawned task that observes
        // the dropped receiver asynchronously, not synchronously on drop -
        // yield until it notices and its RefCountedTempFile guards drop.
        let mut progress = ctx.runtime_env().spilling_progress();
        for _ in 0..1000 {
            if progress.active_files_count == 0 {
                break;
            }
            tokio::task::yield_now().await;
            progress = ctx.runtime_env().spilling_progress();
        }
        assert_eq!(
            progress.active_files_count, 0,
            "dropping the stream must eventually run RefCountedTempFile's Drop impl and clean \
             up any in-flight spill files via ordinary Rust scope exit, got: {progress:?}"
        );
    }

    async fn context_over_memory_provider() -> SessionContext {
        let batch = fixture_to_orders_batch(&fixture_path()).expect("fixture parses");
        let ctx = SessionContext::new();
        ctx.register_table(
            "orders_mem",
            Arc::new(OrdersMemoryTableProvider::new(vec![batch])),
        )
        .expect("register custom TableProvider");
        ctx
    }

    #[tokio::test]
    async fn custom_table_provider_plan_names_its_own_exec() {
        let ctx = context_over_memory_provider().await;

        let plan = ctx
            .sql("EXPLAIN SELECT * FROM orders_mem")
            .await
            .expect("plan query")
            .collect()
            .await
            .expect("collect explain rows");
        let plan_text = arrow::util::pretty::pretty_format_batches(&plan)
            .expect("format explain output")
            .to_string();

        assert!(
            plan_text.contains("OrdersMemoryExec"),
            "expected the custom ExecutionPlan's name in the physical plan, got:\n{plan_text}"
        );
    }

    #[tokio::test]
    async fn custom_table_provider_pushes_projection_into_scan() {
        let ctx = context_over_memory_provider().await;

        let rows = ctx
            .sql("SELECT id, note FROM orders_mem")
            .await
            .expect("plan query")
            .collect()
            .await
            .expect("collect rows");

        let schema = rows[0].schema();
        assert_eq!(
            schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>(),
            vec!["id", "note"],
            "scan's projection argument should have limited OrdersMemoryExec's output \
             to exactly the two selected columns"
        );
    }

    #[tokio::test]
    async fn custom_table_provider_matches_parquet_provider_row_count() {
        let memory_ctx = context_over_memory_provider().await;
        let (parquet_ctx, _parquet) = context_over_fixture().await;

        let memory_rows = memory_ctx
            .sql("SELECT COUNT(*) AS n FROM orders_mem")
            .await
            .expect("plan query")
            .collect()
            .await
            .expect("collect rows");
        let parquet_rows = parquet_ctx
            .sql("SELECT COUNT(*) AS n FROM orders")
            .await
            .expect("plan query")
            .collect()
            .await
            .expect("collect rows");

        assert_eq!(
            memory_rows[0].column(0).as_ref(),
            parquet_rows[0].column(0).as_ref(),
            "the hand-rolled TableProvider and the Parquet-backed one must agree on row \
             count over the same fixture"
        );
    }

    // The three tests below close out the extension-API tour's remaining
    // SQL-surface claims: a CTE, a set operation, and BigQuery-style pipe
    // (`|>`) syntax. Grokking Simplicity lens: each is a Calculation-only
    // check (same `ctx.sql(..).collect()` in, same `Vec<RecordBatch>` out),
    // so the "prove, don't assert" pattern used throughout this file still
    // applies - assert actual row counts/values, not just that planning
    // succeeded. DDIA lens worth naming once: CTEs and `EXCEPT` are
    // standard SQL, portable across engines; the pipe operator is not -
    // DataFusion accepts it under the default `Generic` dialect, but a
    // query written with `|>` will not run unmodified against a database
    // that hasn't adopted this still-spreading syntax. That is the
    // trade-off this syntax buys: easier left-to-right reading at the cost
    // of portability.

    #[tokio::test]
    async fn cte_query_matches_equivalent_subquery() {
        let (ctx, _parquet) = context_over_fixture().await;

        let cte_batches = ctx
            .sql(
                "WITH high_value AS (SELECT id FROM orders WHERE amount > 100) \
                 SELECT id FROM high_value ORDER BY id",
            )
            .await
            .expect("plan CTE query")
            .collect()
            .await
            .expect("execute CTE query");
        let subquery_batches = ctx
            .sql("SELECT id FROM (SELECT id FROM orders WHERE amount > 100) ORDER BY id")
            .await
            .expect("plan subquery query")
            .collect()
            .await
            .expect("execute subquery query");

        assert_eq!(cte_batches, subquery_batches);
        let total_rows: usize = cte_batches.iter().map(RecordBatch::num_rows).sum();
        // amount > 100 keeps ids 2 (250.75), 4 (1000.00), 8 (999999.99).
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn except_set_operation_removes_matching_rows() {
        let (ctx, _parquet) = context_over_fixture().await;

        let batches = ctx
            .sql(
                "SELECT id FROM (
                     SELECT id FROM orders WHERE amount > 40
                     EXCEPT
                     SELECT id FROM orders WHERE amount > 500
                 ) t ORDER BY id",
            )
            .await
            .expect("plan EXCEPT query")
            .collect()
            .await
            .expect("execute EXCEPT query");

        assert_eq!(batches.len(), 1);
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column is Int64");
        // amount > 40 keeps {1, 2, 3, 4, 5, 6, 8}; amount > 500 keeps {4, 8};
        // EXCEPT removes the latter from the former, leaving {1, 2, 3, 5, 6}.
        assert_eq!(ids.values(), &[1, 2, 3, 5, 6]);
    }

    #[tokio::test]
    async fn pipe_operator_syntax_matches_equivalent_standard_sql() {
        let (ctx, _parquet) = context_over_fixture().await;

        let pipe_batches = ctx
            .sql("SELECT * FROM orders |> WHERE amount > 100 |> SELECT id |> ORDER BY id")
            .await
            .expect("plan pipe-syntax query")
            .collect()
            .await
            .expect("execute pipe-syntax query");
        let standard_batches = ctx
            .sql("SELECT id FROM orders WHERE amount > 100 ORDER BY id")
            .await
            .expect("plan standard-syntax query")
            .collect()
            .await
            .expect("execute standard-syntax query");

        assert_eq!(pipe_batches, standard_batches);
    }
}
