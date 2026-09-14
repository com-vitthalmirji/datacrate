//! M3 Week 7: register a Parquet file as a DataFusion table and run the same
//! filter/projection/ordering query and the same aggregation through both the
//! SQL and DataFrame APIs.
//!
//! Scope is deliberately narrow to Week 7's build contract: SQL/DataFrame
//! parity, proven by golden-result tests. Plan reading, pushdown, UDFs
//! (Week 8) and streaming/memory/spill/cancellation (Week 9) are separate,
//! later work.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Decimal128Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use chrono::NaiveDateTime;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::col;
use datafusion::prelude::{SessionContext, lit};

use crate::PipelineIoError;

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

/// One parsed `id,amount,placed_at,note` row. `amount` is stored as an
/// unscaled `i128` (matching `Decimal128(10, 2)`'s scale of 2), `placed_at`
/// as microseconds since the Unix epoch.
struct OrderRow {
    id: i64,
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
    let sign = if whole < 0 || value.starts_with('-') {
        -1
    } else {
        1
    };
    Ok(whole * 100 + sign * frac)
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
    let id_field = record.get(0).unwrap_or_default();
    let id: i64 = id_field.parse().map_err(|_| PipelineIoError::InvalidId {
        row,
        value: id_field.to_string(),
    })?;
    let amount = parse_amount(row, record.get(1).unwrap_or_default())?;
    let placed_at = parse_timestamp(row, record.get(2).unwrap_or_default())?;
    let note = match record.get(3).unwrap_or_default() {
        "" => None,
        note => Some(note.to_string()),
    };
    Ok(OrderRow {
        id,
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
    let file = std::fs::File::open(path).map_err(|source| PipelineIoError::OpenInput {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = csv::Reader::from_reader(file);
    let mut rows = Vec::new();
    for (row, record) in reader.records().enumerate() {
        let record = record.map_err(|source| PipelineIoError::ReadRecord { source })?;
        rows.push(parse_order_row(row, &record)?);
    }

    let ids: Int64Array = rows.iter().map(|r| r.id).collect();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write_parquet;
    use arrow::array::{Array, StringViewArray};
    use parquet::basic::Compression;
    use tempfile::NamedTempFile;

    fn fixture_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/m3/orders.csv")
    }

    async fn context_over_fixture() -> (SessionContext, NamedTempFile) {
        let batch = fixture_to_orders_batch(&fixture_path()).expect("fixture parses");
        let parquet = tempfile::Builder::new()
            .suffix(".parquet")
            .tempfile()
            .expect("temp file");
        write_parquet(&batch, parquet.path(), Compression::SNAPPY).expect("write parquet");

        let ctx = SessionContext::new();
        register_orders(&ctx, parquet.path())
            .await
            .expect("register table");
        (ctx, parquet)
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
}
