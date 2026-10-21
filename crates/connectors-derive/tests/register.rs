#![allow(dead_code)]

use connectors_derive::Connector;
use contracts::Contract;
use datafusion::arrow::array::{Int32Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::lit;
use datafusion::prelude::SessionContext;
use parquet::basic::Compression;
use std::sync::Arc;

#[derive(Contract, Connector)]
struct Order {
    id: i64,
    quantity: i32,
}

fn order_batch() -> RecordBatch {
    let schema = Arc::new(Order::connector_schema().expect("struct maps to an Arrow schema"));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![10, 20])),
        ],
    )
    .expect("batch matches the derived schema")
}

#[tokio::test]
async fn register_memory_exposes_batches_as_a_queryable_table() {
    let ctx = SessionContext::new();
    Order::register_memory(&ctx, "orders", vec![order_batch()]).expect("registration succeeds");

    let df = ctx
        .sql("SELECT quantity FROM orders WHERE id = 2")
        .await
        .expect("query parses and plans");
    let rows = df.collect().await.expect("query executes");
    let total: usize = rows.iter().map(|b| b.num_rows()).sum();

    assert_eq!(total, 1);
}

#[tokio::test]
async fn register_parquet_reads_a_file_under_the_contract_schema() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("orders.parquet");
    pipeline::write_parquet(&order_batch(), &path, Compression::SNAPPY).expect("write parquet");

    let ctx = SessionContext::new();
    Order::register_parquet(&ctx, "orders", path.to_str().expect("utf8 path"))
        .expect("registration succeeds");

    let df = ctx
        .sql("SELECT quantity FROM orders WHERE id = 2")
        .await
        .expect("query parses and plans");
    let rows = df.collect().await.expect("query executes");
    let total: usize = rows.iter().map(|b| b.num_rows()).sum();

    assert_eq!(total, 1);
}

#[tokio::test]
async fn register_csv_reads_a_file_under_the_contract_schema() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("orders.csv");
    std::fs::write(&path, "id,quantity\n1,10\n2,20\n").expect("write csv fixture");

    let ctx = SessionContext::new();
    Order::register_csv(&ctx, "orders", path.to_str().expect("utf8 path"))
        .expect("registration succeeds");

    let df = ctx
        .sql("SELECT quantity FROM orders WHERE id = 2")
        .await
        .expect("query parses and plans");
    let rows = df.collect().await.expect("query executes");
    let total: usize = rows.iter().map(|b| b.num_rows()).sum();

    assert_eq!(total, 1);
}

#[tokio::test]
async fn col_accessors_build_a_filter_expression() {
    let ctx = SessionContext::new();
    Order::register_memory(&ctx, "orders", vec![order_batch()]).expect("registration succeeds");

    let df = ctx.table("orders").await.expect("table registered");
    let filtered = df
        .filter(Order::col_id().eq(lit(2_i64)))
        .expect("filter builds")
        .select(vec![Order::col_quantity()])
        .expect("select builds");
    let rows = filtered.collect().await.expect("query executes");
    let total: usize = rows.iter().map(|b| b.num_rows()).sum();

    assert_eq!(total, 1);
}
