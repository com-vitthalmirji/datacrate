#![allow(dead_code)]

use connectors_derive::Connector;
use contracts::Contract;
use datafusion::arrow::array::{Int32Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

#[derive(Contract, Connector)]
struct Order {
    id: i64,
    quantity: i32,
}

#[tokio::test]
async fn register_exposes_batches_as_a_queryable_table() {
    let schema = Arc::new(Order::connector_schema().expect("struct maps to an Arrow schema"));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![10, 20])),
        ],
    )
    .expect("batch matches the derived schema");

    let ctx = SessionContext::new();
    Order::register(&ctx, "orders", vec![batch]).expect("registration succeeds");

    let df = ctx
        .sql("SELECT quantity FROM orders WHERE id = 2")
        .await
        .expect("query parses and plans");
    let rows = df.collect().await.expect("query executes");
    let total: usize = rows.iter().map(|b| b.num_rows()).sum();

    assert_eq!(total, 1);
}
