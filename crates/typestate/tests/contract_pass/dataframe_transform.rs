#![allow(dead_code)]
use arrow::array::{Array, Int32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use contracts::Contract;
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::prelude::{SessionContext, col, lit};
use std::sync::Arc;
use typestate::contract_pipeline::{CallbackError, ContractPipeline};

#[derive(Contract)]
struct Order {
    id: i64,
    quantity: i32,
}
// DataFusion's `sum()` aggregate always declares its output nullable (an
// empty group's sum is NULL), independent of the input column's
// nullability — so the target contract must declare it `Option<i64>`, not
// `i64`, or schema validation rejects every real batch this stage produces.
#[derive(Contract)]
struct Total {
    total: Option<i64>,
}

fn input_batch() -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("quantity", DataType::Int32, false),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![5, -1, 10])),
        ],
    )
    .expect("build input batch")
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let batch = input_batch();
    let report = ContractPipeline::source::<Order, _>(move || {
        Ok(std::iter::once(Ok(batch)))
    })
    .dataframe_transform::<Total>(&ctx, |df| {
        df.filter(col("quantity").gt(lit(0)))?
            .aggregate(vec![], vec![sum(col("quantity")).alias("total")])
    })
    .sink::<Total>(|batch| {
        let totals = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("expected Int64 total column")?;
        assert_eq!(totals.value(0), 15);
        assert!(!totals.is_null(0));
        Ok::<(), CallbackError>(())
    })
    .build()?
    .run()?;
    assert_eq!(report.batches_processed, 1);
    assert_eq!(report.rows_processed, 1);
    Ok(())
}
