#![allow(dead_code)]
use arrow::array::{Int64Array, RecordBatch, StringArray};
use contracts::{Contract, arrow::schema};
use std::sync::Arc;
use typestate::contract_pipeline::{CallbackError, ContractPipeline};

#[derive(Contract)]
struct Raw {
    id: i64,
}

#[derive(Contract)]
struct Clean {
    id: i64,
    label: String,
}

fn main() -> Result<(), CallbackError> {
    let input = RecordBatch::try_new(
        Arc::new(schema::<Raw>()?),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )?;
    let pipeline = ContractPipeline::source::<Raw, _>(move || Ok(std::iter::once(Ok(input))))
        .transform::<Raw, Clean>(|batch| {
            Ok(RecordBatch::try_new(
                Arc::new(schema::<Clean>()?),
                vec![
                    batch.column(0).clone(),
                    Arc::new(StringArray::from(vec!["one", "two"])),
                ],
            )?)
        })
        .sink::<Clean>(|batch| {
            println!("accepted {} rows", batch.num_rows());
            Ok(())
        })
        .build()?;
    let report = pipeline.run()?;
    assert_eq!(report.rows_processed, 2);
    Ok(())
}
