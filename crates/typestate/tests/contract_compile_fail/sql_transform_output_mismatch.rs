#![allow(dead_code)]
use contracts::Contract;
use datafusion::prelude::SessionContext;
use typestate::contract_pipeline::{CallbackError, ContractPipeline};
#[derive(Contract)]struct A { id:i64 }
#[derive(Contract)]struct B { id:i64 }
#[derive(Contract)]struct Wrong { id:String }
fn input()->Result<std::iter::Empty<Result<arrow::array::RecordBatch,CallbackError>>,CallbackError>{Ok(std::iter::empty())}
fn main(){
    let ctx = SessionContext::new();
    let sql = "SELECT * FROM batch";
    let _ = ContractPipeline::source::<A,_>(input).sql_transform::<B>(&ctx, sql).sink::<Wrong>(|_|Ok(())).build();
}
