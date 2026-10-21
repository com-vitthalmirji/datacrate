#![allow(dead_code)]
use contracts::Contract;
use typestate::contract_pipeline::{ContractPipeline,CallbackError};
#[derive(Contract)]struct A { id:i64 }
#[derive(Contract)]struct Same { id:i64 }
#[derive(Contract)]struct Wrong { id:String }
fn input()->Result<std::iter::Empty<Result<arrow::array::RecordBatch,CallbackError>>,CallbackError>{Ok(std::iter::empty())}
fn main(){let _=ContractPipeline::source::<A,_>(input).build();}
