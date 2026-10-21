#![allow(dead_code)]
use contracts::Contract;
use typestate::contract_pipeline::{ContractPipeline,CallbackError};
#[derive(Contract)]struct A { id:i64 }
#[derive(Contract)]struct Same { id:i64 }
#[derive(Contract)]struct Wrong { id:String }
fn input()->Result<std::iter::Empty<Result<arrow::array::RecordBatch,CallbackError>>,CallbackError>{Ok(std::iter::empty())}
fn direct<P:Contract,C:Contract>() {
    ContractPipeline::source::<P,_>(input).sink::<C>(|_|Ok(())).build().expect("mapping").run().expect("run");
}
fn main(){
    direct::<A,Same>();
    ContractPipeline::source::<A,_>(input).transform::<Same,A>(Ok).transform::<Same,Same>(Ok).sink::<A>(|_|Ok(())).build().expect("mapping").run().expect("run");
}
