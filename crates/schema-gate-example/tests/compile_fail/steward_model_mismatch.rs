// The steward declared `customer_name` as non-nullable `String`, but the
// real Parquet file (`SourceOfTruth`, as `build.rs` would regenerate it)
// has it nullable. This is exactly the drift the compile-time gate exists
// to catch before a pipeline is built against it.
#![allow(dead_code)]
use contracts::{Contract, ExactOrdered, SchemaConforms};

#[derive(Contract)]
struct SourceOfTruth {
    id: i64,
    customer_name: Option<String>,
}

#[derive(Contract)]
struct StewardInputModel {
    id: i64,
    customer_name: String,
}

const _: () = SchemaConforms::<SourceOfTruth, StewardInputModel, ExactOrdered>::CHECK;

fn main() {}
