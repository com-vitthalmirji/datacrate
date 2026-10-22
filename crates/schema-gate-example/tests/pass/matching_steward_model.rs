// Stands in for a `SourceOfTruth` build.rs would generate from a Parquet
// file with an `id: i64` and a nullable `customer_name: Utf8` column —
// the same shape as `fixtures/schema-gate-example/orders.parquet`.
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
    customer_name: Option<String>,
}

const _: () = SchemaConforms::<SourceOfTruth, StewardInputModel, ExactOrdered>::CHECK;

fn main() {}
