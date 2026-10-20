#![allow(non_snake_case)]
use contracts::{Contract, Exact, SchemaConforms};
#[derive(Contract)]
struct Producer { id: i64, ID: String }
#[derive(Contract)]
struct Target { id: i64 }
const _: () = SchemaConforms::<Producer, Target, Exact>::CHECK;
fn main() {}
