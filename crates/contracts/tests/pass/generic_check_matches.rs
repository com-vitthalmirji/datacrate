#![allow(dead_code)]
use contracts::{Contract, Exact};
#[derive(Contract)] struct Producer { id: i64 }
#[derive(Contract)] struct Target { id: i64 }
fn main() { contracts::enforce::<Producer, Target, Exact>(); }
