use contracts::Contract;
use std::collections::HashMap;

#[derive(Contract)]
struct BadKey {
    scores: HashMap<f64, i64>,
}

fn main() {}
