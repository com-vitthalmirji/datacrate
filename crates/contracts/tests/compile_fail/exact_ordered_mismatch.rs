use contracts::{Contract, ExactOrdered, SchemaConforms};

#[derive(Contract)]
struct Producer {
    name: String,
    id: i64,
}

#[derive(Contract)]
struct Contract2 {
    id: i64,
    name: String,
}

const _: () = SchemaConforms::<Producer, Contract2, ExactOrdered>::CHECK;

fn main() {}
