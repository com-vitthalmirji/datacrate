use contracts::{Contract, Exact, SchemaConforms};

#[derive(Contract)]
struct Producer {
    id: i64,
    extra: bool,
}

#[derive(Contract)]
struct Contract2 {
    id: String,
    note: bool,
}

const _: () = SchemaConforms::<Producer, Contract2, Exact>::CHECK;

fn main() {}
