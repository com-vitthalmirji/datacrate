use contracts::{Contract, Exact, SchemaConforms};

#[derive(Contract)]
struct Address {
    city: String,
    zip: i64,
}

#[derive(Contract)]
struct AddressZipMismatch {
    city: String,
    zip: String,
}

#[derive(Contract)]
struct Producer {
    id: i64,
    shipTo: Address,
}

#[derive(Contract)]
struct Contract2 {
    id: i64,
    shipTo: AddressZipMismatch,
}

const _: () = SchemaConforms::<Producer, Contract2, Exact>::CHECK;

fn main() {}
