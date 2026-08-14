use contracts::Contract;

#[derive(Contract)]
struct Generic<T> {
    v: T,
}

fn main() {}
