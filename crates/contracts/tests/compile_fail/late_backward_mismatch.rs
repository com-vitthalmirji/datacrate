use contracts::{Backward, Contract, SchemaConforms};
#[derive(Contract)]
struct Producer { id: String }
#[derive(Contract)]
struct Target {
    a: Option<i64>, b: Option<i64>, c: Option<i64>, d: Option<i64>,
    e: Option<i64>, f: Option<i64>, g: Option<i64>, h: Option<i64>,
    id: i64,
}
const _: () = SchemaConforms::<Producer, Target, Backward>::CHECK;
fn main() {}
