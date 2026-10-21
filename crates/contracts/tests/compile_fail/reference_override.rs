use contracts::{Contract, TypeShape};
struct Local;
impl Contract for Local {
    const SHAPE: TypeShape = TypeShape::Primitive("local");
}
impl Contract for &Local {
    const SHAPE: TypeShape = TypeShape::Primitive("reference");
}
fn main() {}
