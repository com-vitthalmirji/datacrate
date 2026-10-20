#![allow(dead_code)]
use contracts::{conforms, Contract, SchemaPolicy, TypeShape};

struct Local;
impl Contract for Local {
    const SHAPE: TypeShape = TypeShape::Primitive("local");
}

// In 0.1 this separate shape could be implemented directly for &Local.
// In 0.2 a newtype owns that distinction without changing reference semantics.
struct BorrowedContract<'a>(&'a Local);
impl Contract for BorrowedContract<'_> {
    const SHAPE: TypeShape = TypeShape::Primitive("reference");
}

type Shared = &'static Local;
type Mutable = &'static mut Local;
type Reborrowed = &'static Shared;
type Distinct = BorrowedContract<'static>;

#[derive(Contract)]
struct References {
    shared: Shared,
    mutable: Mutable,
    reborrowed: Reborrowed,
    optional: Option<Shared>,
    distinct: Distinct,
}

const _: () = {
    assert!(conforms(Shared::SHAPE, Local::SHAPE, SchemaPolicy::Exact));
    assert!(conforms(Mutable::SHAPE, Local::SHAPE, SchemaPolicy::Exact));
    assert!(conforms(Reborrowed::SHAPE, Local::SHAPE, SchemaPolicy::Exact));
    assert!(conforms(Distinct::SHAPE, TypeShape::Primitive("reference"), SchemaPolicy::Exact));
    assert!(!conforms(Distinct::SHAPE, Local::SHAPE, SchemaPolicy::Exact));
    let TypeShape::Struct(fields) = References::SHAPE else { panic!("expected struct") };
    assert!(conforms(fields[0].shape, Local::SHAPE, SchemaPolicy::Exact));
    assert!(conforms(fields[1].shape, Local::SHAPE, SchemaPolicy::Exact));
    assert!(conforms(fields[2].shape, Local::SHAPE, SchemaPolicy::Exact));
    assert!(conforms(fields[3].shape, TypeShape::Optional(&Local::SHAPE), SchemaPolicy::Exact));
    assert!(conforms(fields[4].shape, Distinct::SHAPE, SchemaPolicy::Exact));
};
fn main() {}
