use contracts::{FieldShape, SchemaPolicy, TypeShape, conforms};
const ID: TypeShape = TypeShape::Primitive("i64");
const ONE: TypeShape = TypeShape::Struct(&[FieldShape {
    name: "id",
    shape: ID,
    has_default: false,
}]);
const COLLISION: TypeShape = TypeShape::Struct(&[
    FieldShape {
        name: "id",
        shape: ID,
        has_default: false,
    },
    FieldShape {
        name: "ID",
        shape: TypeShape::Primitive("String"),
        has_default: false,
    },
]);
#[test]
fn ambiguous_names_are_rejected_on_either_side() {
    for policy in [SchemaPolicy::Exact, SchemaPolicy::ExactUnorderedCI] {
        assert!(!conforms(COLLISION, ONE, policy));
        assert!(!conforms(ONE, COLLISION, policy));
    }
    assert!(conforms(COLLISION, ONE, SchemaPolicy::Full));
}
#[test]
fn ambiguity_is_checked_inside_containers() {
    let pairs = [
        (
            TypeShape::Struct(&[FieldShape {
                name: "nested",
                shape: COLLISION,
                has_default: false,
            }]),
            TypeShape::Struct(&[FieldShape {
                name: "nested",
                shape: ONE,
                has_default: false,
            }]),
        ),
        (TypeShape::Sequence(&COLLISION), TypeShape::Sequence(&ONE)),
        (TypeShape::Map(&ID, &COLLISION), TypeShape::Map(&ID, &ONE)),
    ];
    for (producer, target) in pairs {
        // Container fields exercise the policy-aware recursive walk.
        let p = Box::leak(Box::new([FieldShape {
            name: "value",
            shape: producer,
            has_default: false,
        }]));
        let c = Box::leak(Box::new([FieldShape {
            name: "value",
            shape: target,
            has_default: false,
        }]));
        assert!(!conforms(
            TypeShape::Struct(p),
            TypeShape::Struct(c),
            SchemaPolicy::Exact
        ));
    }
}
#[test]
fn positional_policies_do_not_confuse_duplicate_names_with_ambiguity() {
    assert!(conforms(COLLISION, COLLISION, SchemaPolicy::ExactOrderedCI));
    assert!(conforms(
        COLLISION,
        COLLISION,
        SchemaPolicy::ExactByPosition
    ));
}
#[test]
fn case_sensitive_matching_rejects_only_exact_duplicates() {
    const DUP: TypeShape = TypeShape::Struct(&[
        FieldShape {
            name: "id",
            shape: ID,
            has_default: false,
        },
        FieldShape {
            name: "id",
            shape: ID,
            has_default: false,
        },
    ]);
    assert!(!conforms(DUP, ONE, SchemaPolicy::Backward));
    assert!(!conforms(ONE, DUP, SchemaPolicy::Forward));
    assert!(conforms(COLLISION, COLLISION, SchemaPolicy::Backward));
}
#[test]
fn case_folding_is_ascii_only() {
    const LOWER: TypeShape = TypeShape::Struct(&[FieldShape {
        name: "é",
        shape: ID,
        has_default: false,
    }]);
    const UPPER: TypeShape = TypeShape::Struct(&[FieldShape {
        name: "É",
        shape: ID,
        has_default: false,
    }]);
    assert!(conforms(LOWER, LOWER, SchemaPolicy::Exact));
    assert!(!conforms(LOWER, UPPER, SchemaPolicy::Exact));
}
