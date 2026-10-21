#![cfg(feature = "arrow")]
#![allow(dead_code)]
use arrow_schema::{DataType, Field, Schema};
use contracts::{
    Contract, FieldShape, TypeShape,
    arrow::{MappingError, schema},
};
use std::{collections::BTreeMap, sync::Arc};
#[derive(Contract)]
struct Child {
    count: i32,
}
#[derive(Contract)]
struct Nested {
    child: Option<Child>,
    items: Vec<Option<String>>,
    lookup: BTreeMap<String, Option<i64>>,
    #[contract(default)]
    required: bool,
}
#[test]
fn nested_struct_list_and_map_have_independent_validity() {
    let expected = Schema::new(vec![
        Field::new(
            "child",
            DataType::Struct(vec![Field::new("count", DataType::Int32, false)].into()),
            true,
        ),
        Field::new(
            "items",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new(
            "lookup",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Int64, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            false,
        ),
        Field::new("required", DataType::Boolean, false),
    ]);
    assert_eq!(
        schema::<Nested>().expect("supported nested schema"),
        expected
    );
}
macro_rules! manual {
    ($name:ident, $shape:expr) => {
        struct $name;
        impl Contract for $name {
            const SHAPE: TypeShape = $shape;
        }
    };
}
manual!(
    NestedOption,
    TypeShape::Struct(&[FieldShape {
        name: "value",
        shape: TypeShape::Optional(&TypeShape::Optional(&TypeShape::Primitive("i64"))),
        has_default: false
    }])
);
manual!(
    BadMap,
    TypeShape::Struct(&[FieldShape {
        name: "map",
        shape: TypeShape::Map(
            &TypeShape::Optional(&TypeShape::Primitive("String")),
            &TypeShape::Primitive("i64")
        ),
        has_default: false
    }])
);
manual!(
    Duplicate,
    TypeShape::Struct(
        &[FieldShape {
            name: "x",
            shape: TypeShape::Primitive("i64"),
            has_default: false
        }; 2]
    )
);
static CYCLE: TypeShape = TypeShape::Sequence(&CYCLE);
manual!(
    Cycle,
    TypeShape::Struct(&[FieldShape {
        name: "cycle",
        shape: CYCLE,
        has_default: false
    }])
);
#[test]
fn ambiguous_shapes_return_typed_paths() {
    assert_eq!(
        schema::<NestedOption>(),
        Err(MappingError::NestedOptional {
            path: "value".into()
        })
    );
    assert_eq!(
        schema::<BadMap>(),
        Err(MappingError::InvalidMapKey {
            path: "map.key".into()
        })
    );
    assert_eq!(
        schema::<Duplicate>(),
        Err(MappingError::DuplicateField { path: "x".into() })
    );
    assert!(matches!(
        schema::<Cycle>(),
        Err(MappingError::DepthLimit { .. })
    ));
    assert_eq!(schema::<i64>(), Err(MappingError::RootNotStruct));
}
#[test]
fn primitive_mapping_is_explicit_and_does_not_infer_metadata() {
    // Manual labels test the adapter independently of derive and standard impls.
    for (label, expected) in [
        ("bool", DataType::Boolean),
        ("i8", DataType::Int8),
        ("i16", DataType::Int16),
        ("i32", DataType::Int32),
        ("i64", DataType::Int64),
        ("u8", DataType::UInt8),
        ("u16", DataType::UInt16),
        ("u32", DataType::UInt32),
        ("u64", DataType::UInt64),
        ("f32", DataType::Float32),
        ("f64", DataType::Float64),
        ("String", DataType::Utf8),
        ("str", DataType::Utf8),
    ] {
        assert_eq!(
            contracts::arrow::field("value", TypeShape::Primitive(label))
                .expect("supported scalar"),
            Field::new("value", expected, false)
        );
    }
    for label in [
        "usize",
        "isize",
        "i128",
        "u128",
        "char",
        "Decimal",
        "Timestamp",
        "unknown",
    ] {
        assert_eq!(
            contracts::arrow::field("value", TypeShape::Primitive(label)),
            Err(MappingError::UnsupportedPrimitive {
                path: "value".into(),
                primitive: label
            })
        );
    }
}
