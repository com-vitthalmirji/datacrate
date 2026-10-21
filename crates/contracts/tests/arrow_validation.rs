#![cfg(feature = "arrow-validation")]
#![allow(dead_code)]
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use contracts::{
    Contract,
    arrow::{BatchValidationError, BatchValidator},
};
use std::sync::Arc;
#[derive(Contract)]
struct Row {
    id: i64,
}
#[test]
fn validates_real_batches_against_prepared_contract() {
    let validator = BatchValidator::new::<Row>().expect("mapping");
    let good = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .expect("batch");
    validator.validate(&good).expect("valid");
    let wrong = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "renamed",
            DataType::Int64,
            false,
        )])),
        good.columns().to_vec(),
    )
    .expect("batch");
    assert!(matches!(
        validator.validate(&wrong),
        Err(BatchValidationError::SchemaMismatch { .. })
    ));
}

use arrow_array::builder::{Int64Builder, MapBuilder, MapFieldNames, StringBuilder};
use arrow_array::{Array, ArrayRef, ListArray, MapArray, StringArray, StructArray};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use std::collections::{BTreeMap, HashMap};
#[derive(Contract)]
struct Child {
    id: i64,
}
#[derive(Contract)]
struct Nested {
    child: Option<Child>,
    items: Vec<Option<i64>>,
    lookup: BTreeMap<String, Option<i64>>,
}
fn map() -> MapArray {
    let names = MapFieldNames {
        entry: "entries".into(),
        key: "key".into(),
        value: "value".into(),
    };
    let mut b = MapBuilder::new(Some(names), StringBuilder::new(), Int64Builder::new());
    b.keys().append_value("before");
    b.values().append_value(1);
    b.append(true).expect("map");
    b.keys().append_value("visible");
    b.values().append_null();
    b.append(true).expect("map");
    b.finish()
}
#[test]
fn nested_parent_masks_null_children_and_slices_preserve_visible_offsets() {
    let validator = BatchValidator::new::<Nested>().expect("mapping");
    let child = StructArray::try_new(
        vec![Arc::new(Field::new("id", DataType::Int64, false))].into(),
        vec![Arc::new(Int64Array::from(vec![Some(7), None]))],
        Some(NullBuffer::from(vec![true, false])),
    )
    .expect("masked child");
    let items = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 3])),
        Arc::new(Int64Array::from(vec![Some(99), None, Some(2)])),
        None,
    )
    .expect("list");
    let columns: Vec<ArrayRef> = vec![
        Arc::new(child.slice(1, 1)),
        Arc::new(items.slice(1, 1)),
        Arc::new(map().slice(1, 1)),
    ];
    let batch =
        RecordBatch::try_new(Arc::new(validator.schema().clone()), columns).expect("nested batch");
    validator
        .validate(&batch)
        .expect("masked child, nullable item and map value");
    validator
        .validate(&RecordBatch::new_empty(Arc::new(
            validator.schema().clone(),
        )))
        .expect("empty");
}
#[test]
fn ignores_schema_and_field_metadata_without_ignoring_nullability() {
    let validator = BatchValidator::new::<Row>().expect("mapping");
    let metadata = HashMap::from([("producer".into(), "fixture".into())]);
    let field = Field::new("id", DataType::Int64, false).with_metadata(metadata.clone());
    let schema = Schema::new(vec![field]).with_metadata(metadata);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Int64Array::from(vec![1]))])
        .expect("batch");
    validator.validate(&batch).expect("metadata ignored");
    let nullable = Schema::new(vec![Field::new("id", DataType::Int64, true)]);
    let batch = RecordBatch::try_new(Arc::new(nullable), batch.columns().to_vec()).expect("batch");
    assert!(matches!(
        validator.validate(&batch),
        Err(BatchValidationError::SchemaMismatch { .. })
    ));
}
#[test]
fn safe_arrow_constructors_reject_unmasked_required_nulls_first() {
    let required = Arc::new(Field::new("id", DataType::Int64, false));
    let nulls: ArrayRef = Arc::new(Int64Array::from(vec![None]));
    assert!(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![required.clone()])),
            vec![nulls.clone()]
        )
        .is_err()
    );
    assert!(
        StructArray::try_new(vec![required.clone()].into(), vec![nulls.clone()], None).is_err()
    );
    assert!(
        ListArray::try_new(
            required.clone(),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
            nulls,
            None
        )
        .is_err()
    );
    let nullable_key = Arc::new(Field::new("key", DataType::Utf8, true));
    let value = Arc::new(Field::new("value", DataType::Int64, true));
    let entries = StructArray::try_new(
        vec![nullable_key, value].into(),
        vec![
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(Int64Array::from(vec![None])),
        ],
        None,
    )
    .expect("nullable entries");
    assert!(
        MapArray::try_new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
            entries,
            None,
            false
        )
        .is_err()
    );
}
