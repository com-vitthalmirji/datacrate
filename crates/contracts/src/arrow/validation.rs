//! Prepared validation of supported physical schemas and active nested values.
use super::{MappingError, schema};
use crate::Contract;
use arrow_array::{Array, ListArray, MapArray, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, FieldRef, Schema};

/// A violation of a prepared contract's physical schema or required values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchValidationError {
    /// Field names, order, types or declared nullability differ.
    SchemaMismatch {
        /// Required physical schema; metadata is ignored by validation.
        expected: Box<Schema>,
        /// Actual physical schema.
        found: Box<Schema>,
    },
    /// An active required value is null.
    RequiredNull {
        /// Field path, with list items denoted by `[]`.
        path: String,
        /// Index within the array at this path, including collection offsets.
        row: usize,
    },
    /// An array does not expose the standard nested representation it declares.
    Representation {
        /// Field path.
        path: String,
    },
}
impl std::fmt::Display for BatchValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SchemaMismatch { expected, found } => {
                write!(f, "expected schema {expected}, found {found}")
            }
            Self::RequiredNull { path, row } => {
                write!(f, "required value at {path}[{row}] is null")
            }
            Self::Representation { path } => {
                write!(f, "unsupported nested array representation at {path}")
            }
        }
    }
}
impl std::error::Error for BatchValidationError {}

/// Reusable physical-schema and required-value checks prepared from a contract.
///
/// Checks metadata-independent schema equality and active nested nulls. Null parents
/// mask their children. This does not enforce business rules or map-key uniqueness.
/// It retains only the expected schema and validation plan, never an input batch.
#[derive(Debug)]
pub struct BatchValidator {
    schema: Schema,
    fields: Vec<Node>,
}
impl BatchValidator {
    /// Prepares all physical mappings before a source performs I/O.
    ///
    /// # Errors
    /// Returns a mapping error for unsupported or ambiguous declarations.
    pub fn new<C: Contract>() -> Result<Self, MappingError> {
        let schema = schema::<C>()?;
        let fields = schema
            .fields()
            .iter()
            .map(|field| Node::new(field.clone(), field.name().clone()))
            .collect();
        Ok(Self { schema, fields })
    }

    /// The expected physical schema, suitable for configuring Arrow builders.
    #[must_use]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Checks a batch without copying or retaining its arrays.
    ///
    /// # Errors
    /// Returns schema, required-null or nested-representation violations.
    pub fn validate(&self, batch: &RecordBatch) -> Result<(), BatchValidationError> {
        if !fields_match(self.schema.fields(), batch.schema().fields()) {
            return Err(BatchValidationError::SchemaMismatch {
                expected: Box::new(self.schema.clone()),
                found: Box::new(batch.schema().as_ref().clone()),
            });
        }
        for (node, array) in self.fields.iter().zip(batch.columns()) {
            for row in 0..batch.num_rows() {
                node.validate(array.as_ref(), row)?;
            }
        }
        Ok(())
    }
}

fn fields_match(expected: &[FieldRef], found: &[FieldRef]) -> bool {
    expected.len() == found.len() && expected.iter().zip(found).all(|(a, b)| field_matches(a, b))
}
fn field_matches(a: &Field, b: &Field) -> bool {
    a.name() == b.name()
        && a.is_nullable() == b.is_nullable()
        && match (a.data_type(), b.data_type()) {
            (DataType::Struct(a), DataType::Struct(b)) => fields_match(a, b),
            (DataType::List(a), DataType::List(b)) => field_matches(a, b),
            (DataType::Map(a, sa), DataType::Map(b, sb)) => sa == sb && field_matches(a, b),
            (a, b) => a == b,
        }
}

#[derive(Debug)]
struct Node {
    field: FieldRef,
    path: String,
    children: Children,
}
#[derive(Debug)]
enum Children {
    Leaf,
    Struct(Vec<Node>),
    List(Box<Node>),
    Map(Box<Node>),
}
impl Node {
    fn new(field: FieldRef, path: String) -> Self {
        let children = match field.data_type() {
            DataType::Struct(fields) => Children::Struct(
                fields
                    .iter()
                    .map(|f| Self::new(f.clone(), format!("{path}.{}", f.name())))
                    .collect(),
            ),
            DataType::List(item) => {
                Children::List(Box::new(Self::new(item.clone(), format!("{path}[]"))))
            }
            DataType::Map(entries, _) => {
                Children::Map(Box::new(Self::new(entries.clone(), path.clone())))
            }
            _ => Children::Leaf,
        };
        Self {
            field,
            path,
            children,
        }
    }
    fn validate(&self, array: &dyn Array, row: usize) -> Result<(), BatchValidationError> {
        if array.is_null(row) {
            return if self.field.is_nullable() {
                Ok(())
            } else {
                Err(BatchValidationError::RequiredNull {
                    path: self.path.clone(),
                    row,
                })
            };
        }
        let wrong_representation = || BatchValidationError::Representation {
            path: self.path.clone(),
        };
        match &self.children {
            Children::Leaf => {}
            Children::Struct(children) => {
                let array = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(wrong_representation)?;
                for (child, column) in children.iter().zip(array.columns()) {
                    child.validate(column.as_ref(), row)?;
                }
            }
            Children::List(item) => {
                let array = array
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(wrong_representation)?;
                let offsets = array.value_offsets();
                for index in offsets[row] as usize..offsets[row + 1] as usize {
                    item.validate(array.values().as_ref(), index)?;
                }
            }
            Children::Map(entry) => {
                let array = array
                    .as_any()
                    .downcast_ref::<MapArray>()
                    .ok_or_else(wrong_representation)?;
                let offsets = array.value_offsets();
                for index in offsets[row] as usize..offsets[row + 1] as usize {
                    entry.validate(array.entries(), index)?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        Int64Array, StringArray,
        builder::{Int64Builder, MapBuilder, StringBuilder},
    };
    use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use std::sync::Arc;

    #[test]
    fn required_value_calculation_rejects_nulls_without_unsafe_batches() {
        // Exercise the calculation directly: safe Arrow constructors reject many
        // malformed required-null batches before public validation can see them.
        let required = Arc::new(Field::new("id", DataType::Int64, false));
        let node = Node::new(required.clone(), "id".into());
        assert!(
            matches!(node.validate(&Int64Array::from(vec![None]),0), Err(BatchValidationError::RequiredNull { path, row:0 }) if path=="id")
        );
        let actual = StructArray::new(
            vec![Arc::new(required.as_ref().clone().with_nullable(true))].into(),
            vec![Arc::new(Int64Array::from(vec![None]))],
            None,
        );
        let node = Node::new(
            Arc::new(Field::new(
                "child",
                DataType::Struct(vec![required.clone()].into()),
                true,
            )),
            "child".into(),
        );
        assert!(
            matches!(node.validate(&actual,0), Err(BatchValidationError::RequiredNull { path, .. }) if path=="child.id")
        );
        let list = ListArray::new(
            Arc::new(required.as_ref().clone().with_nullable(true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
            Arc::new(Int64Array::from(vec![None])),
            None,
        );
        let node = Node::new(
            Arc::new(Field::new("items", DataType::List(required), false)),
            "items".into(),
        );
        assert!(
            matches!(node.validate(&list,0), Err(BatchValidationError::RequiredNull { path, .. }) if path=="items[]")
        );
        let mut builder = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        builder.keys().append_value("key");
        builder.values().append_null();
        builder.append(true).expect("entry");
        let map = builder.finish();
        let expected_entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int64, false),
                ]
                .into(),
            ),
            false,
        ));
        let node = Node::new(
            Arc::new(Field::new(
                "lookup",
                DataType::Map(expected_entries, false),
                false,
            )),
            "lookup".into(),
        );
        assert!(
            matches!(node.validate(&map,0), Err(BatchValidationError::RequiredNull { path, .. }) if path=="lookup.value")
        );
        let key = Arc::new(Field::new("key", DataType::Utf8, false));
        let entry = Node::new(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(vec![key.clone()].into()),
                false,
            )),
            "lookup".into(),
        );
        let actual = StructArray::new(
            vec![Arc::new(key.as_ref().clone().with_nullable(true))].into(),
            vec![Arc::new(StringArray::from(vec![None::<&str>]))],
            None,
        );
        assert!(
            matches!(entry.validate(&actual, 0), Err(BatchValidationError::RequiredNull { path, .. }) if path=="lookup.key")
        );
    }
    #[test]
    fn null_parent_and_invisible_list_values_are_not_checked() {
        let required = Arc::new(Field::new("item", DataType::Int64, false));
        let actual = ListArray::new(
            Arc::new(required.as_ref().clone().with_nullable(true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2, 3])),
            Arc::new(Int64Array::from(vec![None, Some(2), None])),
            Some(NullBuffer::from(vec![false, true, false])),
        );
        let node = Node::new(
            Arc::new(Field::new("items", DataType::List(required), true)),
            "items".into(),
        );
        node.validate(&actual, 0).expect("masked parent");
        node.validate(&actual.slice(1, 1), 0)
            .expect("visible slice excludes null elements");
    }
}
