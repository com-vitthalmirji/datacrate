//! Maps declarations to Arrow schemas. The opt-in `arrow-validation` feature
//! also checks actual batch schemas and nested required values.
//!
//! Lists use 32-bit offsets and an `item` field. Maps use unsorted `entries`
//! structs with non-null `key` and potentially nullable `value` fields.
//! Adjacent optional layers are refused because one Arrow validity bit cannot
//! distinguish them. Default annotations do not change physical nullability.
use crate::{Contract, TypeShape};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

/// A physical representation that cannot be inferred from a declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MappingError {
    /// A schema requires a non-optional root struct.
    RootNotStruct,
    /// No unambiguous Arrow representation is defined for this primitive.
    UnsupportedPrimitive {
        /// Field path, including list/map segments.
        path: String,
        /// Declared primitive label.
        primitive: &'static str,
    },
    /// Adjacent optional layers would lose information.
    NestedOptional {
        /// Field path.
        path: String,
    },
    /// Arrow map keys cannot be nullable or structured.
    InvalidMapKey {
        /// Map key path.
        path: String,
    },
    /// Duplicate names cannot identify distinct struct fields reliably.
    DuplicateField {
        /// Duplicate field path.
        path: String,
    },
    /// A manually supplied shape is cyclic or exceeds the supported depth of 128.
    DepthLimit {
        /// Field path at the limit.
        path: String,
    },
}
impl std::fmt::Display for MappingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "contract to Arrow mapping failed: {self:?}")
    }
}
impl std::error::Error for MappingError {}

/// Builds an ordered Arrow schema for a concrete contract.
///
/// This is schema construction, not evidence that arrays contain valid values.
/// Timestamp units/timezones, decimal precision/scale, platform-sized integers,
/// chars and 128-bit integers are not inferred from primitive labels.
///
/// # Errors
/// Returns a typed mapping error for ambiguous or unsupported representations.
pub fn schema<T: Contract>() -> Result<Schema, MappingError> {
    let TypeShape::Struct(fields) = T::SHAPE else {
        return Err(MappingError::RootNotStruct);
    };
    Ok(Schema::new(struct_fields(fields, "", 0)?))
}

/// Maps one named shape for configuring an Arrow array builder or nested field.
///
/// # Errors
/// Returns a typed mapping error when the representation is unsupported.
pub fn field(name: &str, shape: TypeShape) -> Result<Field, MappingError> {
    map_field(name, shape, name, 0)
}

fn struct_fields(
    fields: &[crate::FieldShape],
    path: &str,
    depth: usize,
) -> Result<Vec<Field>, MappingError> {
    let mut mapped = Vec::with_capacity(fields.len());
    for (index, field) in fields.iter().enumerate() {
        let path = if path.is_empty() {
            field.name.to_owned()
        } else {
            format!("{path}.{}", field.name)
        };
        if fields[..index].iter().any(|prior| prior.name == field.name) {
            return Err(MappingError::DuplicateField { path });
        }
        mapped.push(map_field(field.name, field.shape, &path, depth + 1)?);
    }
    Ok(mapped)
}

fn map_field(
    name: &str,
    shape: TypeShape,
    path: &str,
    depth: usize,
) -> Result<Field, MappingError> {
    if depth > 128 {
        return Err(MappingError::DepthLimit {
            path: path.to_owned(),
        });
    }
    let (shape, nullable) = match shape {
        TypeShape::Optional(inner) => (*inner, true),
        shape => (shape, false),
    };
    let data_type = match shape {
        TypeShape::Optional(_) => {
            return Err(MappingError::NestedOptional {
                path: path.to_owned(),
            });
        }
        TypeShape::Primitive(primitive) => match primitive {
            "bool" => DataType::Boolean,
            "i8" => DataType::Int8,
            "i16" => DataType::Int16,
            "i32" => DataType::Int32,
            "i64" => DataType::Int64,
            "u8" => DataType::UInt8,
            "u16" => DataType::UInt16,
            "u32" => DataType::UInt32,
            "u64" => DataType::UInt64,
            "f32" => DataType::Float32,
            "f64" => DataType::Float64,
            "str" | "String" => DataType::Utf8,
            _ => {
                return Err(MappingError::UnsupportedPrimitive {
                    path: path.to_owned(),
                    primitive,
                });
            }
        },
        TypeShape::Sequence(item) => DataType::List(Arc::new(map_field(
            "item",
            *item,
            &format!("{path}[]"),
            depth + 1,
        )?)),
        TypeShape::Struct(fields) => DataType::Struct(struct_fields(fields, path, depth)?.into()),
        TypeShape::Map(key, value) => {
            if !matches!(
                key,
                TypeShape::Primitive(
                    "bool"
                        | "String"
                        | "i8"
                        | "i16"
                        | "i32"
                        | "i64"
                        | "i128"
                        | "u8"
                        | "u16"
                        | "u32"
                        | "u64"
                        | "u128"
                        | "isize"
                        | "usize"
                )
            ) {
                return Err(MappingError::InvalidMapKey {
                    path: format!("{path}.key"),
                });
            }
            let key = map_field("key", *key, &format!("{path}.key"), depth + 1)?;
            let value = map_field("value", *value, &format!("{path}.value"), depth + 1)?;
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(vec![key, value].into()),
                    false,
                )),
                false,
            )
        }
    };
    Ok(Field::new(name, data_type, nullable))
}

#[cfg(feature = "arrow-validation")]
mod validation;
#[cfg(feature = "arrow-validation")]
pub use validation::{BatchValidationError, BatchValidator};
