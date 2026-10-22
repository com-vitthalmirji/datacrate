//! Shared type-name normalization for text-based schema declarations (dbt
//! `schema.yml`, Erwin exports) — both describe columns with free-text type
//! names instead of Arrow's `DataType` enum, so they share one alias table
//! instead of each inventing their own.
#![cfg(any(feature = "dbt", feature = "erwin"))]

use arrow_schema::DataType;

/// Maps a declared type name to the same Arrow primitive set the Parquet
/// path supports. Case-insensitive; a parenthesized precision/scale suffix
/// (`"VARCHAR(50)"`) is ignored rather than parsed, since none of it changes
/// which Rust primitive the type maps to.
pub(crate) fn declared_type_to_arrow(declared: &str) -> Option<DataType> {
    let token = declared
        .split('(')
        .next()
        .unwrap_or(declared)
        .trim()
        .to_ascii_lowercase();
    match token.as_str() {
        "boolean" | "bool" => Some(DataType::Boolean),
        "int8" | "tinyint" => Some(DataType::Int8),
        "int16" | "smallint" => Some(DataType::Int16),
        "int32" | "integer" | "int" => Some(DataType::Int32),
        "int64" | "bigint" => Some(DataType::Int64),
        "uint8" => Some(DataType::UInt8),
        "uint16" => Some(DataType::UInt16),
        "uint32" => Some(DataType::UInt32),
        "uint64" => Some(DataType::UInt64),
        "float32" | "float" | "real" => Some(DataType::Float32),
        "float64" | "double" | "double precision" => Some(DataType::Float64),
        "string" | "varchar" | "text" | "character varying" | "char" => Some(DataType::Utf8),
        _ => None,
    }
}
