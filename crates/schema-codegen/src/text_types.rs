//! Shared type-name normalization for text-based schema declarations (dbt
//! `schema.yml`, Erwin exports) — both describe columns with free-text type
//! names instead of Arrow's `DataType` enum, so they share one alias table
//! instead of each inventing their own.
#![cfg(any(feature = "dbt", feature = "erwin"))]

use arrow_schema::{DataType, Field};

fn primitive_from_token(token: &str) -> Option<DataType> {
    match token {
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

/// Case-insensitively strips `prefix` and a trailing `>` from `s`, returning
/// the text in between. Used to recognize `array<...>`/`struct<...>` without
/// a full parser dependency.
fn strip_wrapper<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    let head = s.get(..prefix.len())?;
    if !head.eq_ignore_ascii_case(prefix) {
        return None;
    }
    s[prefix.len()..].strip_suffix('>')
}

/// Splits `s` on commas that are not nested inside `<...>`, so a
/// `struct<a:int32,b:array<string>>` field list doesn't split on the comma
/// inside `array<string>`.
fn split_top_level_commas(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (index, ch) in s.char_indices() {
        match ch {
            '<' => depth += 1,
            '>' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                parts.push(&s[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

/// Maps a declared type name to the same Arrow type set the Parquet path
/// supports, plus two composite forms written in Spark/Hive-style notation:
/// `array<inner>` (-> `DataType::List`) and `struct<name:type,...>` (->
/// `DataType::Struct`), recursively. Primitive matching is case-insensitive;
/// a parenthesized precision/scale suffix (`"VARCHAR(50)"`) is ignored rather
/// than parsed, since none of it changes which Rust primitive the type maps
/// to. Nested struct fields are always nullable: dbt's `not_null` test is
/// only ever attached to a top-level column, so there is no signal to read
/// for a field nested inside a composite type.
pub(crate) fn declared_type_to_arrow(declared: &str) -> Option<DataType> {
    let trimmed = declared.trim();
    if let Some(inner) = strip_wrapper(trimmed, "array<") {
        let item_type = declared_type_to_arrow(inner)?;
        return Some(DataType::new_list(item_type, true));
    }
    if let Some(inner) = strip_wrapper(trimmed, "struct<") {
        let mut fields = Vec::new();
        for part in split_top_level_commas(inner) {
            let (name, ty) = part.split_once(':')?;
            fields.push(Field::new(name.trim(), declared_type_to_arrow(ty)?, true));
        }
        return Some(DataType::Struct(fields.into()));
    }
    let token = trimmed
        .split('(')
        .next()
        .unwrap_or(trimmed)
        .trim()
        .to_ascii_lowercase();
    primitive_from_token(&token)
}
