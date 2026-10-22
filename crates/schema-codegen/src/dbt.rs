//! Parses a dbt `schema.yml` document into the same `arrow_schema::Schema`
//! shape the Parquet path produces, so `struct_source` renders both the
//! same way. dbt has no explicit nullability field on a column — a column
//! is treated as nullable unless it carries a `not_null` test, since that
//! is the signal a steward actually writes in practice.
#![cfg(feature = "dbt")]

use crate::SchemaCodegenError;
use crate::text_types::declared_type_to_arrow;
use arrow_schema::{Field, Schema};
use serde::Deserialize;

#[derive(Deserialize)]
struct SchemaFile {
    #[serde(default)]
    models: Vec<Model>,
}

#[derive(Deserialize)]
struct Model {
    name: String,
    #[serde(default)]
    columns: Vec<Column>,
}

#[derive(Deserialize)]
struct Column {
    name: String,
    data_type: Option<String>,
    #[serde(default)]
    tests: Vec<serde_yaml::Value>,
}

fn is_not_null_test(test: &serde_yaml::Value) -> bool {
    match test {
        serde_yaml::Value::String(name) => name == "not_null",
        serde_yaml::Value::Mapping(map) => map.keys().any(|key| key.as_str() == Some("not_null")),
        _ => false,
    }
}

/// Parses `yaml` (a dbt `schema.yml` document) and returns the Arrow schema
/// for the model named `model_name`.
///
/// # Errors
/// Returns [`SchemaCodegenError::ParseDbtYaml`] if `yaml` is not valid YAML
/// or doesn't match dbt's `schema.yml` shape, [`SchemaCodegenError::ModelNotFound`]
/// if no model named `model_name` exists, or
/// [`SchemaCodegenError::UnsupportedDeclaredType`] if a column's `data_type`
/// is missing or has no Rust mapping.
pub fn parse_dbt_schema_yml(yaml: &str, model_name: &str) -> Result<Schema, SchemaCodegenError> {
    let file: SchemaFile =
        serde_yaml::from_str(yaml).map_err(|source| SchemaCodegenError::ParseDbtYaml { source })?;
    let model = file
        .models
        .into_iter()
        .find(|model| model.name == model_name)
        .ok_or_else(|| SchemaCodegenError::ModelNotFound {
            model: model_name.to_string(),
        })?;

    model
        .columns
        .into_iter()
        .map(|column| {
            let declared = column.data_type.unwrap_or_else(|| "<none>".to_string());
            let data_type = declared_type_to_arrow(&declared).ok_or_else(|| {
                SchemaCodegenError::UnsupportedDeclaredType {
                    field: column.name.clone(),
                    declared_type: declared.clone(),
                }
            })?;
            let nullable = !column.tests.iter().any(is_not_null_test);
            Ok(Field::new(column.name, data_type, nullable))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Schema::new)
}
