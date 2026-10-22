//! Reads a Parquet file's Arrow schema from its footer and renders the
//! matching `#[derive(Contract, Connector)]` struct source text.
//!
//! The same reverse mapping runs from two call sites: `scaffold-cli` (a
//! developer seeding a starting struct once) and a pipeline crate's
//! `build.rs` (re-deriving the struct from the current reference file on
//! every build, to feed a compile-time `contracts::enforce` check). Keeping
//! the mapping here instead of duplicating it in both avoids two copies
//! drifting apart.

use arrow_schema::{DataType, Schema};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use std::fs::File;
use std::path::{Path, PathBuf};

mod text_types;

#[cfg(feature = "dbt")]
mod dbt;
#[cfg(feature = "dbt")]
pub use dbt::parse_dbt_schema_yml;

#[cfg(feature = "erwin")]
mod erwin;
#[cfg(feature = "erwin")]
pub use erwin::parse_erwin_xml;

/// A Parquet file's schema could not be read, or a field has no
/// corresponding Rust type in the supported primitive set.
#[derive(Debug)]
pub enum SchemaCodegenError {
    /// Opening the Parquet file at `path` failed.
    OpenFile {
        /// The file that could not be opened.
        path: PathBuf,
        /// The underlying I/O failure.
        source: std::io::Error,
    },
    /// Reading the file's Arrow schema from its footer failed.
    ReadSchema {
        /// The underlying Parquet failure.
        source: parquet::errors::ParquetError,
    },
    /// `field`'s Arrow type (`data_type`) has no mapping to a Rust primitive.
    UnsupportedArrowType {
        /// The field whose type has no Rust mapping.
        field: String,
        /// The Arrow type's debug representation.
        data_type: String,
    },
    /// `field`'s name is not a valid Rust identifier.
    InvalidFieldName {
        /// The offending field name.
        field: String,
    },
    /// `field`'s declared type (as text, from a dbt `schema.yml` or Erwin
    /// export) has no mapping to a Rust primitive.
    UnsupportedDeclaredType {
        /// The field whose declared type has no Rust mapping.
        field: String,
        /// The declared type, verbatim from the source document.
        declared_type: String,
    },
    /// No model with this name exists in the dbt `schema.yml` document.
    #[cfg(feature = "dbt")]
    ModelNotFound {
        /// The model name that was requested.
        model: String,
    },
    /// The dbt `schema.yml` document could not be parsed as YAML.
    #[cfg(feature = "dbt")]
    ParseDbtYaml {
        /// The underlying YAML failure.
        source: serde_yaml::Error,
    },
    /// No `Entity` with this name exists in the Erwin export.
    #[cfg(feature = "erwin")]
    EntityNotFound {
        /// The entity name that was requested.
        entity: String,
    },
    /// The Erwin export could not be parsed as XML.
    #[cfg(feature = "erwin")]
    ParseErwinXml {
        /// The underlying XML failure.
        source: roxmltree::Error,
    },
}

impl std::fmt::Display for SchemaCodegenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OpenFile { path, source } => {
                write!(
                    f,
                    "failed to open parquet file {}: {source}",
                    path.display()
                )
            }
            Self::ReadSchema { source } => write!(f, "failed to read parquet schema: {source}"),
            Self::UnsupportedArrowType { field, data_type } => write!(
                f,
                "field `{field}` has Arrow type {data_type}, which has no Rust mapping"
            ),
            Self::InvalidFieldName { field } => {
                write!(f, "field `{field}` is not a valid Rust identifier")
            }
            Self::UnsupportedDeclaredType {
                field,
                declared_type,
            } => write!(
                f,
                "field `{field}` declares type `{declared_type}`, which has no Rust mapping"
            ),
            #[cfg(feature = "dbt")]
            Self::ModelNotFound { model } => write!(f, "no model named `{model}` in schema.yml"),
            #[cfg(feature = "dbt")]
            Self::ParseDbtYaml { source } => write!(f, "failed to parse schema.yml: {source}"),
            #[cfg(feature = "erwin")]
            Self::EntityNotFound { entity } => {
                write!(f, "no Entity named `{entity}` in the Erwin export")
            }
            #[cfg(feature = "erwin")]
            Self::ParseErwinXml { source } => {
                write!(f, "failed to parse Erwin export: {source}")
            }
        }
    }
}

impl std::error::Error for SchemaCodegenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::OpenFile { source, .. } => Some(source),
            Self::ReadSchema { source } => Some(source),
            Self::UnsupportedArrowType { .. }
            | Self::InvalidFieldName { .. }
            | Self::UnsupportedDeclaredType { .. } => None,
            #[cfg(feature = "dbt")]
            Self::ModelNotFound { .. } => None,
            #[cfg(feature = "dbt")]
            Self::ParseDbtYaml { source } => Some(source),
            #[cfg(feature = "erwin")]
            Self::EntityNotFound { .. } => None,
            #[cfg(feature = "erwin")]
            Self::ParseErwinXml { source } => Some(source),
        }
    }
}

/// Reads `path`'s Arrow schema from its Parquet footer, without loading any
/// row data.
///
/// # Errors
/// Returns [`SchemaCodegenError::OpenFile`] if `path` cannot be opened, or
/// [`SchemaCodegenError::ReadSchema`] if its footer cannot be parsed.
pub fn read_parquet_schema(path: &Path) -> Result<Schema, SchemaCodegenError> {
    let file = File::open(path).map_err(|source| SchemaCodegenError::OpenFile {
        path: path.to_path_buf(),
        source,
    })?;
    let metadata = ArrowReaderMetadata::load(&file, ArrowReaderOptions::default())
        .map_err(|source| SchemaCodegenError::ReadSchema { source })?;
    Ok(metadata.schema().as_ref().clone())
}

fn rust_primitive(data_type: &DataType) -> Option<&'static str> {
    match data_type {
        DataType::Boolean => Some("bool"),
        DataType::Int8 => Some("i8"),
        DataType::Int16 => Some("i16"),
        DataType::Int32 => Some("i32"),
        DataType::Int64 => Some("i64"),
        DataType::UInt8 => Some("u8"),
        DataType::UInt16 => Some("u16"),
        DataType::UInt32 => Some("u32"),
        DataType::UInt64 => Some("u64"),
        DataType::Float32 => Some("f32"),
        DataType::Float64 => Some("f64"),
        DataType::Utf8 => Some("String"),
        _ => None,
    }
}

fn is_valid_rust_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Renders `#[derive(Contract, Connector)] struct <struct_name> { ... }`
/// matching `schema`, one field per Arrow field in declaration order, each
/// wrapped in `Option<T>` when the field is nullable.
///
/// # Errors
/// Returns [`SchemaCodegenError::InvalidFieldName`] if a field's name is not
/// a valid Rust identifier, or [`SchemaCodegenError::UnsupportedArrowType`]
/// if a field's Arrow type has no Rust mapping.
pub fn struct_source(struct_name: &str, schema: &Schema) -> Result<String, SchemaCodegenError> {
    let mut fields = String::new();
    for field in schema.fields() {
        if !is_valid_rust_identifier(field.name()) {
            return Err(SchemaCodegenError::InvalidFieldName {
                field: field.name().clone(),
            });
        }
        let Some(primitive) = rust_primitive(field.data_type()) else {
            return Err(SchemaCodegenError::UnsupportedArrowType {
                field: field.name().clone(),
                data_type: format!("{:?}", field.data_type()),
            });
        };
        let rust_type = if field.is_nullable() {
            format!("Option<{primitive}>")
        } else {
            primitive.to_string()
        };
        fields.push_str(&format!("    {}: {rust_type},\n", field.name()));
    }
    Ok(format!(
        "#[derive(Contract, Connector)]\nstruct {struct_name} {{\n{fields}}}\n"
    ))
}
