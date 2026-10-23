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
use quote::{format_ident, quote};
use std::fs::File;
use std::path::{Path, PathBuf};

mod dataframe_ops;
pub use dataframe_ops::{AggExpr, AggFn, Comparator, DfOp, Literal, SortKey};
mod text_types;

#[cfg(feature = "dbt")]
mod dbt;
#[cfg(feature = "dbt")]
pub use dbt::parse_dbt_schema_yml;

#[cfg(feature = "erwin")]
mod erwin;
#[cfg(feature = "erwin")]
pub use erwin::parse_erwin_xml;

#[cfg(feature = "delta")]
mod delta;
#[cfg(feature = "delta")]
pub use delta::read_delta_schema;

mod pipeline_template;
pub use pipeline_template::{
    PipelineSpec, SourceData, SourceFormat, TransformSpec, pipeline_source,
};

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
    /// The `quote!`-built token stream for `struct_name` could not be parsed
    /// back into a `syn::File`. This indicates a bug in the token
    /// construction, not a problem with the input schema.
    RenderStruct {
        /// The struct that failed to render.
        struct_name: String,
        /// The underlying parse failure.
        source: syn::Error,
    },
    /// The `quote!`-built token stream for a generated pipeline's `main()`
    /// could not be parsed back into a `syn::File`. This indicates a bug in
    /// the token construction, not a problem with the input spec.
    RenderMain {
        /// The underlying parse failure.
        source: syn::Error,
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
    /// `uri` is not a valid URL (Delta tables are addressed by URL, e.g.
    /// `file:///path/to/table` or `s3://bucket/key`, not a bare path).
    #[cfg(feature = "delta")]
    InvalidDeltaUri {
        /// The offending URI.
        uri: String,
        /// The underlying parse failure.
        source: url::ParseError,
    },
    /// The current-thread runtime used to drive Delta/DataFusion calls
    /// could not be built.
    #[cfg(feature = "delta")]
    DeltaRuntime {
        /// The underlying I/O failure.
        source: std::io::Error,
    },
    /// The Delta table at a given URI could not be opened.
    #[cfg(feature = "delta")]
    OpenDeltaTable {
        /// The underlying Delta failure.
        source: deltalake::errors::DeltaTableError,
    },
    /// The Delta table's DataFusion table provider could not be built.
    #[cfg(feature = "delta")]
    DeltaTableProvider {
        /// The underlying DataFusion failure.
        source: datafusion::error::DataFusionError,
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
            Self::RenderStruct {
                struct_name,
                source,
            } => write!(f, "failed to render struct `{struct_name}`: {source}"),
            Self::RenderMain { source } => {
                write!(f, "failed to render pipeline main(): {source}")
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
            #[cfg(feature = "delta")]
            Self::InvalidDeltaUri { uri, source } => {
                write!(f, "`{uri}` is not a valid Delta table URL: {source}")
            }
            #[cfg(feature = "delta")]
            Self::DeltaRuntime { source } => {
                write!(f, "failed to build Delta runtime: {source}")
            }
            #[cfg(feature = "delta")]
            Self::OpenDeltaTable { source } => {
                write!(f, "failed to open Delta table: {source}")
            }
            #[cfg(feature = "delta")]
            Self::DeltaTableProvider { source } => {
                write!(f, "failed to build Delta table provider: {source}")
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
            Self::RenderStruct { source, .. } => Some(source),
            Self::RenderMain { source } => Some(source),
            #[cfg(feature = "dbt")]
            Self::ModelNotFound { .. } => None,
            #[cfg(feature = "dbt")]
            Self::ParseDbtYaml { source } => Some(source),
            #[cfg(feature = "erwin")]
            Self::EntityNotFound { .. } => None,
            #[cfg(feature = "erwin")]
            Self::ParseErwinXml { source } => Some(source),
            #[cfg(feature = "delta")]
            Self::InvalidDeltaUri { source, .. } => Some(source),
            #[cfg(feature = "delta")]
            Self::DeltaRuntime { source } => Some(source),
            #[cfg(feature = "delta")]
            Self::OpenDeltaTable { source } => Some(source),
            #[cfg(feature = "delta")]
            Self::DeltaTableProvider { source } => Some(source),
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

/// The same mapping as [`rust_primitive`], parsed into a [`syn::Type`] so
/// callers can splice it directly into a `quote!` field without going
/// through a string.
pub(crate) fn rust_type(data_type: &DataType) -> Option<syn::Type> {
    let primitive = rust_primitive(data_type)?;
    syn::parse_str(primitive).ok()
}

fn is_valid_rust_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn to_pascal_case(name: &str) -> String {
    name.split(['_', '-'])
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

/// Resolves one field's Rust type, recursing into `List`/`Struct` Arrow
/// types. A `Struct` renders its own `#[derive(Contract)] struct` (pushed
/// onto `nested_items`, innermost-first) and is referenced by `type_hint`,
/// which doubles as that nested struct's generated name; a `List` becomes
/// `Vec<T>` over its own recursively-resolved item type. `error_name` is the
/// dotted field path used only for error messages, kept separate from
/// `type_hint` so a leaf-level error on a non-nested field still names the
/// plain field (not a synthesized PascalCase type name).
fn field_type_tokens(
    error_name: &str,
    type_hint: &str,
    data_type: &DataType,
    nested_items: &mut Vec<proc_macro2::TokenStream>,
) -> Result<proc_macro2::TokenStream, SchemaCodegenError> {
    match data_type {
        DataType::List(inner) | DataType::LargeList(inner) => {
            let item_error = format!("{error_name}[]");
            let item_hint = format!("{type_hint}Item");
            let item_type =
                field_type_tokens(&item_error, &item_hint, inner.data_type(), nested_items)?;
            Ok(quote! { Vec<#item_type> })
        }
        DataType::Struct(inner_fields) => {
            let struct_ident = format_ident!("{type_hint}");
            let mut rendered = Vec::with_capacity(inner_fields.len());
            for field in inner_fields.iter() {
                if !is_valid_rust_identifier(field.name()) {
                    return Err(SchemaCodegenError::InvalidFieldName {
                        field: format!("{error_name}.{}", field.name()),
                    });
                }
                let field_error = format!("{error_name}.{}", field.name());
                let field_hint = format!("{type_hint}{}", to_pascal_case(field.name()));
                let field_type =
                    field_type_tokens(&field_error, &field_hint, field.data_type(), nested_items)?;
                let field_ident = format_ident!("{}", field.name());
                rendered.push(if field.is_nullable() {
                    quote! { #field_ident: Option<#field_type> }
                } else {
                    quote! { #field_ident: #field_type }
                });
            }
            nested_items.push(quote! {
                #[derive(Contract)]
                struct #struct_ident {
                    #(#rendered),*
                }
            });
            Ok(quote! { #struct_ident })
        }
        other => {
            let field_type =
                rust_type(other).ok_or_else(|| SchemaCodegenError::UnsupportedArrowType {
                    field: error_name.to_string(),
                    data_type: format!("{other:?}"),
                })?;
            Ok(quote! { #field_type })
        }
    }
}

/// Renders `#[derive(Contract, Connector)] struct <struct_name> { ... }`
/// matching `schema`, one field per Arrow field in declaration order, each
/// wrapped in `Option<T>` when the field is nullable. A `List`/`Struct`
/// field renders as `Vec<T>`/a separately-named nested `#[derive(Contract)]`
/// struct (emitted before `struct_name`'s own definition), recursively.
///
/// # Errors
/// Returns [`SchemaCodegenError::InvalidFieldName`] if a field's name (at any
/// nesting depth) is not a valid Rust identifier, or
/// [`SchemaCodegenError::UnsupportedArrowType`] if a field's Arrow type has
/// no Rust mapping.
pub fn struct_source(struct_name: &str, schema: &Schema) -> Result<String, SchemaCodegenError> {
    let struct_ident = format_ident!("{struct_name}");
    let mut nested_items = Vec::new();
    let mut fields = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        if !is_valid_rust_identifier(field.name()) {
            return Err(SchemaCodegenError::InvalidFieldName {
                field: field.name().clone(),
            });
        }
        let type_hint = format!("{struct_name}{}", to_pascal_case(field.name()));
        let field_type = field_type_tokens(
            field.name(),
            &type_hint,
            field.data_type(),
            &mut nested_items,
        )?;
        let field_ident = format_ident!("{}", field.name());
        fields.push(if field.is_nullable() {
            quote! { #field_ident: Option<#field_type> }
        } else {
            quote! { #field_ident: #field_type }
        });
    }
    let tokens = quote! {
        #(#nested_items)*
        #[derive(Contract, Connector)]
        struct #struct_ident {
            #(#fields),*
        }
    };
    let file: syn::File =
        syn::parse2(tokens).map_err(|source| SchemaCodegenError::RenderStruct {
            struct_name: struct_name.to_string(),
            source,
        })?;
    Ok(prettyplease::unparse(&file))
}
