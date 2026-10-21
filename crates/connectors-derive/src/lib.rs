//! `#[derive(Connector)]`: the registration half of what `#[derive(Contract)]`
//! already provides for schema shape. Generates `connector_schema()`,
//! `register_memory()`/`register_parquet()`/`register_csv()`, and one
//! `col_<field>()` accessor per named field, for a struct that already
//! derives `contracts::Contract`, reusing `contracts::arrow`'s `TypeShape` ->
//! Arrow `Schema` mapping and DataFusion's own `MemTable`/`ListingTable`
//! instead of generating a `TableProvider` per struct.
//!
//! `register_parquet`/`register_csv` pass the derived schema into
//! `ListingTableConfig::with_schema` explicitly, so the contract's shape is
//! authoritative over the file's own inferred schema (schema-on-write,
//! rather than DataFusion's schema-on-read default) — a mismatch surfaces as
//! a scan-time error instead of silently trusting whatever the file says.
//!
//! Sinks (`write_parquet`/`write_csv`) are deliberately not generated here:
//! a sink operates on a `DataFrame` a query already produced, not on `Self`,
//! so there is nothing struct-specific to derive. `pipeline::write_parquet`
//! already covers the Parquet case.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DataStruct, DeriveInput, Fields, parse_macro_input};

/// Derives `connector_schema()`, `register_memory()`, `register_parquet()`,
/// `register_csv()`, and one `col_<field>()` accessor per named field, on a
/// struct that already derives `contracts::Contract`.
///
/// `connector_schema()` maps the struct's `TypeShape` to an Arrow `Schema`
/// via `contracts::arrow::schema`. `register_memory()` wraps a
/// `Vec<RecordBatch>` in a `datafusion::datasource::MemTable` built from that
/// schema and registers it under a name on a `SessionContext`.
/// `register_parquet()`/`register_csv()` register a `ListingTable` over a
/// file path instead, with the derived schema set explicitly. `col_<field>()`
/// returns `datafusion::logical_expr::col("<field>")`, so a renamed Rust
/// field renames the generated accessor and any stale call site fails to
/// compile instead of referencing a dropped column name.
#[proc_macro_derive(Connector)]
pub fn derive_connector(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    if !input.generics.params.is_empty() {
        return syn::Error::new_spanned(
            &input.generics,
            "Connector derive supports only concrete structs; generic parameters and lifetimes are not supported",
        )
        .to_compile_error()
        .into();
    }

    let fields = match &input.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(fields),
            ..
        }) => fields,
        _ => {
            return syn::Error::new_spanned(
                &input,
                "Connector can only be derived for structs with named fields",
            )
            .to_compile_error()
            .into();
        }
    };

    let col_accessors = fields.named.iter().map(|field| {
        let field_name = field
            .ident
            .as_ref()
            .expect("named fields always carry an identifier");
        let field_name_str = field_name.to_string();
        let accessor_name = format_ident!("col_{field_name}");
        quote! {
            /// Returns `datafusion::logical_expr::col(#field_name_str)`, the
            /// typed accessor for this struct's `#field_name_str` field.
            pub fn #accessor_name() -> ::datafusion::logical_expr::Expr {
                ::datafusion::logical_expr::col(#field_name_str)
            }
        }
    });

    let expanded = quote! {
        impl #name {
            /// The Arrow schema this connector's table is registered with,
            /// derived from `#name`'s `contracts::Contract::SHAPE`.
            ///
            /// # Errors
            ///
            /// Returns a `contracts::arrow::MappingError` if the contract's
            /// shape has no unambiguous Arrow representation.
            pub fn connector_schema() -> ::std::result::Result<
                ::datafusion::arrow::datatypes::Schema,
                ::contracts::arrow::MappingError,
            > {
                ::contracts::arrow::schema::<#name>()
            }

            /// Registers `batches` as a DataFusion table named `name` on
            /// `ctx`, using `Self::connector_schema()`.
            ///
            /// # Errors
            ///
            /// Returns a `DataFusionError` if the schema cannot be derived,
            /// the batches don't conform to it, or registration fails.
            pub fn register_memory(
                ctx: &::datafusion::prelude::SessionContext,
                name: &str,
                batches: ::std::vec::Vec<::datafusion::arrow::record_batch::RecordBatch>,
            ) -> ::std::result::Result<(), ::datafusion::error::DataFusionError> {
                let schema = ::std::sync::Arc::new(Self::connector_schema().map_err(|source| {
                    ::datafusion::error::DataFusionError::External(::std::boxed::Box::new(source))
                })?);
                let table = ::datafusion::datasource::MemTable::try_new(schema, ::std::vec![batches])?;
                ctx.register_table(name, ::std::sync::Arc::new(table))?;
                Ok(())
            }

            /// Registers the Parquet file at `path` as a table named `name`
            /// on `ctx`, with `Self::connector_schema()` set explicitly on
            /// the `ListingTableConfig` rather than inferred from the file.
            ///
            /// # Errors
            ///
            /// Returns a `DataFusionError` if the schema cannot be derived,
            /// the listing URL is invalid, or registration fails.
            pub fn register_parquet(
                ctx: &::datafusion::prelude::SessionContext,
                name: &str,
                path: &str,
            ) -> ::std::result::Result<(), ::datafusion::error::DataFusionError> {
                let schema = ::std::sync::Arc::new(Self::connector_schema().map_err(|source| {
                    ::datafusion::error::DataFusionError::External(::std::boxed::Box::new(source))
                })?);
                let table_path = ::datafusion::datasource::listing::ListingTableUrl::parse(path)?;
                let options = ::datafusion::datasource::listing::ListingOptions::new(
                    ::std::sync::Arc::new(::datafusion::datasource::file_format::parquet::ParquetFormat::default()),
                );
                let config = ::datafusion::datasource::listing::ListingTableConfig::new(table_path)
                    .with_listing_options(options)
                    .with_schema(schema);
                let table = ::datafusion::datasource::listing::ListingTable::try_new(config)?;
                ctx.register_table(name, ::std::sync::Arc::new(table))?;
                Ok(())
            }

            /// Registers the CSV file at `path` as a table named `name` on
            /// `ctx`, with `Self::connector_schema()` set explicitly on the
            /// `ListingTableConfig` rather than inferred from the file.
            ///
            /// # Errors
            ///
            /// Returns a `DataFusionError` if the schema cannot be derived,
            /// the listing URL is invalid, or registration fails.
            pub fn register_csv(
                ctx: &::datafusion::prelude::SessionContext,
                name: &str,
                path: &str,
            ) -> ::std::result::Result<(), ::datafusion::error::DataFusionError> {
                let schema = ::std::sync::Arc::new(Self::connector_schema().map_err(|source| {
                    ::datafusion::error::DataFusionError::External(::std::boxed::Box::new(source))
                })?);
                let table_path = ::datafusion::datasource::listing::ListingTableUrl::parse(path)?;
                let options = ::datafusion::datasource::listing::ListingOptions::new(
                    ::std::sync::Arc::new(::datafusion::datasource::file_format::csv::CsvFormat::default()),
                );
                let config = ::datafusion::datasource::listing::ListingTableConfig::new(table_path)
                    .with_listing_options(options)
                    .with_schema(schema);
                let table = ::datafusion::datasource::listing::ListingTable::try_new(config)?;
                ctx.register_table(name, ::std::sync::Arc::new(table))?;
                Ok(())
            }

            #(#col_accessors)*
        }
    };

    expanded.into()
}
