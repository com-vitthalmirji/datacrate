//! `#[derive(Connector)]`: the registration half of what `#[derive(Contract)]`
//! already provides for schema shape. Generates `connector_schema()` and
//! `register()` for a struct that already derives `contracts::Contract`,
//! reusing `contracts::arrow`'s `TypeShape` -> Arrow `Schema` mapping and
//! DataFusion's own `MemTable` instead of generating a `TableProvider` per
//! struct.

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DataStruct, DeriveInput, Fields, parse_macro_input};

/// Derives `connector_schema()` and `register()` on a struct that already
/// derives `contracts::Contract`.
///
/// `connector_schema()` maps the struct's `TypeShape` to an Arrow `Schema`
/// via `contracts::arrow::schema`. `register()` wraps a `Vec<RecordBatch>` in
/// a `datafusion::datasource::MemTable` built from that schema and registers
/// it under a name on a `SessionContext`.
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

    match &input.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(_),
            ..
        }) => {}
        _ => {
            return syn::Error::new_spanned(
                &input,
                "Connector can only be derived for structs with named fields",
            )
            .to_compile_error()
            .into();
        }
    }

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
            pub fn register(
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
        }
    };

    expanded.into()
}
