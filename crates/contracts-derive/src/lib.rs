//! `#[derive(Contract)]`: builds a [`TypeShape`]-equivalent representation
//! of a struct's fields as a compile-time constant, so `contracts::conforms`
//! can compare it against another type's shape entirely at compile time.
//!
//! [`TypeShape`]: https://doc.rust-lang.org/nightly/std/index.html

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DataStruct, DeriveInput, Fields, parse_macro_input};

/// Derives `contracts::Contract` for a struct with named fields, recording
/// each field's name and shape (primitive, `Option`, `Vec`, `HashMap`/
/// `BTreeMap`, recursively) as a `const SHAPE: TypeShape`.
///
/// Generic structs are rejected at the derive site (see `derive_contract`'s
/// generics check) rather than silently accepted: this derive intentionally supports concrete declarations only.
/// Field shapes are resolved by the compiler through `Contract`, including aliases.
///
/// # Panics
///
/// Panics if a named field has no identifier — this indicates malformed
/// input the parser should already have rejected (`Fields::Named` only
/// matches when every field is named), not a case a caller can hit with
/// valid Rust source.
#[proc_macro_derive(Contract, attributes(contract))]
pub fn derive_contract(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    if !input.generics.params.is_empty() {
        return syn::Error::new_spanned(
            &input.generics,
            "Contract derive supports only concrete structs; generic parameters and lifetimes are not supported",
        )
        .to_compile_error()
        .into();
    }

    let fields = match &input.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(named),
            ..
        }) => &named.named,
        _ => {
            return syn::Error::new_spanned(
                &input,
                "Contract can only be derived for structs with named fields",
            )
            .to_compile_error()
            .into();
        }
    };

    let mut field_entries = Vec::with_capacity(fields.len());
    for f in fields {
        let field_name = f
            .ident
            .as_ref()
            .expect("named field guaranteed by Fields::Named")
            .to_string();
        let ty = &f.ty;
        let shape = quote! { <#ty as ::contracts::Contract>::SHAPE };
        let has_default = match has_default_attr(f) {
            Ok(value) => value,
            Err(error) => return error.to_compile_error().into(),
        };
        field_entries.push(quote! {
            ::contracts::FieldShape { name: #field_name, shape: #shape, has_default: #has_default }
        });
    }

    let expanded = quote! {
        impl ::contracts::Contract for #name {
            const SHAPE: ::contracts::TypeShape = ::contracts::TypeShape::Struct(&[
                #(#field_entries),*
            ]);
        }
    };

    expanded.into()
}

/// Detects `#[contract(default)]` on a field — the Rust-side stand-in for a
/// Scala default value, which has no runtime-inspectable equivalent here.
/// A field carrying this attribute is treated as tolerable-if-missing under
/// `SchemaPolicy::Backward`, the same way an `Option<T>` field already is.
fn has_default_attr(field: &syn::Field) -> syn::Result<bool> {
    let mut found = false;
    for attr in &field.attrs {
        if !attr.path().is_ident("contract") {
            continue;
        }
        if found {
            return Err(syn::Error::new_spanned(
                attr,
                "duplicate contract attribute",
            ));
        }
        let mut parsed_default = false;
        attr.parse_nested_meta(|meta| {
            if !meta.path.is_ident("default") {
                return Err(meta.error("expected `default`"));
            }
            if parsed_default {
                return Err(meta.error("duplicate `default`"));
            }
            if !meta.input.is_empty() && !meta.input.peek(syn::Token![,]) {
                return Err(meta.error("`default` does not accept a value or arguments"));
            }
            parsed_default = true;
            Ok(())
        })?;
        if !parsed_default {
            return Err(syn::Error::new_spanned(
                attr,
                "expected `contract(default)`",
            ));
        }
        found = true;
    }
    Ok(found)
}
