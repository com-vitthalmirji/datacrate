//! `#[derive(Contract)]`: builds a [`TypeShape`]-equivalent representation
//! of a struct's fields as a compile-time constant, so `contracts::conforms`
//! can compare it against another type's shape entirely at compile time.
//!
//! [`TypeShape`]: https://doc.rust-lang.org/nightly/std/index.html

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    Data, DataStruct, DeriveInput, Fields, GenericArgument, PathArguments, Type, parse_macro_input,
};

/// Derives `contracts::Contract` for a struct with named fields, recording
/// each field's name and shape (primitive, `Option`, `Vec`, `HashMap`/
/// `BTreeMap`, recursively) as a `const SHAPE: TypeShape`.
///
/// Generic structs are rejected at the derive site (see `derive_contract`'s
/// generics check) rather than silently accepted: a bare type parameter
/// (e.g. `struct Wrapper<T> { v: T }`) has no shape the macro can see, so
/// recording it as an opaque `Primitive("T")` would make `Wrapper<i64>` and
/// `Wrapper<String>` compare as identical shapes — defeating the exact
/// purpose of this crate.
///
/// # Panics
///
/// Panics if a named field has no identifier — this indicates malformed
/// input the parser should already have rejected (`Fields::Named` only
/// matches when every field is named), not a case a caller can hit with
/// valid Rust source.
#[proc_macro_derive(Contract)]
pub fn derive_contract(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    if !input.generics.params.is_empty() {
        return syn::Error::new_spanned(
            &input.generics,
            "Contract cannot be derived for generic structs: a type parameter has no \
             shape the macro can see, so distinct instantiations (e.g. Wrapper<i64> vs \
             Wrapper<String>) would compare as identical",
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

    let field_entries = fields.iter().map(|f| {
        let field_name = f
            .ident
            .as_ref()
            .expect("named field guaranteed by Fields::Named")
            .to_string();
        let shape = shape_tokens(&f.ty);
        quote! {
            ::contracts::FieldShape { name: #field_name, shape: #shape }
        }
    });

    let expanded = quote! {
        impl ::contracts::Contract for #name {
            const SHAPE: ::contracts::TypeShape = ::contracts::TypeShape::Struct(&[
                #(#field_entries),*
            ]);
        }
    };

    expanded.into()
}

/// Builds the tokens for a single field's [`TypeShape`], recursing through
/// `Option<T>`, `Vec<T>`, and `HashMap`/`BTreeMap<K, V>` so nested
/// optionality (e.g. `Vec<Option<T>>`) is preserved rather than collapsed
/// to the outer container's shape alone.
///
/// Any named type recognized as a Rust/std primitive (`bool`, `char`, `str`,
/// `String`, the integer and float types) is recorded as an opaque
/// `Primitive`, comparable only by name. Anything else is assumed to be a
/// nested `#[derive(Contract)]` type and emitted as `<T as Contract>::SHAPE`
/// so `diagnose` can recurse into it and name a mismatched field by its full
/// path (e.g. `shipTo.zip`).
///
/// This is a heuristic, not a type resolution — a proc macro sees syntax,
/// not resolved types, so it cannot actually confirm a field's type
/// implements `Contract`. A field typed with a non-primitive that does *not*
/// derive `Contract` (e.g. `uuid::Uuid` used directly) fails to compile with
/// an unsatisfied-trait-bound error rather than silently comparing wrong —
/// consistent with this crate's premise that drift is a compile error, not a
/// silent gap.
///
/// Container detection matches on the last path segment's *name* only
/// (`"Option"`, `"Vec"`, `"HashMap"`, `"BTreeMap"`) — the same syntactic
/// limitation applies here too. To stay safe under that ambiguity, a segment
/// is only treated as a container when its generic-argument count matches
/// the container's arity (one for `Option`/`Vec`, two for the maps);
/// anything else — including a non-generic type that happens to be named
/// `Vec` — falls through to the primitive/nested-`Contract` case below
/// instead of panicking.
fn shape_tokens(ty: &Type) -> TokenStream2 {
    if let Type::Reference(r) = ty {
        return shape_tokens(&r.elem);
    }

    if let Type::Path(p) = ty {
        let segment = p
            .path
            .segments
            .last()
            .expect("a type path always has at least one segment");
        let ident = segment.ident.to_string();

        match ident.as_str() {
            "Option" => {
                if let Some(inner) = single_type_arg(segment) {
                    let inner = shape_tokens(inner);
                    return quote! { ::contracts::TypeShape::Optional(&#inner) };
                }
            }
            "Vec" => {
                if let Some(inner) = single_type_arg(segment) {
                    let inner = shape_tokens(inner);
                    return quote! { ::contracts::TypeShape::Sequence(&#inner) };
                }
            }
            "HashMap" | "BTreeMap" => {
                if let Some((key, value)) = pair_type_args(segment) {
                    let key = shape_tokens(key);
                    let value = shape_tokens(value);
                    return quote! { ::contracts::TypeShape::Map(&#key, &#value) };
                }
            }
            _ => {}
        }
    }

    let name = quote!(#ty).to_string();
    if is_primitive_name(&name) {
        quote! { ::contracts::TypeShape::Primitive(#name) }
    } else {
        quote! { <#ty as ::contracts::Contract>::SHAPE }
    }
}

/// Rust/std primitive type names left as opaque `Primitive` shapes rather
/// than being treated as a nested `Contract` type. `str` is included even
/// though field types are practically always `String`, not a bare `str`.
fn is_primitive_name(name: &str) -> bool {
    matches!(
        name,
        "bool"
            | "char"
            | "str"
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
            | "usize"
            | "isize"
            | "f32"
            | "f64"
    )
}

/// Returns the segment's sole generic type argument, or `None` if it has
/// zero or more than one — callers treat `None` as "not actually this
/// container" rather than an error.
fn single_type_arg(segment: &syn::PathSegment) -> Option<&Type> {
    let mut args = type_args(segment);
    let first = args.next()?;
    if args.next().is_some() {
        return None;
    }
    Some(first)
}

/// Returns the segment's two generic type arguments (key, value), or `None`
/// if it doesn't have exactly two.
fn pair_type_args(segment: &syn::PathSegment) -> Option<(&Type, &Type)> {
    let mut args = type_args(segment);
    let key = args.next()?;
    let value = args.next()?;
    if args.next().is_some() {
        return None;
    }
    Some((key, value))
}

fn type_args(segment: &syn::PathSegment) -> impl Iterator<Item = &Type> {
    let args = match &segment.arguments {
        PathArguments::AngleBracketed(args) => Some(&args.args),
        _ => None,
    };
    args.into_iter().flatten().filter_map(|arg| match arg {
        GenericArgument::Type(t) => Some(t),
        _ => None,
    })
}
