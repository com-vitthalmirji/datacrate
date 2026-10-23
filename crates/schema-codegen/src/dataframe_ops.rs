//! The closed set of DataFusion `DataFrame` operations `scaffold-cli pipeline
//! --transform-df` can render. Deliberately five variants, not a general
//! expression language: anything beyond them already has a path via
//! `--transform-sql`, so a second mini-parser for e.g. arbitrary filter
//! expressions would duplicate that escape hatch instead of earning its own.

use proc_macro2::TokenStream;
use quote::quote;

/// One stage of a DataFusion `DataFrame` method chain.
#[derive(Debug, Clone, PartialEq)]
pub enum DfOp {
    /// `.select(vec![col(...), ...])`
    Select(Vec<String>),
    /// `.filter(col(...).<cmp>(lit(...)))`
    Filter {
        column: String,
        cmp: Comparator,
        value: Literal,
    },
    /// `.aggregate(vec![group...], vec![agg...])`
    Aggregate {
        group_by: Vec<String>,
        aggs: Vec<AggExpr>,
    },
    /// `.sort(vec![col(...).sort(...), ...])`
    Sort(Vec<SortKey>),
    /// `.limit(0, Some(n))`
    Limit(usize),
}

/// A `filter` op's comparison, mapped onto `Expr`'s own comparison methods.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Comparator {
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
}

/// A `filter` op's literal operand.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(String),
}

/// One `func(column) AS alias` term inside an `aggregate` op.
#[derive(Debug, Clone, PartialEq)]
pub struct AggExpr {
    pub column: String,
    pub func: AggFn,
    pub alias: String,
}

/// An `aggregate` op's function, mapped onto `functions_aggregate::expr_fn`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggFn {
    Sum,
    Count,
    Avg,
    Min,
    Max,
}

/// One `column, direction` term inside a `sort` op.
#[derive(Debug, Clone, PartialEq)]
pub struct SortKey {
    pub column: String,
    pub ascending: bool,
}

impl Comparator {
    fn method_name(self) -> &'static str {
        match self {
            Comparator::Eq => "eq",
            Comparator::Ne => "not_eq",
            Comparator::Gt => "gt",
            Comparator::Lt => "lt",
            Comparator::Ge => "gt_eq",
            Comparator::Le => "lt_eq",
        }
    }
}

impl AggFn {
    fn fn_name(self) -> &'static str {
        match self {
            AggFn::Sum => "sum",
            AggFn::Count => "count",
            AggFn::Avg => "avg",
            AggFn::Min => "min",
            AggFn::Max => "max",
        }
    }
}

fn literal_tokens(value: &Literal) -> TokenStream {
    match value {
        Literal::Int(n) => quote! { #n },
        Literal::Float(f) => quote! { #f },
        Literal::Bool(b) => quote! { #b },
        Literal::Str(s) => quote! { #s },
    }
}

/// Renders one [`DfOp`] into its `.method(...)` call, `?`-chained onto `df`.
fn op_tokens(op: &DfOp) -> TokenStream {
    match op {
        DfOp::Select(columns) => {
            quote! { .select(vec![#(datafusion::prelude::col(#columns)),*])? }
        }
        DfOp::Filter { column, cmp, value } => {
            let cmp_method = syn::Ident::new(cmp.method_name(), proc_macro2::Span::call_site());
            let value = literal_tokens(value);
            quote! {
                .filter(datafusion::prelude::col(#column).#cmp_method(datafusion::prelude::lit(#value)))?
            }
        }
        DfOp::Aggregate { group_by, aggs } => {
            let agg_terms = aggs.iter().map(|agg| {
                let agg_fn = syn::Ident::new(agg.func.fn_name(), proc_macro2::Span::call_site());
                let column = &agg.column;
                let alias = &agg.alias;
                quote! {
                    datafusion::functions_aggregate::expr_fn::#agg_fn(datafusion::prelude::col(#column)).alias(#alias)
                }
            });
            quote! {
                .aggregate(vec![#(datafusion::prelude::col(#group_by)),*], vec![#(#agg_terms),*])?
            }
        }
        DfOp::Sort(keys) => {
            let sort_terms = keys.iter().map(|key| {
                let column = &key.column;
                let ascending = key.ascending;
                quote! { datafusion::prelude::col(#column).sort(#ascending, false) }
            });
            quote! { .sort(vec![#(#sort_terms),*])? }
        }
        DfOp::Limit(n) => quote! { .limit(0, Some(#n))? },
    }
}

/// Renders a full `DfOp` chain as `df#(.method(...))*`, ready to interpolate
/// as the body of a `dataframe_transform` closure.
pub(crate) fn chain_tokens(ops: &[DfOp]) -> TokenStream {
    let steps = ops.iter().map(op_tokens);
    quote! { df #(#steps)* }
}
