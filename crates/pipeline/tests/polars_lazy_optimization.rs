//! Proves (not just asserts the presence of a string) that Polars' lazy
//! query optimizer actually pushes predicates and projections down into the
//! CSV scan node, by diffing the optimized `.explain(true)` plan against the
//! unoptimized `.explain(false)` plan on the same query. Data: the
//! `fixtures/m3/orders.csv` fixture and the plan-string output. Calculation:
//! the marker-text checks below. Action: `.explain()` and `.collect()`.
//!
//! Backs the DataFusion-vs-Polars ecosystem comparison cited in
//! `docs/internals/jotb-lambda-spain/talk-outline.md`
//! (<https://iceberglakehouse.com/posts/2026-05-23-single-node-data-engineering-duckdb-datafusion-polars-lakesail/>).
#![cfg(feature = "polars")]

use std::path::PathBuf;

use polars::prelude::*;

fn fixture_path() -> PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/m3/orders.csv")
}

fn orders_lazy() -> LazyFrame {
    LazyCsvReader::new(PlRefPath::new(
        fixture_path().to_string_lossy().into_owned(),
    ))
    .with_has_header(true)
    .finish()
    .expect("fixtures/m3/orders.csv is valid CSV")
    .filter(col("amount").gt(lit(50.00)))
    .select([col("id"), col("amount")])
}

#[test]
fn polars_lazy_plan_pushes_predicate_below_scan() {
    let lf = orders_lazy();
    let optimized = lf.clone().explain(true).expect("optimized plan");
    let unoptimized = lf.explain(false).expect("unoptimized plan");

    assert!(
        optimized.contains("SELECTION: col(\"amount\") > 50.0"),
        "optimized plan should fold the predicate into the scan node's \
         SELECTION, got: {optimized}"
    );
    assert!(
        !optimized.contains("FILTER"),
        "optimized plan should have no separate FILTER node once the \
         predicate is pushed into the scan, got: {optimized}"
    );
    assert!(
        unoptimized.contains("FILTER col(\"amount\") > 50.0"),
        "unoptimized plan should keep the predicate as a separate FILTER \
         node above the scan, got: {unoptimized}"
    );
}

#[test]
fn polars_lazy_plan_pushes_projection_below_scan() {
    let lf = orders_lazy();
    let optimized = lf.clone().explain(true).expect("optimized plan");
    let unoptimized = lf.explain(false).expect("unoptimized plan");

    assert!(
        optimized.contains("PROJECT 2/4 COLUMNS"),
        "optimized plan should prune the scan to the 2 selected columns, \
         got: {optimized}"
    );
    assert!(
        unoptimized.contains("PROJECT */4 COLUMNS"),
        "unoptimized plan should read all 4 columns, deferring the \
         projection to a separate SELECT node, got: {unoptimized}"
    );
}

#[test]
fn polars_eager_and_lazy_paths_produce_identical_results() {
    let lazy_df = orders_lazy().collect().expect("lazy collect");

    let eager_df = CsvReadOptions::default()
        .with_has_header(true)
        .try_into_reader_with_file_path(Some(fixture_path()))
        .expect("open fixtures/m3/orders.csv")
        .finish()
        .expect("eager read")
        .lazy()
        .filter(col("amount").gt(lit(50.00)))
        .select([col("id"), col("amount")])
        .collect()
        .expect("eager-then-lazy-ops collect");

    assert_eq!(
        lazy_df, eager_df,
        "the lazy scan path and the eager-read-then-filter path must agree \
         on the same fixture and predicate"
    );
}
