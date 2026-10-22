//! Extends `transpose-cost.rs`'s question to nested columns: a `List<Float64>`
//! field, not a fixed set of flat `Float64` columns. Nested types are the
//! harder case for the same reason `transpose-cost.rs`'s own doc comment
//! gives for why it dropped a `Vec<Vec<f64>>` row layout in favor of a flat
//! buffer — per-row heap allocation swamps the signal. A flat column can
//! design that allocation away (`ColumnarBatch`/`RowBatch` never allocate
//! per row). A `List<Float64>` column cannot: each row has a different
//! length, so materializing one row's values always means an owned `Vec`,
//! which is exactly why native accelerators (Comet, Velox, Gluten alike)
//! commonly fall back to JVM/row execution specifically for nested-type
//! operators.
//!
//! Two paths, same shape as `transpose-cost.rs`: transform the whole
//! `List<Float64>` column's flat values buffer directly in one pass (no
//! per-row materialization), vs. walk each row, collect it into an owned
//! `Vec<f64>`, transform, and rebuild. Both must produce identical output;
//! checked before exit.

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, ArrayRef, Float64Array, Float64Builder, ListArray, ListBuilder};
use arrow::datatypes::{DataType, Float64Type};
use clap::Parser;

#[derive(Parser)]
#[command(
    name = "nested-transpose-cost",
    about = "Time a List<Float64> elementwise transform with vs. without per-row materialization"
)]
struct Args {
    /// Row count.
    #[arg(long, default_value_t = 200_000)]
    rows: usize,

    /// Max elements per row; row `r`'s length is `(r % max_len) + 1`.
    #[arg(long, default_value_t = 5)]
    max_len: usize,

    /// Timed repetitions per path.
    #[arg(long, default_value_t = 3)]
    runs: usize,
}

fn generate_nested_column(rows: usize, max_len: usize) -> ListArray {
    ListArray::from_iter_primitive::<Float64Type, _, _>((0..rows).map(|r| {
        let len = (r % max_len) + 1;
        Some((0..len).map(|i| Some((r + i) as f64)).collect::<Vec<_>>())
    }))
}

fn list_field(list_array: &ListArray) -> arrow::datatypes::FieldRef {
    match list_array.data_type() {
        DataType::List(field) => field.clone(),
        other => panic!("expected List type, got {other:?}"),
    }
}

/// No per-row materialization: the flat values buffer is transformed in one
/// pass, and the result is rebuilt from the same offsets/nulls.
fn transform_columnar(list_array: &ListArray) -> ListArray {
    let values = list_array
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("values are Float64Array by construction");
    let transformed: ArrayRef = Arc::new(Float64Array::from_iter_values(
        values.values().iter().map(|v| v + 1.0),
    ));
    ListArray::try_new(
        list_field(list_array),
        list_array.offsets().clone(),
        transformed,
        list_array.nulls().cloned(),
    )
    .expect("same offsets/nulls, same-typed transformed values")
}

/// Per-row materialization: each row's variable-length slice is collected
/// into its own owned `Vec<f64>` before being transformed and rebuilt -
/// the allocation a `List` column can't design away.
fn transform_row_round_trip(list_array: &ListArray) -> ListArray {
    let mut builder = ListBuilder::new(Float64Builder::new());
    for i in 0..list_array.len() {
        let row: Vec<f64> = list_array
            .value(i)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("values are Float64Array by construction")
            .values()
            .iter()
            .map(|v| v + 1.0)
            .collect();
        builder.append_value(row.into_iter().map(Some));
    }
    builder.finish()
}

fn sum_all(list_array: &ListArray) -> f64 {
    list_array
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("values are Float64Array by construction")
        .values()
        .iter()
        .sum()
}

fn time_runs<F: Fn() -> ListArray>(label: &str, runs: usize, f: F) -> f64 {
    let mut last_sum = 0.0_f64;
    let mut durations = Vec::with_capacity(runs);
    for _ in 0..runs {
        let start = Instant::now();
        let result = f();
        durations.push(start.elapsed());
        last_sum = sum_all(&result);
    }
    let formatted: Vec<String> = durations.iter().map(|d| format!("{d:?}")).collect();
    println!("{label}: {} (checksum={last_sum})", formatted.join(", "));
    last_sum
}

fn main() -> ExitCode {
    let args = Args::parse();
    let column = generate_nested_column(args.rows, args.max_len);

    let columnar_sum = time_runs("columnar (no per-row materialization)", args.runs, || {
        transform_columnar(&column)
    });
    let row_trip_sum = time_runs("row round trip (per-row Vec<f64>)", args.runs, || {
        transform_row_round_trip(&column)
    });

    if (columnar_sum - row_trip_sum).abs() > 1e-6 {
        eprintln!(
            "nested-transpose-cost: result mismatch, columnar={columnar_sum} row_trip={row_trip_sum}"
        );
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}
