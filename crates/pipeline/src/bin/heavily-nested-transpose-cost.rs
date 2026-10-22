//! Extends `nested-transpose-cost.rs`'s question two levels deeper: that
//! benchmark measured a single-level `List<Float64>` column. This one
//! measures `List<Struct<List<Struct<List<Float64>>>>>` - five levels,
//! the same `List<Struct<List<Struct>>>` shape Arrow's own `StructBuilder`
//! docs use as their canonical "this is the hard case" example, with one
//! more `List<Float64>` leaf added so each innermost struct still carries a
//! variable-length numeric column. Real heavily-nested data (JSON-derived
//! Parquet, nested Avro/Protobuf) commonly looks like this, not like a flat
//! `List<Float64>`.
//!
//! Same two paths as `nested-transpose-cost.rs`: transform the innermost
//! flat values buffer directly in one pass, rebuilding each of the five
//! levels from its own existing offsets/nulls (no per-row materialization
//! anywhere), vs. walk outer rows, then wrapper structs, then item structs,
//! then leaf values, collecting each leaf into an owned `Vec<f64>` before
//! rebuilding - the allocation a flat column can design away, now paid at
//! three nesting levels instead of one. Both must produce identical
//! output; checked before exit.

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    Array, ArrayBuilder, ArrayRef, Float64Array, Float64Builder, ListArray, ListBuilder,
    StructArray, StructBuilder,
};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use clap::Parser;

#[derive(Parser)]
#[command(
    name = "heavily-nested-transpose-cost",
    about = "Time a List<Struct<List<Struct<List<Float64>>>>> elementwise transform with vs. without per-row materialization"
)]
struct Args {
    /// Outer row count.
    #[arg(long, default_value_t = 20_000)]
    rows: usize,

    /// Max wrapper-struct elements per outer row.
    #[arg(long, default_value_t = 3)]
    max_wrappers: usize,

    /// Max item-struct elements per wrapper's `items` list.
    #[arg(long, default_value_t = 3)]
    max_items: usize,

    /// Max elements per innermost `values` list.
    #[arg(long, default_value_t = 4)]
    max_len: usize,

    /// Timed repetitions per path.
    #[arg(long, default_value_t = 3)]
    runs: usize,
}

fn item_struct_fields() -> Fields {
    vec![Field::new(
        "values",
        DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true))),
        true,
    )]
    .into()
}

fn wrapper_struct_fields() -> Fields {
    let item_struct_type = DataType::Struct(item_struct_fields());
    vec![Field::new(
        "items",
        DataType::List(Arc::new(Field::new_list_field(item_struct_type, true))),
        true,
    )]
    .into()
}

/// Appends one item element (one `values` row) to `item_struct_builder`,
/// navigating the `ListBuilder<Box<dyn ArrayBuilder>>` boxing that
/// `StructBuilder::from_fields` introduces for nested collection fields.
fn append_item(item_struct_builder: &mut StructBuilder, values: impl Iterator<Item = f64>) {
    let list_builder = item_struct_builder
        .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(0)
        .expect("field 0 is a List builder by construction");
    let float_builder = list_builder
        .values()
        .as_any_mut()
        .downcast_mut::<Float64Builder>()
        .expect("values list holds Float64Builder by construction");
    for v in values {
        float_builder.append_value(v);
    }
    list_builder.append(true);
    item_struct_builder.append(true);
}

/// Appends one wrapper element (one `items` row, itself a list of item
/// structs) to `wrapper_struct_builder`. Same boxing caveat as
/// `append_item`, one level up: the `items` list's element builder is a
/// boxed `StructBuilder`, reached via `ListBuilder<Box<dyn ArrayBuilder>>`.
fn append_wrapper(wrapper_struct_builder: &mut StructBuilder, items: Vec<Vec<f64>>) {
    let items_list_builder = wrapper_struct_builder
        .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(0)
        .expect("field 0 is a List builder by construction");
    let item_struct_builder = items_list_builder
        .values()
        .as_any_mut()
        .downcast_mut::<StructBuilder>()
        .expect("items list holds StructBuilder by construction");
    for item_values in items {
        append_item(item_struct_builder, item_values.into_iter());
    }
    items_list_builder.append(true);
    wrapper_struct_builder.append(true);
}

fn generate_nested_column(
    rows: usize,
    max_wrappers: usize,
    max_items: usize,
    max_len: usize,
) -> ListArray {
    let mut outer_builder =
        ListBuilder::new(StructBuilder::from_fields(wrapper_struct_fields(), 0));
    for r in 0..rows {
        let wrapper_count = (r % max_wrappers) + 1;
        let wrapper_struct_builder: &mut StructBuilder = outer_builder.values();
        for w in 0..wrapper_count {
            let item_count = ((r + w) % max_items) + 1;
            let items: Vec<Vec<f64>> = (0..item_count)
                .map(|it| {
                    let len = ((r + w + it) % max_len) + 1;
                    (0..len).map(|i| (r + w + it + i) as f64).collect()
                })
                .collect();
            append_wrapper(wrapper_struct_builder, items);
        }
        outer_builder.append(true);
    }
    outer_builder.finish()
}

fn list_field(list_array: &ListArray) -> FieldRef {
    match list_array.data_type() {
        DataType::List(field) => field.clone(),
        other => panic!("expected List type, got {other:?}"),
    }
}

fn wrapper_structs(outer: &ListArray) -> &StructArray {
    outer
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("outer values are StructArray by construction")
}

fn items_list(wrapper: &StructArray) -> &ListArray {
    wrapper
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("wrapper field 0 is List<Struct<..>> by construction")
}

fn item_structs(items: &ListArray) -> &StructArray {
    items
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("items values are StructArray by construction")
}

fn values_list(item: &StructArray) -> &ListArray {
    item.column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("item field 0 is List<Float64> by construction")
}

fn leaf_values(values: &ListArray) -> &Float64Array {
    values
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("values list holds Float64Array by construction")
}

/// No per-row materialization anywhere: the innermost flat values buffer
/// is transformed in one pass, and all four wrapping levels are rebuilt
/// from their existing offsets/nulls, unchanged.
fn transform_columnar(outer: &ListArray) -> ListArray {
    let wrappers = wrapper_structs(outer);
    let items = items_list(wrappers);
    let items_structs = item_structs(items);
    let values = values_list(items_structs);
    let leaf = leaf_values(values);

    let transformed: ArrayRef = Arc::new(Float64Array::from_iter_values(
        leaf.values().iter().map(|v| v + 1.0),
    ));
    let new_values = ListArray::try_new(
        list_field(values),
        values.offsets().clone(),
        transformed,
        values.nulls().cloned(),
    )
    .expect("same offsets/nulls, same-typed transformed values");
    let new_item_structs = StructArray::try_new(
        items_structs.fields().clone(),
        vec![Arc::new(new_values)],
        items_structs.nulls().cloned(),
    )
    .expect("same fields/nulls, one replaced column");
    let new_items = ListArray::try_new(
        list_field(items),
        items.offsets().clone(),
        Arc::new(new_item_structs),
        items.nulls().cloned(),
    )
    .expect("same offsets/nulls, same-typed transformed values");
    let new_wrappers = StructArray::try_new(
        wrappers.fields().clone(),
        vec![Arc::new(new_items)],
        wrappers.nulls().cloned(),
    )
    .expect("same fields/nulls, one replaced column");
    ListArray::try_new(
        list_field(outer),
        outer.offsets().clone(),
        Arc::new(new_wrappers),
        outer.nulls().cloned(),
    )
    .expect("same offsets/nulls, same-typed transformed values")
}

/// Per-row materialization at every level: each outer row's wrapper
/// elements, each wrapper's item elements, and each item's leaf values are
/// walked and collected into owned `Vec<f64>`s before being transformed
/// and rebuilt through the same boxed-builder navigation `generate_nested_column` uses.
fn transform_row_round_trip(outer: &ListArray) -> ListArray {
    let mut outer_builder =
        ListBuilder::new(StructBuilder::from_fields(wrapper_struct_fields(), 0));
    for i in 0..outer.len() {
        let row = outer.value(i);
        let row_wrappers = row
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("outer rows are StructArray by construction");
        let row_items = items_list(row_wrappers);

        let wrapper_struct_builder: &mut StructBuilder = outer_builder.values();
        for w in 0..row_wrappers.len() {
            let item_row = row_items.value(w);
            let row_item_structs = item_row
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("item rows are StructArray by construction");
            let row_values = values_list(row_item_structs);

            let items: Vec<Vec<f64>> = (0..row_item_structs.len())
                .map(|it| {
                    row_values
                        .value(it)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .expect("values are Float64Array by construction")
                        .values()
                        .iter()
                        .map(|v| v + 1.0)
                        .collect()
                })
                .collect();
            append_wrapper(wrapper_struct_builder, items);
        }
        outer_builder.append(true);
    }
    outer_builder.finish()
}

fn sum_all(outer: &ListArray) -> f64 {
    let wrappers = wrapper_structs(outer);
    let items = items_list(wrappers);
    let items_structs = item_structs(items);
    let values = values_list(items_structs);
    leaf_values(values).values().iter().sum()
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
    let column = generate_nested_column(args.rows, args.max_wrappers, args.max_items, args.max_len);

    let columnar_sum = time_runs("columnar (no per-row materialization)", args.runs, || {
        transform_columnar(&column)
    });
    let row_trip_sum = time_runs("row round trip (per-leaf Vec<f64>)", args.runs, || {
        transform_row_round_trip(&column)
    });

    if (columnar_sum - row_trip_sum).abs() > 1e-6 {
        eprintln!(
            "heavily-nested-transpose-cost: result mismatch, columnar={columnar_sum} row_trip={row_trip_sum}"
        );
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}
