use arrow_schema::{DataType, Field, Schema};
use schema_codegen::{
    AggExpr, AggFn, Comparator, DfOp, Literal, PipelineSpec, SortKey, SourceData, SourceFormat,
    TransformSpec, pipeline_source,
};

fn order_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("quantity", DataType::Int32, false),
    ])
}

/// `prettyplease` may wrap a long turbofish (e.g. `.sink::<OrderTarget>(`)
/// onto several lines depending on identifier length. Collapsing all
/// whitespace to single spaces keeps assertions stable across that
/// formatting choice without hand-matching its exact line breaks.
fn normalize(rendered: &str) -> String {
    rendered.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[test]
fn passthrough_parquet_pipeline_declares_one_struct_and_no_transform_stage() {
    let schema = order_schema();
    let spec = PipelineSpec {
        name: "Order",
        source_schema: &schema,
        target_schema: None,
        source_data: SourceData {
            location: "fixtures/orders.parquet",
            format: SourceFormat::Parquet,
        },
        transform: None,
        sink_parquet: "/tmp/out.parquet",
    };

    let rendered = pipeline_source(&spec).expect("passthrough spec renders");

    let normalized = normalize(&rendered);
    assert!(rendered.contains("struct OrderSource"));
    assert!(!rendered.contains("struct OrderTarget"));
    assert!(rendered.contains("OrderSource::register_parquet(&ctx, \"batch\""));
    assert!(!normalized.contains(".sql_transform"));
    assert!(normalized.contains(".sink::< OrderSource, >("));
    assert!(rendered.contains("write_parquet(&batch, Path::new(\"/tmp/out.parquet\")"));
}

#[test]
fn parquet_pipeline_with_sql_transform_declares_both_structs_and_the_transform_stage() {
    let source_schema = order_schema();
    let target_schema = Schema::new(vec![Field::new("total", DataType::Int64, false)]);
    let spec = PipelineSpec {
        name: "Order",
        source_schema: &source_schema,
        target_schema: Some(&target_schema),
        source_data: SourceData {
            location: "fixtures/orders.parquet",
            format: SourceFormat::Parquet,
        },
        transform: Some(TransformSpec::Sql(
            "SELECT SUM(quantity) AS total FROM batch",
        )),
        sink_parquet: "/tmp/out.parquet",
    };

    let rendered = pipeline_source(&spec).expect("sql_transform spec renders");

    let normalized = normalize(&rendered);
    assert!(rendered.contains("struct OrderSource"));
    assert!(rendered.contains("struct OrderTarget"));
    assert!(rendered.contains(
        ".sql_transform::<OrderTarget>(&ctx, \"SELECT SUM(quantity) AS total FROM batch\")"
    ));
    assert!(normalized.contains(".sink::< OrderTarget, >("));
}

#[test]
fn delta_source_inlines_the_open_table_and_register_sequence() {
    let schema = order_schema();
    let spec = PipelineSpec {
        name: "Order",
        source_schema: &schema,
        target_schema: None,
        source_data: SourceData {
            location: "file:///data/orders",
            format: SourceFormat::Delta,
        },
        transform: None,
        sink_parquet: "/tmp/out.parquet",
    };

    let rendered = pipeline_source(&spec).expect("delta spec renders");

    assert!(rendered.contains("deltalake::open_table(\"file:///data/orders\")"));
    assert!(rendered.contains("table.update_datafusion_session(&ctx.state())?"));
    assert!(rendered.contains("ctx.register_table(\"batch\", provider)?"));
}

#[test]
fn unsupported_arrow_type_in_source_schema_is_rejected() {
    let schema = Schema::new(vec![Field::new("payload", DataType::Binary, false)]);
    let spec = PipelineSpec {
        name: "Bad",
        source_schema: &schema,
        target_schema: None,
        source_data: SourceData {
            location: "fixtures/bad.parquet",
            format: SourceFormat::Parquet,
        },
        transform: None,
        sink_parquet: "/tmp/out.parquet",
    };

    assert!(pipeline_source(&spec).is_err());
}

#[test]
fn parquet_pipeline_with_dataframe_transform_declares_both_structs_and_the_transform_stage() {
    let source_schema = order_schema();
    let target_schema = Schema::new(vec![Field::new("total", DataType::Int64, false)]);
    let spec = PipelineSpec {
        name: "Order",
        source_schema: &source_schema,
        target_schema: Some(&target_schema),
        source_data: SourceData {
            location: "fixtures/orders.parquet",
            format: SourceFormat::Parquet,
        },
        transform: Some(TransformSpec::DataFrame(vec![
            DfOp::Filter {
                column: "quantity".to_string(),
                cmp: Comparator::Gt,
                value: Literal::Int(0),
            },
            DfOp::Aggregate {
                group_by: vec![],
                aggs: vec![AggExpr {
                    column: "quantity".to_string(),
                    func: AggFn::Sum,
                    alias: "total".to_string(),
                }],
            },
            DfOp::Sort(vec![SortKey {
                column: "total".to_string(),
                ascending: false,
            }]),
            DfOp::Limit(10),
        ])),
        sink_parquet: "/tmp/out.parquet",
    };

    let rendered = pipeline_source(&spec).expect("dataframe_transform spec renders");

    let normalized = normalize(&rendered);
    assert!(rendered.contains("struct OrderSource"));
    assert!(rendered.contains("struct OrderTarget"));
    assert!(normalized.contains(".dataframe_transform::< OrderTarget, >("));
    assert!(normalized.contains("|df| Ok("));
    assert!(normalized.contains(".filter( datafusion::prelude::col(\"quantity\")"));
    assert!(rendered.contains("datafusion::functions_aggregate::expr_fn::sum"));
    assert!(normalized.contains(".sort(vec![datafusion::prelude::col(\"total\")"));
    assert!(normalized.contains(".limit(0, Some(10usize))?"));
    assert!(normalized.contains(".sink::< OrderTarget, >("));
}

/// S3/MinIO, like a dbt or Erwin source, is an external system this crate
/// scaffolds support for — it is never actually connected to here. This test
/// proves the renderer, not a live bucket: a real endpoint check belongs to
/// `crates/pipeline`'s own MinIO-container tests, not to schema-codegen's.
#[test]
fn nested_struct_and_list_fields_plus_s3_source_and_sink_render_correctly() {
    let source_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "ship_to",
            DataType::Struct(
                vec![
                    Field::new("city", DataType::Utf8, true),
                    Field::new("zip", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        ),
        Field::new("tags", DataType::new_list(DataType::Utf8, true), true),
    ]);
    let spec = PipelineSpec {
        name: "NestedS3",
        source_schema: &source_schema,
        target_schema: None,
        source_data: SourceData {
            location: "s3://datacrate-workshop/orders/nested.parquet",
            format: SourceFormat::Parquet,
        },
        transform: None,
        sink_parquet: "s3://datacrate-workshop/out/nested.parquet",
    };

    let rendered = pipeline_source(&spec).expect("nested s3 spec renders");
    let normalized = normalize(&rendered);

    assert!(rendered.contains("struct NestedS3Source"));
    assert!(rendered.contains("struct NestedS3SourceShipTo"));
    assert!(normalized.contains("city: Option<String>, zip: Option<String>, }"));
    assert!(normalized.contains("ship_to: Option<NestedS3SourceShipTo>,"));
    assert!(normalized.contains("tags: Option<Vec<String>>,"));

    assert!(rendered.contains("use object_store::ObjectStoreExt;"));
    assert!(rendered.contains("object_store::aws::AmazonS3Builder::from_env()"));
    assert!(rendered.contains(".with_bucket_name(\"datacrate-workshop\")"));
    assert!(normalized.contains("ctx.runtime_env() .register_object_store("));
    assert!(rendered.contains("fn put_object_s3(bucket: &str, key: &str, bytes: Vec<u8>)"));
    assert!(
        rendered.contains("put_object_s3(\"datacrate-workshop\", \"out/nested.parquet\", bytes)")
    );
}

/// The complete one-command demo: a real dbt `schema.yml` (nested
/// `customer.address` struct column, same `struct<...>` grammar
/// `declared_type_to_arrow` already supports), an S3/MinIO Parquet source,
/// a business transformation (filter + group-by aggregate), and an S3/MinIO
/// Parquet sink — exactly what `scaffold-cli pipeline` renders for
/// `fixtures/dbt/orders_schema.yml`. Proves the renderer's output, not a
/// live dbt/S3 endpoint, per this file's external-systems framing above.
#[cfg(feature = "dbt")]
#[test]
fn dbt_schema_with_nested_struct_plus_s3_source_transform_and_sink_render_correctly() {
    let yaml = std::fs::read_to_string("../../fixtures/dbt/orders_schema.yml")
        .expect("read dbt schema.yml fixture");
    let source_schema =
        schema_codegen::parse_dbt_schema_yml(&yaml, "orders").expect("orders model parses");
    let target_schema = schema_codegen::parse_dbt_schema_yml(&yaml, "orders_totals")
        .expect("orders_totals model parses");

    let spec = PipelineSpec {
        name: "OrdersPipeline",
        source_schema: &source_schema,
        target_schema: Some(&target_schema),
        source_data: SourceData {
            location: "s3://datacrate-raw/orders.parquet",
            format: SourceFormat::Parquet,
        },
        transform: Some(TransformSpec::DataFrame(vec![
            DfOp::Filter {
                column: "quantity".to_string(),
                cmp: Comparator::Gt,
                value: Literal::Int(0),
            },
            DfOp::Aggregate {
                group_by: vec!["region".to_string()],
                aggs: vec![AggExpr {
                    column: "quantity".to_string(),
                    func: AggFn::Sum,
                    alias: "total_quantity".to_string(),
                }],
            },
        ])),
        sink_parquet: "s3://datacrate-curated/orders_totals.parquet",
    };

    let rendered = pipeline_source(&spec).expect("dbt nested s3 spec renders");
    let normalized = normalize(&rendered);

    assert!(rendered.contains("struct OrdersPipelineSourceCustomerAddress"));
    assert!(rendered.contains("struct OrdersPipelineSourceCustomer"));
    assert!(normalized.contains("customer: Option<OrdersPipelineSourceCustomer>,"));
    assert!(rendered.contains("struct OrdersPipelineTarget"));
    assert!(normalized.contains("total_quantity: Option<i64>,"));

    assert!(rendered.contains(".with_bucket_name(\"datacrate-raw\")"));
    assert!(rendered.contains(".with_bucket_name(\"datacrate-curated\")"));
    assert!(normalized.contains("OrdersPipelineSource::register_parquet( &ctx, \"batch\","));
    assert!(normalized.contains(".dataframe_transform::< OrdersPipelineTarget, >("));
    assert!(normalized.contains(".aggregate( vec![datafusion::prelude::col(\"region\")]"));
    assert!(rendered.contains("put_object_s3(\"datacrate-curated\", \"orders_totals.parquet\""));
}
