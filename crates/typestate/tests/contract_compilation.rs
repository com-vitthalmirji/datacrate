#[test]
fn automatic_connections_and_stage_states_are_checked_by_builds() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/contract_pass/*.rs");
    cases.compile_fail("tests/contract_compile_fail/*.rs");
}

/// Proves `schema_codegen::pipeline_source()`'s own output is real, compiling
/// code, not just plausible-looking text: renders a passthrough, a
/// sql_transform, and a dataframe_transform spec, writes each into
/// `tests/contract_pass/`, and asserts all three compile under the same
/// trybuild harness every other fixture here uses.
#[test]
fn generated_pipeline_scaffolds_compile() {
    use arrow::datatypes::{DataType, Field, Schema};
    use schema_codegen::{
        AggExpr, AggFn, Comparator, DfOp, Literal, PipelineSpec, SortKey, SourceData, SourceFormat,
        TransformSpec, pipeline_source,
    };

    let source_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("quantity", DataType::Int32, false),
    ]);

    let passthrough_spec = PipelineSpec {
        name: "GeneratedPassthrough",
        source_schema: &source_schema,
        target_schema: None,
        source_data: SourceData {
            location: "fixtures/m2/orders.parquet",
            format: SourceFormat::Parquet,
        },
        transform: None,
        sink_parquet: "/tmp/generated_passthrough_out.parquet",
    };
    let passthrough_rendered =
        pipeline_source(&passthrough_spec).expect("passthrough spec renders");
    std::fs::write(
        "tests/contract_pass/generated_pipeline_passthrough.rs",
        passthrough_rendered,
    )
    .expect("write passthrough fixture");

    let target_schema = Schema::new(vec![Field::new("total", DataType::Int64, false)]);
    let sql_transform_spec = PipelineSpec {
        name: "GeneratedSqlTransform",
        source_schema: &source_schema,
        target_schema: Some(&target_schema),
        source_data: SourceData {
            location: "fixtures/m2/orders.parquet",
            format: SourceFormat::Parquet,
        },
        transform: Some(TransformSpec::Sql(
            "SELECT SUM(quantity) AS total FROM batch",
        )),
        sink_parquet: "/tmp/generated_sql_transform_out.parquet",
    };
    let sql_transform_rendered =
        pipeline_source(&sql_transform_spec).expect("sql_transform spec renders");
    std::fs::write(
        "tests/contract_pass/generated_pipeline_sql_transform.rs",
        sql_transform_rendered,
    )
    .expect("write sql_transform fixture");

    let dataframe_transform_spec = PipelineSpec {
        name: "GeneratedDataframeTransform",
        source_schema: &source_schema,
        target_schema: Some(&target_schema),
        source_data: SourceData {
            location: "fixtures/m2/orders.parquet",
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
        sink_parquet: "/tmp/generated_dataframe_transform_out.parquet",
    };
    let dataframe_transform_rendered =
        pipeline_source(&dataframe_transform_spec).expect("dataframe_transform spec renders");
    std::fs::write(
        "tests/contract_pass/generated_pipeline_dataframe_transform.rs",
        dataframe_transform_rendered,
    )
    .expect("write dataframe_transform fixture");

    let cases = trybuild::TestCases::new();
    cases.pass("tests/contract_pass/generated_pipeline_passthrough.rs");
    cases.pass("tests/contract_pass/generated_pipeline_sql_transform.rs");
    cases.pass("tests/contract_pass/generated_pipeline_dataframe_transform.rs");
}

// Nested struct/list fields and s3:// source/sink codegen are covered by
// `crates/schema-codegen/tests/pipeline_template.rs` as rendered-text
// assertions, not here: S3/MinIO is an external system, like a dbt or Erwin
// source, and proving the renderer's output doesn't require actually running
// generated code against a live bucket (`trybuild::TestCases::pass` compiles
// *and executes* the binary, which a real `AmazonS3Builder::from_env()` call
// would need a live endpoint for).
