#![cfg(feature = "dbt")]

use arrow_schema::DataType;
use schema_codegen::SchemaCodegenError;

#[test]
fn parse_dbt_schema_yml_infers_nullability_from_not_null_test() {
    let yaml = r#"
models:
  - name: orders
    columns:
      - name: id
        data_type: bigint
        tests:
          - not_null
      - name: customer_name
        data_type: varchar(100)
"#;

    let schema = schema_codegen::parse_dbt_schema_yml(yaml, "orders").expect("model exists");

    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert!(!schema.field(0).is_nullable());
    assert_eq!(schema.field(1).name(), "customer_name");
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    assert!(schema.field(1).is_nullable());
}

#[test]
fn parse_dbt_schema_yml_errors_on_unknown_model() {
    let yaml = "models: []\n";

    let err = schema_codegen::parse_dbt_schema_yml(yaml, "orders")
        .expect_err("no model named orders exists");

    assert!(matches!(
        err,
        SchemaCodegenError::ModelNotFound { model } if model == "orders"
    ));
}

#[test]
fn parse_dbt_schema_yml_errors_on_unmapped_declared_type() {
    let yaml = r#"
models:
  - name: orders
    columns:
      - name: payload
        data_type: jsonb
"#;

    let err = schema_codegen::parse_dbt_schema_yml(yaml, "orders")
        .expect_err("jsonb has no Rust mapping");

    assert!(matches!(
        err,
        SchemaCodegenError::UnsupportedDeclaredType { field, .. } if field == "payload"
    ));
}
