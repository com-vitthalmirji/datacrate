#![cfg(feature = "erwin")]

use arrow_schema::DataType;
use schema_codegen::SchemaCodegenError;

#[test]
fn parse_erwin_xml_reads_declared_nullability() {
    let xml = r#"
<Entities>
  <Entity name="Orders">
    <Attribute name="id" type="bigint" nullable="false"/>
    <Attribute name="customer_name" type="varchar(100)" nullable="true"/>
  </Entity>
</Entities>
"#;

    let schema = schema_codegen::parse_erwin_xml(xml, "Orders").expect("entity exists");

    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert!(!schema.field(0).is_nullable());
    assert_eq!(schema.field(1).name(), "customer_name");
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    assert!(schema.field(1).is_nullable());
}

#[test]
fn parse_erwin_xml_errors_on_unknown_entity() {
    let xml = "<Entities></Entities>";

    let err =
        schema_codegen::parse_erwin_xml(xml, "Orders").expect_err("no entity named Orders exists");

    assert!(matches!(
        err,
        SchemaCodegenError::EntityNotFound { entity } if entity == "Orders"
    ));
}

#[test]
fn parse_erwin_xml_errors_on_unmapped_declared_type() {
    let xml = r#"
<Entities>
  <Entity name="Orders">
    <Attribute name="payload" type="jsonb"/>
  </Entity>
</Entities>
"#;

    let err =
        schema_codegen::parse_erwin_xml(xml, "Orders").expect_err("jsonb has no Rust mapping");

    assert!(matches!(
        err,
        SchemaCodegenError::UnsupportedDeclaredType { field, .. } if field == "payload"
    ));
}
