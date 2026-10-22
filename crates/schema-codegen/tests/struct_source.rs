use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::ArrowWriter;
use schema_codegen::SchemaCodegenError;

fn write_parquet(schema: Arc<Schema>, batch: &RecordBatch) -> tempfile::TempPath {
    let file = tempfile::NamedTempFile::new().expect("temp file");
    let path = file.into_temp_path();
    let out = std::fs::File::create(&path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(out, schema, None).expect("arrow writer");
    writer.write(batch).expect("write batch");
    writer.close().expect("close writer");
    path
}

#[test]
fn struct_source_renders_supported_fields_with_option_for_nullable() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a"), None])),
        ],
    )
    .expect("batch matches schema");
    let path = write_parquet(schema, &batch);

    let read_schema = schema_codegen::read_parquet_schema(&path).expect("schema reads back");
    let source =
        schema_codegen::struct_source("Order", &read_schema).expect("fields are supported");

    assert_eq!(
        source,
        "#[derive(Contract, Connector)]\nstruct Order {\n    id: i64,\n    name: Option<String>,\n}\n"
    );
}

#[test]
fn struct_source_errors_on_an_unsupported_arrow_type() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "created_at",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(TimestampMicrosecondArray::from(vec![0]))],
    )
    .expect("batch matches schema");
    let path = write_parquet(schema, &batch);

    let read_schema = schema_codegen::read_parquet_schema(&path).expect("schema reads back");
    let err = schema_codegen::struct_source("Event", &read_schema)
        .expect_err("timestamp has no Rust mapping");

    assert!(matches!(
        err,
        SchemaCodegenError::UnsupportedArrowType { field, .. } if field == "created_at"
    ));
}
