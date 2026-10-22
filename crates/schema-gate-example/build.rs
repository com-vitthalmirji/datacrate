//! Re-derives one `SourceOfTruth` struct per schema-codegen input format on
//! every build, so `src/lib.rs`'s compile-time gates always check against
//! what each reference file currently contains, not a stale copy.
use std::env;
use std::path::{Path, PathBuf};

fn main() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/schema-gate-example");

    let parquet_fixture = fixtures.join("orders.parquet");
    println!("cargo::rerun-if-changed={}", parquet_fixture.display());
    let parquet_schema = schema_codegen::read_parquet_schema(&parquet_fixture)
        .expect("reference parquet file is readable");
    let parquet_source = schema_codegen::struct_source("SourceOfTruth", &parquet_schema)
        .expect("reference parquet file uses only supported Arrow types");

    let dbt_fixture = fixtures.join("orders_dbt_schema.yml");
    println!("cargo::rerun-if-changed={}", dbt_fixture.display());
    let dbt_yaml =
        std::fs::read_to_string(&dbt_fixture).expect("reference dbt schema.yml is readable");
    let dbt_schema = schema_codegen::parse_dbt_schema_yml(&dbt_yaml, "orders")
        .expect("reference dbt schema.yml has an `orders` model with supported column types");
    let dbt_source = schema_codegen::struct_source("DbtSourceOfTruth", &dbt_schema)
        .expect("reference dbt schema.yml uses only supported Arrow types");

    let erwin_fixture = fixtures.join("orders_erwin.xml");
    println!("cargo::rerun-if-changed={}", erwin_fixture.display());
    let erwin_xml =
        std::fs::read_to_string(&erwin_fixture).expect("reference erwin export is readable");
    let erwin_schema = schema_codegen::parse_erwin_xml(&erwin_xml, "Orders")
        .expect("reference erwin export has an `Orders` entity with supported attribute types");
    let erwin_source = schema_codegen::struct_source("ErwinSourceOfTruth", &erwin_schema)
        .expect("reference erwin export uses only supported Arrow types");

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("cargo sets OUT_DIR"));
    std::fs::write(
        out_dir.join("generated_schema.rs"),
        format!("{parquet_source}\n{dbt_source}\n{erwin_source}"),
    )
    .expect("writing generated_schema.rs into OUT_DIR");
}
