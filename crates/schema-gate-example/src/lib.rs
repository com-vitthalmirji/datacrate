//! Proves the compile-time feasibility gate end to end, against three
//! independent schema source formats: `build.rs` reads a reference
//! Parquet file, dbt `schema.yml`, and Erwin XML export (three different
//! real-world ways a data steward's source of truth shows up) and
//! generates [`SourceOfTruth`], [`DbtSourceOfTruth`], and
//! [`ErwinSourceOfTruth`] fresh on every build; `StewardInputModel` is the
//! data steward's hand-written declaration of what a pipeline expects to
//! ingest. Each `const _` check below fails `cargo build` the moment its
//! source drifts from that declaration, turning a schema mismatch into a
//! compiler error instead of a pipeline failure discovered at runtime.
#![allow(dead_code)]
use connectors_derive::Connector;
use contracts::{Contract, ExactOrdered, SchemaConforms};

include!(concat!(env!("OUT_DIR"), "/generated_schema.rs"));

/// The data steward's declared input schema for this pipeline. Checked
/// against [`SourceOfTruth`] below, not against the Parquet file directly.
#[derive(Contract, Connector)]
pub struct StewardInputModel {
    id: i64,
    customer_name: Option<String>,
}

// A plain call to `contracts::enforce` is a normal (non-const) function and
// is only monomorphized if something actually calls it; referencing the
// associated const directly is what forces evaluation during every build,
// matching the pattern `contracts`' own compile-fail tests use.
const _: () = SchemaConforms::<SourceOfTruth, StewardInputModel, ExactOrdered>::CHECK;
const _: () = SchemaConforms::<DbtSourceOfTruth, StewardInputModel, ExactOrdered>::CHECK;
const _: () = SchemaConforms::<ErwinSourceOfTruth, StewardInputModel, ExactOrdered>::CHECK;
