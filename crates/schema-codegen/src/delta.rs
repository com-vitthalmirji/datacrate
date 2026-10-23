//! Reads a Delta table's current Arrow schema from its transaction log,
//! without scanning any data file — the same "schema only" contract
//! `read_parquet_schema` gives for a footer, just behind a log instead of a
//! file.

use crate::SchemaCodegenError;
use arrow_schema::Schema;
use url::Url;

/// Opens the Delta table at `table_uri` (e.g. `file:///path/to/table` or
/// `s3://bucket/key`) and returns its current Arrow schema.
///
/// Drives the async `deltalake`/DataFusion calls on a single current-thread
/// runtime, the same choice `typestate::contract_pipeline::sql_transform`
/// makes for the same reason: this function's own contract is synchronous,
/// and there is no surrounding async runtime to borrow one from.
///
/// # Errors
/// Returns [`SchemaCodegenError::InvalidDeltaUri`] if `table_uri` is not a
/// valid URL, [`SchemaCodegenError::DeltaRuntime`] if the runtime cannot be
/// built, [`SchemaCodegenError::OpenDeltaTable`] if the table cannot be
/// opened, or [`SchemaCodegenError::DeltaTableProvider`] if its table
/// provider cannot be built.
pub fn read_delta_schema(table_uri: &str) -> Result<Schema, SchemaCodegenError> {
    let url = Url::parse(table_uri).map_err(|source| SchemaCodegenError::InvalidDeltaUri {
        uri: table_uri.to_string(),
        source,
    })?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| SchemaCodegenError::DeltaRuntime { source })?;
    runtime.block_on(async {
        let table = deltalake::open_table(url)
            .await
            .map_err(|source| SchemaCodegenError::OpenDeltaTable { source })?;
        let provider = table
            .table_provider()
            .await
            .map_err(|source| SchemaCodegenError::DeltaTableProvider { source })?;
        Ok(provider.schema().as_ref().clone())
    })
}
