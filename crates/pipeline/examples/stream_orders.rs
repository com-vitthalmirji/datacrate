//! Consume the ordered row query one output batch at a time.
//! Run with a four-column orders Parquet file: `stream_orders path.parquet`.

use std::path::PathBuf;

use datafusion::prelude::SessionContext;
use futures_util::TryStreamExt;
use pipeline::datafusion_query::{register_orders, row_query_plan};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let path = PathBuf::from(args.next().ok_or("usage: stream_orders ORDERS.parquet")?);
    if args.next().is_some() {
        return Err("usage: stream_orders ORDERS.parquet".into());
    }
    let (batches, rows) = consume_orders(&path).await?;
    println!("consumed {batches} batches, {rows} rows");
    Ok(())
}

async fn consume_orders(
    path: &std::path::Path,
) -> Result<(usize, usize), datafusion::error::DataFusionError> {
    let ctx = SessionContext::new();
    register_orders(&ctx, path).await?;
    // ORDER BY may buffer or spill upstream; streaming bounds only the
    // result retained by this consumer, not the complete query's memory.
    let mut stream = row_query_plan(&ctx).await?.execute_stream().await?;
    let mut batches = 0usize;
    let mut rows = 0usize;
    while let Some(batch) = stream.try_next().await? {
        rows = rows.checked_add(batch.num_rows()).ok_or_else(|| {
            datafusion::error::DataFusionError::Execution("row count overflow".to_owned())
        })?;
        batches += 1;
    }
    Ok((batches, rows))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn consumes_fixture_without_retaining_output_batches() {
        let input = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/m3/orders.csv");
        let batch = pipeline::datafusion_query::fixture_to_orders_batch(&input).expect("fixture");
        let file = tempfile::Builder::new()
            .suffix(".parquet")
            .tempfile()
            .expect("Parquet temp file");
        pipeline::write_parquet(&batch, file.path(), parquet::basic::Compression::SNAPPY)
            .expect("Parquet");
        let (batches, rows) = consume_orders(file.path()).await.expect("consume");
        assert!(batches > 0);
        assert_eq!(rows, 5);
    }
}
