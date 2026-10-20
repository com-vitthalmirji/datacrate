//! Download an identity-bound completed output and inspect its declared row count.
//!
//! Uses the same AWS environment variables as run-s3-pipeline. Identity validation
//! does not recompute the non-cryptographic logical fingerprint or establish latest.

use clap::Parser;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pipeline::object_store_io::download_completed_output;
use std::error::Error;
use std::path::{Path, PathBuf};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    bucket: String,
    #[arg(long)]
    output_key: String,
    #[arg(long)]
    destination: PathBuf,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let store = AmazonS3Builder::from_env()
        .with_bucket_name(&args.bucket)
        .build()?;
    let completion = download_completed_output(
        &store,
        &ObjectPath::from(args.output_key),
        &args.destination,
    )?;
    verify_row_count(&args.destination, completion.row_count)?;
    println!(
        "downloaded recorded output identity; version={} rows={} digest={} (fingerprint not recomputed)",
        completion.schema_version, completion.row_count, completion.content_digest
    );
    Ok(())
}

/// Decode bounded batches; the completion fingerprint is deliberately not verified here.
fn verify_row_count(path: &Path, expected: usize) -> Result<(), Box<dyn Error>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path)?)?
        .with_batch_size(4096)
        .build()?;
    let mut rows = 0usize;
    for batch in reader {
        rows = rows
            .checked_add(batch?.num_rows())
            .ok_or_else(|| std::io::Error::other("decoded row count overflow"))?;
    }
    if rows != expected {
        return Err(std::io::Error::other(format!(
            "completion reports {expected} rows, downloaded Parquet contains {rows}"
        ))
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn verifies_rows_across_decode_batches_and_rejects_wrong_count() {
        let dir = tempfile::tempdir().expect("directory");
        let path = dir.path().join("output.parquet");
        let batch = RecordBatch::try_new(
            pipeline::schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..9000)),
                Arc::new(StringArray::from_iter_values((0..9000).map(|_| "Ada"))),
                Arc::new(StringArray::from(vec![None::<&str>; 9000])),
            ],
        )
        .expect("batch");
        pipeline::write_parquet(&batch, &path, parquet::basic::Compression::SNAPPY).expect("write");
        verify_row_count(&path, 9000).expect("matching count");
        assert!(
            verify_row_count(&path, 8999)
                .expect_err("mismatch")
                .to_string()
                .contains("contains 9000")
        );
    }
}
