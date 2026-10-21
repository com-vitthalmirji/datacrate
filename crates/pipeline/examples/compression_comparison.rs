//! Bounded, explicit Parquet layout experiment. Generation and equality checks are untimed.
//! Run one candidate per process; interleave processes externally for repeated samples.
use arrow::{
    array::{
        ArrayRef, Decimal128Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    },
    datatypes::SchemaRef,
};
use clap::{Parser, ValueEnum};
use datafusion::{
    physical_plan::ExecutionPlan,
    prelude::{SessionConfig, SessionContext},
};
use futures_util::TryStreamExt;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use serde_json::{Value, json};
use std::{error::Error, fs::File, path::Path, sync::Arc, time::Instant};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
#[derive(Clone, Copy, Debug, ValueEnum)]
enum Dataset {
    Ingest,
    Orders,
}
#[derive(Clone, Copy, Debug, ValueEnum)]
enum Candidate {
    Baseline,
    Zstd3,
    SmallGroups,
}
#[derive(Parser, Debug)]
struct Args {
    #[arg(long, value_enum)]
    dataset: Dataset,
    #[arg(long, value_enum)]
    candidate: Candidate,
    #[arg(long)]
    rows: usize,
    #[arg(long)]
    shuffled: bool,
    #[arg(long)]
    high_cardinality: bool,
    #[arg(long)]
    long_notes: bool,
}
fn validate(args: &Args) -> Result<()> {
    if args.rows == 0 || args.rows > 250_000 || (args.long_notes && args.rows > 20_000) {
        return Err(
            std::io::Error::other("rows must be 1..250000, or 1..20000 with long notes").into(),
        );
    }
    Ok(())
}
fn schema(dataset: Dataset) -> SchemaRef {
    match dataset {
        Dataset::Ingest => pipeline::schema(),
        Dataset::Orders => pipeline::datafusion_query::orders_schema(),
    }
}
fn ids(args: &Args) -> Vec<i64> {
    let mut ids = (0..args.rows as i64).collect::<Vec<_>>();
    if args.shuffled {
        let mut state = 42u64;
        for index in (1..ids.len()).rev() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ids.swap(index, (state % (index as u64 + 1)) as usize);
        }
    }
    ids
}
fn batch(args: &Args, ids: &[i64]) -> Result<RecordBatch> {
    let notes = StringArray::from_iter(ids.iter().map(|id| {
        if id % 7 == 0 {
            None
        } else {
            let key = if args.high_cardinality { *id } else { id % 5 };
            Some(format!(
                "雪,{key}\n{}",
                "x".repeat(if args.long_notes { 4096 } else { 16 })
            ))
        }
    }));
    let mut columns: Vec<ArrayRef> =
        vec![Arc::new(Int64Array::from_iter_values(ids.iter().copied()))];
    match args.dataset {
        Dataset::Ingest => columns
            .push(Arc::new(StringArray::from_iter_values(ids.iter().map(
                |id| format!("name-{}", if args.high_cardinality { *id } else { id % 5 }),
            )))),
        Dataset::Orders => {
            columns.push(Arc::new(
                Decimal128Array::from_iter_values(
                    ids.iter().map(|id| i128::from(*id) * 100 - 5000),
                )
                .with_precision_and_scale(10, 2)?,
            ));
            columns.push(Arc::new(TimestampMicrosecondArray::from_iter_values(
                ids.iter().map(|id| id * 1_000_000 - 86_400_000_000),
            )));
        }
    }
    columns.push(Arc::new(notes));
    Ok(RecordBatch::try_new(schema(args.dataset), columns)?)
}
fn properties(candidate: Candidate) -> Result<WriterProperties> {
    let base = WriterProperties::builder();
    Ok(match candidate {
        Candidate::Baseline => base.set_compression(Compression::SNAPPY),
        Candidate::Zstd3 => base.set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?)),
        Candidate::SmallGroups => base
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_row_count(Some(65_536)),
    }
    .build())
}
fn verify(args: &Args, ids: &[i64], path: &Path) -> Result<Value> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    if builder.schema().as_ref() != schema(args.dataset).as_ref() {
        return Err("schema changed".into());
    }
    let metadata = builder.metadata();
    let groups = metadata.row_groups().iter().map(|group| json!({
        "rows": group.num_rows(),
        "columns_with_min_max": group.columns().iter().filter(|column|
            column.statistics().is_some_and(|stats| stats.min_bytes_opt().is_some() && stats.max_bytes_opt().is_some())).count(),
        "compressed_bytes": group.compressed_size(),
    })).collect::<Vec<_>>();
    let mut offset = 0;
    for decoded in builder.with_batch_size(1024).build()? {
        let decoded = decoded?;
        let end = offset + decoded.num_rows();
        if end > ids.len() || decoded != batch(args, &ids[offset..end])? {
            return Err("round-trip values or nulls changed".into());
        }
        offset = end;
    }
    if offset != ids.len() {
        return Err("round-trip row count changed".into());
    }
    use std::io::{Read, Seek, SeekFrom};
    let mut file = File::open(path)?;
    file.seek(SeekFrom::End(-8))?;
    let mut trailer = [0; 8];
    file.read_exact(&mut trailer)?;
    let footer_bytes = u32::from_le_bytes(trailer[..4].try_into()?) as u64;
    Ok(json!({"row_groups": groups, "footer_metadata_bytes": footer_bytes}))
}
fn metrics(plan: &Arc<dyn ExecutionPlan>, output: &mut Vec<Value>) {
    if let Some(metrics) = plan.metrics() {
        for metric in metrics.iter() {
            let name = metric.value().name();
            if name.contains("bytes_scanned")
                || name.contains("row_groups")
                || name == "output_rows"
            {
                output.push(json!({"operator": plan.name(), "name": name, "value": metric.value().as_usize()}));
            }
        }
    }
    for child in plan.children() {
        metrics(child, output);
    }
}
async fn scan(path: &Path, cutoff: Option<i64>) -> Result<Value> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    ctx.register_parquet(
        "sample",
        path.to_str().ok_or("non-UTF8 path")?,
        Default::default(),
    )
    .await?;
    let sql = match cutoff {
        Some(cutoff) => format!("SELECT id FROM sample WHERE id >= {cutoff}"),
        None => "SELECT * FROM sample".to_owned(),
    };
    let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
    let started = Instant::now();
    let mut stream = datafusion::physical_plan::execute_stream(Arc::clone(&plan), ctx.task_ctx())?;
    let mut rows = 0usize;
    let mut id_sum = 0i128;
    while let Some(batch) = stream.try_next().await? {
        rows += batch.num_rows();
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("id type changed")?;
        id_sum += ids.values().iter().map(|id| i128::from(*id)).sum::<i128>();
    }
    let nanos = started.elapsed().as_nanos();
    let mut observed = Vec::new();
    metrics(&plan, &mut observed);
    Ok(
        json!({"wall_ns": nanos, "rows": rows, "id_sum": id_sum.to_string(), "engine_metrics": observed,
        "object_store_requests": null, "wire_bytes": null, "stage_cpu_ns": null}),
    )
}
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    validate(&args)?;
    let ids = ids(&args);
    // All generation is outside write timing; fixture is explicitly capped.
    let batches = ids
        .chunks(1024)
        .map(|ids| batch(&args, ids))
        .collect::<Result<Vec<_>>>()?;
    let properties = properties(args.candidate)?;
    let effective = format!("{properties:?}");
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("sample.parquet");
    let started = Instant::now();
    let mut writer =
        ArrowWriter::try_new(File::create(&path)?, schema(args.dataset), Some(properties))?;
    for batch in &batches {
        writer.write(batch)?;
    }
    writer.close()?;
    let write_ns = started.elapsed().as_nanos();
    drop(batches);
    let metadata = verify(&args, &ids, &path)?;
    let cutoff = args.rows as i64 * 99 / 100;
    let full = scan(&path, None).await?;
    let selective = scan(&path, Some(cutoff)).await?;
    for (scan, start) in [(&full, 0), (&selective, cutoff)] {
        let count = args.rows as i64 - start;
        let expected_sum = (start as i128 + args.rows as i128 - 1) * count as i128 / 2;
        if scan["rows"] != json!(count) || scan["id_sum"] != json!(expected_sum.to_string()) {
            return Err("scan result differs from independent arithmetic oracle".into());
        }
    }
    println!(
        "{}",
        json!({"settings": format!("{args:?}"), "seed": 42, "effective_writer_properties": effective,
        "file_bytes": std::fs::metadata(&path)?.len(), "write_wall_ns": write_ns, "metadata": metadata,
        "full_scan": full, "selective_scan": selective,
        "timing": "write excludes generation and includes close, no fsync; scans exclude planning and follow untimed equality decode (warm cache)"})
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn layouts_preserve_schema_nulls_decimal_timestamp_and_metadata() {
        for dataset in [Dataset::Ingest, Dataset::Orders] {
            let args = Args {
                dataset,
                candidate: Candidate::Baseline,
                rows: 2051,
                shuffled: true,
                high_cardinality: true,
                long_notes: false,
            };
            let ids = ids(&args);
            let first = batch(&args, &[0, 1, 7]).expect("known batch");
            assert!(first.column(first.num_columns() - 1).is_null(0));
            assert!(!first.column(first.num_columns() - 1).is_null(1));
            if matches!(dataset, Dataset::Orders) {
                assert_eq!(
                    first
                        .column(1)
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .expect("decimal")
                        .value(1),
                    -4900
                );
                assert_eq!(
                    first
                        .column(2)
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .expect("time")
                        .value(1),
                    -86_399_000_000
                );
            }
            for candidate in [
                Candidate::Baseline,
                Candidate::Zstd3,
                Candidate::SmallGroups,
            ] {
                let dir = tempfile::tempdir().expect("directory");
                let path = dir.path().join("sample.parquet");
                let mut writer = ArrowWriter::try_new(
                    File::create(&path).expect("file"),
                    schema(dataset),
                    Some(properties(candidate).expect("properties")),
                )
                .expect("writer");
                for chunk in ids.chunks(1024) {
                    writer
                        .write(&batch(&args, chunk).expect("batch"))
                        .expect("write");
                }
                writer.close().expect("close");
                let meta = verify(&args, &ids, &path).expect("schema and every value");
                assert!(meta["footer_metadata_bytes"].as_u64().expect("footer") > 0);
                assert_eq!(meta["row_groups"][0]["rows"], 2051);
                assert_eq!(
                    meta["row_groups"][0]["columns_with_min_max"],
                    first.num_columns()
                );
            }
        }
    }
    #[test]
    fn small_group_candidate_changes_physical_boundaries_without_changing_rows() {
        let args = Args {
            dataset: Dataset::Orders,
            candidate: Candidate::SmallGroups,
            rows: 70000,
            shuffled: false,
            high_cardinality: false,
            long_notes: false,
        };
        let ids = ids(&args);
        let dir = tempfile::tempdir().expect("directory");
        let path = dir.path().join("groups.parquet");
        let mut writer = ArrowWriter::try_new(
            File::create(&path).expect("file"),
            schema(args.dataset),
            Some(properties(args.candidate).expect("policy")),
        )
        .expect("writer");
        for chunk in ids.chunks(1024) {
            writer
                .write(&batch(&args, chunk).expect("batch"))
                .expect("write");
        }
        writer.close().expect("close");
        let meta = verify(&args, &ids, &path).expect("round trip");
        assert_eq!(meta["row_groups"].as_array().expect("groups").len(), 2);
        assert_eq!(meta["row_groups"][0]["rows"], 65536);
        assert_eq!(meta["row_groups"][1]["rows"], 4464);
    }

    #[test]
    fn rejects_unbounded_fixture_sizes_before_creating_output() {
        let args = Args {
            dataset: Dataset::Ingest,
            candidate: Candidate::Baseline,
            rows: 250001,
            shuffled: false,
            high_cardinality: false,
            long_notes: false,
        };
        assert!(validate(&args).is_err());
        assert!(
            validate(&Args {
                rows: 20001,
                long_notes: true,
                ..args
            })
            .is_err()
        );
    }
}
