//! Point-read tuning over immutable copies of the existing 5M-row orders fixture.
//! Physical rebinding below supports only this single-file id-equality query.
//! It is an experimental scan template, not a general prepared-statement cache.

use std::{
    error::Error,
    fs::File,
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use arrow::{
    array::{Array, Decimal128Array, Int64Array, StringArray, TimestampMicrosecondArray},
    record_batch::RecordBatch,
};
use clap::Parser;
use datafusion::{
    common::{ScalarValue, Statistics},
    datasource::{
        physical_plan::{FileScanConfig, FileScanConfigBuilder, ParquetSource},
        source::DataSourceExec,
    },
    logical_expr::Operator,
    physical_expr::{
        PhysicalExpr,
        expressions::{BinaryExpr, Column, Literal},
    },
    physical_plan::{
        ExecutionPlan, displayable, execute_stream, filter::FilterExec, metrics::MetricValue,
    },
    prelude::{SessionConfig, SessionContext},
};
use futures_util::TryStreamExt;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
    schema::types::ColumnPath,
};
use pipeline::datafusion_query::{orders_schema, register_orders};
use serde::Serialize;
use serde_json::{Value, json};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
#[cfg(feature = "point-query-mimalloc")]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

const ROWS: i64 = 5_000_000;
const BOUNDARIES: &[i64] = &[
    -1,
    0,
    1,
    20479,
    20480,
    131071,
    131072,
    1048575,
    1048576,
    ROWS - 1,
    ROWS,
];

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "benchmark/spark-comet/orders.parquet")]
    input: PathBuf,
    #[arg(
        long,
        default_value = "docs/internals/point-query-tuning-2026-10-25/layouts"
    )]
    layouts: PathBuf,
    /// Create bounded streaming rewrites and check every row, then exit.
    #[arg(long)]
    prepare_files: bool,
    #[arg(long, default_value_t = 64)]
    lookups: usize,
    #[arg(long, default_value_t = 5)]
    rounds: usize,
    #[arg(long, default_value_t = 42)]
    seed: u64,
    /// Compare Tokio's current-thread runtime with its normal multi-thread runtime.
    #[arg(long)]
    current_thread: bool,
}

fn invalid(message: &str) -> Box<dyn Error> {
    io::Error::other(message).into()
}

fn reader(path: &Path) -> Result<ParquetRecordBatchReaderBuilder<File>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    if reader.metadata().file_metadata().num_rows() != ROWS
        || reader.schema().as_ref() != orders_schema().as_ref()
    {
        return Err(invalid(
            "requires the 5M-row orders benchmark fixture schema",
        ));
    }
    Ok(reader)
}

/// Independent arithmetic oracle for all four columns, including nullable notes.
fn verify_file(path: &Path) -> Result<Value> {
    let builder = reader(path)?;
    let groups = builder
        .metadata()
        .row_groups()
        .iter()
        .map(|g| g.num_rows())
        .collect::<Vec<_>>();
    let mut offset = 0i64;
    for batch in builder.with_batch_size(8192).build()? {
        let batch = batch?;
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("id type changed"))?;
        let amounts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .ok_or_else(|| invalid("amount type changed"))?;
        let timestamps = batch
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| invalid("timestamp type changed"))?;
        let notes = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| invalid("note type changed"))?;
        let expected_notes = [
            Some("first order"),
            Some("rush"),
            Some("bulk"),
            Some("backfill"),
            None,
        ];
        for row in 0..batch.num_rows() {
            let id = offset + row as i64;
            let note = (!notes.is_null(row)).then(|| notes.value(row));
            if ids.is_null(row)
                || amounts.is_null(row)
                || timestamps.is_null(row)
                || ids.value(row) != id
                || amounts.value(row) != i128::from(id % 100 + 1) * 100
                || timestamps.value(row) != id * 86_400_000_000 / ROWS
                || note != expected_notes[id as usize % expected_notes.len()]
            {
                return Err(invalid("fixture values differ from the independent oracle"));
            }
        }
        offset += batch.num_rows() as i64;
    }
    if offset != ROWS {
        return Err(invalid("verification row count changed"));
    }
    Ok(
        json!({"path": path, "bytes": path.metadata()?.len(), "rows_verified": offset, "row_group_rows": groups}),
    )
}

fn layouts(args: &Args) -> Vec<(&'static str, PathBuf)> {
    vec![
        ("original", args.input.clone()),
        ("small-snappy", args.layouts.join("small-snappy.parquet")),
        ("small-lz4", args.layouts.join("small-lz4.parquet")),
        ("small-zstd", args.layouts.join("small-zstd.parquet")),
        (
            "small-zstd-no-dictionary",
            args.layouts.join("small-zstd-no-dictionary.parquet"),
        ),
    ]
}

fn prepare(args: &Args) -> Result<()> {
    std::fs::create_dir_all(&args.layouts)?;
    let mut results = vec![verify_file(&args.input)?];
    for (name, path) in layouts(args).into_iter().skip(1) {
        let compression = match name {
            "small-snappy" => Compression::SNAPPY,
            "small-lz4" => Compression::LZ4_RAW,
            _ => Compression::ZSTD(ZstdLevel::try_new(3)?),
        };
        let mut props = WriterProperties::builder()
            .set_compression(compression)
            .set_max_row_group_row_count(Some(131_072))
            .set_data_page_row_count_limit(2048)
            .set_column_dictionary_enabled(ColumnPath::from("id"), false);
        if name.ends_with("no-dictionary") {
            props = props.set_dictionary_enabled(false);
        }
        let props = props.build();
        let settings = format!("{props:?}");
        let started = Instant::now();
        let output = File::options().write(true).create_new(true).open(&path)?;
        let mut writer = ArrowWriter::try_new(output, orders_schema(), Some(props))?;
        for batch in reader(&args.input)?.with_batch_size(8192).build()? {
            writer.write(&batch?)?;
        }
        writer.close()?;
        let write_ns = started.elapsed().as_nanos();
        let verified = verify_file(&path)?;
        results.push(json!({"layout": name, "settings": settings, "write_ns": write_ns, "verified": verified}));
    }
    println!(
        "{}",
        json!({"preparation": results, "timing": "streaming rewrite includes input decoding; full verification outside timing; no fsync"})
    );
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum Planning {
    Sql,
    Logical,
    Rebind,
}

struct Case {
    name: String,
    ctx: SessionContext,
    mode: Planning,
    pushdown: bool,
    template: Option<FileScanConfig>,
    setup_ns: u128,
}

async fn make_case(
    layout: &str,
    path: &Path,
    mode: Planning,
    pushdown: bool,
    one_partition: bool,
) -> Result<Case> {
    let started = Instant::now();
    let mut config = SessionConfig::new();
    if one_partition {
        config = config.with_target_partitions(1);
    }
    config.options_mut().execution.parquet.pushdown_filters = pushdown;
    config.options_mut().execution.parquet.pruning = true;
    config.options_mut().execution.parquet.enable_page_index = true;
    let ctx = SessionContext::new_with_config(config);
    register_orders(&ctx, path).await?;
    let template = match mode {
        Planning::Logical => {
            ctx.sql("PREPARE point(BIGINT) AS SELECT id, amount FROM orders WHERE id = $1")
                .await?;
            None
        }
        Planning::Rebind => {
            let plan = ctx
                .sql("SELECT id, amount FROM orders WHERE id = 0")
                .await?
                .create_physical_plan()
                .await?;
            let scan = if plan.downcast_ref::<DataSourceExec>().is_some() {
                Arc::clone(&plan)
            } else if plan.downcast_ref::<FilterExec>().is_some() && plan.children().len() == 1 {
                Arc::clone(plan.children()[0])
            } else {
                return Err(invalid(
                    "scan template only supports a single scan with an optional filter",
                ));
            };
            let source = scan
                .downcast_ref::<DataSourceExec>()
                .ok_or_else(|| invalid("expected DataSourceExec"))?;
            let config = source
                .data_source()
                .downcast_ref::<FileScanConfig>()
                .ok_or_else(|| invalid("expected FileScanConfig"))?
                .clone();
            if config.file_groups.len() != 1
                || config.file_groups[0].len() != 1
                || config.limit.is_some()
                || config.file_source.downcast_ref::<ParquetSource>().is_none()
            {
                return Err(invalid(
                    "scan template requires one complete immutable Parquet file and no limit",
                ));
            }
            let file = config.file_groups[0]
                .iter()
                .next()
                .ok_or_else(|| invalid("missing file"))?;
            if !file.extensions.is_empty()
                || file
                    .range
                    .as_ref()
                    .is_some_and(|r| r.start != 0 || r.end as u64 != file.object_meta.size)
            {
                return Err(invalid(
                    "key-specific access plans and partial file ranges are unsupported",
                ));
            }
            Some(config)
        }
        Planning::Sql => None,
    };
    Ok(Case {
        name: format!(
            "{layout}/{}/{}/{mode:?}",
            if one_partition { "one" } else { "default" },
            if pushdown { "pushdown" } else { "no-pushdown" }
        ),
        ctx,
        mode,
        pushdown,
        template,
        setup_ns: started.elapsed().as_nanos(),
    })
}

fn rebind(config: &FileScanConfig, pushdown: bool, key: i64) -> Result<Arc<dyn ExecutionPlan>> {
    // Fresh predicate and source recreate pruning state and metrics for every key.
    // Reusing the old source or execution state could silently query the old key.
    let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("id", 0)),
        Operator::Eq,
        Arc::new(Literal::new(ScalarValue::Int64(Some(key)))),
    ));
    let original = config
        .file_source
        .downcast_ref::<ParquetSource>()
        .ok_or_else(|| invalid("expected ParquetSource"))?;
    let factory = original.parquet_file_reader_factory().ok_or_else(|| {
        invalid("scan template requires the registered metadata-cache reader factory")
    })?;
    let mut source = ParquetSource::new(config.file_source.table_schema().clone())
        .with_table_parquet_options(original.table_parquet_options().clone())
        .with_parquet_file_reader_factory(Arc::clone(factory))
        .with_predicate(Arc::clone(&predicate));
    if let Some(hint) = original.table_parquet_options().global.metadata_size_hint {
        source = source.with_metadata_size_hint(hint);
    }
    let config = FileScanConfigBuilder::from(config.clone())
        .with_source(Arc::new(source))
        .with_statistics(Statistics::new_unknown(
            config.file_source.table_schema().table_schema(),
        ))
        .with_projection_indices(Some(vec![0, 1]))?
        .build();
    let scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
    if pushdown {
        Ok(scan)
    } else {
        Ok(Arc::new(FilterExec::try_new(predicate, scan)?))
    }
}

async fn plan(case: &Case, key: i64) -> Result<Arc<dyn ExecutionPlan>> {
    match case.mode {
        Planning::Sql => Ok(case
            .ctx
            .sql(&format!("SELECT id, amount FROM orders WHERE id = {key}"))
            .await?
            .create_physical_plan()
            .await?),
        Planning::Logical => Ok(case
            .ctx
            .sql(&format!("EXECUTE point({key})"))
            .await?
            .create_physical_plan()
            .await?),
        Planning::Rebind => rebind(
            case.template
                .as_ref()
                .ok_or_else(|| invalid("missing scan template"))?,
            case.pushdown,
            key,
        ),
    }
}

fn verify_result(batch: &RecordBatch, key: i64) -> Result<usize> {
    if batch.num_columns() != 2 {
        return Err(invalid("lookup projection changed"));
    }
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| invalid("id type changed"))?;
    let amounts = batch
        .column(1)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| invalid("amount type changed"))?;
    for row in 0..batch.num_rows() {
        if ids.is_null(row)
            || amounts.is_null(row)
            || ids.value(row) != key
            || amounts.value(row) != i128::from(key % 100 + 1) * 100
        {
            return Err(invalid("lookup returned a stale key or wrong amount"));
        }
    }
    Ok(batch.num_rows())
}

#[derive(Serialize)]
struct Sample {
    key: i64,
    planning_ns: u128,
    execution_ns: u128,
    total_ns: u128,
}

async fn lookup(case: &Case, key: i64, diagnostic: bool) -> Result<(Sample, Option<Value>)> {
    let started = Instant::now();
    let plan = plan(case, key).await?;
    let planning_ns = started.elapsed().as_nanos();
    let execution = Instant::now();
    let mut stream = execute_stream(Arc::clone(&plan), case.ctx.task_ctx())?;
    let mut rows = 0;
    while let Some(batch) = stream.try_next().await? {
        rows += verify_result(&batch, key)?;
    }
    if rows != usize::from((0..ROWS).contains(&key)) {
        return Err(invalid("lookup row count changed"));
    }
    let execution_ns = execution.elapsed().as_nanos();
    let total_ns = started.elapsed().as_nanos();
    let diagnostic = diagnostic.then(|| {
        let mut metrics = Vec::new();
        collect_metrics(&plan, &mut metrics);
        json!({"key": key, "plan": displayable(plan.as_ref()).indent(true).to_string(), "metrics": metrics})
    });
    Ok((
        Sample {
            key,
            planning_ns,
            execution_ns,
            total_ns,
        },
        diagnostic,
    ))
}

fn collect_metrics(plan: &Arc<dyn ExecutionPlan>, output: &mut Vec<Value>) {
    if let Some(metrics) = plan.metrics() {
        for metric in metrics.iter() {
            let name = metric.value().name();
            if name.contains("pruned") || name == "bytes_scanned" || name == "output_rows" {
                let value = match metric.value() {
                    MetricValue::PruningMetrics {
                        pruning_metrics, ..
                    } => {
                        json!({"pruned": pruning_metrics.pruned(), "matched": pruning_metrics.matched()})
                    }
                    value => json!(value.as_usize()),
                };
                output.push(json!({"operator": plan.name(), "name": name, "value": value}));
            }
        }
    }
    for child in plan.children() {
        collect_metrics(child, output);
    }
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn median(values: impl Iterator<Item = u128>) -> u128 {
    let mut values = values.collect::<Vec<_>>();
    values.sort_unstable();
    let n = values.len();
    if n % 2 == 0 {
        (values[n / 2 - 1] + values[n / 2]) / 2
    } else {
        values[n / 2]
    }
}

async fn measure(args: &Args) -> Result<()> {
    let paths = layouts(args);
    let mut cases = Vec::new();
    for (layout, path) in &paths {
        reader(path)?;
        if *layout == "original" {
            cases.push(make_case(layout, path, Planning::Sql, false, false).await?);
        }
        for pushdown in [false, true] {
            for mode in [Planning::Sql, Planning::Logical, Planning::Rebind] {
                cases.push(make_case(layout, path, mode, pushdown, true).await?);
            }
        }
    }
    // Untimed boundary/miss controls also warm each independent metadata cache.
    for case in &cases {
        for &key in BOUNDARIES {
            lookup(case, key, false).await?;
        }
    }
    let mut state = args.seed;
    let mut rounds = Vec::new();
    for round in 0..args.rounds {
        let keys = (0..args.lookups)
            .map(|_| (next_random(&mut state) % ROWS as u64) as i64)
            .collect::<Vec<_>>();
        let mut results = Vec::new();
        // Rotate and reverse case order; every case receives identical new keys.
        for offset in 0..cases.len() {
            let index = if round % 2 == 0 {
                (offset + round) % cases.len()
            } else {
                (cases.len() - 1 - offset + round) % cases.len()
            };
            let case = &cases[index];
            let started = Instant::now();
            let mut samples = Vec::with_capacity(keys.len());
            for &key in &keys {
                samples.push(lookup(case, key, false).await?.0);
            }
            let elapsed_ns = started.elapsed().as_nanos();
            results.push(json!({"case": case.name, "median_planning_ns": median(samples.iter().map(|s| s.planning_ns)),
                "median_execution_ns": median(samples.iter().map(|s| s.execution_ns)), "median_total_ns": median(samples.iter().map(|s| s.total_ns)),
                "serial_queries_per_second": args.lookups as f64 * 1e9 / elapsed_ns as f64, "elapsed_ns": elapsed_ns, "samples": samples}));
        }
        rounds.push(json!({"round": round, "results": results}));
    }
    let mut diagnostics = Vec::new();
    for case in &cases {
        diagnostics.push(json!({"case": case.name, "setup_ns": case.setup_ns, "observed": lookup(case, 1_048_576, true).await?.1}));
    }
    let files = paths
        .iter()
        .map(|(layout, path)| {
            Ok(json!({"layout": layout, "path": path, "bytes": path.metadata()?.len()}))
        })
        .collect::<Result<Vec<_>>>()?;
    println!(
        "{}",
        json!({"seed": args.seed, "lookups_per_case_per_round": args.lookups, "rounds": rounds, "diagnostics": diagnostics, "files": files,
        "runtime": if args.current_thread {"current-thread"} else {"multi-thread"},
        "allocator": if cfg!(feature = "point-query-mimalloc") {"mimalloc"} else {"system"},
        "timing": "warm single-client sequential requests; total includes planning/rebinding, execution, stream drain and ID/amount validation; excludes registration, preparation and diagnostics; shared OS cache; no cold-cache or p99 claim",
        "physical_rebinding_scope": "one immutable complete Parquet file; SELECT id, amount WHERE id = new_key; fresh predicate, source and execution state per key; no result cache or per-key plan cache"})
    );
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    if !(8..=1024).contains(&args.lookups) || !(3..=15).contains(&args.rounds) || args.seed == 0 {
        return Err(invalid(
            "lookups must be 8..1024, rounds 3..15, seed nonzero",
        ));
    }
    if args.prepare_files {
        return prepare(&args);
    }
    let runtime = if args.current_thread {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
    } else {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
    };
    runtime.block_on(measure(&args))
}
