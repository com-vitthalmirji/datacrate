//! Bounded point-lookup probe over an existing sorted orders Parquet file.
//! Compares only row-level Parquet filter pushdown; both contexts use one
//! target partition and a warm metadata cache; both read the same file.

use std::{
    error::Error,
    fs::File,
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use arrow::array::{Array, Decimal128Array, Int64Array};
use clap::Parser;
use datafusion::{
    physical_plan::{ExecutionPlan, displayable, execute_stream, metrics::MetricValue},
    prelude::{SessionConfig, SessionContext},
};
use futures_util::TryStreamExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pipeline::datafusion_query::register_orders;
use serde_json::{Value, json};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Parser)]
struct Args {
    /// Existing 5M-row comet benchmark fixture; this probe assumes its value formula.
    #[arg(long, default_value = "benchmark/spark-comet/orders.parquet")]
    input: PathBuf,
    /// Distinct keys per setting; 2..=64 keeps the probe bounded.
    #[arg(long, default_value_t = 16)]
    lookups: usize,
}

async fn context(input: &Path, pushdown: bool) -> Result<SessionContext> {
    let mut config = SessionConfig::new().with_target_partitions(1);
    config.options_mut().execution.parquet.pruning = true;
    config.options_mut().execution.parquet.enable_page_index = true;
    config.options_mut().execution.parquet.pushdown_filters = pushdown;
    let ctx = SessionContext::new_with_config(config);
    register_orders(&ctx, input).await?;
    Ok(ctx)
}

fn selected_metrics(plan: &Arc<dyn ExecutionPlan>, output: &mut Vec<Value>) {
    if let Some(metrics) = plan.metrics() {
        for metric in metrics.iter() {
            let name = metric.value().name();
            if name.contains("row_groups")
                || name.contains("page_index")
                || name.contains("pushdown")
                || name.contains("bytes_scanned")
                || name == "output_rows"
            {
                let value = match metric.value() {
                    MetricValue::PruningMetrics {
                        pruning_metrics, ..
                    } => json!({
                        "pruned": pruning_metrics.pruned(),
                        "matched": pruning_metrics.matched(),
                        "fully_matched": pruning_metrics.fully_matched(),
                    }),
                    value => json!(value.as_usize()),
                };
                output.push(json!({
                    "operator": plan.name(),
                    "name": name,
                    "value": value,
                }));
            }
        }
    }
    for child in plan.children() {
        selected_metrics(child, output);
    }
}

async fn lookup(ctx: &SessionContext, key: i64) -> Result<Value> {
    let planning_start = Instant::now();
    let plan = ctx
        .sql(&format!("SELECT id, amount FROM orders WHERE id = {key}"))
        .await?
        .create_physical_plan()
        .await?;
    let planning_ns = planning_start.elapsed().as_nanos();

    let execution_start = Instant::now();
    let mut stream = execute_stream(Arc::clone(&plan), ctx.task_ctx())?;
    let mut rows = 0;
    while let Some(batch) = stream.try_next().await? {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| io::Error::other("point lookup changed id type"))?;
        let amounts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .ok_or_else(|| io::Error::other("point lookup changed amount type"))?;
        let expected = i128::from(key % 100 + 1) * 100;
        for row in 0..amounts.len() {
            if ids.value(row) != key || amounts.value(row) != expected {
                return Err(io::Error::other("point lookup returned the wrong row").into());
            }
            rows += 1;
        }
    }
    if rows != 1 {
        return Err(io::Error::other(format!("expected one id={key}, got {rows}")).into());
    }
    let execution_ns = execution_start.elapsed().as_nanos();
    let mut metrics = Vec::new();
    selected_metrics(&plan, &mut metrics);
    Ok(json!({
        "key": key,
        "planning_ns": planning_ns,
        "execution_ns": execution_ns,
        "physical_plan": displayable(plan.as_ref()).indent(true).to_string(),
        "scan_metrics": metrics,
    }))
}

fn median_ns(samples: &[Value], field: &str) -> u64 {
    let mut values = samples
        .iter()
        .map(|sample| sample[field].as_u64().expect("recorded nanoseconds"))
        .collect::<Vec<_>>();
    values.sort_unstable();
    let middle = values.len() / 2;
    if values.len() % 2 == 0 {
        ((u128::from(values[middle - 1]) + u128::from(values[middle])) / 2) as u64
    } else {
        values[middle]
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if !(2..=64).contains(&args.lookups) {
        return Err(io::Error::other("--lookups must be in 2..=64").into());
    }
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&args.input)?)?;
    let rows = reader.metadata().file_metadata().num_rows();
    if rows != 5_000_000 {
        return Err(io::Error::other("probe requires the 5M-row comet benchmark fixture").into());
    }
    let row_groups = reader.metadata().row_groups().len();

    let off = context(&args.input, false).await?;
    let on = context(&args.input, true).await?;
    lookup(&off, rows / 2).await?;
    lookup(&on, rows / 2).await?;

    let mut off_samples = Vec::with_capacity(args.lookups);
    let mut on_samples = Vec::with_capacity(args.lookups);
    for index in 0..args.lookups {
        let key = ((index as i128 * i128::from(rows - 1)) / (args.lookups as i128 - 1)) as i64;
        if index % 2 == 0 {
            off_samples.push(lookup(&off, key).await?);
            on_samples.push(lookup(&on, key).await?);
        } else {
            on_samples.push(lookup(&on, key).await?);
            off_samples.push(lookup(&off, key).await?);
        }
    }
    println!(
        "{}",
        json!({
            "input": args.input,
            "rows": rows,
            "row_groups": row_groups,
            "lookups_per_setting": args.lookups,
            "target_partitions": 1,
            "page_index": true,
            "pruning": true,
            "timing": "one process; both contexts warmed once; planning and execution separate; shared OS cache; no cold-cache or tail-latency claim",
            "pushdown_off": {
                "median_planning_ns": median_ns(&off_samples, "planning_ns"),
                "median_execution_ns": median_ns(&off_samples, "execution_ns"),
                "samples": off_samples,
            },
            "pushdown_on": {
                "median_planning_ns": median_ns(&on_samples, "planning_ns"),
                "median_execution_ns": median_ns(&on_samples, "execution_ns"),
                "samples": on_samples,
            }
        })
    );
    Ok(())
}
