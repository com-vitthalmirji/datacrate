//! M3-gate Spark-vs-DataFusion parity probe: runs the timestamp/null/
//! ordering/window query from `benchmark/parity/query.sql` (the `SELECT`
//! portion only — DataFusion registers the table via the Rust API instead
//! of Spark's `CREATE TEMPORARY VIEW` DDL) against DataFusion, as the
//! correctness reference for the equivalent `spark-sql` run.
//!
//! Run: `cargo run --release --package pipeline --bin parity-datafusion -- \
//!   --input benchmark/parity/orders.parquet`

use std::path::PathBuf;
use std::process::ExitCode;

use arrow::util::pretty::pretty_format_batches;
use clap::Parser;
use datafusion::prelude::SessionContext;
use pipeline::datafusion_query::register_orders;

const QUERY: &str = "
SELECT
  id,
  amount,
  note,
  EXTRACT(DAY FROM placed_at) AS placed_day,
  SUM(amount) OVER (ORDER BY placed_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
  RANK() OVER (ORDER BY amount DESC) AS amount_rank
FROM orders
ORDER BY note NULLS FIRST, id
";

#[derive(Parser)]
#[command(
    name = "parity-datafusion",
    about = "Run the Spark/DataFusion timestamp-null-ordering-window parity query against DataFusion"
)]
struct Args {
    /// Path to the Parquet file written by the `parity_fixture` example.
    #[arg(long)]
    input: PathBuf,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    let ctx = SessionContext::new();
    if let Err(err) = register_orders(&ctx, &args.input).await {
        eprintln!("parity-datafusion: {err}");
        return ExitCode::FAILURE;
    }
    let df = match ctx.sql(QUERY).await {
        Ok(df) => df,
        Err(err) => {
            eprintln!("parity-datafusion: {err}");
            return ExitCode::FAILURE;
        }
    };
    let batches = match df.collect().await {
        Ok(batches) => batches,
        Err(err) => {
            eprintln!("parity-datafusion: {err}");
            return ExitCode::FAILURE;
        }
    };
    match pretty_format_batches(&batches) {
        Ok(formatted) => {
            println!("{formatted}");
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("parity-datafusion: {err}");
            ExitCode::FAILURE
        }
    }
}
