//! Proves that a `#[derive(Contract, Connector)]` struct's `register_memory`
//! works unchanged when the `SessionContext` it registers against is backed
//! by a real, distributed Ballista cluster instead of a local one. This is
//! not a test of Ballista itself (that's `ballista_app`'s own lifecycle
//! tests) - it only shows the same connector and the same query
//! ([`aggregate_query_sql`]) produce the same result locally and
//! distributed.
//!
//! Requires `ballista-scheduler` on `PATH` (`just ballista-install`), so it
//! is `#[ignore]`d like every other real-cluster Ballista proof in this
//! crate (`bin/ballista-aggregate.rs` already assumes manually-started
//! infrastructure rather than spawning it itself). Run explicitly with
//! `cargo test --features ballista --test ballista_distributed_query -- --ignored`.
#![cfg(feature = "ballista")]
#![allow(dead_code)]

use std::process::{Child, Command};
use std::sync::Arc;
use std::time::Duration;

use ballista::prelude::*;
use ballista_core::error::Result as BallistaResult;
use ballista_executor::executor_process::{ExecutorProcessConfig, start_executor_process};
use connectors_derive::Connector;
use contracts::Contract;
use datafusion::arrow::array::{Float64Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use pipeline::ballista_app::{BallistaApp, ExecutorLayer, SchedulerLayer};
use pipeline::datafusion_query::aggregate_query_sql;
use tokio::sync::oneshot;

const SCHEDULER_PORT: u16 = 51050;
const EXECUTOR_PORT: u16 = 51061;
const EXECUTOR_GRPC_PORT: u16 = 51062;

#[derive(Contract, Connector)]
struct Order {
    id: i64,
    amount: f64,
}

fn orders_batch() -> RecordBatch {
    let schema = Arc::new(Order::connector_schema().expect("struct maps to an Arrow schema"));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Float64Array::from(vec![10.0, 60.0, 75.5, 40.0, 90.25])),
        ],
    )
    .expect("batch matches the derived schema")
}

struct RealScheduler {
    bind_port: u16,
}

impl SchedulerLayer for RealScheduler {
    fn run_scheduler(&self) -> std::io::Result<Child> {
        Command::new("ballista-scheduler")
            .arg("--bind-port")
            .arg(self.bind_port.to_string())
            .spawn()
    }
}

/// Runs the in-process executor until `start_executor_process` returns on
/// its own, or until `stop` fires - `start_executor_process` otherwise only
/// returns on a process-wide Ctrl+C/SIGTERM or a scheduler-initiated stop
/// request, neither of which this test can trigger cooperatively.
struct InProcessExecutorUntilStopped {
    config: Arc<ExecutorProcessConfig>,
    stop: oneshot::Receiver<()>,
}

impl ExecutorLayer for InProcessExecutorUntilStopped {
    async fn run_executor(self) -> BallistaResult<()> {
        tokio::select! {
            result = start_executor_process(self.config) => result,
            _ = self.stop => Ok(()),
        }
    }
}

fn executor_config() -> ExecutorProcessConfig {
    ExecutorProcessConfig {
        port: EXECUTOR_PORT,
        grpc_port: EXECUTOR_GRPC_PORT,
        scheduler_port: SCHEDULER_PORT,
        ..Default::default()
    }
}

/// Registers `Order`'s batch under `name` and runs [`aggregate_query_sql`],
/// retrying while the cluster is still forming (scheduler accepting
/// connections before an executor has heartbeat-registered with it).
async fn register_and_aggregate_with_retry(
    ctx: &SessionContext,
    name: &str,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let mut last_err = None;
    for _ in 0..50 {
        match Order::register_memory(ctx, name, vec![orders_batch()]) {
            Ok(()) => match aggregate_query_sql(ctx).await {
                Ok(rows) => return Ok(rows),
                Err(err) => last_err = Some(err),
            },
            Err(err) => last_err = Some(err),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err(last_err.expect("at least one attempt ran"))
}

#[tokio::test]
#[ignore = "requires ballista-scheduler on PATH (just ballista-install)"]
async fn derived_connector_matches_local_result_on_a_distributed_context() {
    let local_ctx = SessionContext::new();
    Order::register_memory(&local_ctx, "orders", vec![orders_batch()])
        .expect("local registration succeeds");
    let local_rows = aggregate_query_sql(&local_ctx)
        .await
        .expect("local aggregate query succeeds");

    let (stop_tx, stop_rx) = oneshot::channel();
    let app = BallistaApp::new(
        RealScheduler {
            bind_port: SCHEDULER_PORT,
        },
        InProcessExecutorUntilStopped {
            config: Arc::new(executor_config()),
            stop: stop_rx,
        },
    );
    let app_handle = tokio::spawn(app.run());

    let config = SessionConfig::new_with_ballista()
        .with_ballista_job_name("derived connector on a distributed context");
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    let distributed_ctx =
        SessionContext::remote_with_state(&format!("df://localhost:{SCHEDULER_PORT}"), state)
            .await
            .expect("builds a remote session state");

    let distributed_rows = register_and_aggregate_with_retry(&distributed_ctx, "orders")
        .await
        .expect("distributed aggregate query succeeds once the cluster is ready");

    let _ = stop_tx.send(());
    let app_result = app_handle.await.expect("app task does not panic");

    assert_eq!(
        local_rows, distributed_rows,
        "the same derived connector and the same query must produce the same \
         result locally and on a distributed Ballista context"
    );
    assert!(
        app_result.is_ok(),
        "expected a clean shutdown, got {app_result:?}"
    );
}
