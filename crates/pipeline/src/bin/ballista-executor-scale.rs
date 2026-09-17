//! Drop-in replacement for the prebuilt `ballista-executor` binary that also
//! raises `datafusion.runtime.max_temp_directory_size` (upstream default:
//! 100GB), via the only extension point that exposes it —
//! `ExecutorProcessConfig::override_runtime_producer`. Neither the CLI flags
//! nor `--memory-pool-size` reach this setting: `--memory-pool-size` sizes
//! the in-memory `FairSpillPool` that decides *when* a query starts
//! spilling, not the on-disk quota for how much spilled data may
//! accumulate. See docs/internals/notes/decisions.md, M3.8 disk-spill-limit
//! entry.
//!
//! Accepts every `ballista-executor` flag (via `#[command(flatten)]`) plus
//! `--max-temp-directory-size`.

use ballista_core::error::{BallistaError, Result as BallistaResult};
use ballista_executor::config::Config;
use ballista_executor::executor_process::{ExecutorProcessConfig, start_executor_process};
use ballista_executor::health::spawn_health_server;
use clap::Parser;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::SessionConfig;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[derive(clap::Parser, Debug)]
#[command(version, about)]
struct Args {
    #[command(flatten)]
    inner: Config,

    /// Disk quota for DataFusion's spill-to-disk temp files (upstream
    /// default when this flag is absent: 100GB). Accepts "GB"/"GiB"/plain
    /// byte-count suffixes.
    #[arg(long, default_value = "100GB")]
    max_temp_directory_size: bytesize::ByteSize,
}

#[tokio::main]
async fn main() -> BallistaResult<()> {
    let args = Args::parse();
    let bind_host = args.inner.bind_host.clone();
    let bind_health_port = args.inner.bind_health_port;
    let work_dir = args.inner.work_dir.clone();
    let max_temp_directory_size = args.max_temp_directory_size.as_u64();

    let mut config: ExecutorProcessConfig = args.inner.try_into()?;
    config.override_runtime_producer = Some(Arc::new(move |_: &SessionConfig| {
        let mut builder =
            RuntimeEnvBuilder::new().with_max_temp_directory_size(max_temp_directory_size);
        if let Some(wd) = &work_dir {
            builder = builder.with_temp_file_path(wd.clone());
        }
        Ok(Arc::new(builder.build()?))
    }));

    let rust_log = std::env::var(EnvFilter::DEFAULT_ENV);
    let log_filter = EnvFilter::new(rust_log.unwrap_or(config.special_mod_log_level.clone()));
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(log_filter)
        .init();

    // Kubernetes-style health probes, wired the same way as upstream's
    // bin/main.rs: a concern of the standalone binary, not the library.
    let health = config.health.clone();
    let health_addr: SocketAddr = format!("{bind_host}:{bind_health_port}")
        .parse()
        .map_err(|e| BallistaError::General(format!("invalid --bind-health-port address: {e}")))?;
    let (health_shutdown_tx, health_shutdown_rx) = tokio::sync::oneshot::channel();
    let health_handle = spawn_health_server(health_addr, health, health_shutdown_rx);

    let result = start_executor_process(Arc::new(config)).await;

    let _ = health_shutdown_tx.send(());
    if let Err(e) = health_handle.await {
        log::warn!("health server task join error: {e}");
    }
    result
}
