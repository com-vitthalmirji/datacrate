//! Composes a Ballista scheduler and executor the way a deployment actually
//! runs them: two separate OS processes tied together only by the
//! scheduler's gRPC address. `SchedulerLayer` and `ExecutorLayer` each own
//! one concrete startup method; `BallistaApp` is the one place that decides
//! how their lifetimes relate (the executor is useless once its scheduler
//! is gone, so a scheduler exit is treated as fatal to the whole app).

use ballista_core::error::{BallistaError, Result as BallistaResult};
use std::future::Future;
use std::process::Child;
use std::time::Duration;

/// How often to check whether the scheduler process is still alive while
/// the executor runs. Cheap enough to poll frequently without measurably
/// delaying detection of a crashed scheduler.
const SCHEDULER_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Starts the Ballista scheduler this app depends on.
pub trait SchedulerLayer {
    /// Spawns the scheduler process and returns a handle to it. The caller
    /// owns the returned `Child` and is responsible for reaping it.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if the scheduler process could not be spawned.
    fn run_scheduler(&self) -> std::io::Result<Child>;
}

/// Runs the Ballista executor this app depends on, in-process.
pub trait ExecutorLayer {
    /// Runs the executor until it shuts down.
    ///
    /// # Errors
    ///
    /// Returns a `BallistaError` if the executor fails to start or exits
    /// with an error.
    fn run_executor(self) -> impl Future<Output = BallistaResult<()>> + Send;
}

/// Ties a `SchedulerLayer` and an `ExecutorLayer` together into one
/// runnable app.
pub struct BallistaApp<S, E> {
    scheduler: S,
    executor: E,
}

impl<S: SchedulerLayer, E: ExecutorLayer> BallistaApp<S, E> {
    /// Builds an app from a scheduler layer and an executor layer.
    pub fn new(scheduler: S, executor: E) -> Self {
        Self {
            scheduler,
            executor,
        }
    }

    /// Spawns the scheduler, then runs the executor in-process.
    ///
    /// Returns as soon as either finishes: the executor's own result if it
    /// finishes first, or an error if the scheduler process exits while the
    /// executor is still running. Either way, the scheduler process is
    /// killed and reaped before returning.
    ///
    /// # Errors
    ///
    /// Returns a `BallistaError` if the scheduler process cannot be
    /// spawned, if the scheduler exits before the executor does, or if the
    /// executor itself returns an error.
    pub async fn run(self) -> BallistaResult<()> {
        let mut scheduler_process = self.scheduler.run_scheduler().map_err(|source| {
            BallistaError::General(format!("failed to start ballista-scheduler: {source}"))
        })?;

        let watch_scheduler = async {
            loop {
                match scheduler_process.try_wait() {
                    Ok(Some(status)) => {
                        break Err(BallistaError::General(format!(
                            "ballista-scheduler exited unexpectedly: {status}"
                        )));
                    }
                    Ok(None) => tokio::time::sleep(SCHEDULER_POLL_INTERVAL).await,
                    Err(source) => {
                        break Err(BallistaError::General(format!(
                            "failed to poll ballista-scheduler: {source}"
                        )));
                    }
                }
            }
        };

        let result = tokio::select! {
            executor_result = self.executor.run_executor() => executor_result,
            scheduler_result = watch_scheduler => scheduler_result,
        };

        let _ = scheduler_process.kill();
        let _ = scheduler_process.wait();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    struct ShellScheduler {
        script: &'static str,
    }

    impl SchedulerLayer for ShellScheduler {
        fn run_scheduler(&self) -> std::io::Result<Child> {
            Command::new("sh").arg("-c").arg(self.script).spawn()
        }
    }

    struct SleepThenSucceed {
        delay: Duration,
    }

    impl ExecutorLayer for SleepThenSucceed {
        async fn run_executor(self) -> BallistaResult<()> {
            tokio::time::sleep(self.delay).await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn executor_finishing_first_returns_its_own_result() {
        let app = BallistaApp::new(
            ShellScheduler { script: "sleep 5" },
            SleepThenSucceed {
                delay: Duration::from_millis(10),
            },
        );

        let result = app.run().await;

        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }

    #[tokio::test]
    async fn scheduler_exiting_first_is_treated_as_fatal() {
        let app = BallistaApp::new(
            ShellScheduler { script: "true" },
            SleepThenSucceed {
                delay: Duration::from_secs(5),
            },
        );

        let result = app.run().await;

        assert!(result.is_err(), "expected Err, got {result:?}");
    }
}
