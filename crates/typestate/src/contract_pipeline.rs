//! Streaming composition with automatic compile-time declaration checks.
//!
//! Every transform and sink connection enforces `ExactOrdered` during a build
//! of the instantiated call. Runtime checks validate actual source/transform
//! batches. Opaque callbacks may still fail or perform external effects.
use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use contracts::{
    Contract, ExactOrdered,
    arrow::{BatchValidationError, BatchValidator, MappingError},
};
use datafusion::dataframe::DataFrame;
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use std::sync::Arc;
use std::{error::Error, marker::PhantomData};

/// A callback's recoverable error, preserved as the original boxed source.
pub type CallbackError = Box<dyn Error + Send + Sync + 'static>;
type BatchIterator<'a> = Box<dyn Iterator<Item = Result<RecordBatch, CallbackError>> + 'a>;
type Source<'a> = Box<dyn FnOnce() -> Result<BatchIterator<'a>, CallbackError> + 'a>;
type Transform<'a> = Box<dyn FnMut(RecordBatch) -> Result<RecordBatch, CallbackError> + 'a>;
type Sink<'a> = Box<dyn FnMut(RecordBatch) -> Result<(), CallbackError> + 'a>;
type Prepare = fn() -> Result<BatchValidator, MappingError>;

/// The operation whose preflight, callback or output validation failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    /// Opening the input iterator, before any batch is read.
    SourceFactory,
    /// Reading or validating source batches; also source mapping preflight.
    Source,
    /// A zero-based transform index.
    Transform(usize),
    /// Preparing or invoking the terminal sink.
    Sink,
}
/// The underlying category of a contextual pipeline failure.
#[derive(Debug)]
pub enum Failure {
    /// A declaration has no supported physical Arrow representation.
    Mapping(MappingError),
    /// Actual batch schema or required values violate the declaration.
    Validation(BatchValidationError),
    /// A source or user callback failed.
    Callback(CallbackError),
    /// Reporting the next successful batch would overflow usize counters.
    CountOverflow,
}
/// A pipeline failure with stage, batch index and original source.
#[derive(Debug)]
pub struct ContractPipelineError {
    /// Failing stage.
    pub stage: Stage,
    /// Zero-based input batch index; None during preflight or iterator creation.
    pub batch_index: Option<usize>,
    /// Original failure category and source.
    pub failure: Failure,
}
impl std::fmt::Display for ContractPipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "contract pipeline {:?} at batch {:?}: ",
            self.stage, self.batch_index
        )?;
        match &self.failure {
            Failure::Mapping(e) => write!(f, "{e}"),
            Failure::Validation(e) => write!(f, "{e}"),
            Failure::Callback(e) => write!(f, "{e}"),
            Failure::CountOverflow => write!(f, "processed batch or row count overflow"),
        }
    }
}
impl Error for ContractPipelineError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.failure {
            Failure::Mapping(e) => Some(e),
            Failure::Validation(e) => Some(e),
            Failure::Callback(e) => Some(e.as_ref()),
            Failure::CountOverflow => None,
        }
    }
}
fn error(stage: Stage, batch_index: Option<usize>, failure: Failure) -> ContractPipelineError {
    ContractPipelineError {
        stage,
        batch_index,
        failure,
    }
}
fn prepare(make: Prepare, stage: Stage) -> Result<BatchValidator, ContractPipelineError> {
    make().map_err(|e| error(stage, None, Failure::Mapping(e)))
}
fn validate(
    validator: &BatchValidator,
    batch: &RecordBatch,
    stage: Stage,
    index: usize,
) -> Result<(), ContractPipelineError> {
    validator
        .validate(batch)
        .map_err(|e| error(stage, Some(index), Failure::Validation(e)))
}

/// Shared by `sql_transform` and `dataframe_transform`: both (re)register
/// `batch` as a `MemTable` on `ctx`, drive a current-thread Tokio runtime to
/// bridge DataFusion's async API, and collect+concatenate the resulting
/// `DataFrame` back into one batch. Only how that `DataFrame` is obtained
/// (`ctx.sql(sql)` vs. a `.select()/.filter()/...` chain) differs between the
/// two callers, so that ceremony is written once here instead of twice.
fn run_via_dataframe(
    ctx: &SessionContext,
    batch: RecordBatch,
    get_df: impl AsyncFnOnce(&SessionContext) -> Result<DataFrame, DataFusionError>,
) -> Result<RecordBatch, CallbackError> {
    let schema = batch.schema();
    ctx.deregister_table("batch")
        .map_err(|e| Box::new(e) as CallbackError)?;
    let table =
        MemTable::try_new(schema, vec![vec![batch]]).map_err(|e| Box::new(e) as CallbackError)?;
    ctx.register_table("batch", Arc::new(table))
        .map_err(|e| Box::new(e) as CallbackError)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| Box::new(e) as CallbackError)?;
    let (out_schema, batches) = runtime
        .block_on(async {
            let df = get_df(ctx).await?;
            let out_schema = Arc::new(df.schema().as_arrow().clone());
            let batches = df.collect().await?;
            Ok::<_, DataFusionError>((out_schema, batches))
        })
        .map_err(|e| Box::new(e) as CallbackError)?;
    concat_batches(&out_schema, &batches).map_err(|e| Box::new(e) as CallbackError)
}

struct TransformSpec<'a> {
    callback: Transform<'a>,
    prepare: Prepare,
}
struct Plan<'a> {
    source: Source<'a>,
    prepare_source: Prepare,
    transforms: Vec<TransformSpec<'a>>,
}

/// A source followed by zero or more transforms, carrying the last declaration.
///
/// C is declaration information, not evidence that an arbitrary batch is valid.
/// Only sink attachment makes build available. Callbacks may borrow local state;
/// no Send, Sync or thread-sharing requirement is imposed on them.
#[must_use]
pub struct ContractPipeline<'a, C> {
    plan: Plan<'a>,
    declaration: PhantomData<fn() -> C>,
}
impl ContractPipeline<'static, ()> {
    /// Declares source output and defers opening a fallible input iterator.
    /// Source I/O starts only in run, after all mappings pass build preflight.
    pub fn source<'a, C: Contract, I>(
        factory: impl FnOnce() -> Result<I, CallbackError> + 'a,
    ) -> ContractPipeline<'a, C>
    where
        I: Iterator<Item = Result<RecordBatch, CallbackError>> + 'a,
    {
        ContractPipeline {
            plan: Plan {
                source: Box::new(move || {
                    factory().map(|input| Box::new(input) as BatchIterator<'a>)
                }),
                prepare_source: BatchValidator::new::<C>,
                transforms: Vec::new(),
            },
            declaration: PhantomData,
        }
    }
}
impl<'a, C: Contract> ContractPipeline<'a, C> {
    /// Connects the current output declaration to a transform's required input.
    /// Incompatible declarations fail const evaluation during cargo build of
    /// this call, without a caller-written CHECK. O declares the returned batch.
    /// No projection, reordering, cast or default-value construction is implied.
    pub fn transform<I: Contract, O: Contract>(
        mut self,
        callback: impl FnMut(RecordBatch) -> Result<RecordBatch, CallbackError> + 'a,
    ) -> ContractPipeline<'a, O> {
        contracts::enforce::<C, I, ExactOrdered>();
        self.plan.transforms.push(TransformSpec {
            callback: Box::new(callback),
            prepare: BatchValidator::new::<O>,
        });
        ContractPipeline {
            plan: self.plan,
            declaration: PhantomData,
        }
    }
    /// Like `transform`, but produces the next stage by running `sql` through
    /// `ctx` instead of a Rust closure. The same compile-time `ExactOrdered`
    /// check `transform` runs still applies to `O`; only how the batch is
    /// produced differs.
    ///
    /// Each input batch is (re)registered on `ctx` as a table named `batch`
    /// — `sql` must query that name — then `sql`'s result is collected and
    /// concatenated back into the single batch `O` is validated against.
    /// A fresh current-thread Tokio runtime drives the query per call, since
    /// the pipeline itself stays synchronous; this is the minimum viable way
    /// to bridge DataFusion's async API, not a pooled or reused runtime.
    pub fn sql_transform<O: Contract>(
        self,
        ctx: &'a SessionContext,
        sql: &'a str,
    ) -> ContractPipeline<'a, O> {
        self.transform::<C, O>(move |batch| {
            run_via_dataframe(ctx, batch, async |ctx| ctx.sql(sql).await)
        })
    }
    /// Like `sql_transform`, but builds the next stage from DataFusion's
    /// `DataFrame` API (`.select()/.filter()/.aggregate()/...`) instead of a
    /// SQL string. `build` receives the `batch` table as a `DataFrame` and
    /// returns the chain applied to it; the same compile-time `ExactOrdered`
    /// check `transform` runs still applies to `O`.
    pub fn dataframe_transform<O: Contract>(
        self,
        ctx: &'a SessionContext,
        build: impl Fn(DataFrame) -> Result<DataFrame, DataFusionError> + 'a,
    ) -> ContractPipeline<'a, O> {
        self.transform::<C, O>(move |batch| {
            run_via_dataframe(ctx, batch, async |ctx| {
                let df = ctx.table("batch").await?;
                build(df)
            })
        })
    }
    /// Checks the final declared connection and terminates composition.
    /// The sink receives batches only after source/transform validation succeeds.
    /// Its actions and prior successful calls are not rolled back on later errors.
    pub fn sink<I: Contract>(
        self,
        callback: impl FnMut(RecordBatch) -> Result<(), CallbackError> + 'a,
    ) -> ContractSink<'a> {
        contracts::enforce::<C, I, ExactOrdered>();
        ContractSink {
            plan: self.plan,
            sink: Box::new(callback),
            prepare_sink: BatchValidator::new::<I>,
        }
    }
}
/// A terminal sink attached to a declared pipeline; no more stages can be added.
#[must_use]
pub struct ContractSink<'a> {
    plan: Plan<'a>,
    sink: Sink<'a>,
    prepare_sink: Prepare,
}
impl<'a> ContractSink<'a> {
    /// Prepares every declared physical representation before any source I/O.
    ///
    /// # Errors
    /// Returns a contextual mapping error for unsupported physical declarations.
    pub fn build(self) -> Result<PreparedPipeline<'a>, ContractPipelineError> {
        let source_validator = prepare(self.plan.prepare_source, Stage::Source)?;
        let transforms = self
            .plan
            .transforms
            .into_iter()
            .enumerate()
            .map(|(index, step)| {
                Ok(PreparedTransform {
                    callback: step.callback,
                    validator: prepare(step.prepare, Stage::Transform(index))?,
                })
            })
            .collect::<Result<Vec<_>, ContractPipelineError>>()?;
        // Compile-time ExactOrdered connects this to the last output's identical
        // shape; preflight it, then avoid a redundant second value scan at the sink.
        prepare(self.prepare_sink, Stage::Sink)?;
        Ok(PreparedPipeline {
            source: self.plan.source,
            source_validator,
            transforms,
            sink: self.sink,
        })
    }
}
struct PreparedTransform<'a> {
    callback: Transform<'a>,
    validator: BatchValidator,
}
/// A single-use, preflighted pipeline that processes one input batch at a time.
/// Callback allocations and source buffering remain owned by those callbacks;
/// this composition layer introduces no input collection or worker threads.
#[must_use]
pub struct PreparedPipeline<'a> {
    source: Source<'a>,
    source_validator: BatchValidator,
    transforms: Vec<PreparedTransform<'a>>,
    sink: Sink<'a>,
}
/// Counts batches and rows accepted by a successful sink invocation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ContractRunReport {
    /// Number of successful sink calls, including empty batches.
    pub batches_processed: usize,
    /// Number of rows delivered to successful sink calls, after transformations.
    pub rows_processed: usize,
}
impl PreparedPipeline<'_> {
    /// Opens the source once, validates outputs and runs callbacks in order.
    ///
    /// Failure drops the iterator and retained work before returning. Source,
    /// transform and sink side effects require their own recovery protocol.
    ///
    /// # Errors
    /// Returns the failing stage, input batch index and original callback,
    /// validation or counter-overflow error. Panics in user code are not caught.
    pub fn run(self) -> Result<ContractRunReport, ContractPipelineError> {
        let Self {
            source,
            source_validator,
            mut transforms,
            mut sink,
        } = self;
        let input =
            source().map_err(|e| error(Stage::SourceFactory, None, Failure::Callback(e)))?;
        let mut report = ContractRunReport::default();
        for received in input {
            let index = report.batches_processed;
            let mut batch =
                received.map_err(|e| error(Stage::Source, Some(index), Failure::Callback(e)))?;
            validate(&source_validator, &batch, Stage::Source, index)?;
            for (step_index, step) in transforms.iter_mut().enumerate() {
                let stage = Stage::Transform(step_index);
                batch = (step.callback)(batch)
                    .map_err(|e| error(stage, Some(index), Failure::Callback(e)))?;
                validate(&step.validator, &batch, stage, index)?;
            }
            let batches = report.batches_processed.checked_add(1);
            let rows = report.rows_processed.checked_add(batch.num_rows());
            let (Some(batches_processed), Some(rows_processed)) = (batches, rows) else {
                return Err(error(Stage::Sink, Some(index), Failure::CountOverflow));
            };
            sink(batch).map_err(|e| error(Stage::Sink, Some(index), Failure::Callback(e)))?;
            report = ContractRunReport {
                batches_processed,
                rows_processed,
            };
        }
        Ok(report)
    }
}
