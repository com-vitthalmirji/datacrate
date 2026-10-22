//! `struct` prints a `#[derive(Contract, Connector)]` struct matching an
//! existing Parquet file's schema. `pipeline` goes further: given a
//! source schema (Parquet, dbt, Erwin, or Delta), an optional target
//! schema, an optional transform SQL stage, and a Parquet sink, it renders
//! a complete, compiling `ContractPipeline` binary.
//!
//! The compile-time feasibility check against a real file on every build is
//! a separate mechanism (a pipeline crate's own `build.rs`), not this CLI.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use arrow_schema::Schema;
use clap::{Parser, Subcommand, ValueEnum};
use schema_codegen::{
    AggExpr, AggFn, Comparator, DfOp, Literal, PipelineSpec, SchemaCodegenError, SortKey,
    SourceData, SourceFormat, TransformSpec,
};

#[derive(Parser)]
#[command(
    name = "scaffold-cli",
    about = "Generate pipeline data models and runnable pipelines from steward-declared schemas"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Generate a Contract/Connector struct from a Parquet file's schema.
    Struct(StructArgs),
    /// Generate a complete ContractPipeline binary from a source (and
    /// optional target) schema.
    Pipeline(PipelineArgs),
}

#[derive(Parser)]
struct StructArgs {
    /// Path to the Parquet file whose schema to read.
    from_parquet: PathBuf,

    /// Name of the generated struct.
    #[arg(long)]
    name: String,

    /// Write the generated struct here instead of stdout.
    #[arg(long)]
    out: Option<PathBuf>,
}

#[derive(Parser)]
struct PipelineArgs {
    /// Shape of the source: `parquet:<path>` | `dbt:<yaml-path>:<model>` |
    /// `erwin:<xml-path>:<entity>` | `delta:<table-uri>`.
    #[arg(long)]
    source_schema: String,

    /// Where the pipeline reads physical data from at run time. Defaults to
    /// the path/URI embedded in `--source-schema` for `parquet:`/`delta:`
    /// specs; required for `dbt:`/`erwin:`, which only declare shape.
    #[arg(long)]
    source_data: Option<String>,

    /// Overrides the inferred source format.
    #[arg(long, value_enum)]
    source_format: Option<CliSourceFormat>,

    /// Shape of the target, same grammar as `--source-schema`. Omitted
    /// means target = source (a passthrough pipeline).
    #[arg(long)]
    target_schema: Option<String>,

    /// SQL querying the literal table name `batch`, run as a
    /// `ContractPipeline::sql_transform` stage. Omitted skips the stage.
    /// Mutually exclusive with `--transform-df`.
    #[arg(long)]
    transform_sql: Option<String>,

    /// One DataFusion `DataFrame`-API op, rendered in occurrence order into a
    /// `ContractPipeline::dataframe_transform` chain. Repeat for each stage:
    /// `select:col1,col2` | `filter:column,cmp,value` (`cmp` is one of
    /// `eq|ne|gt|lt|ge|le`) | `aggregate:group=col1,col2;sum(col)=alias,...`
    /// (`group=` may be empty; agg func is one of `sum|count|avg|min|max`) |
    /// `sort:col,asc;col2,desc` | `limit:N`. Mutually exclusive with
    /// `--transform-sql`.
    #[arg(long = "transform-df")]
    transform_df: Vec<String>,

    /// Path the generated pipeline writes its sink Parquet file to.
    #[arg(long)]
    sink_parquet: String,

    /// Base name for the generated struct(s).
    #[arg(long)]
    name: String,

    /// Write the generated pipeline here instead of stdout.
    #[arg(long)]
    out: Option<PathBuf>,
}

#[derive(Clone, Copy, ValueEnum)]
enum CliSourceFormat {
    Parquet,
    Csv,
    Delta,
}

impl From<CliSourceFormat> for SourceFormat {
    fn from(value: CliSourceFormat) -> Self {
        match value {
            CliSourceFormat::Parquet => SourceFormat::Parquet,
            CliSourceFormat::Csv => SourceFormat::Csv,
            CliSourceFormat::Delta => SourceFormat::Delta,
        }
    }
}

/// A `--source-schema`/`--target-schema` spec, parsed once at the CLI
/// boundary.
enum SchemaSpec {
    Parquet(PathBuf),
    Dbt { yaml: PathBuf, model: String },
    Erwin { xml: PathBuf, entity: String },
    Delta(String),
}

#[derive(Debug)]
enum CliError {
    Schema {
        source: SchemaCodegenError,
    },
    WriteOutput {
        path: PathBuf,
        source: std::io::Error,
    },
    InvalidSpec {
        spec: String,
    },
    MissingSourceData {
        spec: String,
    },
    UnknownSourceFormat {
        location: String,
    },
    ConflictingTransform,
    InvalidTransformDf {
        op: String,
    },
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CliError::Schema { source } => write!(f, "{source}"),
            CliError::WriteOutput { path, source } => {
                write!(f, "failed to write '{}': {source}", path.display())
            }
            CliError::InvalidSpec { spec } => {
                write!(f, "'{spec}' is not a valid schema spec")
            }
            CliError::MissingSourceData { spec } => write!(
                f,
                "--source-data is required when --source-schema is '{spec}' (it only declares shape, not a data location)"
            ),
            CliError::UnknownSourceFormat { location } => write!(
                f,
                "cannot infer a source format for '{location}'; pass --source-format explicitly"
            ),
            CliError::ConflictingTransform => write!(
                f,
                "--transform-sql and --transform-df are mutually exclusive; pass only one"
            ),
            CliError::InvalidTransformDf { op } => {
                write!(f, "'{op}' is not a valid --transform-df op")
            }
        }
    }
}

impl std::error::Error for CliError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CliError::Schema { source } => Some(source),
            CliError::WriteOutput { source, .. } => Some(source),
            CliError::InvalidSpec { .. }
            | CliError::MissingSourceData { .. }
            | CliError::UnknownSourceFormat { .. }
            | CliError::ConflictingTransform
            | CliError::InvalidTransformDf { .. } => None,
        }
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match &cli.command {
        Command::Struct(args) => run_struct(args),
        Command::Pipeline(args) => run_pipeline(args),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("scaffold-cli: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run_struct(args: &StructArgs) -> Result<(), CliError> {
    let schema = schema_codegen::read_parquet_schema(&args.from_parquet)
        .map_err(|source| CliError::Schema { source })?;
    let source = schema_codegen::struct_source(&args.name, &schema)
        .map_err(|source| CliError::Schema { source })?;
    write_output(&args.out, &source)
}

fn run_pipeline(args: &PipelineArgs) -> Result<(), CliError> {
    let source_spec = parse_schema_spec(&args.source_schema)?;
    let source_schema =
        resolve_schema(&source_spec).map_err(|source| CliError::Schema { source })?;

    let target_schema = match &args.target_schema {
        Some(spec_text) => {
            let spec = parse_schema_spec(spec_text)?;
            Some(resolve_schema(&spec).map_err(|source| CliError::Schema { source })?)
        }
        None => None,
    };

    let (location, inferred_format) =
        resolve_source_data(&source_spec, args.source_data.as_deref())?;
    let format = match args.source_format {
        Some(explicit) => explicit.into(),
        None => inferred_format.ok_or_else(|| CliError::UnknownSourceFormat {
            location: location.clone(),
        })?,
    };

    let transform = parse_transform(args.transform_sql.as_deref(), &args.transform_df)?;

    let spec = PipelineSpec {
        name: &args.name,
        source_schema: &source_schema,
        target_schema: target_schema.as_ref(),
        source_data: SourceData {
            location: &location,
            format,
        },
        transform,
        sink_parquet: &args.sink_parquet,
    };

    let rendered =
        schema_codegen::pipeline_source(&spec).map_err(|source| CliError::Schema { source })?;
    write_output(&args.out, &rendered)
}

fn write_output(out: &Option<PathBuf>, text: &str) -> Result<(), CliError> {
    match out {
        Some(path) => std::fs::write(path, text).map_err(|source| CliError::WriteOutput {
            path: path.clone(),
            source,
        }),
        None => {
            print!("{text}");
            Ok(())
        }
    }
}

fn parse_schema_spec(spec: &str) -> Result<SchemaSpec, CliError> {
    let mut parts = spec.splitn(2, ':');
    let kind = parts.next().unwrap_or_default();
    let rest = parts.next().ok_or_else(|| CliError::InvalidSpec {
        spec: spec.to_string(),
    })?;
    match kind {
        "parquet" => Ok(SchemaSpec::Parquet(PathBuf::from(rest))),
        "delta" => Ok(SchemaSpec::Delta(rest.to_string())),
        "dbt" => {
            let (yaml, model) = rest.rsplit_once(':').ok_or_else(|| CliError::InvalidSpec {
                spec: spec.to_string(),
            })?;
            Ok(SchemaSpec::Dbt {
                yaml: PathBuf::from(yaml),
                model: model.to_string(),
            })
        }
        "erwin" => {
            let (xml, entity) = rest.rsplit_once(':').ok_or_else(|| CliError::InvalidSpec {
                spec: spec.to_string(),
            })?;
            Ok(SchemaSpec::Erwin {
                xml: PathBuf::from(xml),
                entity: entity.to_string(),
            })
        }
        _ => Err(CliError::InvalidSpec {
            spec: spec.to_string(),
        }),
    }
}

fn resolve_schema(spec: &SchemaSpec) -> Result<Schema, SchemaCodegenError> {
    match spec {
        SchemaSpec::Parquet(path) => schema_codegen::read_parquet_schema(path),
        SchemaSpec::Delta(uri) => schema_codegen::read_delta_schema(uri),
        SchemaSpec::Dbt { yaml, model } => {
            let text = read_to_string(yaml)?;
            schema_codegen::parse_dbt_schema_yml(&text, model)
        }
        SchemaSpec::Erwin { xml, entity } => {
            let text = read_to_string(xml)?;
            schema_codegen::parse_erwin_xml(&text, entity)
        }
    }
}

fn read_to_string(path: &Path) -> Result<String, SchemaCodegenError> {
    std::fs::read_to_string(path).map_err(|source| SchemaCodegenError::OpenFile {
        path: path.to_path_buf(),
        source,
    })
}

/// Resolves `--source-data`'s location and an inferred format, defaulting
/// to the location embedded in a self-describing `--source-schema` spec
/// (`parquet:`/`delta:`) when `--source-data` is omitted.
fn resolve_source_data(
    spec: &SchemaSpec,
    source_data: Option<&str>,
) -> Result<(String, Option<SourceFormat>), CliError> {
    if let Some(location) = source_data {
        return Ok((location.to_string(), infer_format(location)));
    }
    match spec {
        SchemaSpec::Parquet(path) => Ok((
            path.to_string_lossy().into_owned(),
            Some(SourceFormat::Parquet),
        )),
        SchemaSpec::Delta(uri) => Ok((uri.clone(), Some(SourceFormat::Delta))),
        SchemaSpec::Dbt { .. } => Err(CliError::MissingSourceData {
            spec: "dbt:...".to_string(),
        }),
        SchemaSpec::Erwin { .. } => Err(CliError::MissingSourceData {
            spec: "erwin:...".to_string(),
        }),
    }
}

/// Resolves `--transform-sql`/`--transform-df` into a single [`TransformSpec`],
/// rejecting the case where both were passed (api-parse-dont-validate: every
/// `--transform-df` op is parsed into a [`DfOp`] here, once, so nothing
/// downstream re-parses its text).
fn parse_transform<'a>(
    transform_sql: Option<&'a str>,
    transform_df: &'a [String],
) -> Result<Option<TransformSpec<'a>>, CliError> {
    match (transform_sql, transform_df.is_empty()) {
        (Some(_), false) => Err(CliError::ConflictingTransform),
        (Some(sql), true) => Ok(Some(TransformSpec::Sql(sql))),
        (None, true) => Ok(None),
        (None, false) => {
            let ops = transform_df
                .iter()
                .map(|op| parse_df_op(op))
                .collect::<Result<Vec<_>, CliError>>()?;
            Ok(Some(TransformSpec::DataFrame(ops)))
        }
    }
}

fn parse_df_op(op: &str) -> Result<DfOp, CliError> {
    let invalid = || CliError::InvalidTransformDf { op: op.to_string() };
    let mut parts = op.splitn(2, ':');
    let kind = parts.next().ok_or_else(invalid)?;
    let rest = parts.next().ok_or_else(invalid)?;
    match kind {
        "select" => Ok(DfOp::Select(rest.split(',').map(str::to_string).collect())),
        "filter" => {
            let mut parts = rest.splitn(3, ',');
            let column = parts.next().ok_or_else(invalid)?.to_string();
            let cmp = parse_comparator(parts.next().ok_or_else(invalid)?).ok_or_else(invalid)?;
            let value = parse_literal(parts.next().ok_or_else(invalid)?);
            Ok(DfOp::Filter { column, cmp, value })
        }
        "aggregate" => parse_aggregate(rest).ok_or_else(invalid),
        "sort" => parse_sort(rest).ok_or_else(invalid),
        "limit" => rest
            .parse::<usize>()
            .map(DfOp::Limit)
            .map_err(|_| invalid()),
        _ => Err(invalid()),
    }
}

fn parse_comparator(text: &str) -> Option<Comparator> {
    match text {
        "eq" => Some(Comparator::Eq),
        "ne" => Some(Comparator::Ne),
        "gt" => Some(Comparator::Gt),
        "lt" => Some(Comparator::Lt),
        "ge" => Some(Comparator::Ge),
        "le" => Some(Comparator::Le),
        _ => None,
    }
}

/// Tries `i64`, then `f64`, then `bool`, else keeps `text` as a string
/// literal — the same cascading inference the plan settled on so a bare
/// `--transform-df "filter:qty,gt,0"` doesn't need its value type spelled out.
fn parse_literal(text: &str) -> Literal {
    if let Ok(n) = text.parse::<i64>() {
        Literal::Int(n)
    } else if let Ok(f) = text.parse::<f64>() {
        Literal::Float(f)
    } else if let Ok(b) = text.parse::<bool>() {
        Literal::Bool(b)
    } else {
        Literal::Str(text.to_string())
    }
}

fn parse_agg_fn(text: &str) -> Option<AggFn> {
    match text {
        "sum" => Some(AggFn::Sum),
        "count" => Some(AggFn::Count),
        "avg" => Some(AggFn::Avg),
        "min" => Some(AggFn::Min),
        "max" => Some(AggFn::Max),
        _ => None,
    }
}

/// Parses `group=col1,col2;sum(col)=alias,count(col)=alias` into
/// `(group_by, aggs)`. `group=` may be empty for a whole-table aggregate.
fn parse_aggregate(rest: &str) -> Option<DfOp> {
    let (group_part, agg_part) = rest.split_once(';')?;
    let group_cols = group_part.strip_prefix("group=")?;
    let group_by = if group_cols.is_empty() {
        Vec::new()
    } else {
        group_cols.split(',').map(str::to_string).collect()
    };
    let aggs = agg_part
        .split(',')
        .map(parse_agg_term)
        .collect::<Option<Vec<_>>>()?;
    Some(DfOp::Aggregate { group_by, aggs })
}

/// Parses one `func(col)=alias` term.
fn parse_agg_term(term: &str) -> Option<AggExpr> {
    let (func_col, alias) = term.split_once('=')?;
    let (func_name, column) = func_col.split_once('(')?;
    let column = column.strip_suffix(')')?;
    let func = parse_agg_fn(func_name)?;
    Some(AggExpr {
        column: column.to_string(),
        func,
        alias: alias.to_string(),
    })
}

/// Parses `col,asc;col2,desc` into a list of [`SortKey`]s.
fn parse_sort(rest: &str) -> Option<DfOp> {
    let keys = rest
        .split(';')
        .map(|term| {
            let (column, direction) = term.split_once(',')?;
            let ascending = match direction {
                "asc" => true,
                "desc" => false,
                _ => return None,
            };
            Some(SortKey {
                column: column.to_string(),
                ascending,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(DfOp::Sort(keys))
}

fn infer_format(location: &str) -> Option<SourceFormat> {
    if location.ends_with(".csv") {
        Some(SourceFormat::Csv)
    } else if location.ends_with(".parquet") {
        Some(SourceFormat::Parquet)
    } else {
        None
    }
}

#[cfg(test)]
mod transform_df_grammar_tests {
    use super::*;

    #[test]
    fn select_op_splits_columns_on_comma() {
        assert_eq!(
            parse_df_op("select:id,quantity").expect("select op parses"),
            DfOp::Select(vec!["id".to_string(), "quantity".to_string()])
        );
    }

    #[test]
    fn filter_op_parses_column_comparator_and_value() {
        assert_eq!(
            parse_df_op("filter:quantity,gt,0").expect("filter op parses"),
            DfOp::Filter {
                column: "quantity".to_string(),
                cmp: Comparator::Gt,
                value: Literal::Int(0),
            }
        );
    }

    #[test]
    fn aggregate_op_parses_group_and_agg_terms() {
        assert_eq!(
            parse_df_op("aggregate:group=region;sum(quantity)=total,count(id)=n")
                .expect("aggregate op parses"),
            DfOp::Aggregate {
                group_by: vec!["region".to_string()],
                aggs: vec![
                    AggExpr {
                        column: "quantity".to_string(),
                        func: AggFn::Sum,
                        alias: "total".to_string(),
                    },
                    AggExpr {
                        column: "id".to_string(),
                        func: AggFn::Count,
                        alias: "n".to_string(),
                    },
                ],
            }
        );
    }

    #[test]
    fn aggregate_op_allows_empty_group_for_whole_table_aggregate() {
        assert_eq!(
            parse_df_op("aggregate:group=;sum(quantity)=total")
                .expect("whole-table aggregate op parses"),
            DfOp::Aggregate {
                group_by: vec![],
                aggs: vec![AggExpr {
                    column: "quantity".to_string(),
                    func: AggFn::Sum,
                    alias: "total".to_string(),
                }],
            }
        );
    }

    #[test]
    fn sort_op_parses_column_direction_pairs() {
        assert_eq!(
            parse_df_op("sort:total,desc;id,asc").expect("sort op parses"),
            DfOp::Sort(vec![
                SortKey {
                    column: "total".to_string(),
                    ascending: false,
                },
                SortKey {
                    column: "id".to_string(),
                    ascending: true,
                },
            ])
        );
    }

    #[test]
    fn limit_op_parses_row_count() {
        assert_eq!(
            parse_df_op("limit:10").expect("limit op parses"),
            DfOp::Limit(10)
        );
    }

    #[test]
    fn unknown_op_kind_is_rejected() {
        assert!(matches!(
            parse_df_op("bogus:1"),
            Err(CliError::InvalidTransformDf { .. })
        ));
    }

    #[test]
    fn filter_value_inference_prefers_int_then_float_then_bool_then_string() {
        assert_eq!(parse_literal("0"), Literal::Int(0));
        assert_eq!(parse_literal("1.5"), Literal::Float(1.5));
        assert_eq!(parse_literal("true"), Literal::Bool(true));
        assert_eq!(
            parse_literal("region-a"),
            Literal::Str("region-a".to_string())
        );
    }

    #[test]
    fn transform_sql_and_transform_df_together_is_rejected() {
        let df = vec!["limit:10".to_string()];
        assert!(matches!(
            parse_transform(Some("SELECT 1"), &df),
            Err(CliError::ConflictingTransform)
        ));
    }

    #[test]
    fn transform_sql_alone_is_accepted() {
        let df: Vec<String> = vec![];
        let transform = parse_transform(Some("SELECT 1"), &df).expect("sql-only transform parses");
        assert!(matches!(transform, Some(TransformSpec::Sql("SELECT 1"))));
    }

    #[test]
    fn transform_df_alone_is_parsed_into_ops() {
        let df = vec!["limit:5".to_string()];
        let transform = parse_transform(None, &df).expect("dataframe-only transform parses");
        assert!(matches!(
            transform,
            Some(TransformSpec::DataFrame(ops)) if ops == vec![DfOp::Limit(5)]
        ));
    }

    #[test]
    fn neither_transform_flag_yields_no_transform() {
        let df: Vec<String> = vec![];
        assert!(matches!(parse_transform(None, &df), Ok(None)));
    }
}
