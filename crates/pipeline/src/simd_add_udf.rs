//! Exposes [`crate::simd_add::wrapping_add_simd`] as a DataFusion scalar
//! UDF, so the explicit-SIMD kernel runs through DataFusion's own
//! planner/executor instead of only being provable as a bare function call
//! (`docs/internals/experiment.md`'s architecture amendment: "the Rust SIMD
//! kernel must participate in DataFusion execution").
//!
//! This module is the integration/Action layer; `simd_add` itself stays an
//! unmodified, pure-Calculation module (same split already recorded for
//! that module: deterministic input-to-output, no I/O, no time-dependence).
//! Separating them means a change to how DataFusion is wired here can never
//! accidentally change the kernel's own correctness, and vice versa.
//!
//! Follows this crate's existing scalar-UDF shape exactly
//! (`datafusion_query::days_since_epoch_udf`/`invoke_days_since_epoch`): a
//! public `*_udf() -> ScalarUDF` constructor built from `create_udf`, and a
//! private `invoke_*` function matching DataFusion's
//! `&[ColumnarValue] -> Result<ColumnarValue, DataFusionError>` signature.

use std::sync::Arc;

use arrow::array::Int32Array;
use arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};

use crate::simd_add::wrapping_add_simd;

/// The SQL/DataFrame-visible function name.
pub const WRAPPING_ADD_SIMD_UDF_NAME: &str = "wrapping_add_simd";

/// `wrapping_add_simd(column, k) -> INT` — wrapping `i32` addition of the
/// literal `k` to every value in `column`, computed by the same
/// AVX2/SSE2-dispatching kernel `simd_add`'s own tests check against a
/// scalar oracle. Declared `Volatility::Immutable`: identical inputs always
/// produce identical output with no side effects, which is also true of
/// the underlying kernel by construction (it is a pure Calculation).
#[must_use]
pub fn wrapping_add_simd_udf() -> datafusion::logical_expr::ScalarUDF {
    let implementation = Arc::new(invoke_wrapping_add_simd);
    create_udf(
        WRAPPING_ADD_SIMD_UDF_NAME,
        vec![DataType::Int32, DataType::Int32],
        DataType::Int32,
        Volatility::Immutable,
        implementation,
    )
}

/// Scope, deliberately narrow, matching `simd_add`'s own documented scope:
///
/// - The first argument must be an `Int32` **array** (a column). A bare
///   scalar in that position is rejected explicitly rather than silently
///   wrapped into a one-element array — DataFusion can constant-fold a
///   scalar argument into either position, and this UDF's job here is to
///   prove the array/SIMD path runs, not to also define scalar semantics
///   nobody asked for.
/// - The second argument must be a non-null `Int32` **literal** — the
///   constant added to every element. A second array argument (a
///   column-plus-column add) is a different, unimplemented operation, not
///   a relaxation of this one.
/// - Nullable first-argument arrays are rejected by `wrapping_add_simd`
///   itself ([`crate::simd_add::SimdAddError::Nullable`]); this function
///   does not re-implement or relax that check, only reports it as a
///   `DataFusionError`.
fn invoke_wrapping_add_simd(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    let [column, constant] = args else {
        return Err(DataFusionError::Execution(format!(
            "{WRAPPING_ADD_SIMD_UDF_NAME} expects exactly two arguments (array, constant), got {}",
            args.len()
        )));
    };
    let ColumnarValue::Scalar(ScalarValue::Int32(Some(k))) = constant else {
        return Err(DataFusionError::Execution(format!(
            "{WRAPPING_ADD_SIMD_UDF_NAME}'s second argument must be a non-null INT32 literal"
        )));
    };
    let ColumnarValue::Array(array) = column else {
        return Err(DataFusionError::Execution(format!(
            "{WRAPPING_ADD_SIMD_UDF_NAME}'s first argument must be an INT32 array; \
             a bare scalar column is outside this UDF's scope"
        )));
    };
    let array = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "{WRAPPING_ADD_SIMD_UDF_NAME} requires an INT32 array, got {:?}",
            array.data_type()
        ))
    })?;

    let result = wrapping_add_simd(array, *k)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;

    Ok(ColumnarValue::Array(Arc::new(result)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simd_add::wrapping_add_reference;
    use arrow::array::RecordBatch;
    use arrow::datatypes::{Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    const K: i32 = 7;

    fn values_batch(values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))])
            .expect("one non-null Int32 column")
    }

    async fn context_with_values(values: Vec<i32>) -> SessionContext {
        let ctx = SessionContext::new();
        let batch = values_batch(values);
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).expect("equal schemas");
        ctx.register_table("t", Arc::new(table))
            .expect("register table");
        ctx.register_udf(wrapping_add_simd_udf());
        ctx
    }

    /// The acceptance criterion the architecture amendment actually asks
    /// for: not that the UDF is *registered*, but that running it through
    /// real DataFusion SQL planning and execution (`.collect().await`, not
    /// a bare function call) produces the same result the kernel's own
    /// scalar oracle does. A planning-only check (`EXPLAIN`) would prove
    /// DataFusion intends to call this UDF; only `.collect()` proves it
    /// actually ran and returned a value DataFusion accepted as the query
    /// result.
    #[tokio::test]
    async fn sql_query_through_datafusion_matches_the_kernel_oracle() {
        let values: Vec<i32> = (0..37).collect();
        let ctx = context_with_values(values.clone()).await;

        let query = format!(
            "SELECT wrapping_add_simd(value, CAST({K} AS INT)) AS result FROM t ORDER BY value"
        );
        let batches = ctx
            .sql(&query)
            .await
            .expect("plan the query")
            .collect()
            .await
            .expect("execute the query");

        let oracle = wrapping_add_reference(&Int32Array::from(values), K).expect("no nulls");

        assert_eq!(batches.len(), 1, "one partition, no need to concatenate");
        let result = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("UDF declared INT32 output");
        assert_eq!(result.values(), oracle.values());
    }

    /// Plan evidence per the amendment ("capture relevant plan evidence...
    /// verify the intended operator or function appears"): confirms the
    /// UDF call survives planning instead of being constant-folded or
    /// rewritten into something else, as a textual check on the plan
    /// DataFusion reports for this query shape — a weaker claim than the
    /// execution test above, kept as an independent, cheaper signal.
    #[tokio::test]
    async fn the_udf_call_appears_in_the_logical_plan() {
        let ctx = context_with_values(vec![1, 2, 3]).await;
        let explained = ctx
            .sql(&format!(
                "EXPLAIN SELECT wrapping_add_simd(value, CAST({K} AS INT)) FROM t"
            ))
            .await
            .expect("plan the explain query")
            .collect()
            .await
            .expect("execute the explain query");

        let plan_text: String = explained
            .iter()
            .flat_map(|batch| {
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .expect("EXPLAIN's second column is plan text")
                    .iter()
                    .flatten()
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
            .join("\n");

        assert!(
            plan_text.contains(WRAPPING_ADD_SIMD_UDF_NAME),
            "expected {WRAPPING_ADD_SIMD_UDF_NAME} in the plan, got:\n{plan_text}"
        );
    }

    /// Both arguments here are literals, and the UDF is declared
    /// `Volatility::Immutable` — the same combination
    /// `explain_days_since_epoch_udf_vs_builtin` relies on elsewhere in
    /// this crate to make DataFusion constant-fold a UDF call during
    /// planning. That means this rejection may surface at `.sql(...).await`
    /// (planning) rather than `.collect().await` (execution), depending on
    /// whether the optimizer folds it — this test asserts only that the
    /// query fails *somewhere*, not which stage, so it does not depend on
    /// an optimizer behavior this module has no reason to pin down.
    #[tokio::test]
    async fn a_scalar_first_argument_is_rejected_rather_than_silently_coerced() {
        let ctx = context_with_values(vec![1]).await;
        let query = format!("SELECT wrapping_add_simd(CAST({K} AS INT), CAST({K} AS INT))");

        let planned = ctx.sql(&query).await;
        let failed = match planned {
            Err(_) => true,
            Ok(dataframe) => dataframe.collect().await.is_err(),
        };

        assert!(
            failed,
            "a scalar first argument must be rejected, not coerced"
        );
    }
}
