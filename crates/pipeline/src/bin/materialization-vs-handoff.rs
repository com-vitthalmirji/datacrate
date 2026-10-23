//! Phase B (`docs/internals/experiment.md` section 10): distinguishes
//! representation conversion from kernel execution for
//! `pipeline::simd_add`, across the matrix the plan asks for —
//! {explicit materialization, Arrow-native handoff} x
//! {optimized ordinary Rust, explicit SIMD} — four cells, each reporting
//! conversion time, kernel time, and combined duration separately. None
//! of the three is inferred from the other two.
//!
//! "Arrow-native handoff": the `value` column already exists as an
//! `Int32Array` when the kernel runs (identical to `simd-wrapping-add.rs`'s
//! input shape).
//!
//! "Explicit materialization": the source is `Vec<SourceRow>`, a row-shaped
//! struct (`id`/`value`/`placed_at`/`note`) modeling data that did not
//! start out column-oriented. Pulling `value` out into an `Int32Array`
//! requires copying one field out of each row's interleaved storage —
//! `Int32Array::from_iter_values` always allocates and copies here; there
//! is no existing contiguous `i32` buffer to reuse.
//!
//! This binary also checks, rather than assumes, whether Arrow's
//! `Int32Array::from(Vec<i32>)` reuses the source `Vec`'s own heap
//! allocation (arrow-rs's `Buffer::from_vec` is documented to do this) by
//! comparing the `Vec`'s pointer, captured before the conversion consumes
//! it, against the resulting array's buffer pointer. If they match, that
//! specific construction is a real no-payload-copy case — stated as "no
//! copy," not "zero allocation" (`Buffer::from_vec` still wraps the
//! allocation in new tracking metadata), per the plan's explicit
//! distinction between those two claims.

use std::hint::black_box;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use arrow::array::Int32Array;
use clap::Parser;
use pipeline::simd_add::{wrapping_add_scalar, wrapping_add_simd};

#[derive(Parser)]
#[command(
    name = "materialization-vs-handoff",
    about = "Time explicit materialization vs. Arrow-native handoff, crossed with optimized-scalar vs. explicit-SIMD kernels"
)]
struct Args {
    /// Element/row count.
    #[arg(long, default_value_t = 10_000_000)]
    len: usize,

    /// Timed repetitions per cell.
    #[arg(long, default_value_t = 5)]
    runs: usize,
}

const K: i32 = 7;

/// A row-shaped source: `value` is interleaved with other fields, the way
/// a struct pulled from a non-Arrow source (a JSON record, a JVM object,
/// a CSV row) would be, not already column-oriented.
struct SourceRow {
    id: i64,
    value: i32,
    placed_at: i64,
    note: String,
}

/// Not timed: generating the fixture is not part of either the conversion
/// or the kernel being measured.
fn generate_source_rows(len: usize) -> Vec<SourceRow> {
    (0..len)
        .map(|i| SourceRow {
            id: i as i64,
            value: (i % 1_000_000) as i32,
            placed_at: 1_700_000_000 + i as i64,
            note: format!("row-{i}"),
        })
        .collect()
}

/// Not timed, matching `generate_source_rows`: the Arrow-native column's
/// `Vec<i32>` fixture is generated once per run outside the timed
/// conversion, exactly like the row source above.
fn generate_arrow_native_source(len: usize) -> Vec<i32> {
    (0..len as i64).map(|i| (i % 1_000_000) as i32).collect()
}

/// Explicit materialization: copies `value` out of each row. Always a real
/// copy — there is no existing contiguous `i32` buffer inside `Vec<SourceRow>`
/// to reuse. `black_box`-reads `id`/`placed_at`/`note` per row, matching
/// `row-vs-columnar.rs`'s own convention for the same reason: a real
/// row-shaped source makes the full stride resident in cache on every
/// iteration (JVM-row-style locality cost), not just the one field this
/// path actually needs, and leaving those fields truly unread would (a)
/// misrepresent that cost and (b) fail `-D dead-code` outright, since
/// nothing else in this binary reads them.
fn materialize_value_column(rows: &[SourceRow]) -> Int32Array {
    Int32Array::from_iter_values(rows.iter().map(|row| {
        black_box(row.id);
        black_box(row.placed_at);
        black_box(&row.note);
        row.value
    }))
}

/// Arrow-native handoff's conversion step: constructs the column directly
/// from a contiguous `Vec<i32>`. Whether this reuses `source`'s own
/// allocation (no copy) or not is checked separately, by pointer
/// comparison, not assumed here.
fn arrow_native_handoff(source: Vec<i32>) -> Int32Array {
    Int32Array::from(source)
}

/// One matrix cell: times `conversion_runs` independent (conversion time,
/// combined time) pairs, using a dedicated conversion-time repetition
/// separate from the dedicated kernel-only repetitions done afterward in
/// `main` — a shared loop would conflate them.
fn run_cell(
    cell_label: &str,
    runs: usize,
    conversion: impl Fn() -> Int32Array,
    kernel: impl Fn(&Int32Array, i32) -> Int32Array,
) -> Int32Array {
    let mut conversion_durations = Vec::with_capacity(runs);
    let mut combined_durations = Vec::with_capacity(runs);
    let mut last_array = None;
    for _ in 0..runs {
        let combined_start = Instant::now();
        let conversion_start = Instant::now();
        let built = conversion();
        let conversion_duration = conversion_start.elapsed();
        let _ = black_box(kernel(black_box(&built), black_box(K)));
        combined_durations.push(combined_start.elapsed());
        conversion_durations.push(conversion_duration);
        last_array = Some(built);
    }
    let formatted_conversion: Vec<String> = conversion_durations
        .iter()
        .map(|d| format!("{d:?}"))
        .collect();
    let formatted_combined: Vec<String> = combined_durations
        .iter()
        .map(|d| format!("{d:?}"))
        .collect();
    println!("{cell_label}:");
    println!(
        "  conversion only:            {}",
        formatted_conversion.join(", ")
    );
    println!(
        "  combined (conversion+kernel): {}",
        formatted_combined.join(", ")
    );
    last_array.expect("runs is always > 0 for a meaningful benchmark invocation")
}

fn time_kernel_runs(
    label: &str,
    runs: usize,
    array: &Int32Array,
    f: impl Fn(&Int32Array, i32) -> Int32Array,
) -> (Vec<Duration>, Int32Array) {
    let mut durations = Vec::with_capacity(runs);
    let mut last = None;
    for _ in 0..runs {
        let start = Instant::now();
        let result = f(black_box(array), black_box(K));
        durations.push(start.elapsed());
        last = Some(result);
    }
    let result = last.expect("runs is always > 0 for a meaningful benchmark invocation");
    let formatted: Vec<String> = durations.iter().map(|d| format!("{d:?}")).collect();
    println!("  kernel only ({label}): {}", formatted.join(", "));
    (durations, black_box(result))
}

fn main() -> ExitCode {
    let args = Args::parse();
    if args.runs == 0 {
        eprintln!("materialization-vs-handoff: --runs must be at least 1");
        return ExitCode::FAILURE;
    }

    let scalar = |a: &Int32Array, k: i32| {
        wrapping_add_scalar(a, k).expect("input is constructed without nulls")
    };
    let simd = |a: &Int32Array, k: i32| {
        wrapping_add_simd(a, k).expect("input is constructed without nulls")
    };

    // Buffer identity check, done once outside the timed cells: captures
    // the Vec's pointer before `Int32Array::from` consumes it, per the
    // plan's "buffer identity checks occur while relevant owners remain
    // alive." This is a construction-mechanism fact, not a per-run timing
    // claim, so it is deliberately not folded into the timed loop below.
    let identity_check_source = generate_arrow_native_source(args.len);
    let identity_check_ptr = identity_check_source.as_ptr();
    let identity_check_array = arrow_native_handoff(identity_check_source);
    let reused_allocation = identity_check_array.values().as_ptr() == identity_check_ptr;
    println!(
        "buffer identity: Int32Array::from(Vec<i32>) {} the source Vec's allocation",
        if reused_allocation {
            "reused (no payload copy)"
        } else {
            "did NOT reuse (a copy occurred)"
        }
    );
    println!();

    let native_scalar = run_cell(
        "[Arrow-native handoff x optimized scalar]",
        args.runs,
        || arrow_native_handoff(generate_arrow_native_source(args.len)),
        scalar,
    );
    let native_simd = run_cell(
        "[Arrow-native handoff x explicit SIMD]",
        args.runs,
        || arrow_native_handoff(generate_arrow_native_source(args.len)),
        simd,
    );

    let rows = generate_source_rows(args.len);
    let materialized_scalar = run_cell(
        "[Explicit materialization x optimized scalar]",
        args.runs,
        || materialize_value_column(&rows),
        scalar,
    );
    let materialized_simd = run_cell(
        "[Explicit materialization x explicit SIMD]",
        args.runs,
        || materialize_value_column(&rows),
        simd,
    );

    println!();
    println!("kernel-only timing (conversion excluded), reusing one already-built array per path:");
    let (_, native_scalar_kernel) = time_kernel_runs(
        "Arrow-native, optimized scalar",
        args.runs,
        &native_scalar,
        scalar,
    );
    let (_, native_simd_kernel) =
        time_kernel_runs("Arrow-native, explicit SIMD", args.runs, &native_simd, simd);
    let (_, materialized_scalar_kernel) = time_kernel_runs(
        "materialized, optimized scalar",
        args.runs,
        &materialized_scalar,
        scalar,
    );
    let (_, materialized_simd_kernel) = time_kernel_runs(
        "materialized, explicit SIMD",
        args.runs,
        &materialized_simd,
        simd,
    );

    if native_scalar_kernel.values() != native_simd_kernel.values()
        || materialized_scalar_kernel.values() != materialized_simd_kernel.values()
        || native_scalar_kernel.values() != materialized_scalar_kernel.values()
    {
        eprintln!("materialization-vs-handoff: result mismatch between implementations or paths");
        return ExitCode::FAILURE;
    }

    println!();
    println!(
        "materialization-vs-handoff: len={} runs={} all cells agree on output",
        args.len, args.runs
    );
    println!(
        "materialization-vs-handoff: raw per-run wall time only; no speedup ratio asserted \
         (shared/noisy hardware caveat, see docs/internals/experiment.md section 16)"
    );

    ExitCode::SUCCESS
}
