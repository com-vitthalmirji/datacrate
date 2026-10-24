//! Measures `pipeline::simd_add`'s three implementations against identical
//! input, isolated from fixture generation (`docs/internals/experiment.md`,
//! Phase A, acceptance: "benchmarks exclude fixture generation from
//! kernel-only timing").
//!
//! This binary is evidence collection, not a performance claim by itself.
//! Shared CI runners are noisy; the numbers it prints are raw per-run
//! durations for a human (or a later, dedicated-hardware run) to read, not
//! a ratio this binary asserts on. See the module docs on
//! `pipeline::simd_add` for the correctness guarantee these three
//! implementations share — this binary assumes that guarantee holds and
//! re-checks it once per run anyway, because a benchmark that silently
//! drifted from its own correctness tests would be worse than no benchmark.

use std::hint::black_box;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use arrow::array::Int32Array;
use clap::Parser;
use pipeline::simd_add::{wrapping_add_reference, wrapping_add_scalar, wrapping_add_simd};

#[derive(Parser)]
#[command(
    name = "simd-wrapping-add",
    about = "Time reference vs. optimized-scalar vs. explicit-SIMD wrapping i32 addition"
)]
struct Args {
    /// Element count. Large enough that per-run overhead doesn't dominate.
    #[arg(long, default_value_t = 10_000_000)]
    len: usize,

    /// Timed repetitions per implementation.
    #[arg(long, default_value_t = 5)]
    runs: usize,
}

const K: i32 = 7;

/// Not timed: generating the fixture is not part of the kernel being
/// measured, so it happens once, before any `Instant::now()` call below.
fn generate_input(len: usize) -> Int32Array {
    Int32Array::from(
        (0..len as i64)
            .map(|i| (i % 1_000_000) as i32)
            .collect::<Vec<_>>(),
    )
}

/// Runs `f` `runs` times, returning each run's wall time. `black_box` on
/// both the array reference going in and the result coming out: without
/// it, a sufficiently aggressive optimizer could in principle prove the
/// result is unused across the whole timed loop and elide the call
/// entirely, which would silently time nothing. This is the "apply
/// optimizer barriers fairly" requirement applied to all three
/// implementations identically, not just the one expected to win.
fn time_runs(
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
    println!("{label}: {}", formatted.join(", "));
    (durations, black_box(result))
}

fn main() -> ExitCode {
    let args = Args::parse();
    if args.runs == 0 {
        eprintln!("simd-wrapping-add: --runs must be at least 1");
        return ExitCode::FAILURE;
    }

    let array = generate_input(args.len);

    let (_, reference) = time_runs(
        "reference (scalar loop, oracle)",
        args.runs,
        &array,
        |a, k| wrapping_add_reference(a, k).expect("input is constructed without nulls"),
    );
    let (_, scalar) = time_runs(
        "optimized scalar (iterator chain)",
        args.runs,
        &array,
        |a, k| wrapping_add_scalar(a, k).expect("input is constructed without nulls"),
    );
    let (_, simd) = time_runs("explicit SIMD (std::arch)", args.runs, &array, |a, k| {
        wrapping_add_simd(a, k).expect("input is constructed without nulls")
    });

    // Correctness before any number is trusted (plan §16: "Every result
    // must include correctness evidence"). Re-checked here, not only in
    // `cargo test`, because this binary is what actually produces the
    // numbers that would go in a report.
    if reference.values() != scalar.values() || reference.values() != simd.values() {
        eprintln!("simd-wrapping-add: result mismatch between implementations");
        return ExitCode::FAILURE;
    }

    println!(
        "simd-wrapping-add: len={} runs={} all implementations agree on output",
        args.len, args.runs
    );
    println!(
        "simd-wrapping-add: this binary reports raw per-run wall time only; \
         it does not assert a speedup ratio (shared/noisy hardware caveat, \
         see docs/internals/experiment.md section 16)"
    );

    ExitCode::SUCCESS
}
