//! Phase F (`docs/internals/experiment.md` section 14): encodings and
//! nested representations, reusing `pipeline::simd_add`'s handler where
//! supported and documenting it explicitly where not.
//!
//! `wrapping_add_simd` accepts only a plain `Int32Array`. None of the
//! encodings below can feed it directly — this binary does not write a
//! custom encoded SIMD kernel to change that (the plan's explicit
//! instruction: "do not write custom encoded SIMD kernels merely to
//! complete this checklist"). For each encoding this reports: construction
//! cost, retained storage (`Array::get_array_memory_size`), whether the
//! kernel operates directly on the encoding (always "no" here), the
//! explicit decode/normalization step's own cost, and a correctness check
//! against an oracle computed directly from each fixture's own known
//! formula — never through another encoded array.

use std::hint::black_box;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, ArrayRef, DictionaryArray, Int32Array, ListArray, RunArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Int32Type};
use pipeline::simd_add::wrapping_add_simd;

const K: i32 = 7;

fn time<T>(f: impl FnOnce() -> T) -> (T, Duration) {
    let start = Instant::now();
    let result = f();
    (result, start.elapsed())
}

/// Low cardinality: 100 distinct values, cycling. Decoded value at index
/// `i` is `i % 100` — the oracle below computes this directly, never by
/// reading the dictionary back.
fn dictionary_low_cardinality(len: usize) -> (DictionaryArray<Int32Type>, Vec<i32>) {
    const DISTINCT: i32 = 100;
    let keys = Int32Array::from((0..len as i32).map(|i| i % DISTINCT).collect::<Vec<_>>());
    let values: ArrayRef = Arc::new(Int32Array::from((0..DISTINCT).collect::<Vec<_>>()));
    let dict = DictionaryArray::<Int32Type>::try_new(keys, values).expect("valid dictionary");
    let oracle = (0..len as i32).map(|i| i % DISTINCT).collect();
    (dict, oracle)
}

/// High cardinality: every key distinct — the degenerate case where a
/// dictionary provides no compression at all. Decoded value at index `i`
/// is `i`.
fn dictionary_high_cardinality(len: usize) -> (DictionaryArray<Int32Type>, Vec<i32>) {
    let keys = Int32Array::from((0..len as i32).collect::<Vec<_>>());
    let values: ArrayRef = Arc::new(Int32Array::from((0..len as i32).collect::<Vec<_>>()));
    let dict = DictionaryArray::<Int32Type>::try_new(keys, values).expect("valid dictionary");
    let oracle = (0..len as i32).collect();
    (dict, oracle)
}

/// Long runs: `run_length` consecutive elements share one value, value
/// increments per run. Decoded value at index `i` is `i / run_length`.
fn run_end_encoded(len: usize, run_length: usize) -> (RunArray<Int32Type>, Vec<i32>) {
    let run_count = len.div_ceil(run_length);
    let run_ends = Int32Array::from(
        (1..=run_count)
            .map(|r| (r * run_length).min(len) as i32)
            .collect::<Vec<_>>(),
    );
    let values = Int32Array::from((0..run_count as i32).collect::<Vec<_>>());
    let run_array =
        RunArray::<Int32Type>::try_new(&run_ends, &values).expect("valid run-end array");
    let oracle = (0..len as i32).map(|i| i / run_length as i32).collect();
    (run_array, oracle)
}

/// Decodes a dictionary-encoded `Int32` array back to a plain `Int32Array`
/// via `cast` — the documented, general unpacking path, not a hand-rolled
/// one.
fn decode_dictionary(dict: &DictionaryArray<Int32Type>) -> Int32Array {
    let decoded =
        cast(dict, &DataType::Int32).expect("cast unpacks a dictionary to its value type");
    decoded
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("cast target was Int32")
        .clone()
}

/// Decodes a run-end-encoded `Int32` array back to a plain `Int32Array` by
/// walking each logical index through `get_physical_index` — there is no
/// single general "unrun" kernel in this arrow-rs version, so this is the
/// explicit normalization step the plan asks to report, not a shortcut
/// around one that already exists.
fn decode_run_array(run_array: &RunArray<Int32Type>) -> Int32Array {
    let values = run_array
        .values()
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("run array values are Int32");
    let decoded: Vec<i32> = (0..run_array.len())
        .map(|i| values.value(run_array.get_physical_index(i)))
        .collect();
    Int32Array::from(decoded)
}

fn report_encoding(label: &str, construction: Duration, memory_size: usize) {
    println!("{label}:");
    println!("  construction cost: {construction:?}");
    println!("  retained storage (get_array_memory_size): {memory_size} bytes");
    println!(
        "  kernel operates directly on the encoding: no (wrapping_add_simd requires Int32Array)"
    );
}

fn decode_and_run_kernel(label: &str, decode: impl FnOnce() -> Int32Array, oracle: &[i32]) -> bool {
    let (decoded, decode_duration) = time(decode);
    println!("  explicit decode/normalization cost: {decode_duration:?}");

    let decoded_matches_oracle = decoded.values().iter().copied().eq(oracle.iter().copied());
    if !decoded_matches_oracle {
        eprintln!(
            "encodings-and-kernel: {label}: decoded values do not match the independent oracle"
        );
        return false;
    }

    let result =
        wrapping_add_simd(black_box(&decoded), black_box(K)).expect("decoded array has no nulls");
    let expected: Vec<i32> = oracle.iter().map(|v| v.wrapping_add(K)).collect();
    let kernel_matches_oracle = result.values().iter().copied().eq(expected.iter().copied());
    if !kernel_matches_oracle {
        eprintln!(
            "encodings-and-kernel: {label}: kernel result does not match the independent oracle"
        );
        return false;
    }
    true
}

fn main() -> ExitCode {
    let len = 500_000usize;
    let mut all_correct = true;

    let (low_card, low_card_oracle) = {
        let (result, duration) = time(|| dictionary_low_cardinality(len));
        report_encoding(
            "Dictionary<Int32>, low cardinality (100 distinct values)",
            duration,
            result.0.get_array_memory_size(),
        );
        result
    };
    all_correct &= decode_and_run_kernel(
        "dictionary low cardinality",
        || decode_dictionary(&low_card),
        &low_card_oracle,
    );
    println!();

    let (high_card, high_card_oracle) = {
        let (result, duration) = time(|| dictionary_high_cardinality(len));
        report_encoding(
            "Dictionary<Int32>, high cardinality (every key distinct)",
            duration,
            result.0.get_array_memory_size(),
        );
        result
    };
    all_correct &= decode_and_run_kernel(
        "dictionary high cardinality",
        || decode_dictionary(&high_card),
        &high_card_oracle,
    );
    println!();

    let (long_runs, long_runs_oracle) = {
        let (result, duration) = time(|| run_end_encoded(len, 10_000));
        report_encoding(
            "RunArray<Int32>, long runs (10,000 elements/run)",
            duration,
            result.0.get_array_memory_size(),
        );
        result
    };
    all_correct &= decode_and_run_kernel(
        "run-end long runs",
        || decode_run_array(&long_runs),
        &long_runs_oracle,
    );
    println!();

    let (short_runs, short_runs_oracle) = {
        let (result, duration) = time(|| run_end_encoded(len, 2));
        report_encoding(
            "RunArray<Int32>, short runs (2 elements/run)",
            duration,
            result.0.get_array_memory_size(),
        );
        result
    };
    all_correct &= decode_and_run_kernel(
        "run-end short runs",
        || decode_run_array(&short_runs),
        &short_runs_oracle,
    );
    println!();

    // Nested offset/child-buffer path: construction cost and retained
    // storage only. Unlike dictionary/run-end encoding, there is no
    // single natural "flatten to Int32Array" operation for a nested list
    // the kernel could plausibly want — deciding what that would even
    // mean is a separate design question this binary does not resolve,
    // per the plan's own scope: investigate the existing nested
    // offset/child-buffer path, not invent a new decode semantic for it.
    let nested_data: Vec<Option<Vec<Option<i32>>>> = (0..len / 2)
        .map(|i| Some(vec![Some(i as i32), Some(i as i32 + 1)]))
        .collect();
    let (nested, nested_duration) =
        time(|| ListArray::from_iter_primitive::<Int32Type, _, _>(nested_data));
    report_encoding(
        "List<Int32>, 2 elements/sub-list (construction/storage only, no decode attempted)",
        nested_duration,
        nested.get_array_memory_size(),
    );
    println!();

    if !all_correct {
        eprintln!("encodings-and-kernel: one or more encodings failed their correctness check");
        return ExitCode::FAILURE;
    }

    println!(
        "encodings-and-kernel: all decoded encodings match their independent oracles and agree with wrapping_add_simd's output"
    );
    ExitCode::SUCCESS
}
