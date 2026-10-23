//! Explicit SIMD over an Arrow `Int32Array`'s value buffer, isolated from
//! persistence, transport, or any other conversion cost
//! (`docs/internals/experiment.md`, Phase A).
//!
//! Three implementations of the same operation — wrapping `i32` addition of
//! a constant to every element — so a speedup claim, if any, is backed by
//! comparing them against an identical reference, not asserted:
//!
//! - [`wrapping_add_reference`]: the simplest correct loop. Defines what
//!   "correct" means for the other two.
//! - [`wrapping_add_scalar`]: ordinary optimized Rust. LLVM may already
//!   autovectorize this; the assembly decides that, not this doc comment.
//! - [`wrapping_add_simd`]: explicit `std::arch::x86_64` intrinsics (SSE2,
//!   always available on x86_64; AVX2 behind a runtime feature check), with
//!   a scalar fallback on every other target and for the non-multiple tail.
//!
//! Scope, deliberately narrow for this first slice:
//!
//! - Only `Int32Array`. Dictionary, run-end, string, and nested arrays are
//!   different physical representations; reinterpreting their storage as
//!   plain `i32` would silently produce wrong answers, not a generalization.
//! - Nullable input is rejected explicitly ([`SimdAddError::Nullable`]), not
//!   silently treated as all-valid. Null semantics for this kernel are a
//!   separate, deferred decision (plan §7).
//! - `array.values()` already returns the array's *logical* slice — Arrow
//!   applies the array's offset internally — so a sliced array is handled
//!   for free by operating on that slice. The one consequence this forces:
//!   the slice's start address is **not guaranteed aligned** to any SIMD
//!   width, so every load/store below is an unaligned intrinsic
//!   (`loadu`/`storeu`), never `load`/`store`. Using an aligned intrinsic on
//!   Arrow data is a real bug a benchmark that only tests unsliced,
//!   freshly-built arrays would never catch.

use std::fmt;

use arrow::array::{Array, Int32Array};

/// Why [`wrapping_add_reference`], [`wrapping_add_scalar`], or
/// [`wrapping_add_simd`] refused an input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimdAddError {
    /// The array has one or more nulls. This kernel does not define a null
    /// behavior yet (plan §7); silently skipping or zeroing a null position
    /// would be a semantics decision made by accident, not by evidence.
    Nullable,
}

impl fmt::Display for SimdAddError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Nullable => write!(f, "nullable input is not supported by this kernel"),
        }
    }
}

impl std::error::Error for SimdAddError {}

fn reject_nulls(array: &Int32Array) -> Result<(), SimdAddError> {
    if array.null_count() > 0 {
        return Err(SimdAddError::Nullable);
    }
    Ok(())
}

/// The simplest correct implementation: a scalar loop, `wrapping_add` per
/// element. This is the oracle the other two implementations are checked
/// against — not a baseline to beat.
///
/// # Errors
///
/// Returns [`SimdAddError::Nullable`] if `array` has any null value.
pub fn wrapping_add_reference(array: &Int32Array, k: i32) -> Result<Int32Array, SimdAddError> {
    reject_nulls(array)?;
    let out: Vec<i32> = array.values().iter().map(|&x| x.wrapping_add(k)).collect();
    Ok(Int32Array::from(out))
}

/// Ordinary optimized Rust: an iterator chain over the contiguous slice
/// `array.values()` already exposes. Whether LLVM turns this into SIMD
/// instructions is an empirical question answered by inspecting the
/// compiled assembly (`just` target / CI job), not claimed here.
///
/// # Errors
///
/// Returns [`SimdAddError::Nullable`] if `array` has any null value.
pub fn wrapping_add_scalar(array: &Int32Array, k: i32) -> Result<Int32Array, SimdAddError> {
    reject_nulls(array)?;
    let values = array.values();
    let mut out = Vec::with_capacity(values.len());
    out.extend(values.iter().map(|&x| x.wrapping_add(k)));
    Ok(Int32Array::from(out))
}

/// Explicit SIMD: dispatches to AVX2 if the running CPU supports it (checked
/// once per call via `is_x86_feature_detected!`, not assumed from the
/// compile target), otherwise SSE2 (the x86_64 baseline — every x86_64 CPU
/// has it, no runtime check needed), otherwise the portable scalar fallback
/// on non-x86_64 targets.
///
/// # Errors
///
/// Returns [`SimdAddError::Nullable`] if `array` has any null value.
pub fn wrapping_add_simd(array: &Int32Array, k: i32) -> Result<Int32Array, SimdAddError> {
    reject_nulls(array)?;
    let values = array.values();
    let mut out = vec![0i32; values.len()];

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            // SAFETY: AVX2 support was just confirmed on this CPU via
            // `is_x86_feature_detected!`, which is the documented
            // precondition for calling an `avx2`-gated function.
            unsafe { add_avx2(values, &mut out, k) };
        } else {
            // SSE2 is part of the x86_64 baseline ISA (every x86_64 CPU has
            // it), so no runtime feature check is needed before calling it.
            // SAFETY: SSE2 is guaranteed present on this target architecture.
            unsafe { add_sse2(values, &mut out, k) };
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        // No explicit SIMD implementation exists for this target yet. This
        // is the plan's required "correct fallback for unsupported
        // targets" — scalar, not a silent wrong answer.
        for (dst, &src) in out.iter_mut().zip(values.iter()) {
            *dst = src.wrapping_add(k);
        }
    }

    Ok(Int32Array::from(out))
}

/// 4 lanes per instruction (128-bit / 32-bit). Every load and store is
/// unaligned (`loadu`/`storeu`): Arrow's buffer offset after a slice is not
/// guaranteed to land on a 16-byte boundary, and using an aligned intrinsic
/// on unaligned data is undefined behavior, not just a missed optimization.
///
/// # Safety
///
/// The caller must have confirmed SSE2 is available on the running CPU
/// (true for all x86_64 targets) before calling this function, and `dst`
/// and `src` must have equal length.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn add_sse2(src: &[i32], dst: &mut [i32], k: i32) {
    use std::arch::x86_64::{_mm_add_epi32, _mm_loadu_si128, _mm_set1_epi32, _mm_storeu_si128};

    debug_assert_eq!(src.len(), dst.len());
    const LANES: usize = 4;
    let len = src.len();
    let chunks = len / LANES;

    // `_mm_set1_epi32` only broadcasts `k` into a register; it touches no
    // memory, so current stable Rust classifies it as a safe intrinsic to
    // call directly (unlike `_mm_loadu_si128`/`_mm_storeu_si128` below,
    // which still require `unsafe` because they dereference a pointer).
    let k_vec = _mm_set1_epi32(k);
    for i in 0..chunks {
        let offset = i * LANES;
        // SAFETY: `offset + LANES <= len` for every `i < chunks` by
        // construction (`chunks = len / LANES`), so both the 16-byte load
        // from `src` and the 16-byte store into `dst` stay within each
        // slice's logical bounds. Pointers are not required to be aligned
        // because these are the `u`-suffixed (unaligned) intrinsics.
        unsafe {
            let src_ptr = src.as_ptr().add(offset).cast();
            let dst_ptr = dst.as_mut_ptr().add(offset).cast();
            let v = _mm_loadu_si128(src_ptr);
            let sum = _mm_add_epi32(v, k_vec);
            _mm_storeu_si128(dst_ptr, sum);
        }
    }

    // Tail: 0..3 remaining elements that don't fill a full vector. Plain
    // scalar `wrapping_add`, identical semantics to the SIMD lanes above.
    for (d, s) in dst.iter_mut().zip(src.iter()).skip(chunks * LANES) {
        *d = s.wrapping_add(k);
    }
}

/// 8 lanes per instruction (256-bit / 32-bit). Same unaligned-load
/// requirement and tail handling as [`add_sse2`], doubled in width.
///
/// # Safety
///
/// The caller must have confirmed AVX2 is available on the running CPU
/// before calling this function, and `dst` and `src` must have equal
/// length.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn add_avx2(src: &[i32], dst: &mut [i32], k: i32) {
    use std::arch::x86_64::{
        _mm256_add_epi32, _mm256_loadu_si256, _mm256_set1_epi32, _mm256_storeu_si256,
    };

    debug_assert_eq!(src.len(), dst.len());
    const LANES: usize = 8;
    let len = src.len();
    let chunks = len / LANES;

    // See `add_sse2`: `_mm256_set1_epi32` touches no memory, so current
    // stable Rust classifies it as safe to call directly.
    let k_vec = _mm256_set1_epi32(k);
    for i in 0..chunks {
        let offset = i * LANES;
        // SAFETY: see `add_sse2` — `offset + LANES <= len` by construction,
        // and `u`-suffixed intrinsics do not require alignment.
        unsafe {
            let src_ptr = src.as_ptr().add(offset).cast();
            let dst_ptr = dst.as_mut_ptr().add(offset).cast();
            let v = _mm256_loadu_si256(src_ptr);
            let sum = _mm256_add_epi32(v, k_vec);
            _mm256_storeu_si256(dst_ptr, sum);
        }
    }

    for (d, s) in dst.iter_mut().zip(src.iter()).skip(chunks * LANES) {
        *d = s.wrapping_add(k);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Builder;

    const K: i32 = 7;

    /// All three implementations must agree, across every boundary-length
    /// case the plan requires: empty, shorter than one vector, exactly one
    /// vector, a non-multiple of the vector width, and a visible Arrow
    /// slice (which moves the logical start away from the buffer's own
    /// allocation boundary, the case most likely to break an unaligned-load
    /// assumption if it were ever wrong).
    fn assert_all_implementations_agree(values: &[i32]) {
        let array = Int32Array::from(values.to_vec());
        let reference = wrapping_add_reference(&array, K).expect("no nulls");
        let scalar = wrapping_add_scalar(&array, K).expect("no nulls");
        let simd = wrapping_add_simd(&array, K).expect("no nulls");
        assert_eq!(reference.values(), scalar.values(), "scalar vs reference");
        assert_eq!(reference.values(), simd.values(), "simd vs reference");
        for (i, &v) in values.iter().enumerate() {
            assert_eq!(reference.values()[i], v.wrapping_add(K));
        }
    }

    #[test]
    fn empty_input() {
        assert_all_implementations_agree(&[]);
    }

    #[test]
    fn shorter_than_one_sse2_vector() {
        assert_all_implementations_agree(&[1, 2, 3]);
    }

    #[test]
    fn exact_sse2_vector_width() {
        assert_all_implementations_agree(&[1, 2, 3, 4]);
    }

    #[test]
    fn exact_avx2_vector_width() {
        assert_all_implementations_agree(&[1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn non_multiple_of_vector_width() {
        // 19 = 2 AVX2 vectors (16) + 1 SSE2 vector worth of remainder (4)
        // handled as plain scalar (3) in add_avx2's own tail — exercises
        // both SIMD widths' tail paths in one input.
        let values: Vec<i32> = (0..19).collect();
        assert_all_implementations_agree(&values);
    }

    #[test]
    fn large_input_many_vectors() {
        let values: Vec<i32> = (0..10_000).map(|i| i - 5_000).collect();
        assert_all_implementations_agree(&values);
    }

    #[test]
    fn wrapping_at_i32_boundary() {
        // The whole point of "wrapping" semantics: this must not panic in
        // debug builds and must match `i32::wrapping_add`'s defined result.
        assert_all_implementations_agree(&[i32::MAX, i32::MAX - 1, i32::MIN, i32::MIN + 1]);
    }

    #[test]
    fn a_visible_arrow_slice_is_handled_without_assuming_alignment() {
        // slice(3, _) moves the logical start 3 elements into the backing
        // buffer — on a typical allocator this is very likely not 16- or
        // 32-byte aligned, which is exactly the case `loadu`/`storeu`
        // exist for. An aligned load here would be the realistic way this
        // kernel could silently misbehave outside the test suite.
        let full = Int32Array::from((0..37).collect::<Vec<i32>>());
        let sliced = full.slice(3, 23);
        let sliced = sliced
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32Array");

        let reference = wrapping_add_reference(sliced, K).expect("no nulls");
        let simd = wrapping_add_simd(sliced, K).expect("no nulls");
        assert_eq!(reference.values(), simd.values());
        assert_eq!(sliced.values().len(), 23);
    }

    #[test]
    fn nullable_input_is_rejected_explicitly_by_every_implementation() {
        let mut builder = Int32Builder::new();
        builder.append_value(1);
        builder.append_null();
        builder.append_value(3);
        let array = builder.finish();

        assert_eq!(
            wrapping_add_reference(&array, K),
            Err(SimdAddError::Nullable)
        );
        assert_eq!(wrapping_add_scalar(&array, K), Err(SimdAddError::Nullable));
        assert_eq!(wrapping_add_simd(&array, K), Err(SimdAddError::Nullable));
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn sse2_and_avx2_paths_individually_match_the_reference() {
        // wrapping_add_simd only exercises whichever path the CPU running
        // the test actually supports. Call both unsafe functions directly
        // so CI (which may or may not have AVX2) still proves both, not
        // just whichever one is_x86_feature_detected! picked today.
        let values: Vec<i32> = (0..19).collect();
        let reference = wrapping_add_reference(&Int32Array::from(values.clone()), K)
            .expect("no nulls")
            .values()
            .to_vec();

        let mut sse2_out = vec![0i32; values.len()];
        // SAFETY: SSE2 is guaranteed present on every x86_64 target.
        unsafe { add_sse2(&values, &mut sse2_out, K) };
        assert_eq!(sse2_out, reference, "sse2 path");

        if is_x86_feature_detected!("avx2") {
            let mut avx2_out = vec![0i32; values.len()];
            // SAFETY: AVX2 support confirmed on this CPU above.
            unsafe { add_avx2(&values, &mut avx2_out, K) };
            assert_eq!(avx2_out, reference, "avx2 path");
        }
    }
}
