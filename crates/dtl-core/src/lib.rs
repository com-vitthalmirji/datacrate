//! Hands-on Rust fundamentals, organized by topic.
//!
//! - [`ownership`] — moves, clones, and `Copy`.
//! - [`borrowing`] — shared vs. exclusive borrows.
//! - [`references`] — reference validity and mutation.
//! - [`slices`] — borrowing a contiguous part of a collection.
//! - [`csv_zero_copy`] — a lifetime-bound, zero-copy CSV row splitter.

#![warn(missing_docs)]

pub mod borrowing;
pub mod csv_zero_copy;
pub mod ownership;
pub mod references;
pub mod slices;
