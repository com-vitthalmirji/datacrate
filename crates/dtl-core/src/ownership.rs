//! Ownership determines which binding is responsible for a value.
//!
//! Moving a `String` transfers ownership and invalidates the old binding.
//! Cloning a `String` explicitly duplicates its heap data. Types implementing
//! `Copy`, such as `i32`, remain usable after assignment.

/// Consumes a `String` and transfers ownership back to the caller.
pub fn takes_and_gives_back(value: String) -> String {
    value
}

/// Creates an owned `String` and an explicit deep clone of it.
pub fn clone_owned(value: &str) -> (String, String) {
    let original = value.to_owned();
    let cloned = original.clone();
    (original, cloned)
}

/// Demonstrates assignment of a value implementing `Copy`.
pub fn copy_integer(value: i32) -> (i32, i32) {
    let copied = value;
    (value, copied)
}
