//! Borrowing lets code use a value without taking ownership of it.
//!
//! Shared borrows observe a value. An exclusive mutable borrow may change it,
//! but no other reference may use the value during that mutable borrow.

/// Observes text through a shared borrow.
pub fn calculate_length(value: &str) -> usize {
    value.len()
}

/// Mutates an owned string through an exclusive borrow.
pub fn append_suffix(value: &mut String, suffix: &str) {
    value.push_str(suffix);
}
