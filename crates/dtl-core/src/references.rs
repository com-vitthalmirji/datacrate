//! References are non-owning pointers that must always remain valid.
//!
//! Multiple shared references may coexist. A mutable reference is exclusive
//! for the duration in which it is used.

/// Returns a shared view tied to the input reference and its byte length.
pub fn inspect(value: &str) -> (&str, usize) {
    (value, value.len())
}

/// Replaces a string's contents through an exclusive reference.
pub fn overwrite(value: &mut String, replacement: &str) {
    value.clear();
    value.push_str(replacement);
}
