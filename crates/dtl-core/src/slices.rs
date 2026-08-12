//! Slices borrow a contiguous part of a collection without taking ownership.

/// Returns the text before the first ASCII space, or the whole input.
pub fn first_word(value: &str) -> &str {
    for (index, byte) in value.bytes().enumerate() {
        if byte == b' ' {
            return &value[..index];
        }
    }

    value
}

/// Returns at most `length` values from the beginning of a number slice.
pub fn number_prefix(values: &[i32], length: usize) -> &[i32] {
    &values[..length.min(values.len())]
}
