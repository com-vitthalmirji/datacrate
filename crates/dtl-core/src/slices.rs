//! Slices borrow a contiguous part of a collection without taking ownership.

/// Returns the text before the first ASCII space, or the whole input.
///
/// # Examples
///
/// ```
/// use dtl_core::slices::first_word;
///
/// assert_eq!(first_word("hello world"), "hello");
/// assert_eq!(first_word("hello"), "hello");
/// ```
pub fn first_word(value: &str) -> &str {
    for (index, byte) in value.bytes().enumerate() {
        if byte == b' ' {
            return &value[..index];
        }
    }

    value
}

/// Returns at most `length` values from the beginning of a number slice.
///
/// # Examples
///
/// ```
/// use dtl_core::slices::number_prefix;
///
/// assert_eq!(number_prefix(&[1, 2, 3], 2), &[1, 2]);
/// assert_eq!(number_prefix(&[1, 2, 3], 10), &[1, 2, 3]);
/// ```
pub fn number_prefix(values: &[i32], length: usize) -> &[i32] {
    &values[..length.min(values.len())]
}
