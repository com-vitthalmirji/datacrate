use dtl_core::slices;

#[test]
fn returns_the_first_word() {
    assert_eq!(slices::first_word("hello world"), "hello");
}

#[test]
fn returns_the_whole_input_without_a_space() {
    assert_eq!(slices::first_word("hello"), "hello");
}

#[test]
fn handles_empty_and_unicode_input() {
    assert_eq!(slices::first_word(""), "");
    assert_eq!(slices::first_word("नमस्ते Rust"), "नमस्ते");
}

#[test]
fn returns_a_bounded_number_prefix() {
    let values = [1, 2, 3];

    assert_eq!(slices::number_prefix(&values, 2), &[1, 2]);
    assert_eq!(slices::number_prefix(&values, 10), &[1, 2, 3]);
}
