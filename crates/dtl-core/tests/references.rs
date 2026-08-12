use dtl_core::references;

#[test]
fn returns_a_shared_view_of_the_input() {
    let value = String::from("hello");

    let (view, length) = references::inspect(&value);

    assert_eq!(view, "hello");
    assert_eq!(length, 5);
    assert_eq!(value, "hello");
}

#[test]
fn overwrites_through_an_exclusive_reference() {
    let mut value = String::from("before");

    references::overwrite(&mut value, "after");

    assert_eq!(value, "after");
}
