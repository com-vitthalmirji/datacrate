use dtl_core::ownership;

#[test]
fn transfers_ownership_back_to_the_caller() {
    let value = String::from("hello");

    let returned = ownership::takes_and_gives_back(value);

    assert_eq!(returned, "hello");
}

#[test]
fn clones_owned_text_explicitly() {
    let (mut original, cloned) = ownership::clone_owned("hello");

    original.push_str(", world!");

    assert_eq!(original, "hello, world!");
    assert_eq!(cloned, "hello");
}

#[test]
fn copies_stack_only_integer_values() {
    assert_eq!(ownership::copy_integer(5), (5, 5));
}
