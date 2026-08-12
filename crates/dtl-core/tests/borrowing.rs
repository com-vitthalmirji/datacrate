use dtl_core::borrowing;

#[test]
fn observes_without_taking_ownership() {
    let value = String::from("hello");

    let length = borrowing::calculate_length(&value);

    assert_eq!(length, 5);
    assert_eq!(value, "hello");
}

#[test]
fn mutates_through_an_exclusive_borrow() {
    let mut value = String::from("hello");

    borrowing::append_suffix(&mut value, ", world!");

    assert_eq!(value, "hello, world!");
}
