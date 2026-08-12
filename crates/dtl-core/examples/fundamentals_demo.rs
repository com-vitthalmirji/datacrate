use dtl_core::{borrowing, ownership, references, slices};

fn main() {
    println!("== Ownership ==");
    let owned = String::from("hello");
    let owned = ownership::takes_and_gives_back(owned);
    let (original, cloned) = ownership::clone_owned(&owned);
    let (first_number, copied_number) = ownership::copy_integer(5);
    println!("returned: {owned}");
    println!("original: {original}, clone: {cloned}");
    println!("copy values: {first_number}, {copied_number}");

    println!("\n== Borrowing ==");
    let mut borrowed = String::from("hello");
    let length = borrowing::calculate_length(&borrowed);
    borrowing::append_suffix(&mut borrowed, ", world!");
    println!("length before mutation: {length}");
    println!("after mutable borrow: {borrowed}");

    println!("\n== References ==");
    let (view, view_length) = references::inspect(&borrowed);
    println!("shared reference: {view} ({view_length} bytes)");
    references::overwrite(&mut borrowed, "references stay valid");
    println!("after exclusive reference: {borrowed}");

    println!("\n== Slices ==");
    let sentence = String::from("hello Rust");
    let word = slices::first_word(&sentence);
    let numbers = [1, 2, 3, 4];
    println!("first word: {word}");
    println!("number prefix: {:?}", slices::number_prefix(&numbers, 2));
}
