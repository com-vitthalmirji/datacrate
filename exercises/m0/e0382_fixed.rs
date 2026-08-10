fn consume(value: String) -> String {
    value
}

fn main() {
    let value = String::from("ownership");
    let value = consume(value);
    println!("after return: {value}");
}
