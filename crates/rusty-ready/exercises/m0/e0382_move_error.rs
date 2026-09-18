fn consume(value: String) -> String {
    value
}

fn main() {
    let value = String::from("ownership");
    consume(value);
    println!("after move: {value}");
}
