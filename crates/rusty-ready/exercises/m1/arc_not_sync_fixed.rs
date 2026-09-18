use std::sync::{Arc, Mutex};
use std::thread;

fn main() {
    let counter = Arc::new(Mutex::new(0));
    let thread_counter = Arc::clone(&counter);

    let handle = thread::spawn(move || {
        *thread_counter.lock().unwrap() += 1;
    });

    handle.join().unwrap();
    println!("counter: {}", *counter.lock().unwrap());
}
