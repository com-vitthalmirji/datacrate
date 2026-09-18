use std::sync::Arc;
use std::thread;

fn main() {
    let callback: Arc<dyn Fn()> = Arc::new(|| println!("called"));

    let handle = thread::spawn(move || {
        callback();
    });

    handle.join().unwrap();
}
