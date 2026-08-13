use std::cell::RefCell;
use std::rc::Rc;
use std::thread;

fn main() {
    let counter = Rc::new(RefCell::new(0));

    let handle = thread::spawn(move || {
        *counter.borrow_mut() += 1;
    });

    handle.join().unwrap();
}
