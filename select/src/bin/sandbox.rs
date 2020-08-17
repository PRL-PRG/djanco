use std::cell::RefCell;

struct State {
    value: u32,
}

fn make_closure() -> Box<dyn FnMut() -> u32> {
    let mut s = 0u32; //State{ value: 0u32 };
    Box::new(move || {
        s/*.value*/ += 1;
        s/*.value*/
    })
}

fn main() {
    let mut closure = make_closure();
    println!("{}", closure());
    println!("{}", closure());
    println!("{}", closure());
    // println!("{}", (*closure)());
    // println!("{}", (*closure)());
}