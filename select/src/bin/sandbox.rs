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

struct IteratorHolder<I: Iterator<Item = u32>> {
    iter: I,
}

impl<I> Iterator for IteratorHolder<I> where I: Iterator<Item=u32> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}



fn main() {
    let mut closure = make_closure();
    let vec: Vec<u32> = vec![];

    IteratorHolder { iter: vec.into_iter() };

    // println!("{}", closure());
    // println!("{}", closure());
    // println!("{}", closure());
    // println!("{}", (*closure)());
    // println!("{}", (*closure)());
}