use std::cell::RefCell;
use select::query::project::GroupKey;
use dcd::{Project, Database, DCD};
use select::mockdb::MockDatabase;

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

trait Doom {
    fn iterator<'a>(d: &'a impl Database) -> IteratorHolder<'a> {
        let vec: Vec<(GroupKey,Vec<Project>)> = vec![];
        IteratorHolder { iter: Box::new(vec.into_iter()), d}
    }
}

struct IteratorHolder<'a> {
    iter: Box<dyn Iterator<Item=(GroupKey,Vec<Project>)>>,
    d: &'a dyn Database,
}

impl<'a> Iterator for IteratorHolder<'a> {
    type Item = (GroupKey,Vec<Project>);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}



fn main() {
    let mut closure = make_closure();
    let vec: Vec<(GroupKey,Vec<Project>)> = vec![];

    //IteratorHolder { iter: Box::new(vec.into_iter()), d: &() };

    // println!("{}", closure());
    // println!("{}", closure());
    // println!("{}", closure());
    // println!("{}", (*closure)());
    // println!("{}", (*closure)());
}