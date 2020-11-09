use std::rc::Rc;
use std::cell::RefCell;
use crate::data::{Database};
use std::marker::PhantomData;
use std::collections::VecDeque;
use crate::objects::{Project, ProjectId, UserId, User, PathId, SnapshotId, Snapshot, CommitId, Commit, Path, Identifiable};
//use std::iter::FromIterator;

//pub type DataPtr = Rc<RefCell<Data>>;
pub type DatabasePtr = Rc<RefCell<Database>>;

pub struct IterWithData<'a, T, I: Iterator<Item=T> + 'a> { data: &'a Database, iterator: I/*, _t: PhantomData<&'a T>*/ }

impl<'a, T, I> IterWithData<'a, T, I> where I: Iterator<Item=T> + 'a {
    // pub fn new(data: &'a Datax, iterator: I) -> Self {
    //     IterWithData { data, iterator }
    // }
    // pub fn from_generator<F>(data: &'a Datax, generator: F) -> Self where F: FnMut(&'a Datax) -> I  {
    //     let mut me = IterWithData { data, iterator: None };
    //     me.iterator = generator(data);
    //     me
    // }
}

impl<'a, T, I> Iterator for IterWithData<'a, T, I> where I: Iterator<Item=T> {
    type Item = ItemWithData<'a, T>;
    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.next().map(|e| ItemWithData::new(&self.data, e))
    }
}

pub struct ItemWithData<'a, T> { pub data: &'a Database, pub element: T }
impl<'a, T> ItemWithData<'a, T> {
    pub fn new(data: &'a Database, element: T) -> Self {
        ItemWithData { data, element }
    }
}

//------------------------------------------------------------------------------------------------//

pub struct IterWithDatabasePtr<T, I: Iterator<Item=T>> { data_ptr: DatabasePtr, iterator: I }
impl<T, I> IterWithDatabasePtr<T, I> where I: Iterator<Item=T> {
    // pub fn new__<F>(data_ptr: DataPtr, generator: F) -> Self where F: FnMut(DataPtr) -> I {
    //      //let data_ptr = Rc::new(RefCell::new(data));
    //      IterWithData { data_ptr, iterator: generator(data_ptr.clone()) }
    // }
    // pub fn new(data: Data, iterator: I) -> Self {
    //     let data_ptr = Rc::new(RefCell::new(data));
    //     IterWithData { data_ptr, iterator }
    // }
    //
    // pub fn new_(data_ptr: DataPtr, iterator: I) -> Self {
    //     //let data_ptr = Rc::new(RefCell::new(data));
    //     IterWithData { data_ptr, iterator }
    // }
}
impl<T, I> Iterator for IterWithDatabasePtr<T, I> where I: Iterator<Item=T> {
    type Item = ItemWithDatabasePtr<T>;
    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.next().map(|e| ItemWithDatabasePtr::new(&self.data_ptr, e))
    }
}

pub struct ItemWithDatabasePtr<T> { pub data: DatabasePtr, pub element: T }
impl<T> ItemWithDatabasePtr<T> {
    pub fn new(data: &DatabasePtr, element: T) -> Self {
        ItemWithDatabasePtr { data: data.clone(), element }
    }
}

// impl<T> From<ItemWithDatabasePtr<T>> for T {
//     fn from(_: ItemWithDatabasePtr<T>) -> Self {
//         unimplemented!()
//     }
// }

impl Into<Project> for ItemWithDatabasePtr<Project> { fn into(self) -> Project { self.element } }
impl Into<Commit> for ItemWithDatabasePtr<Commit> { fn into(self) -> Commit { self.element } }
impl Into<User> for ItemWithDatabasePtr<User> { fn into(self) -> User { self.element } }
impl Into<Path> for ItemWithDatabasePtr<Path> { fn into(self) -> Path { self.element } }
impl Into<Snapshot> for ItemWithDatabasePtr<Snapshot> { fn into(self) -> Snapshot { self.element } }

impl Into<ProjectId> for ItemWithDatabasePtr<ProjectId> { fn into(self) -> ProjectId { self.element } }
impl Into<CommitId> for ItemWithDatabasePtr<CommitId> { fn into(self) -> CommitId { self.element } }
impl Into<UserId> for ItemWithDatabasePtr<UserId> { fn into(self) -> UserId { self.element } }
impl Into<PathId> for ItemWithDatabasePtr<PathId> { fn into(self) -> PathId { self.element } }
impl Into<SnapshotId> for ItemWithDatabasePtr<SnapshotId> { fn into(self) -> SnapshotId { self.element } }

impl Into<String> for ItemWithDatabasePtr<String> { fn into(self) -> String { self.element } }
impl Into<u64> for ItemWithDatabasePtr<u64> { fn into(self) -> u64 { self.element } }
impl Into<u32> for ItemWithDatabasePtr<u32> { fn into(self) -> u32 { self.element } }
impl Into<i64> for ItemWithDatabasePtr<i64> { fn into(self) -> i64 { self.element } }
impl Into<i32> for ItemWithDatabasePtr<i32> { fn into(self) -> i32 { self.element } }
impl Into<f64> for ItemWithDatabasePtr<f64> { fn into(self) -> f64 { self.element } }
impl Into<f32> for ItemWithDatabasePtr<f32> { fn into(self) -> f32 { self.element } }
impl Into<usize> for ItemWithDatabasePtr<usize> { fn into(self) -> usize { self.element } }

impl<A,B> Into<(A,B)> for ItemWithDatabasePtr<(A,B)> { fn into(self) -> (A,B) { self.element } }

// ---------------------------------------------------------------------------------------------- //

pub struct QuincunxIter<'a, T: Identifiable> {
    data: &'a Database,
    ids: VecDeque<T::Identity>,
    _type: PhantomData<T>,
}

impl<'a> QuincunxIter<'a, Project> {
    pub fn new(data: &'a Database) -> Self {
        QuincunxIter { data, ids: VecDeque::from(data.all_project_ids()), _type: PhantomData }
    }
}

impl<'a> QuincunxIter<'a, Commit> {
    pub fn new(data: &'a Database) -> Self {
        QuincunxIter { data, ids: VecDeque::from(data.all_commit_ids()), _type: PhantomData }
    }
}

impl<'a> QuincunxIter<'a, User> {
    pub fn new(data: &'a Database) -> Self {
        QuincunxIter { data, ids: VecDeque::from(data.all_user_ids()), _type: PhantomData }
    }
}

impl<'a> QuincunxIter<'a, Snapshot> {
    pub fn new(data: &'a Database) -> Self {
        QuincunxIter { data, ids: VecDeque::from(data.all_snapshot_ids()), _type: PhantomData }
    }
}

impl<'a> QuincunxIter<'a, Path> {
    pub fn new(data: &'a Database) -> Self {
        QuincunxIter { data, ids: VecDeque::from(data.all_path_ids()), _type: PhantomData }
    }
}

impl<'a> QuincunxIter<'a, Project> {
    fn reify(&'a self, id: &ProjectId) -> Option<Project> { self.data.project(id) }
}

impl<'a> QuincunxIter<'a, Commit> {
    fn reify(&'a self, id: &CommitId) -> Option<Commit> { self.data.commit(id) }
}

impl<'a> QuincunxIter<'a, User> {
    fn reify(&'a self, id: &UserId) -> Option<User> { self.data.user(id) }
}

impl<'a> QuincunxIter<'a, Path> {
    fn reify(&'a self, id: &PathId) -> Option<Path> { self.data.path(id) }
}

impl<'a> QuincunxIter<'a, Snapshot> {
    fn reify(&'a self, id: &SnapshotId) -> Option<Snapshot> { self.data.snapshot(id) }
}

macro_rules! get_next {
    ($self:expr) => {{
        loop {
            if $self.ids.is_empty() {
                return None
            }

            let id = $self.ids.pop_front().unwrap();
            let element = $self.reify(&id);

            if element.is_some() {
                return Some(ItemWithData::new(&$self.data, element.unwrap()));
            }
        }
    }}
}

impl<'a> Iterator for QuincunxIter<'a, Project> { // Ideally, make generic
    type Item = ItemWithData<'a, Project>;
    fn next(&mut self) -> Option<Self::Item> {
        get_next!(self)
    }
}

impl<'a> Iterator for QuincunxIter<'a, User> { // Ideally, make generic
type Item = ItemWithData<'a, User>;
    fn next(&mut self) -> Option<Self::Item> {
        get_next!(self)
    }
}

impl<'a> Iterator for QuincunxIter<'a, Commit> { // Ideally, make generic
type Item = ItemWithData<'a, Commit>;
    fn next(&mut self) -> Option<Self::Item> {
        get_next!(self)
    }
}

impl<'a> Iterator for QuincunxIter<'a, Path> { // Ideally, make generic
type Item = ItemWithData<'a, Path>;
    fn next(&mut self) -> Option<Self::Item> {
        get_next!(self)
    }
}

impl<'a> Iterator for QuincunxIter<'a, Snapshot> { // Ideally, make generic
type Item = ItemWithData<'a, Snapshot>;
    fn next(&mut self) -> Option<Self::Item> {
        get_next!(self)
    }
}

pub trait DropKey<K, V> {
    type Iterator;
    fn drop_key(self) -> std::iter::Map<Self::Iterator, Box<dyn FnMut((K, V)) -> V>>;
}

// pub struct KeyDropper;
// impl<K,V> Fn((K, V)) -> V for KeyDropper {
//     fn call(&self, args: ((K, V))) -> Self::Output { args.1 }
// }

impl<K,V,I> DropKey<K,V> for I where I: Iterator<Item=(K, V)> {
    type Iterator = I;
    fn drop_key(self) -> std::iter::Map<Self::Iterator, Box<dyn FnMut((K, V)) -> V>> {
        self.map(Box::new(|(_,b)| b))
    }
}
