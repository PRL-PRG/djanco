use std::rc::Rc;
use std::cell::RefCell;
use crate::data::{Data, Datax};
use std::marker::PhantomData;
use std::borrow::BorrowMut;
use std::collections::VecDeque;
use crate::objects::{Project, ProjectId, UserId, User, PathId, SnapshotId, Snapshot, CommitId, Commit, Path};

pub type DataPtr = Rc<RefCell<Data>>;
pub type DataxPtr = Rc<RefCell<Datax>>;

pub struct IterWithData<'a, T, I: Iterator<Item=T> + 'a> { data: &'a Datax, iterator: I/*, _t: PhantomData<&'a T>*/ }

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

pub struct ItemWithData<'a, T> { pub data: &'a Datax, pub element: T }
impl<'a, T> ItemWithData<'a, T> {
    pub fn new(data: &'a Datax, element: T) -> Self {
        ItemWithData { data, element }
    }
}

//------------------------------------------------------------------------------------------------//

pub struct IterWithDataPtr<T, I: Iterator<Item=T>> { data_ptr: DataPtr, iterator: I }
impl<T, I> IterWithDataPtr<T, I> where I: Iterator<Item=T> {
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
impl<T, I> Iterator for IterWithDataPtr<T, I> where I: Iterator<Item=T> {
    type Item = ItemWithDataPtr<T>;
    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.next().map(|e| ItemWithDataPtr::new(&self.data_ptr, e))
    }
}

pub struct ItemWithDataPtr<T> { pub data: DataPtr, pub element: T }
impl<T> ItemWithDataPtr<T> {
    pub fn new(data: &DataPtr, element: T) -> Self {
        ItemWithDataPtr { data: data.clone(), element }
    }
}

// ---------------------------------------------------------------------------------------------- //


struct QuincunxIter<'a, K, V> {
    data: &'a Datax,
    ids: VecDeque<K>,
    _type: PhantomData<V>,
}

impl<'a> QuincunxIter<'a, ProjectId, Project> {
    pub fn new(data: &'a Datax) -> Self {
        QuincunxIter { data, ids: VecDeque::from(data.all_project_ids()), _type: PhantomData }
    }
}

impl<'a> QuincunxIter<'a, CommitId, Commit> {
    pub fn new(data: &'a Datax) -> Self {
        QuincunxIter { data, ids: VecDeque::from(data.all_commit_ids()), _type: PhantomData }
    }
}

impl<'a> QuincunxIter<'a, UserId, User> {
    pub fn new(data: &'a Datax) -> Self {
        QuincunxIter { data, ids: VecDeque::from(data.all_user_ids()), _type: PhantomData }
    }
}

impl<'a> QuincunxIter<'a, SnapshotId, Snapshot> {
    pub fn new(data: &'a Datax) -> Self {
        QuincunxIter { data, ids: VecDeque::from(data.all_snapshot_ids()), _type: PhantomData }
    }
}

impl<'a> QuincunxIter<'a, PathId, Path> {
    pub fn new(data: &'a Datax) -> Self {
        QuincunxIter { data, ids: VecDeque::from(data.all_path_ids()), _type: PhantomData }
    }
}

impl<'a> QuincunxIter<'a, ProjectId, Project> {
    fn reify(&'a self, id: &ProjectId) -> Option<Project> { self.data.project(id) }
}

impl<'a> QuincunxIter<'a, CommitId, Commit> {
    fn reify(&'a self, id: &CommitId) -> Option<Commit> { self.data.commit(id) }
}

impl<'a> QuincunxIter<'a, UserId, User> {
    fn reify(&'a self, id: &UserId) -> Option<User> { self.data.user(id) }
}

impl<'a> QuincunxIter<'a, PathId, Path> {
    fn reify(&'a self, id: &PathId) -> Option<Path> { self.data.path(id) }
}

impl<'a> QuincunxIter<'a, SnapshotId, Snapshot> {
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

impl<'a> Iterator for QuincunxIter<'a, ProjectId, Project> { // Ideally, make generic
    type Item = ItemWithData<'a, Project>;
    fn next(&mut self) -> Option<Self::Item> {
        get_next!(self)
    }
}

impl<'a> Iterator for QuincunxIter<'a, UserId, User> { // Ideally, make generic
type Item = ItemWithData<'a, User>;
    fn next(&mut self) -> Option<Self::Item> {
        get_next!(self)
    }
}

impl<'a> Iterator for QuincunxIter<'a, CommitId, Commit> { // Ideally, make generic
type Item = ItemWithData<'a, Commit>;
    fn next(&mut self) -> Option<Self::Item> {
        get_next!(self)
    }
}

impl<'a> Iterator for QuincunxIter<'a, PathId, Path> { // Ideally, make generic
type Item = ItemWithData<'a, Path>;
    fn next(&mut self) -> Option<Self::Item> {
        get_next!(self)
    }
}

impl<'a> Iterator for QuincunxIter<'a, SnapshotId, Snapshot> { // Ideally, make generic
type Item = ItemWithData<'a, Snapshot>;
    fn next(&mut self) -> Option<Self::Item> {
        get_next!(self)
    }
}