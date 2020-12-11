use std::collections::VecDeque;
use std::iter::Map;

use crate::objects::*;
use crate::data::*;

pub struct IterWithData<'a, T, I: Iterator<Item=T> + 'a> { data: &'a Database, iterator: I }

impl<'a, T, I> Iterator for IterWithData<'a, T, I> where I: Iterator<Item=T> {
    type Item = ItemWithData<'a, T>;
    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.next().map(|e| ItemWithData::new(&self.data, e))
    }
}

pub trait IterWithoutData<T> where Self: Iterator<Item=T> + Sized {
    fn attach_data_to_each<'a>(self, data: &'a Database) -> IterWithData<'a, T, Self>;
}
impl<I, T> IterWithoutData<T> for I where I: Iterator<Item=T> + Sized {
    fn attach_data_to_each<'a>(self, data: &'a Database) -> IterWithData<'a, T, Self> {
        IterWithData { data, iterator: self }
    }
}

pub trait CollectionWithoutData<'a> {
    type IntoCollection;
    fn attach_data_to_each(self, data: &'a Database) -> Self::IntoCollection;
}
impl<'a, T: 'a> CollectionWithoutData<'a> for Vec<T> where T: Sized {
    type IntoCollection = Vec<ItemWithData<'a, T>>;
    fn attach_data_to_each(self, data: &'a Database) -> Self::IntoCollection {
        self.into_iter().attach_data_to_each(data).collect()
    }
}
impl<'a, T: 'a> CollectionWithoutData<'a> for Option<Vec<T>> where T: Sized {
    type IntoCollection = Option<Vec<ItemWithData<'a, T>>>;
    fn attach_data_to_each(self, data: &'a Database) -> Self::IntoCollection {
        self.map(|vector| vector.into_iter().attach_data_to_each(data).collect())
    }
}

pub struct QuincunxIter<'a, T: Identifiable> {
    data: &'a Database,
    ids: VecDeque<T::Identity>
}

impl<'a> QuincunxIter<'a, Project> {
    pub fn new(data: &'a Database) -> Self {
        QuincunxIter { data, ids: VecDeque::from(data.all_project_ids()) }
    }
}

impl<'a> QuincunxIter<'a, Commit> {
    pub fn new(data: &'a Database) -> Self {
        QuincunxIter { data, ids: VecDeque::from(data.all_commit_ids()) }
    }
}

impl<'a> QuincunxIter<'a, User> {
    pub fn new(data: &'a Database) -> Self {
        QuincunxIter { data, ids: VecDeque::from(data.all_user_ids()) }
    }
}

impl<'a> QuincunxIter<'a, Path> {
    pub fn new(data: &'a Database) -> Self {
        QuincunxIter { data, ids: VecDeque::from(data.all_path_ids()), }
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

impl<K,V,I> DropKey<K,V> for I where I: Iterator<Item=(K, V)> {
    type Iterator = I;
    fn drop_key(self) -> std::iter::Map<Self::Iterator, Box<dyn FnMut((K, V)) -> V>> {
        self.map(Box::new(|(_,b)| b))
    }
}

pub trait DropData<'a, T> {
    type Iterator;
    fn drop_database(self) -> std::iter::Map<Self::Iterator, Box<dyn FnMut(ItemWithData<'a, T>) -> T>>;
}

impl<'a,T,I> DropData<'a, T> for I where I: Iterator<Item=ItemWithData<'a, T>> {
    type Iterator = I;
    fn drop_database(self) -> Map<Self::Iterator, Box<dyn FnMut(ItemWithData<'a, T>) -> T>> {
        self.map(Box::new(|ItemWithData{ item, data: _ }| item))
    }
}
