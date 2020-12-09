use std::collections::VecDeque;
use std::iter::Map;

use crate::objects::*;
use crate::data::*;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

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


pub trait ItemWithoutData where Self: Sized {
    fn attach_data<'a>(self, data: &'a Database) -> ItemWithData<'a, Self>;
}
impl<T> ItemWithoutData for T where T: Sized {
    fn attach_data<'a>(self, data: &'a Database) -> ItemWithData<'a, Self> {
        ItemWithData { data, item: self }
    }
}

pub trait OptionWithoutData<T> where T: ItemWithoutData {
    fn attach_data_to_inner<'a>(self, data: &'a Database) -> Option<ItemWithData<'a, T>>;
}
impl<T> OptionWithoutData<T> for Option<T> where T: ItemWithoutData {
    fn attach_data_to_inner<'a>(self, data: &'a Database) -> Option<ItemWithData<'a, T>> {
        self.map(|inner| inner.attach_data(data) )
    }
}

pub struct ItemWithData<'a, T> { pub data: &'a Database, pub item: T }
impl<'a, T> ItemWithData<'a, T> {
    pub fn new(data: &'a Database, item: T) -> Self {
        ItemWithData { data, item }
    }
    //pub fn as_ref(&self) -> ItemWithData<&T> { Self::new(self.data, self.item) }
    pub fn rewrap<Tb>(&self, object: Tb) -> ItemWithData<Tb> {
        ItemWithData::<Tb>::new(self.data, object)
    }
}
impl<'a, T> Clone for ItemWithData<'a, T> where T: Clone {
    fn clone(&self) -> Self {
        ItemWithData::new(self.data, self.item.clone())
    }
}

impl<'a, T> PartialEq for ItemWithData<'a, T> where T: PartialEq {
    fn eq(&self, other: &Self) -> bool {
        self.item.eq(&other.item)
    }
}

impl<'a, T> Eq for ItemWithData<'a, T> where ItemWithData<'a, T>: PartialEq, T: Eq {}

impl<'a, T> PartialOrd for ItemWithData<'a, T> where T: PartialOrd {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.item.partial_cmp(&other.item)
    }
}

impl<'a, T> Ord for ItemWithData<'a, T> where T: Ord, ItemWithData<'a, T>: Eq {
    fn cmp(&self, other: &Self) -> Ordering {
        self.item.cmp(&other.item)
    }
}

impl<'a, T> Hash for ItemWithData<'a, T> where T: Hash {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.item.hash(state)
    }
}

impl<'a> Into<Project> for ItemWithData<'a, Project> { fn into(self) -> Project { self.item } }
impl<'a> Into<Commit> for ItemWithData<'a, Commit> { fn into(self) -> Commit { self.item } }
impl<'a> Into<User> for ItemWithData<'a, User> { fn into(self) -> User { self.item } }
impl<'a> Into<Path> for ItemWithData<'a, Path> { fn into(self) -> Path { self.item } }
impl<'a> Into<Snapshot> for ItemWithData<'a, Snapshot> { fn into(self) -> Snapshot { self.item } }
impl<'a> Into<Head> for ItemWithData<'a, Head> { fn into(self) -> Head { self.item } }

impl<'a> Into<ProjectId> for ItemWithData<'a, ProjectId> { fn into(self) -> ProjectId { self.item } }
impl<'a> Into<CommitId> for ItemWithData<'a, CommitId> { fn into(self) -> CommitId { self.item } }
impl<'a> Into<UserId> for ItemWithData<'a, UserId> { fn into(self) -> UserId { self.item } }
impl<'a> Into<PathId> for ItemWithData<'a, PathId> { fn into(self) -> PathId { self.item } }
impl<'a> Into<SnapshotId> for ItemWithData<'a, SnapshotId> { fn into(self) -> SnapshotId { self.item } }

impl<'a> Into<String> for ItemWithData<'a, String> { fn into(self) -> String { self.item } }
impl<'a> Into<u64> for ItemWithData<'a, u64> { fn into(self) -> u64 { self.item } }
impl<'a> Into<u32> for ItemWithData<'a, u32> { fn into(self) -> u32 { self.item } }
impl<'a> Into<i64> for ItemWithData<'a, i64> { fn into(self) -> i64 { self.item } }
impl<'a> Into<i32> for ItemWithData<'a, i32> { fn into(self) -> i32 { self.item } }
impl<'a> Into<f64> for ItemWithData<'a, f64> { fn into(self) -> f64 { self.item } }
impl<'a> Into<f32> for ItemWithData<'a, f32> { fn into(self) -> f32 { self.item } }
impl<'a> Into<usize> for ItemWithData<'a, usize> { fn into(self) -> usize { self.item } }

impl<'a,A,B> Into<(A,B)> for ItemWithData<'a, (A,B)> { fn into(self) -> (A,B) { self.item } }

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
