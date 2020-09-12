use std::path::PathBuf;
use crate::objects::*;
use crate::data::*;
use crate::log::LogLevel;
use crate::attrib::{LoadFilter, Group, FilterEach, Filter};
use std::marker::PhantomData;
use std::cell::RefCell;
use itertools::Itertools;
use std::collections::HashMap;
use std::hash::Hash;

#[derive(Clone)]
pub struct Spec {
    pub path: PathBuf,
    pub seed: u128,
    pub timestamp: Month,
    pub log_level: LogLevel,
}

impl Spec {
    pub fn new<S: Into<String>>(path: S, seed: u128, timestamp: Month, log_level: LogLevel) -> Self {
        Spec { path: PathBuf::from(path.into()), seed, timestamp, log_level }
    }
}

/** Pre-load operations **/
pub struct Lazy {
    pub spec: Spec,
    pub(crate) filters: Vec<Box<dyn LoadFilter + 'static>>,
}

impl From<Spec> for Lazy {
    fn from(spec: Spec) -> Self { Lazy { spec, filters: vec![] } }
}

impl From<&Spec> for Lazy {
    fn from(spec: &Spec) -> Self { Lazy { spec: spec.clone(), filters: vec![] } }
}

impl /* LoadFiltering for */ Lazy {
    pub fn with_filter<F>(mut self, filter: F) -> Self where F: LoadFilter + 'static {
        self.filters.push(Box::new(filter)); self
    }
}

impl /* VerbositySetting for */ Lazy {
    pub fn with_log_level(mut self, log_level: LogLevel) -> Self {
        self.spec.log_level = log_level; self
    }
}

impl /* Quincunx for */ Lazy {
    pub fn projects(self ) -> QuincunxIter<Project> { QuincunxIter::from(self) }
    pub fn commits(self)   -> QuincunxIter<Commit>  { QuincunxIter::from(self) }
    pub fn users(self)     -> QuincunxIter<User>    { QuincunxIter::from(self) }
    pub fn paths(self)     -> QuincunxIter<Path>    { QuincunxIter::from(self) }
    //pub fn snapshots(self) -> Loaded<Snapshot> { Loaded::from(self) }
    // TODO the remainder
}

/** A single strand out of the five main strands in the database **/
pub struct QuincunxIter<T> {
    spec: Spec,
    data: DataPtr,
    sourcex: Option<Vec<T>>, // Serves the iterator: None -> n elements -> ... -> 0 elements
}

impl<T> /* LazyLoad for */ QuincunxIter<T> where T: Quincunx {
    fn borrow_source(&mut self) -> &mut Vec<T> {
        if self.sourcex.is_none() {
            self.sourcex = Some(T::stream_from(&self.data))
        }
        self.sourcex.as_mut().unwrap()
    }
    fn consume_source(mut self) -> Vec<T> {
        if self.sourcex.is_none() {
            self.sourcex = Some(T::stream_from(&self.data))
        }
        self.sourcex.unwrap()
    }
}

impl<T> WithData for QuincunxIter<T> {
    fn get_database_ptr(&self) -> DataPtr {
        self.data.clone()
    }
}

impl<T> Iterator for QuincunxIter<T> where T: Quincunx {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self.borrow_source().pop()
    }
}

impl<T> /* Query for */ QuincunxIter<T> where T: Quincunx {
    pub fn group_by_attrib<K, G>(mut self, mut attrib: G) -> GroupIter<K, T> where G: Group<T, Key=K>, K: Hash + Eq {
        let data = self.data.clone();
        let spec = self.spec.clone();
        let source = attrib.execute(self.data.clone(), self.consume_source());
        GroupIter { spec, data, source }
    }

    pub fn filter<F>(mut self, mut attrib: F) -> Iter<T> where F: Filter<T> {
        let data = self.data.clone();
        let spec = self.spec.clone();
        let source: Vec<T> = attrib.execute(self.data.clone(), self.consume_source());
        Iter { spec, data, source }
    }
}

impl<T> From<Spec> for QuincunxIter<T> {
    fn from(_spec: Spec) -> Self { unimplemented!() }
}

impl<T> From<&Spec> for QuincunxIter<T> {
    fn from(_spec: &Spec) -> Self { unimplemented!() }
}

impl<T> From<Lazy> for QuincunxIter<T> {
    fn from(lazy: Lazy) -> Self {
        let data = DataPtr::from(&lazy);
        QuincunxIter { spec: lazy.spec, data, sourcex: None }
    }
}

/**
 * A general version of QuincunxITer that is already initialized, and therefore can contain any
 *  element type.
 **/
pub struct Iter<T> {
    spec: Spec,
    data: DataPtr,
    source: Vec<T>,
}

impl<T> WithData for Iter<T> {
    fn get_database_ptr(&self) -> DataPtr {
        self.data.clone()
    }
}

impl<T> Iterator for Iter<T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self.source.pop()
    }
}

impl<T> /* Query for */ Iter<T> {
    pub fn group_by_attrib<K, G>(self, mut attrib: G) -> GroupIter<K, T> where G: Group<T, Key=K>, K: Hash + Eq {
        let source = attrib.execute(self.data.clone(), self.source);
        GroupIter { spec: self.spec.clone(), data: self.data.clone(), source }
    }

    pub fn filter<F>(mut self, mut attrib: F) -> Iter<T> where F: Filter<T> {
        let source = attrib.execute(self.data.clone(), self.source);
        Iter { spec: self.spec.clone(), data: self.data.clone(), source }
    }
}

// TODO: I think this is a potentially fun idea fror laziness, but I will implement a simple eager
//       solution for now.
struct TransformedSource<T, Transform> {
    source: QuincunxIter<T>,
    transform: Transform,
}

impl<K, Transform> From<(QuincunxIter<Project>, Transform)> for TransformedSource<Project, Transform> where Transform: Group<Project, Key=K> {
    fn from(source_and_transform: (QuincunxIter<Project>, Transform)) -> Self {
        TransformedSource {
            source: source_and_transform.0,
            transform: source_and_transform.1,
        }
    }
}

/**
 * Group iterator, probably the most used iterator we build.s
 */
pub struct GroupIter<K, T> {
    spec: Spec,
    data: DataPtr,
    source: Vec<(K, Vec<T>)>
}

impl<K, T> WithData for GroupIter<K, T> {
    fn get_database_ptr(&self) -> DataPtr {
        self.data.clone()
    }
}

impl<K, T> Iterator for GroupIter<K, T> {
    type Item = (K, Vec<T>);
    fn next(&mut self) -> Option<Self::Item> {
        self.source.pop()
    }
}