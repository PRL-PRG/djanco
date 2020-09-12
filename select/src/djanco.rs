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
    source: Option<Vec<T>>, // Serves the iterator: None -> n elements -> ... -> 0 elements
}

impl<T> WithData for QuincunxIter<T> {
    fn get_database_ptr(&self) -> DataPtr {
        self.data.clone()
    }
}

impl<T> Iterator for QuincunxIter<T> where T: Quincunx {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.source.is_none() {
            self.source = Some(Self::Item::stream_from(&self.data))
        }
        let mut source = self.source.as_mut().unwrap();
        let pop = source.pop();
        pop
    }
}

impl<T> /* Query for */ QuincunxIter<T> where T: Quincunx {
    pub fn group_by_attrib<K, G>(self, attrib: G) -> QuincunxGroupIter<K, T> where G: Group<T, Key=K>, K: Hash + Eq {
        let db = self.data.clone();
        let source: Vec<(K, Vec<T>)> = match self.source {
            None => vec![],
            Some(stream) => {
                stream.into_iter()
                    .map(|e| (attrib.select(db.clone(), &e), e))
                    .into_group_map()
                    .into_iter()
                    .collect()
            },
        };
        QuincunxGroupIter {
            spec: self.spec.clone(),
            data: self.data.clone(),
            source,
        }
    }

    pub fn filter<F>(mut self, attrib: F) -> QuincunxIter<T> where F: Filter<T> {
        let db = self.data.clone();
        let source: Vec<T> = match self.source {
            None => vec![],
            Some(stream) => {
                stream.into_iter()
                    .filter(|e| attrib.filter(db.clone(), &e))
                    .collect()
            },
        };
        QuincunxIter {
            spec: self.spec.clone(),
            data: self.data.clone(),
            source: Some(source),
        }
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
        QuincunxIter { spec: lazy.spec, data, source: None }
    }
}

// TODO: I think this is a potentially fun idea fror laziness, but I will implement a simple eager
//       solution for now.
struct TransformedSouce<T, Transform> {
    source: QuincunxIter<T>,
    transform: Transform,
}

impl<K, Transform> From<(QuincunxIter<Project>, Transform)> for TransformedSouce<Project, Transform> where Transform: Group<Project, Key=K> {
    fn from(source_and_transform: (QuincunxIter<Project>, Transform)) -> Self {
        TransformedSouce {
            source: source_and_transform.0,
            transform: source_and_transform.1,
        }
    }
}

pub struct QuincunxGroupIter<K, T> {
    spec: Spec,
    data: DataPtr,
    source: Vec<(K,Vec<T>)>
}

impl<K, T> WithData for QuincunxGroupIter<K, T> {
    fn get_database_ptr(&self) -> DataPtr {
        self.data.clone()
    }
}

impl<K, T> Iterator for QuincunxGroupIter<K, T> where T: Quincunx {
    type Item = (K, Vec<T>);

    fn next(&mut self) -> Option<Self::Item> {
        self.source.pop()
    }
}

impl<K, T> QuincunxGroupIter<K, T> {
    pub fn new(spec: &Spec, data: DataPtr, source: Vec<(K, Vec<T>)>) -> Self {
        QuincunxGroupIter{ spec: spec.clone(), data, source }
    }
}