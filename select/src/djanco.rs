use std::path::PathBuf;
use crate::objects::*;
use crate::data::*;
use crate::log::LogLevel;
use crate::attrib::LoadFilter;
use std::marker::PhantomData;
use std::cell::RefCell;

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
    pub fn projects(self ) -> Loaded<Project> { Loaded::from(self) }
    pub fn commits(self)   -> Loaded<Commit>  { Loaded::from(self) }
    pub fn users(self)     -> Loaded<User>    { Loaded::from(self) }
    pub fn paths(self)     -> Loaded<Path>    { Loaded::from(self) }
    //pub fn snapshots(self) -> Loaded<Snapshot> { Loaded::from(self) }
    // TODO the remainder
}

/** Warehouse was initialized **/
pub struct Loaded<T> {
    spec: Spec,
    data: DataPtr,
    stream: Option<Vec<T>>, // Serves the iterator: None -> n elements -> ... -> 0 elements
    _entity: PhantomData<T>,
}

impl<T> Iterator for Loaded<T> where T: Quincunx {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stream.is_none() {
            self.stream = Some(Self::Item::stream_from(&self.data))
        }
        let mut stream = self.stream.as_mut().unwrap();
        let pop = stream.pop();
        pop
    }
}

impl<T> /* Query for */ Loaded<T> {

}

impl<T> From<Spec> for Loaded<T> {
    fn from(_spec: Spec) -> Self { unimplemented!() }
}

impl<T> From<&Spec> for Loaded<T> {
    fn from(_spec: &Spec) -> Self { unimplemented!() }
}

impl<T> From<Lazy> for Loaded<T> {
    fn from(lazy: Lazy) -> Self {
        let data = DataPtr::from(&lazy);
        Loaded { spec: lazy.spec, data, _entity: PhantomData, stream: None }
    }
}