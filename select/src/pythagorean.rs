use crate::log::LogLevel;
use std::path::PathBuf;
use crate::objects::{Month, ProjectId, Project, Identifiable, Identity};
use crate::{LoadFilter, Group, FilterEach, SampleEach, SelectEach};
use std::hash::Hash;
use std::marker::PhantomData;
use crate::data::Data;
use std::rc::Rc;
use std::borrow::Borrow;
use std::cell::RefCell;
use dcd::DCD;
use std::collections::BTreeMap;

/**
 * This is a Djanco API starting point. Query and database construction starts here.
 */
struct Djanco;

impl Djanco {
    pub fn from<S: Into<String>>(path: S, seed: u128, timestamp: Month) -> DjancoPrototype {
        DjancoPrototype::from(path, seed, timestamp)
    }
}

/**
 * This is a Djanco configuration object which accepts configuration options and eventually yields a
 * Djanco instance.
 */
struct DjancoPrototype {
    seed: u128,
    timestamp: Month,
    verbosity: LogLevel,
    path: PathBuf,
    filters: Vec<Box<dyn LoadFilter>>,
}

impl DjancoPrototype {
    pub fn from<S: Into<String>>(path: S, seed: u128, timestamp: Month) -> Self  {
        // TODO check path
        DjancoPrototype {
            timestamp: timestamp.into(),
            verbosity: LogLevel::Quiet,
            path: PathBuf::from(path.into()),
            filters: vec![],
            seed,
        }
    }

    pub fn with_log_level(mut self, level: LogLevel) -> Self {
        self.verbosity = level; self
    }

    pub fn with_project_filter<F>(mut self, filter: F) -> Self where F: LoadFilter + 'static {
        self.filters.push(Box::new(filter)); self
    }
}

/** DjancoPrototype iterators **/
impl DjancoPrototype {
    fn projects(self) -> DjancoInstance<Project> { DjancoInstance::from(self) }
}

pub trait GroupKey: PartialEq + Eq + Hash {} // TODO move to lib.rs

pub trait Filter<T> { // TODO move to lib.rs
//    fn decide(&self, database: Rc<RefCell<Data>>, object: &&T) -> bool;
    fn decide(&self, database: &Data, object: &&T) -> bool;
}

pub trait Sample<Id,T>: Clone where T: Identifiable<Id>, Id: Identity {
    fn sample_ids(self, database: &Data, iter: &mut dyn Iterator<Item=&T>) -> Vec<Id>;
    fn sample(self, database: &Data, iter: &mut dyn Iterator<Item=T>) -> Vec<T>;
}

#[derive(Clone)]
struct Top(usize);

impl<Id,T> Sample<Id,T> for Top where T: Identifiable<Id>, Id: Identity {
    fn sample_ids(self, database: &Data, iter: &mut dyn Iterator<Item=&T>) -> Vec<Id> {
        iter.take(self.0).map(|p| p.id()).collect()
    }
    fn sample(self, database: &Data, iter: &mut dyn Iterator<Item=T>) -> Vec<T> {
        iter.take(self.0).collect()
    }
}

struct And<T> { // TODO move to lib.rs
    left: Box<dyn Filter<T>>,
    right: Box<dyn Filter<T>>,
}

impl<T> Filter<T> for And<T> {
    //fn decide(&self, database: Rc<RefCell<Data>>, object: &&T) -> bool {
    fn decide(&self, database: &Data, object: &&T) -> bool {
        if self.right.decide(database.clone(), object) {
            self.left.decide(database, object)
        } else {
            false
        }
    }
}

struct DjancoInstance<T> {
    database: Data,
    filters: Vec<Box<dyn Filter<T>>>,
    seed: u128,
    timestamp: Month,
    verbosity: LogLevel,
    path: PathBuf,
    _entity: PhantomData<T>,
}

impl<T> From<DjancoPrototype> for DjancoInstance<T> {
    fn from(prototype: DjancoPrototype) -> Self {
        DjancoInstance {
            database: Data::from(&prototype.path, &prototype.timestamp, &prototype.verbosity),
            filters: vec![],
            seed: prototype.seed,
            timestamp: prototype.timestamp,
            verbosity: prototype.verbosity,
            path: prototype.path,
            _entity: PhantomData,
        }
    }
}

impl<T> DjancoInstance<T> {
    pub fn filter_by_attrib(mut self, attrib: impl Filter<T> + 'static) -> Self {
        self.filters.push(Box::new(attrib));
        self
    }
}

impl DjancoInstance<Project> {
    fn filtered_project_ids(&self) -> Vec<ProjectId> {
        self.database.projects.iter().filter(|(project_id, project)| {
            self.filters.iter().all(|filter| filter.decide(&self.database, project))
        }).map(|(_, project)| project.id).collect()
    }

    fn filtered_projects(&self) -> Vec<&Project> {
        self.database.projects.iter().filter(|(project_id, project)| {
            self.filters.iter().all(|filter| filter.decide(&self.database, project))
        }).map(|(_, project)| project).collect()
    }

    fn filtered_and_sampled_project_ids(&self, attrib: impl Sample<ProjectId, Project>) -> Vec<ProjectId> {
        let mut iter =
            self.database.projects.iter()
                .filter(|(_, project)| {
                    self.filters.iter().all(|filter| filter.decide(&self.database, project))
                })
                .map(|(_, project)| project);
        attrib.sample_ids(&self.database, &mut iter)
    }

    pub fn collect(self) -> DjancoSelection<ProjectId, Project> {
        let selection = self.filtered_project_ids();
        let mut instance = DjancoSelection::from(self);
        instance.selection.extend(selection);
        instance
    }

    pub fn sample_by_attrib(self, attrib: impl Sample<ProjectId, Project>) -> DjancoSelection<ProjectId, Project> {
        let selection = self.filtered_and_sampled_project_ids(attrib);
        let mut instance = DjancoSelection::from(self);
        instance.selection.extend(selection);
        instance
    }

    // pub fn select<EId, E>(self, attrib: impl Select<EId, E>) -> DjancoInstance<EId, E> {
    //     unimplemented!()
    // }

    pub fn group_by_attrib<K>(self, attrib: impl Group<Key=K>) -> DjancoGroupInstance<K,Project> where K: GroupKey {
        unimplemented!()
    }
}

struct DjancoGroupInstance<K,T> {
    _key: PhantomData<K>,
    _value: PhantomData<T>,
}

struct DjancoSelection<Id: Identity, T: Identifiable<Id>> {
    selection: Vec<Id>,
    database: Data,
    seed: u128,
    timestamp: Month,
    verbosity: LogLevel,
    path: PathBuf,
    _entity: PhantomData<T>,
}

impl From<DjancoInstance<Project>> for DjancoSelection<ProjectId, Project> {
    fn from(instance: DjancoInstance<Project>) -> Self {
        DjancoSelection {
            selection: vec![],
            database: instance.database,
            seed: instance.seed,
            timestamp: instance.timestamp,
            verbosity: instance.verbosity,
            path: instance.path,
            _entity: PhantomData,
        }
    }
}

