use std::marker::PhantomData;
use dcd::DCD;
use crate::objects::Project;
use crate::DatabasePtr;
use crate::csv::WithNames;

pub trait Attribute {}

pub struct AttributeValue<A: Attribute, T> {
    pub value: T,
    attribute_type: PhantomData<A>,
}

impl<A, T> AttributeValue<A, T> where A: Attribute {
    pub fn new(_attribute: &A, value: T) -> AttributeValue<A, T> {
        AttributeValue { value, attribute_type: PhantomData }
    }
}

pub trait LoadFilter {
    fn filter(&self, database: &DCD, /*key: &Self::Key,*/ project_id: &dcd::ProjectId, commit_ids: &Vec<dcd::CommitId>) -> bool;
}

pub trait Group {
    type Key;
    fn select(&self, project: &Project) -> Self::Key;
}

pub trait SortEach {
    /*type Key;*/ // TODO
    fn sort(&self, database: DatabasePtr, /*key: &Self::Key,*/ projects: &mut Vec<Project>);
}

pub trait FilterEach {
    /*type Key;*/ // TODO
    fn filter(&self, database: DatabasePtr, /*key: &Self::Key,*/ project: &Project) -> bool;
}

pub trait SampleEach {
    /*type Key;*/ // TODO
    fn sample(&self, database: DatabasePtr, /*key: &Self::Key,*/ projects: Vec<Project>) -> Vec<Project>;
}


pub trait SelectEach: WithNames {
    type Entity;
    fn select(&self, database: DatabasePtr, /*key: &Self::Key,*/ project: Project) -> Self::Entity;
}

pub trait NumericalAttribute {
    type Entity;
    fn calculate(&self, database: DatabasePtr, entity: &Self::Entity) -> usize;
}

pub trait CollectionAttribute {
    type Entity;
    //fn calculate(&self, database: DatabasePtr, entity: &Self::Entity) -> usize;
}

pub trait StringAttribute {
    type Entity;
    fn extract(&self, database: DatabasePtr, entity: &Self::Entity) -> String;
}