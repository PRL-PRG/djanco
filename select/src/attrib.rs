use std::marker::PhantomData;
use dcd::DCD;
use crate::data::DataPtr;
use itertools::Itertools;
use std::hash::Hash;
use crate::names::WithNames;

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
    fn clone_box(&self) -> Box<dyn LoadFilter>;
}

pub trait Group<T> {
    type Key;
    fn select(&self, data: DataPtr, element: &T) -> Self::Key;
    fn execute(&mut self, data: DataPtr, vector: Vec<T>) -> Vec<(Self::Key, Vec<T>)> where <Self as Group<T>>::Key: Hash + Eq {
        vector.into_iter()
            .map(|e| (self.select(data.clone(), &e), e))
            .into_group_map()
            .into_iter()
            .collect()
    }
}

pub trait Filter<T> {
    fn filter(&self, data: DataPtr, element: &T) -> bool;
    fn execute(&mut self, data: DataPtr, vector: Vec<T>) -> Vec<T> {
        vector.into_iter()
            .filter(|e| self.filter(data.clone(), &e))
            .collect()
    }
}

pub trait Sort<T> {
    fn execute(&mut self, data: DataPtr, vector: Vec<T>) -> Vec<T>;
}

pub trait Sample<T> {
    fn execute(&mut self, data: DataPtr, vector: Vec<T>) -> Vec<T>;
}

pub trait Select<T>: WithNames {
    type Entity; // TODO rename
    fn select(&self, data: DataPtr, project: T) -> Self::Entity;
    fn execute(&mut self, data: DataPtr, vector: Vec<T>) -> Vec<Self::Entity> {
        vector.into_iter()
            .map(|e| self.select(data.clone(), e))
            .collect()
    }
}

// pub trait SortEach {
//     /*type Key;*/ // TODO
//     fn sort(&self, database: DataPtr, /*key: &Self::Key,*/ projects: &mut Vec<Project>);
// }
//
// pub trait FilterEach {
//     /*type Key;*/ // TODO
//     fn filter(&self, database: DataPtr, /*key: &Self::Key,*/ project: &Project) -> bool;
// }
//
// pub trait SampleEach {
//     /*type Key;*/ // TODO
//     fn sample(&self, database: DataPtr, /*key: &Self::Key,*/ projects: Vec<Project>) -> Vec<Project>;
// }
//
// pub trait SelectEach: WithNames {
//     type Entity;
//     fn select(&self, database: DataPtr, /*key: &Self::Key,*/ project: Project) -> Self::Entity;
// }

pub trait NumericalAttribute {
    type Entity;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> usize;
}

pub trait CollectionAttribute {
    type Entity;
    //fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> usize;
}

pub trait StringAttribute {
    type Entity;
    fn extract(&self, database: DataPtr, entity: &Self::Entity) -> String;
}

pub mod raw {
    pub trait NumericalAttribute {
        type Entity;
        fn calculate(&self, database: &dcd::DCD, project_id: &u64, commit_ids: &Vec<u64>) -> usize;
    }

    pub trait CollectionAttribute {
        type Entity;
        //fn calculate(&self, database: &dcd::DCD, project_id: &u64, commit_ids: &Vec<u64>) -> usize;
    }

    pub trait StringAttribute {
        type Entity;
        fn extract(&self, database: &dcd::DCD, project_id: &u64, commit_ids: &Vec<u64>) -> String;
    }

}