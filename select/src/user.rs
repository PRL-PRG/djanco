use crate::objects::*;
use crate::attrib::*;
use crate::data::*;
//use crate::meta::*;
use crate::time::*;

//use dcd::{DCD, Database};
//use itertools::Itertools;

#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Id;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Email;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Name;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Experience;

#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub enum Commits { Authored, Committed }
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub enum CommitsWith<F: Filter<Entity=Commit>> { Authored(F), Committed(F) }

impl Attribute for Id         {}
impl Attribute for Email      {}
impl Attribute for Name       {}
impl Attribute for Experience {}
impl Attribute for Commits    {}

impl<F> Attribute for CommitsWith<F> where F: Filter<Entity=Commit> {}

impl NumericalAttribute for Id {
    type Entity = User;
    type Number = usize;
    fn calculate(&self, _database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(entity.id.into())
    }
}

impl NumericalAttribute for Experience {
    type Entity = User;
    type Number = Seconds;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
       entity.experience(database)
    }
}

impl NumericalAttribute for Commits {
    type Entity = User;
    type Number = usize;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        match self {
            Commits::Authored  => { Some(entity.authored_commits(database).len())  }
            Commits::Committed => { Some(entity.committed_commits(database).len()) }
        }
    }
}

//CollectionAttribute

// ExistentialAttribute


// StringAttribute

// Group<Project>
// Sort<Project>
// Select<Project>


