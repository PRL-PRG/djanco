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

impl<F> NumericalAttribute for CommitsWith<F> where F: Filter<Entity=Commit> {
    type Entity = User;
    type Number = usize;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(match self {
            CommitsWith::Authored(f)  => {
                entity.authored_commits(database.clone()).into_iter()
                    .filter(|u| f.filter(database.clone(), u))
                    .count()
            }
            CommitsWith::Committed(f) => {
                entity.committed_commits(database.clone()).into_iter()
                    .filter(|u| f.filter(database.clone(), u))
                    .count()
            }
        })
    }
}

impl CollectionAttribute for Commits where {
    type Entity = User;
    type Item = Commit;
    fn items(&self, database: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        match self {
            Commits::Authored  => {
                entity.authored_commits(database.clone()).into_iter().collect()
            }
            Commits::Committed => {
                entity.committed_commits(database.clone()).into_iter().collect()
            }
        }
    }
    fn len(&self, database: DataPtr, entity: &Self::Entity) -> usize {
        match self {
            Commits::Authored  => {
                entity.authored_commits(database.clone()).into_iter().count()
            }
            Commits::Committed => {
                entity.committed_commits(database.clone()).into_iter().count()
            }
        }
    }
    fn parent_len(&self, database: DataPtr, entity: &Self::Entity) -> usize {
        self.len(database, entity)
    }
}

impl<F> CollectionAttribute for CommitsWith<F> where F: Filter<Entity=Commit> {
    type Entity = User;
    type Item = Commit;
    fn items(&self, database: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        match self {
            CommitsWith::Authored(f)  => {
                entity.authored_commits(database.clone()).into_iter()
                    .filter(|u| f.filter(database.clone(), u))
                    .collect()
            }
            CommitsWith::Committed(f) => {
                entity.committed_commits(database.clone()).into_iter()
                    .filter(|u| f.filter(database.clone(), u))
                    .collect()
            }
        }
    }
    fn len(&self, database: DataPtr, entity: &Self::Entity) -> usize {
        match self {
            CommitsWith::Authored(f)  => {
                entity.authored_commits(database.clone()).into_iter()
                    .filter(|u| f.filter(database.clone(), u))
                    .count()
            }
            CommitsWith::Committed(f) => {
                entity.committed_commits(database.clone()).into_iter()
                    .filter(|u| f.filter(database.clone(), u))
                    .count()
            }
        }
    }
    fn parent_len(&self, database: DataPtr, entity: &Self::Entity) -> usize {
        match self {
            CommitsWith::Authored(_)  => {
                entity.authored_commits(database.clone()).into_iter().count()
            }
            CommitsWith::Committed(_) => {
                entity.committed_commits(database.clone()).into_iter().count()
            }
        }
    }
}

impl StringAttribute for Id {
    type Entity = User;
    fn extract(&self, _database: DataPtr, entity: &Self::Entity) -> String {
        entity.id.to_string()
    }
}

impl StringAttribute for Email {
    type Entity = User;
    fn extract(&self, _database: DataPtr, entity: &Self::Entity) -> String {
        entity.email.clone()
    }
}

impl StringAttribute for Name {
    type Entity = User;
    fn extract(&self, _database: DataPtr, entity: &Self::Entity) -> String { entity.name.clone() }
}

impl StringAttribute for Experience {
    type Entity = User;
    fn extract(&self, database: DataPtr, entity: &Self::Entity) -> String {
        entity.experience(database).map_or(String::new(), |e| e.to_string())
    }
}

impl Group<User> for Id {
    type Key = UserId;
    fn select(&self, _: DataPtr, user: &User) -> Self::Key { user.id }
}

impl Group<User> for Email {
    type Key = AttributeValue<Self, String>;
    fn select(&self, _: DataPtr, user: &User) -> Self::Key {
        AttributeValue::new(self, user.email.clone())
    }
}

impl Group<User> for Name {
    type Key = AttributeValue<Self, String>;
    fn select(&self, _: DataPtr, user: &User) -> Self::Key {
        AttributeValue::new(self, user.name.clone())
    }
}

impl Group<User> for Experience {
    type Key = AttributeValue<Self, Seconds>;
    fn select(&self, data: DataPtr, user: &User) -> Self::Key {
        AttributeValue::new(self, user.experience(data)
            .map_or(Seconds(0), |e| e.clone()))
    }
}

impl Sort<User> for Id {
    fn execute(&mut self, _: DataPtr, mut vector: Vec<User>) -> Vec<User> {
        vector.sort_by_key(|u| u.id);
        vector
    }
}

impl Sort<User> for Email {
    fn execute(&mut self, _: DataPtr, mut vector: Vec<User>) -> Vec<User> {
        vector.sort_by_key(|u| u.email.clone()); vector
    }
}

impl Sort<User> for Name {
    fn execute(&mut self, _: DataPtr, mut vector: Vec<User>) -> Vec<User> {
        vector.sort_by_key(|u| u.name.clone()); vector
    }
}

impl Sort<User> for Experience {
    fn execute(&mut self, data: DataPtr, mut vector: Vec<User>) -> Vec<User> {
        vector.sort_by_key(|u| u.experience(data.clone()).clone()); vector
    }
}

impl Sort<User> for Commits {
    fn execute(&mut self, data: DataPtr, mut vector: Vec<User>) -> Vec<User> {
        match self {
            Commits::Authored  => {
                vector.sort_by_key(|u| u.authored_commits(data.clone()).len())
            }
            Commits::Committed => {
                vector.sort_by_key(|u| u.committed_commits(data.clone()).len())
            }
        };
        vector
    }
}

impl<F> Sort<User> for CommitsWith<F> where F: Filter<Entity=Commit> {
    fn execute(&mut self, data: DataPtr, mut vector: Vec<User>) -> Vec<User> {
        match self {
            CommitsWith::Authored(f) => {
                vector.sort_by_key(|u| {
                    u.authored_commits(data.clone()).iter()
                        .filter(|c| f.filter(data.clone(), c)).count()
                })
            }
            CommitsWith::Committed(f) => {
                vector.sort_by_key(|u| {
                    u.committed_commits(data.clone()).iter()
                        .filter(|c| f.filter(data.clone(), c)).count()
                })
            }
        };
        vector
    }
}

impl Select<User> for Id {
    type Entity = AttributeValue<Id, UserId>;
    fn select(&self, _: DataPtr, user: User) -> Self::Entity {
        AttributeValue::new(self, user.id.clone())
    }
}

impl Select<User> for Name {
    type Entity = AttributeValue<Name, String>;
    fn select(&self, _: DataPtr, user: User) -> Self::Entity {
        AttributeValue::new(self, user.name.clone())
    }
}

impl Select<User> for Email {
    type Entity = AttributeValue<Email, String>;
    fn select(&self, _: DataPtr, user: User) -> Self::Entity {
        AttributeValue::new(self, user.email.clone())
    }
}

impl Select<User> for Experience {
    type Entity = AttributeValue<Experience, Option<Seconds>>;
    fn select(&self, data: DataPtr, user: User) -> Self::Entity {
        let experience = user.experience(data.clone());
        AttributeValue::new(self, experience.clone())
    }
}

impl Select<User> for Commits {
    type Entity = Vec<Commit>;
    fn select(&self, database: DataPtr, user: User) -> Self::Entity {
        match self {
            Commits::Authored => { user.authored_commits(database) }
            Commits::Committed => { user.committed_commits(database) }
        }
    }
}
