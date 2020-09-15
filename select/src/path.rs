use crate::objects;
use crate::objects::*;
use crate::attrib::*;
use crate::data::*;

#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Id;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Path;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Language;

impl Attribute for Id         {}
impl Attribute for Path       {}
impl Attribute for Language   {}

impl NumericalAttribute for Id {
    type Entity = objects::Path;
    type Number = usize;
    fn calculate(&self, _database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(entity.id.into())
    }
}

impl StringAttribute for Id {
    type Entity = objects::Path;
    fn extract(&self, _database: DataPtr, entity: &Self::Entity) -> String {
        entity.id.to_string()
    }
}

impl StringAttribute for Path {
    type Entity = objects::Path;
    fn extract(&self, _database: DataPtr, entity: &Self::Entity) -> String {
        entity.path.clone()
    }
}

impl StringAttribute for Language{
    type Entity = objects::Path;
    fn extract(&self, _database: DataPtr, entity: &Self::Entity) -> String {
        entity.language().unwrap_or(String::new())
    }
}

impl Group<objects::Path> for Id {
    type Key = PathId;
    fn select(&self, _: DataPtr, user: &objects::Path) -> Self::Key { user.id }
}

impl Group<objects::Path> for Path {
    type Key = AttributeValue<Self, String>;
    fn select(&self, _: DataPtr, path: &objects::Path) -> Self::Key {
        AttributeValue::new(self, path.path.clone())
    }
}

impl Group<objects::Path> for Language {
    type Key = AttributeValue<Self, Option<String>>;
    fn select(&self, _: DataPtr, path: &objects::Path) -> Self::Key {
        AttributeValue::new(self, path.language())
    }
}

impl Sort<objects::Path> for Id {
    fn execute(&mut self, _: DataPtr, mut vector: Vec<objects::Path>) -> Vec<objects::Path> {
        vector.sort_by_key(|p| p.id);
        vector
    }
}

impl Sort<objects::Path> for Path {
    fn execute(&mut self, _: DataPtr, mut vector: Vec<objects::Path>) -> Vec<objects::Path> {
        vector.sort_by_key(|p| p.path.clone());
        vector
    }
}

impl Sort<objects::Path> for Language {
    fn execute(&mut self, _: DataPtr, mut vector: Vec<objects::Path>) -> Vec<objects::Path> {
        vector.sort_by_key(|p| p.language());
        vector
    }
}


impl Select<objects::Path> for Id {
    type Entity = AttributeValue<Id, PathId>;
    fn select(&self, _: DataPtr, path: objects::Path) -> Self::Entity {
        AttributeValue::new(self, path.id.clone())
    }
}

impl Select<objects::Path> for Path {
    type Entity = AttributeValue<Path, String>;
    fn select(&self, _: DataPtr, path: objects::Path) -> Self::Entity {
        AttributeValue::new(self, path.path.clone())
    }
}

impl Select<objects::Path> for Language {
    type Entity = AttributeValue<Language, Option<String>>;
    fn select(&self, _: DataPtr, path: objects::Path) -> Self::Entity {
        AttributeValue::new(self, path.language())
    }
}