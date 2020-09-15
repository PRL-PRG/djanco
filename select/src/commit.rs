use crate::objects;
use crate::objects::*;
use crate::attrib::*;
use crate::data::*;

// TODO fill all these in
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Id;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Hash;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Author;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Committer;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct AuthorTime;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct CommitterTime;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Additions;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Deletions;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Message;

#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Users;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Parents;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Paths;

#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct ParentsWith<F>(pub F) where F: Filter<Entity=Commit>;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct UsersWith<F>(pub F)   where F: Filter<Entity=User>;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct PathsWith<F>(pub F)    where F: Filter<Entity=Path>;

impl Attribute for Id            {}
impl Attribute for Hash          {}
impl Attribute for Author        {}
impl Attribute for Committer     {}
impl Attribute for AuthorTime    {}
impl Attribute for CommitterTime {}
impl Attribute for Additions     {}
impl Attribute for Deletions     {}
impl Attribute for Message       {}

impl Attribute for Users         {}
impl Attribute for Parents       {}
impl Attribute for Paths         {}

impl<F> Attribute for ParentsWith<F> where F: Filter<Entity=Commit> {}
impl<F> Attribute for UsersWith<F>   where F: Filter<Entity=User>   {}
impl<F> Attribute for PathsWith<F>   where F: Filter<Entity=Path>   {}

impl NumericalAttribute for Id {
    type Entity = Commit;
    type Number = usize;
    fn calculate(&self, _database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(entity.id.into())
    }
}

impl NumericalAttribute for Author {
    type Entity = Commit;
    type Number = usize;
    fn calculate(&self, _database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(entity.author.into())
    }
}

impl NumericalAttribute for Committer {
    type Entity = Commit;
    type Number = usize;
    fn calculate(&self, _database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(entity.committer.into())
    }
}

impl NumericalAttribute for AuthorTime {
    type Entity = Commit;
    type Number = i64;
    fn calculate(&self, _database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(entity.author_time)
    }
}

impl NumericalAttribute for CommitterTime {
    type Entity = Commit;
    type Number = i64;
    fn calculate(&self, _database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(entity.committer_time)
    }
}

impl NumericalAttribute for Additions {
    type Entity = Commit;
    type Number = u64;
    fn calculate(&self, _database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        entity.additions
    }
}

impl NumericalAttribute for Deletions {
    type Entity = Commit;
    type Number = u64;
    fn calculate(&self, _database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        entity.deletions
    }
}

impl NumericalAttribute for Parents {
    type Entity = Commit;
    type Number = usize;
    fn calculate(&self, _database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(entity.parents.len())
    }
}

impl NumericalAttribute for Users {
    type Entity = Commit;
    type Number = usize;
    fn calculate(&self, _database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(entity.users().len())
    }
}

impl NumericalAttribute for Message {
    type Entity = Commit;
    type Number = usize;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        entity.message(database).map(|m| m.contents.len())
    }
}

impl NumericalAttribute for Paths {
    type Entity = Commit;
    type Number = usize;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(entity.path_count(database))
    }
}

impl<F> NumericalAttribute for ParentsWith<F> where F: Filter<Entity=Commit> {
    type Entity = Commit;
    type Number = usize;
    fn calculate(&self, _database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(entity.parents.len())
    }
}

impl<F> NumericalAttribute for UsersWith<F> where F: Filter<Entity=User> {
    type Entity = Commit;
    type Number = usize;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(entity.users().iter()
            .flat_map(|id| untangle_mut!(database).user(id).map(|u| u.clone()))
            .filter(|user| self.0.filter(database.clone(), &user))
            .count())
    }
}

impl<F> NumericalAttribute for PathsWith<F> where F: Filter<Entity=Path> {
    type Entity = Commit;
    type Number = usize;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(entity.paths(database.clone()).iter()
            .filter(|p| self.0.filter(database.clone(), p)).count())
    }
}

impl StringAttribute for Id {
    type Entity = Commit;
    fn extract(&self, _: DataPtr, entity: &Self::Entity) -> String {
        entity.id.to_string()
    }
}

impl StringAttribute for Hash {
    type Entity = Commit;
    fn extract(&self, _: DataPtr, entity: &Self::Entity) -> String { entity.hash.to_string() }
}

impl StringAttribute for Author {
    type Entity = Commit;
    fn extract(&self, _: DataPtr, entity: &Self::Entity) -> String {
        entity.author.to_string()
    }
}

impl StringAttribute for Committer {
    type Entity = Commit;
    fn extract(&self, _: DataPtr, entity: &Self::Entity) -> String {
        entity.committer.to_string()
    }
}

impl StringAttribute for CommitterTime {
    type Entity = Commit;
    fn extract(&self, _: DataPtr, _entity: &Self::Entity) -> String {
        unimplemented!()
    }
}

impl StringAttribute for AuthorTime {
    type Entity = Commit;
    fn extract(&self, _: DataPtr, _entity: &Self::Entity) -> String {
        unimplemented!()
    }
}

impl StringAttribute for Additions {
    type Entity = Commit;
    fn extract(&self, _: DataPtr, entity: &Self::Entity) -> String {
        entity.additions.map_or(String::new(), |e| e.to_string())
    }
}

impl StringAttribute for Deletions{
    type Entity = Commit;
    fn extract(&self, _: DataPtr, entity: &Self::Entity) -> String {
        entity.deletions.map_or(String::new(), |e| e.to_string())
    }
}

impl StringAttribute for Message {
    type Entity = Commit;
    fn extract(&self, data: DataPtr, entity: &Self::Entity) -> String {
        match entity.message(data) {
            None => String::new(),
            Some(message) =>
                String::from_utf8(message.contents)
                    .map_or(String::new(), |str| str.to_string())
        }

    }
}

impl Group<Commit> for Id {
    type Key = CommitId;
    fn select(&self, _: DataPtr, commit: &Commit) -> Self::Key { commit.id }
}

impl Group<Commit> for Author {
    type Key = UserId;
    fn select(&self, _: DataPtr, commit: &Commit) -> Self::Key { commit.author }
}

impl Group<Commit> for Committer {
    type Key = UserId;
    fn select(&self, _: DataPtr, commit: &Commit) -> Self::Key { commit.committer }
}

impl Group<Commit> for AuthorTime {
    type Key = AttributeValue<Self, i64>;
    fn select(&self, _: DataPtr, commit: &Commit) -> Self::Key {
        AttributeValue::new(self, commit.author_time)
    }
}

impl Group<Commit> for CommitterTime {
    type Key = AttributeValue<Self, i64>;
    fn select(&self, _: DataPtr, commit: &Commit) -> Self::Key {
        AttributeValue::new(self, commit.committer_time)
    }
}

impl Group<Commit> for Additions {
    type Key = AttributeValue<Self, Option<u64>>;
    fn select(&self, _: DataPtr, commit: &Commit) -> Self::Key {
        AttributeValue::new(self, commit.additions)
    }
}

impl Group<Commit> for Deletions {
    type Key = AttributeValue<Self, Option<u64>>;
    fn select(&self, _: DataPtr, commit: &Commit) -> Self::Key {
        AttributeValue::new(self, commit.deletions)
    }
}

impl Group<Commit> for Message {
    type Key = AttributeValue<Self, Option<objects::Message>>;
    fn select(&self, data: DataPtr, commit: &Commit) -> Self::Key {
        AttributeValue::new(self, commit.message(data))
    }
}

// TODO sort select