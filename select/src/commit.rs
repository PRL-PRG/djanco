use crate::{objects, retrieve, project, commit};
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
        Some(entity.user_ids().len())
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
        Some(entity.users(database.clone())
            .iter()
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

impl Sort<Commit> for Id {
    fn execute(&mut self, _: DataPtr, mut vector: Vec<Commit>) -> Vec<Commit> {
        vector.sort_by_key(|c| c.id);
        vector
    }
}

impl Sort<Commit> for Hash {
    fn execute(&mut self, _: DataPtr, mut vector: Vec<Commit>) -> Vec<Commit> {
        vector.sort_by(|c1, c2| c1.hash.cmp(&c2.hash));
        vector
    }
}

impl Sort<Commit> for Author {
    fn execute(&mut self, _: DataPtr, mut vector: Vec<Commit>) -> Vec<Commit> {
        vector.sort_by_key(|c| c.author);
        vector
    }
}

impl Sort<Commit> for Committer {
    fn execute(&mut self, _: DataPtr, mut vector: Vec<Commit>) -> Vec<Commit> {
        vector.sort_by_key(|c| c.committer);
        vector
    }
}

impl Sort<Commit> for AuthorTime {
    fn execute(&mut self, _: DataPtr, mut vector: Vec<Commit>) -> Vec<Commit> {
        vector.sort_by_key(|c| c.author_time);
        vector
    }
}

impl Sort<Commit> for CommitterTime {
    fn execute(&mut self, _: DataPtr, mut vector: Vec<Commit>) -> Vec<Commit> {
        vector.sort_by_key(|c| c.committer_time);
        vector
    }
}

impl Sort<Commit> for Additions {
    fn execute(&mut self, _: DataPtr, mut vector: Vec<Commit>) -> Vec<Commit> {
        vector.sort_by_key(|c| c.additions);
        vector
    }
}

impl Sort<Commit> for Deletions {
    fn execute(&mut self, _: DataPtr, mut vector: Vec<Commit>) -> Vec<Commit> {
        vector.sort_by_key(|c| c.deletions);
        vector
    }
}

impl Sort<Commit> for Message {
    fn execute(&mut self, data: DataPtr, mut vector: Vec<Commit>) -> Vec<Commit> {
        vector.sort_by_key(|c| c.message(data.clone()));
        vector
    }
}

impl Sort<Commit> for Users {
    fn execute(&mut self, _: DataPtr, mut vector: Vec<Commit>) -> Vec<Commit> {
        vector.sort_by_key(|c| (c.user_ids().len(), c.user_ids())); // Probably a bad idea
        vector
    }
}

impl Sort<Commit> for Parents {
    fn execute(&mut self, _: DataPtr, mut vector: Vec<Commit>) -> Vec<Commit> {
        vector.sort_by_key(|c| (c.parents.len()));
        vector
    }
}

impl Sort<Commit> for Paths {
    fn execute(&mut self, data: DataPtr, mut vector: Vec<Commit>) -> Vec<Commit> {
        vector.sort_by_key(|c| c.path_count(data.clone()));
        vector
    }
}

impl<F> Sort<Commit> for UsersWith<F> where F: Filter<Entity=User> {
    fn execute(&mut self, data: DataPtr, mut vector: Vec<Commit>) -> Vec<Commit> {
        vector.sort_by_key(|c| {
            let users: Vec<UserId> = c.user_ids().into_iter()
                .flat_map(|id| untangle_mut!(data).user(&id).map(|e| e.clone()))
                .filter(|u| self.0.filter(data.clone(), u))
                .map(|u| u.id.clone())
                .collect();
            (users.len(), users)
        });
        vector
    }
}

impl<F> Sort<Commit> for ParentsWith<F> where F: Filter<Entity=Commit> {
    fn execute(&mut self, data: DataPtr, mut vector: Vec<Commit>) -> Vec<Commit> {
        vector.sort_by_key(|c| {
            c.parents.iter()
                .filter(|id| {
                    untangle_mut!(data.clone()).commit(id).map_or(false, |c| {
                        self.0.filter(data.clone(), c)
                    })
                })
                .count()
        });
        vector
    }
}

impl<F> Sort<Commit> for PathsWith<F> where F: Filter<Entity=Path> {
    fn execute(&mut self, data: DataPtr, mut vector: Vec<Commit>) -> Vec<Commit> {
        vector.sort_by_key(|c| {
            c.paths(data.clone()).into_iter().filter(|p| {
                self.0.filter(data.clone(), p)
            })
            .count()
        });
        vector
    }
}

impl Select<Commit> for Id {
    type Entity = CommitId;
    fn select(&self, _: DataPtr, commit: Commit) -> Self::Entity {
        commit.id.clone()
    }
}

impl Select<Commit> for Hash {
    type Entity = AttributeValue<Hash, String>;
    fn select(&self, _: DataPtr, commit: Commit) -> Self::Entity {
        AttributeValue::new(self, commit.hash.clone())
    }
}

impl Select<Commit> for Author {
    type Entity = AttributeValue<Author, Option<User>>;
    fn select(&self, data: DataPtr, commit: Commit) -> Self::Entity {
        AttributeValue::new(self,
                            untangle_mut!(data).user(&commit.author).map(|c| c.clone()))
    }
}

impl Select<Commit> for Committer {
    type Entity = AttributeValue<Committer, Option<User>>;
    fn select(&self, data: DataPtr, commit: Commit) -> Self::Entity {
        AttributeValue::new(self,
                            untangle_mut!(data).user(&commit.committer).map(|c| c.clone()))
    }
}

impl Select<Commit> for AuthorTime {
    type Entity = AttributeValue<AuthorTime, i64>;
    fn select(&self, _: DataPtr, commit: Commit) -> Self::Entity {
        AttributeValue::new(self, commit.author_time)
    }
}

impl Select<Commit> for CommitterTime {
    type Entity = AttributeValue<CommitterTime, i64>;
    fn select(&self, _: DataPtr, commit: Commit) -> Self::Entity {
        AttributeValue::new(self, commit.committer_time)
    }
}


impl Select<Commit> for Additions {
    type Entity = AttributeValue<Additions, Option<u64>>;
    fn select(&self, _: DataPtr, commit: Commit) -> Self::Entity {
        AttributeValue::new(self, commit.additions)
    }
}

impl Select<Commit> for Deletions {
    type Entity = AttributeValue<Deletions, Option<u64>>;
    fn select(&self, _: DataPtr, commit: Commit) -> Self::Entity {
        AttributeValue::new(self, commit.deletions)
    }
}

impl Select<Commit> for Message {
    type Entity = Option<objects::Message>;
    fn select(&self, data: DataPtr, commit: Commit) -> Self::Entity {
        commit.message(data)
    }
}

impl Select<Commit> for Users {
    type Entity = Vec<User>;
    fn select(&self, data: DataPtr, commit: Commit) -> Self::Entity {
        commit.user_ids().iter()
            .flat_map(|id| untangle_mut!(data).user(id).map(|c| c.clone()))
            .collect()
    }
}

impl<F> Select<Commit> for UsersWith<F> where F: Filter<Entity=User> {
    type Entity = Vec<User>;
    fn select(&self, data: DataPtr, commit: Commit) -> Self::Entity {
        commit.users(data.clone()).into_iter()
            .filter(|u| self.0.filter(data.clone(), &u))
            .collect()
    }
}

impl Select<Commit> for Parents {
    type Entity = Vec<Commit>;
    fn select(&self, data: DataPtr, commit: Commit) -> Self::Entity {
        commit.parents.iter()
            .flat_map(|id| untangle_mut!(data).commit(id).map(|c| c.clone()))
            .collect()
    }
}

impl Select<Commit> for Paths {
    type Entity = Vec<Path>;
    fn select(&self, data: DataPtr, commit: Commit) -> Self::Entity {
        commit.paths(data)
    }
}

impl<F> Select<Commit> for ParentsWith<F> where F: Filter<Entity=Commit> {
    type Entity = Vec<Commit>;
    fn select(&self, data: DataPtr, commit: Commit) -> Self::Entity {
        commit.parents.iter()
            .flat_map(|id| {
                untangle_mut!(data.clone()).commit(id).map(|c| c.clone())
            })
            .filter(|c| {
                self.0.filter(data.clone(), &c)
            })
            .collect()
    }
}

impl<F> Select<Commit> for PathsWith<F> where F: Filter<Entity=Path> {
    type Entity = Vec<Path>;
    fn select(&self, data: DataPtr, commit: Commit) -> Self::Entity {
        commit
            .paths(data.clone()).into_iter()
            .filter(|p| {
                self.0.filter(data.clone(), p)
            })
            .collect()
    }
}

impl CollectionAttribute for Users {
    type Entity = Commit;
    type Item = User;
    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        entity.users(data)
    }

    fn len(&self, _: DataPtr, entity: &Self::Entity) -> usize {
        entity.user_ids().len()
    }

    fn parent_len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.len(data, entity)
    }
}

impl CollectionAttribute for Paths {
    type Entity = Commit;
    type Item = Path;
    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        entity.paths(data)
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        entity.path_count(data)
    }

    fn parent_len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.len(data, entity)
    }
}

impl CollectionAttribute for Parents {
    type Entity = Commit;
    type Item = Commit;
    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        entity.parents.iter()
            .flat_map(|id| untangle_mut!(data).commit(id).map(|u| u.clone()))
            .collect()
    }

    fn len(&self, _: DataPtr, entity: &Self::Entity) -> usize {
        entity.parents.len()
    }

    fn parent_len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.len(data, entity)
    }
}

impl<F> CollectionAttribute for UsersWith<F> where F: Filter<Entity=User> {
    type Entity = Commit;
    type Item = User;
    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        entity.users(data.clone()).into_iter()
            .filter(|u| self.0.filter(data.clone(), &u))
            .collect()
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        entity.users(data.clone()).iter()
            .filter(|u| self.0.filter(data.clone(), &u))
            .count()
    }

    fn parent_len(&self, _: DataPtr, entity: &Self::Entity) -> usize {
        entity.user_ids().len()
    }
}

impl<F> CollectionAttribute for PathsWith<F> where F: Filter<Entity=Path> {
    type Entity = Commit;
    type Item = Path;
    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        entity.paths(data.clone()).into_iter()
            .filter(|p| {
                self.0.filter(data.clone(), p)
            })
            .collect()
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        entity.paths(data.clone()).into_iter()
            .filter(|p| {
                self.0.filter(data.clone(), p)
            })
            .count()
    }

    fn parent_len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        entity.path_count(data)
    }
}

impl<F> CollectionAttribute for ParentsWith<F> where F: Filter<Entity=Commit> {
    type Entity = Commit;
    type Item = Commit;
    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        entity.parents.iter()
            .flat_map(|id| untangle_mut!(data).commit(id).map(|u| u.clone()))
            .filter(|e| self.0.filter(data.clone(), &e))
            .collect()
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        entity.parents.iter()
            .flat_map(|id| untangle_mut!(data).commit(id).map(|u| u.clone()))
            .filter(|e| self.0.filter(data.clone(), &e))
            .count()
    }

    fn parent_len(&self, _: DataPtr, entity: &Self::Entity) -> usize {
        entity.parents.len()
    }
}



