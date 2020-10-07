use crate::project;
use crate::commit;
use crate::objects::*;
use crate::attrib::*;
use crate::data::DataPtr;

pub struct From<E: Attribute, A: Attribute>(pub E, pub A);

impl<I,C,E,A> From<C, A> where C: Attribute + CollectionAttribute<Entity=E, Item=I>, A: Attribute {
    fn flat_map_items<F,U,T>(&self, data: DataPtr, entity: &E, f: F) -> Vec<T> where F: FnMut(I) -> U, U: IntoIterator<Item=T> {
        self.0.items(data, entity).into_iter().flat_map(f).collect()
    }
    fn flat_count_items<F,U,T>(&self, data: DataPtr, entity: &E, f: F) -> usize where F: FnMut(I) -> U, U: IntoIterator<Item=T> {
        self.0.items(data, entity).into_iter().flat_map(f).count()
    }
    fn map_items<F,T>(&self, data: DataPtr, entity: &E, f: F) -> Vec<T> where F: FnMut(I) -> T {
        self.0.items(data, entity).into_iter().map(f).collect()
    }
    fn count_items(&self, data: DataPtr, entity: &E) -> usize {
        self.0.len(data, entity)
    }

}

impl CollectionAttribute for From<project::Commits, commit::Message> {
    type Entity = Project;
    type Item = Message;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        self.flat_map_items(data.clone(), entity, |c: Commit| c.message(data.clone()))
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.flat_count_items(data.clone(), entity, |c: Commit| c.message(data.clone()))
    }

    fn parent_len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.count_items(data, entity)
    }
}

impl CollectionAttribute for From<project::Commits, commit::Id> {
    type Entity = Project;
    type Item = CommitId;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        self.map_items(data.clone(), entity, |e: Commit| e.id)
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.count_items(data, entity)
    }
}

impl CollectionAttribute for From<project::Commits, commit::Hash> {
    type Entity = Project;
    type Item = String;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        self.map_items(data.clone(), entity, |e: Commit| e.hash)
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.count_items(data, entity)
    }
}

impl CollectionAttribute for From<project::Commits, commit::Author> {
    type Entity = Project;
    type Item = User;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        self.flat_map_items(data.clone(), entity, |e: Commit| e.author(data.clone()))
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.flat_count_items(data.clone(), entity, |e: Commit| e.author(data.clone()))
    }

    fn parent_len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.count_items(data, entity)
    }
}

impl CollectionAttribute for From<project::Commits, commit::Committer> {
    type Entity = Project;
    type Item = User;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        self.flat_map_items(data.clone(), entity, |e: Commit| e.committer(data.clone()))
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.flat_count_items(data.clone(), entity, |e: Commit| e.committer(data.clone()))
    }

    fn parent_len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.count_items(data, entity)
    }
}

impl CollectionAttribute for From<project::Commits, commit::AuthorTime> {
    type Entity = Project;
    type Item = i64;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        self.map_items(data.clone(), entity, |e: Commit| e.author_time)
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.count_items(data, entity)
    }
}

impl CollectionAttribute for From<project::Commits, commit::CommitterTime> {
    type Entity = Project;
    type Item = i64;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        self.map_items(data.clone(), entity, |e: Commit| e.committer_time)
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.count_items(data, entity)
    }
}

impl CollectionAttribute for From<project::Commits, commit::Additions> {
    type Entity = Project;
    type Item = u64;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        self.flat_map_items(data.clone(), entity, |e: Commit| e.additions)
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.flat_count_items(data.clone(), entity, |e: Commit| e.additions)
    }

    fn parent_len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.count_items(data, entity)
    }
}

impl CollectionAttribute for From<project::Commits, commit::Deletions> {
    type Entity = Project;
    type Item = u64;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        self.flat_map_items(data.clone(), entity, |e: Commit| e.deletions)
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.flat_count_items(data.clone(), entity, |e: Commit| e.deletions)
    }

    fn parent_len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.count_items(data, entity)
    }
}

impl CollectionAttribute for From<project::Commits, commit::Users> {
    type Entity = Project;
    type Item = Vec<User>;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        self.map_items(data.clone(), entity, |e: Commit| e.users(data.clone()))
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.count_items(data, entity)
    }
}

impl CollectionAttribute for From<project::Commits, commit::Parents> {
    type Entity = Project;
    type Item = Vec<Commit>;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        self.map_items(data.clone(), entity, |e: Commit| e.parents(data.clone()))
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.count_items(data, entity)
    }
}

impl CollectionAttribute for From<project::Commits, commit::Paths> {
    type Entity = Project;
    type Item = Vec<Path>;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        self.map_items(data.clone(), entity, |e: Commit| e.paths(data.clone()))
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.count_items(data, entity)
    }
}

impl<F> CollectionAttribute for From<project::Commits, commit::ParentsWith<F>> where F: Filter<Entity=Commit> {
    type Entity = Project;
    type Item = Vec<Commit>;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        self.map_items(data.clone(), entity, |e: Commit| self.1.items(data.clone(), &e))
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.count_items(data, entity)
    }
}

impl<F> CollectionAttribute for From<project::Commits, commit::UsersWith<F>> where F: Filter<Entity=User> {
    type Entity = Project;
    type Item = Vec<User>;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        self.map_items(data.clone(), entity, |e: Commit| self.1.items(data.clone(), &e))
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.count_items(data, entity)
    }
}

impl<F> CollectionAttribute for From<project::Commits, commit::PathsWith<F>> where F: Filter<Entity=Path> {
    type Entity = Project;
    type Item = Vec<Path>;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        self.map_items(data.clone(), entity, |e: Commit| self.1.items(data.clone(), &e))
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.count_items(data, entity)
    }
}