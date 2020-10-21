use crate::commit;
use crate::objects::*;
use crate::attrib::*;
use crate::message;
use crate::data::DataPtr;

pub struct From<E, A: Attribute>(pub E, pub A);
//pub struct Count;

impl<E,A> Attribute for From<E, A> where A: Attribute {}
//impl<A>   Attribute for Count      where A: Attribute {}

//select::retrieve::Each<select::project::CommitsWith<select::require::Exists<select::commit::Paths>>>
// impl<I,C,E> CollectionAttribute for Each<C> where C: Attribute + CollectionAttribute<Entity=E, Item=I> {
//     type Entity = E;
//     type Item = Vec<I>;
//
//     fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
//         self.0.items(data, entity)
//     }
//
//     fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
//         unimplemented!()
//     }
// }

impl<I,C,E,A> From<C, A> where C: CollectionAttribute<Entity=E, Item=I>, A: Attribute {
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

impl<C, E> CollectionAttribute for From<C, message::Length> where C: CollectionAttribute<Entity=E, Item=Message> {
    type Entity = E;
    type Item = usize;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        self.0.items(data, entity).into_iter().map(|m| m.contents.len()).collect()
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.0.len(data, entity)
    }
}

impl<C,E> CollectionAttribute for From<C, commit::Id> where C: CollectionAttribute<Entity=E, Item=Commit> {
    type Entity = E;
    type Item = CommitId;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        self.map_items(data.clone(), entity, |e: Commit| e.id)
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.count_items(data, entity)
    }
}

impl<C,E> CollectionAttribute for From<C, commit::Hash> where C: CollectionAttribute<Entity=E, Item=Commit> {
    type Entity = E;
    type Item = String;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        self.map_items(data.clone(), entity, |e: Commit| e.hash)
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.count_items(data, entity)
    }
}

impl<C,E> CollectionAttribute for From<C, commit::Author> where C: CollectionAttribute<Entity=E, Item=Commit> {
    type Entity = E;
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

impl<C,E> CollectionAttribute for From<C, commit::Committer> where C: CollectionAttribute<Entity=E, Item=Commit> {
    type Entity = E;
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

impl<C,E> CollectionAttribute for From<C, commit::AuthorTime> where C: CollectionAttribute<Entity=E, Item=Commit> {
    type Entity = E;
    type Item = i64;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        self.map_items(data.clone(), entity, |e: Commit| e.author_time)
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.count_items(data, entity)
    }
}

impl<C,E> CollectionAttribute for From<C, commit::CommitterTime> where C: CollectionAttribute<Entity=E, Item=Commit> {
    type Entity = E;
    type Item = i64;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        self.map_items(data.clone(), entity, |e: Commit| e.committer_time)
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.count_items(data, entity)
    }
}

impl<C,E> CollectionAttribute for From<C, commit::Additions> where C: CollectionAttribute<Entity=E, Item=Commit> {
    type Entity = E;
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

impl<C,E> CollectionAttribute for From<C, commit::Deletions> where C: CollectionAttribute<Entity=E, Item=Commit> {
    type Entity = E;
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

impl<C,E> CollectionAttribute for From<C, commit::Message> where C: CollectionAttribute<Entity=E, Item=Commit> {
    type Entity = E;
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

impl<C,E> CollectionAttribute for From<C, commit::Users> where C: CollectionAttribute<Entity=E, Item=Commit> {
    type Entity = E;
    type Item = Vec<User>;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        self.map_items(data.clone(), entity, |e: Commit| e.users(data.clone()))
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.count_items(data, entity)
    }
}

impl<C,E> CollectionAttribute for From<C, commit::Parents> where C: CollectionAttribute<Entity=E, Item=Commit> {
    type Entity = E;
    type Item = Vec<Commit>;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        self.map_items(data.clone(), entity, |e: Commit| e.parents(data.clone()))
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.count_items(data, entity)
    }
}

impl<C,E> CollectionAttribute for From<C, commit::Paths> where C: CollectionAttribute<Entity=E, Item=Commit> {
    type Entity = E;
    type Item = Vec<Path>;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        eprintln!("From<Commit,Paths>.items -> Vec<Path>");
        self.map_items(data.clone(), entity, |e: Commit| e.paths(data.clone()))
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize { eprintln!("From<Commit,Paths>.len -> usize"); self.count_items(data, entity) }
}

impl<F,C,E> CollectionAttribute for From<C, commit::ParentsWith<F>> where C: CollectionAttribute<Entity=E, Item=Commit>, F: Filter<Entity=Commit> {
    type Entity = E;
    type Item = Vec<Commit>;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        self.map_items(data.clone(), entity, |e: Commit| self.1.items(data.clone(), &e))
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.count_items(data, entity)
    }
}

impl<F,C,E> CollectionAttribute for From<C, commit::UsersWith<F>> where C: CollectionAttribute<Entity=E, Item=Commit>, F: Filter<Entity=User> {
    type Entity = E;
    type Item = Vec<User>;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        self.map_items(data.clone(), entity, |e: Commit| self.1.items(data.clone(), &e))
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.count_items(data, entity)
    }
}

impl<F,C,E> CollectionAttribute for From<C, commit::PathsWith<F>> where C: CollectionAttribute<Entity=E, Item=Commit>, F: Filter<Entity=Path> {
//impl<F> CollectionAttribute for From<project::Commits, commit::PathsWith<F>> where F: Filter<Entity=Path> {
    type Entity = E;
    type Item = Vec<Path>;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        self.map_items(data.clone(), entity, |e: Commit| self.1.items(data.clone(), &e))
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        self.count_items(data, entity)
    }
}