use crate::project;
use crate::commit;
use crate::objects;
use crate::attrib::*;
use crate::data::DataPtr;

pub struct From<E: Attribute, A: Attribute>(pub E, pub A);

impl CollectionAttribute for From<project::Commits, commit::Message> {
    type Entity = objects::Project;
    type Item = objects::Message;

    fn items(&self, data: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        entity.commits(data.clone()).iter().flat_map(|c| c.message(data.clone())).collect()
    }

    fn len(&self, data: DataPtr, entity: &Self::Entity) -> usize {
        entity.commits(data.clone()).iter().flat_map(|c| c.message(data.clone())).count()
    }
}

// impl<C,E> NumericalAttribute for C where C: CollectionAttribute<Entity=E, Item=objects::Message> {
//     type Entity = E;
//     type Number = N;
//
//     fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
//         Some(self.len(database, entity))
//     }
// }