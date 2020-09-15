use crate::attrib::{NumericalAttribute, CollectionAttribute};
use crate::data::DataPtr;

#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Count<C>(pub C);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Min<C>(pub C);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Max<C>(pub C);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Mean<C>(pub C);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Median<C>(pub C);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Ratio<C>(pub C);

impl<C,I,E> NumericalAttribute for Count<C> where C: CollectionAttribute<Item=I, Entity=E> {
    type Entity = E;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> usize {
        self.0.items(database, entity).len()
    }
}

// impl<C,I,E> NumericalAttribute for Min<C> where C: CollectionAttribute<Item=I, Entity=E>, I: Into<usize> {
//     type Entity = E;
//     fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> usize {
//         self.0.items(database, entity).iter().min().unwrap().(|e| e.)
//     }
// }

