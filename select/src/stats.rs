use crate::attrib::{NumericalAttribute, CollectionAttribute};
use crate::data::DataPtr;
use itertools::Itertools;
// use std::iter::Sum;

#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Count<C>(pub C);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Min<C>(pub C);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Max<C>(pub C);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Mean<C>(pub C);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Median<C>(pub C);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Ratio<C>(pub C);

impl<I,C,E> NumericalAttribute for Count<C> where C: CollectionAttribute<Item=I, Entity=E> {
    type Entity = E;
    type Number = usize;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(self.0.len(database,entity))
    }
}

impl<I,C,E>/*baby*/ NumericalAttribute for Min<C> where C: CollectionAttribute<Item=I, Entity=E>, I: Into<usize> + Ord {
    type Entity = E;
    type Number = usize;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        self.0.items(database, entity).into_iter().min().map(|n| n.into())
    }
}

impl<I,C,E> NumericalAttribute for Max<C> where C: CollectionAttribute<Item=I, Entity=E>, I: Into<usize> + Ord {
    type Entity = E;
    type Number = usize;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        self.0.items(database, entity).into_iter().max().map(|n| n.into())
    }
}

impl<I,C,E>/*baby*/ NumericalAttribute for Mean<C> where C: CollectionAttribute<Item=I, Entity=E>, I: Into<usize> + Ord {
    type Entity = E;
    type Number = f64;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        let items: Vec<usize> =
            self.0.items(database, entity).into_iter().sorted().map(|n| n.into()).collect();

        if items.len() == 0 { None }
        else { Some(items.iter().sum::<usize>() as f64 / items.len() as f64) }
    }
}

impl<I,C,E> NumericalAttribute for Median<C> where C: CollectionAttribute<Item=I, Entity=E>, I: Into<usize> + Ord {
    type Entity = E;
    type Number = f64;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        let mut items: Vec<usize> =
            self.0.items(database, entity).into_iter().sorted().map(|n| n.into()).collect();

        items.sort();

        match items.len() {
            0usize => None,
            1usize => items.get(0).map(|e| *e as f64),

            odd  if odd % 2 != 0usize => {
                items.get(odd / 2).map(|e| *e as f64)
            }

            even /* if even % 2 == 0usize */ => {
                let left = items.get((even / 2) - 1);
                let right = items.get(even / 2);
                if left.is_none() || right.is_none() { None }
                else { Some((*left.unwrap() + *right.unwrap()) as f64 / 2f64) }
            }
        }
    }
}

impl<I,C,E>/*baby*/ NumericalAttribute for Ratio<C> where C: CollectionAttribute<Item=I, Entity=E>, I: Into<usize> + Ord {
    type Entity = E;
    type Number = f64;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(self.0.len(database.clone(), entity) as f64 / self.0.parent_len(database, entity) as f64)
    }
}