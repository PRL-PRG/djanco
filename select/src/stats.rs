use std::cmp::Ordering;
use std::f64::NAN;

use itertools::Itertools;

use crate::attrib::*;
use crate::data::DataPtr;
use crate::helpers;

#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Count<C>(pub C);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Min<C>(pub C);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Max<C>(pub C);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Mean<C>(pub C);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Median<C>(pub C);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Ratio<C>(pub C);

pub trait Numeric {
    fn as_f64(&self) -> f64;
    fn as_ord_f64(&self) -> OrdF64 { OrdF64(self.as_f64()) }
}

impl Numeric for usize      { fn as_f64(&self) -> f64 { *self as f64 } }
impl Numeric for u64        { fn as_f64(&self) -> f64 { *self as f64 } }
impl Numeric for u32        { fn as_f64(&self) -> f64 { *self as f64 } }
impl Numeric for u8         { fn as_f64(&self) -> f64 { *self as f64 } }
impl Numeric for f32        { fn as_f64(&self) -> f64 { *self as f64 } }
impl Numeric for f64        { fn as_f64(&self) -> f64 { *self        } }
impl Numeric for OrdF64     { fn as_f64(&self) -> f64 { self.0       } }

impl<N> Numeric for Option<N> where N: Numeric {
    fn as_f64(&self) -> f64 { match self { Some(n) => n.as_f64(), None => NAN } }
}

pub trait Collection { fn len(&self) -> usize; }
impl<T> Collection for Vec<T> { fn len(&self) -> usize { self.len() } }
impl<T> Collection for std::collections::VecDeque<T> { fn len(&self) -> usize { self.len() } }
impl<C> Numeric for C where C: Collection { fn as_f64(&self) -> f64 { self.len() as f64 } }

#[derive(Clone, Copy, PartialEq, PartialOrd)] pub struct OrdF64(f64);
impl Eq for OrdF64 {}
impl Ord for OrdF64 {
    fn cmp(&self, other: &Self) -> Ordering { helpers::f64_cmp(self.0, other.0) }
}

impl<I,C,E> Sort<E> for Count<C> where C: CollectionAttribute<Item=I, Entity=E> {
    fn execute(&mut self, data: DataPtr, vector: Vec<E>, direction: sort::Direction) -> Vec<E> {
        let mut vector: Vec<E> = vector.into_iter()
            .sorted_by_key(|e| self.calculate(data.clone(), &e))
            .collect();
        if direction.descending() { vector.reverse() }
        vector
    }
}

impl<I,C,E>/*baby*/ Sort<E> for Min<C> where C: CollectionAttribute<Item=I, Entity=E>, I: Ord {
    fn execute(&mut self, data: DataPtr, vector: Vec<E>, direction: sort::Direction) -> Vec<E> {
        let mut vector: Vec<E> = vector.into_iter()
            .sorted_by_key(|e| self.calculate(data.clone(), &e))
            .collect();
        if direction.descending() { vector.reverse() }
        vector
    }
}

impl<I,C,E> Sort<E> for Max<C> where C: CollectionAttribute<Item=I, Entity=E>, I: Ord {
    fn execute(&mut self, data: DataPtr, vector: Vec<E>, direction: sort::Direction) -> Vec<E> {
        let mut vector: Vec<E> = vector.into_iter()
            .sorted_by_key(|e| self.calculate(data.clone(), &e))
            .collect();
        if direction.descending() { vector.reverse() }
        vector
    }
}

impl<I,C,E>/*baby*/ Sort<E> for Mean<C> where C: CollectionAttribute<Item=I, Entity=E>, I: Numeric {
    fn execute(&mut self, data: DataPtr, vector: Vec<E>, direction: sort::Direction) -> Vec<E> {
        let mut vector: Vec<E> = vector.into_iter()
            .map(|e| (self.calculate(data.clone(), &e), e))
            .sorted_by(|a, b| helpers::option_f64_cmp(&a.0, &b.0))
            .map(|(_, e)| e)
            .collect();
        if direction.descending() { vector.reverse() }
        vector
    }
}

impl<I,C,E> Sort<E> for Median<C> where C: CollectionAttribute<Item=I, Entity=E>, I: Ord + Numeric {
    fn execute(&mut self, data: DataPtr, vector: Vec<E>, direction: sort::Direction) -> Vec<E> {
        let mut vector: Vec<E> = vector.into_iter()
            .map(|e| (self.calculate(data.clone(), &e), e))
            .sorted_by(|a, b| helpers::option_f64_cmp(&a.0, &b.0))
            .map(|(_, e)| e)
            .collect();
        if direction.descending() { vector.reverse() }
        vector
    }
}

impl<I,C,E>/*baby*/ NumericalAttribute for Count<C> where C: CollectionAttribute<Item=I, Entity=E> {
    type Entity = E;
    type Number = usize;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(self.0.len(database,entity))
    }
}

impl<I,C,E> NumericalAttribute for Min<C> where C: CollectionAttribute<Item=I, Entity=E>, I: Ord {
    type Entity = E;
    type Number = I;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        self.0.items(database, entity).into_iter().min()
    }
}

impl<I,C,E>/*baby*/ NumericalAttribute for Max<C> where C: CollectionAttribute<Item=I, Entity=E>, I: Ord {
    type Entity = E;
    type Number = I;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        self.0.items(database, entity).into_iter().max()
    }
}

impl<I,C,E> NumericalAttribute for Mean<C> where C: CollectionAttribute<Item=I, Entity=E>, I: Numeric {
    type Entity = E;
    type Number = f64;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        let items: Vec<f64> =
            self.0.items(database, entity).into_iter().map(|n| n.as_f64()).collect();

        if items.len() == 0 { None }
        else { Some(items.iter().sum::<f64>() as f64 / items.len() as f64) }
    }
}

// FIXME change Option<Self::Number> in result to Self::Number
impl<I,C,E>/*baby*/ NumericalAttribute for Median<C> where C: CollectionAttribute<Item=I, Entity=E>, I: Ord + Numeric /*Into<f64> + Clone*/ {
    type Entity = E;
    type Number = f64;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        let mut items= self.0.items(database, entity);

        let length = items.len();
        if length == 0 { return None }
        if length == 1 { return Some(f64::from(items[0].as_f64())) }

        items.sort();

        if length % 2 != 0usize { return Some(items[length / 2].as_f64()) }

        let left = items[(length / 2) - 1].as_f64();
        let right = items[(length / 2)].as_f64();
        return Some((left + right) / 2f64)
    }
}

impl<I,C,E> NumericalAttribute for Ratio<C> where C: CollectionAttribute<Item=I, Entity=E> {
    type Entity = E;
    type Number = f64;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(self.0.len(database.clone(), entity) as f64 / self.0.parent_len(database, entity) as f64)
    }
}

impl<I,C,E>/*baby*/ Select<E> for Count<C> where C: CollectionAttribute<Item=I, Entity=E> {
    type Entity = Option<usize>;
    fn select(&self, data: DataPtr, entity: E) -> Self::Entity {
        self.calculate(data, &entity)
    }
}

impl<I,C,E> Select<E> for Min<C> where C: CollectionAttribute<Item=I, Entity=E>, I: Ord  {
    type Entity = Option<I>;
    fn select(&self, data: DataPtr, entity: E) -> Self::Entity {
        self.calculate(data, &entity)
    }
}

impl<I,C,E>/*baby*/ Select<E> for Max<C> where C: CollectionAttribute<Item=I, Entity=E>, I: Ord {
    type Entity = Option<I>;
    fn select(&self, data: DataPtr, entity: E) -> Self::Entity {
        self.calculate(data, &entity)
    }
}

impl<I,C,E> Select<E> for Mean<C> where C: CollectionAttribute<Item=I, Entity=E>, I: Numeric {
    type Entity = f64;
    fn select(&self, data: DataPtr, entity: E) -> Self::Entity {
        self.calculate(data, &entity).unwrap_or(NAN)
    }
}

impl<I,C,E>/*baby*/ Select<E> for Median<C> where C: CollectionAttribute<Item=I, Entity=E>, I: Ord + Numeric {
    type Entity = f64;
    fn select(&self, data: DataPtr, entity: E) -> Self::Entity {
        self.calculate(data, &entity).unwrap_or(NAN)
    }
}