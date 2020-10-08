use crate::attrib::{NumericalAttribute, CollectionAttribute, Sort};
use crate::data::DataPtr;
use itertools::Itertools;
use crate::helpers;
use std::f64::NAN;
use std::cmp::Ordering;

// use std::iter::Sum;

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

#[derive(Clone, Copy, PartialEq, PartialOrd)] pub struct OrdF64(f64);
impl Eq for OrdF64 {}
impl Ord for OrdF64 {
    fn cmp(&self, other: &Self) -> Ordering { helpers::f64_cmp(self.0, other.0) }
}

//TODO count etc
impl<I,C,E> Sort<E> for Median<C> where C: CollectionAttribute<Item=I, Entity=E>, I: Ord + Numeric {
    fn execute(&mut self, data: DataPtr, vector: Vec<E>) -> Vec<E> {
        vector.into_iter()
            .map(|e| (self.calculate(data.clone(), &e), e))
            .sorted_by(|a, b| helpers::option_f64_cmp(&a.0, &b.0))
            .map(|(_, e)| e)
            .collect()
    }
}

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

// FIXME change Option<Self::Number> in result to Self::Number
impl<I,C,E> NumericalAttribute for Median<C> where C: CollectionAttribute<Item=I, Entity=E>, I: Ord + Numeric /*Into<f64> + Clone*/ {
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
        return Some(left + right / 2f64)
    }
}

impl<I,C,E>/*baby*/ NumericalAttribute for Ratio<C> where C: CollectionAttribute<Item=I, Entity=E> {
    type Entity = E;
    type Number = f64;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(self.0.len(database.clone(), entity) as f64 / self.0.parent_len(database, entity) as f64)
    }
}

// impl<C,I,T> Sort<T> for Median<C> where C: CollectionAttribute<Item=I, Entity=T> {
//     fn execute(&mut self, data: DataPtr, vector: Vec<T>) -> Vec<T> {
//         // vector.sort_by_key(|c| )
//         // self.0.items(data, )
//         unimplemented!()
//     }
// }