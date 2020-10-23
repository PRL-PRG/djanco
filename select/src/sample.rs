use rand::seq::IteratorRandom;
use rand_pcg::Pcg64Mcg;
use rand::SeedableRng;

use crate::data::*;
use crate::attrib::*;
use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};
use itertools::Itertools;
use std::iter::FromIterator;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)] pub struct Top(pub usize);
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)] pub struct Distinct<S, C>(pub S, pub C);
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)] pub struct Random(pub usize);

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)] pub struct IdenticalCommits;

pub trait SimilarityCriterion<T> {
    type Item;
    type Similarity: Similarity<Self::Item>;
    fn from(&self, data: DataPtr, thing: &T) -> Self::Similarity;
}
pub trait Similarity<T>: Eq + Hash { }

pub struct MinRatio<T> { min_ratio: f64, things: BTreeSet<T> }
impl<T> Hash for MinRatio<T> {
    // Everything needs to be compared explicitly.
    fn hash<H: Hasher>(&self, state: &mut H) { state.write_u64(42) }
}
impl<T> Eq for MinRatio<T> where T: Ord {}
impl<T> PartialEq for MinRatio<T> where T: Ord {
    fn eq(&self, other: &Self) -> bool {
        let mine: f64 = self.things.len() as f64;
        let same: f64 = self.things.intersection(&other.things).count() as f64;
        same / mine > self.min_ratio
    }
}
impl<T> Similarity<T> for MinRatio<T> where T: Ord {}

#[derive(Debug, Clone, Copy)] pub struct Ratio<A>(pub A, pub f64);
impl<A,I,T> SimilarityCriterion<T> for Ratio<A> where A: CollectionAttribute<Entity=T, Item=I>, I: Ord {
    type Item = I;
    type Similarity = MinRatio<Self::Item>;
    fn from(&self, data: DataPtr, thing: &T) -> Self::Similarity {
        let things = self.0.items(data, thing);
        MinRatio { min_ratio: self.1, things: BTreeSet::from_iter(things.into_iter()) }
    }
}

impl<S,T,C> Sample<T> for Distinct<S, C> where S: Sample<T>, C: SimilarityCriterion<T> {
    fn make_selection(&mut self, data: DataPtr, iter: impl Iterator<Item=T>) -> Vec<T> {
        let data_for_filtering = data.clone();
        let criterion = &self.1;
        let filtered_iter= iter.unique_by(|p| {
            criterion.from(data_for_filtering.clone(), p)
        });
        self.0.make_selection(data, filtered_iter)
    }
}

impl<T> Sample<T> for Top {
    fn make_selection(&mut self, _: DataPtr, iter: impl Iterator<Item=T>) -> Vec<T> {
        iter.take(self.0).collect()
    }
}

impl<T> Sample<T> for Random {
    fn make_selection(&mut self, data: DataPtr, iter: impl Iterator<Item=T>) -> Vec<T> {
        let mut rng = Pcg64Mcg::from_seed(untangle!(data).seed().to_be_bytes());
        iter.choose_multiple(&mut rng, self.0)
    }
}