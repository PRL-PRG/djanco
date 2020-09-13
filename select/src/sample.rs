use rand::seq::IteratorRandom;
use rand_pcg::Pcg64Mcg;
use rand::SeedableRng;

use crate::data::*;
use crate::attrib::*;

#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Top(pub usize);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Unique<D>(pub usize, D);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Random(pub usize);

impl<T> Sample<T> for Top {
    fn execute(&mut self, _: DataPtr, vector: Vec<T>) -> Vec<T> {
        vector.into_iter().take(self.0).collect()
    }
}

impl<T> Sample<T> for Random {
    fn execute(&mut self,data: DataPtr, vector: Vec<T>) -> Vec<T> {
        let mut rng = Pcg64Mcg::from_seed(untangle!(data).seed().to_be_bytes());
        vector.into_iter().choose_multiple(&mut rng, self.0)
    }
}