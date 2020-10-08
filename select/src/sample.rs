use rand::seq::IteratorRandom;
use rand_pcg::Pcg64Mcg;
use rand::SeedableRng;

use crate::data::*;
use crate::attrib::*;
use crate::log::*;
use crate::objects::NamedEntity;

#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Top(pub usize);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Unique<D>(pub usize, D);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Random(pub usize);

impl<T> Sample<T> for Top where T: NamedEntity {
    fn execute(&mut self, data: DataPtr, vector: Vec<T>) -> Vec<T> {
        log_item!(untangle_mut!(data).spec().log_level,
                  format!("sampling top {} {}", self.0, T::plural()));
        let results: Vec<T> = vector.into_iter().take(self.0).collect();
        log_addendum!(untangle_mut!(data).spec().log_level,
                      format!("{} {} after sampling", results.len(), T::plural()));
        results
    }
}

impl<T> Sample<T> for Random where T: NamedEntity {
    fn execute(&mut self, data: DataPtr, vector: Vec<T>) -> Vec<T> {
        log_item!(untangle_mut!(data).spec().log_level,
                  format!("sampling random {} {}", self.0, T::plural()));
        let mut rng = Pcg64Mcg::from_seed(untangle!(data).seed().to_be_bytes());
        let results: Vec<T> = vector.into_iter().choose_multiple(&mut rng, self.0);
        log_addendum!(untangle_mut!(data).spec().log_level,
                      format!("{} {} after sampling", results.len(), T::plural()));
        results
    }
}