use rand::seq::IteratorRandom;
use rand_pcg::Pcg64Mcg;
use rand::SeedableRng;
use crate::objects::Project;
use crate::attrib::SampleEach;
use crate::data::DataPtr;
use std::borrow::Borrow;

#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Top(pub usize);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Unique<D>(pub usize, D);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Random(pub usize);

impl SampleEach for Top {
    fn sample(&self, _database: DataPtr, /*key: &Self::Key,*/ projects: Vec<Project>) -> Vec<Project> {
        projects.into_iter().take(self.0).collect()
    }
}

impl SampleEach for Random {
    fn sample(&self, database: DataPtr, /*key: &Self::Key,*/ projects: Vec<Project>) -> Vec<Project> {
        let mut rng = Pcg64Mcg::from_seed(database.as_ref().borrow().seed().to_be_bytes());
        projects.into_iter().choose_multiple(&mut rng, self.0)
    }
}