use std::collections::BTreeMap;
use std::path::PathBuf;
use std::marker::PhantomData;
use std::fs::{File, create_dir_all};
use std::error::Error;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::log::{Log, Verbosity};
use crate::weights_and_measures::*;

use super::source::Source;

pub static PERSISTENT_EXTENSION: &str = "cbor";

pub trait Persistent: Serialize + DeserializeOwned {}
impl<T> Persistent for T where T: Serialize + DeserializeOwned {}

pub trait VectorExtractor {
    type Value: Clone + Persistent + Weighed;
}

pub trait SourceVectorExtractor: VectorExtractor {
    fn extract(source: &Source) -> Vec<Self::Value>;
}

pub trait SingleVectorExtractor: VectorExtractor {
    type A;
    fn extract(source: &Source, a: &Self::A) -> Vec<Self::Value>;
}

pub trait DoubleVectorExtractor: VectorExtractor {
    type A; type B;
    fn extract(source: &Source, a: &Self::A, b: &Self::B) -> Vec<Self::Value>;
}

pub trait TripleVectorExtractor: VectorExtractor {
    type A; type B; type C;
    fn extract(source: &Source, a: &Self::A, b: &Self::B, c: &Self::C) -> Vec<Self::Value>;
}

pub trait MapExtractor {
    type Key:   Ord + Persistent + Weighed;
    type Value: Clone + Persistent + Countable + Weighed;
}

pub trait SourceMapExtractor: MapExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value>;
}

pub trait SingleMapExtractor: MapExtractor {
    type A;
    fn extract(source: &Source, a: &Self::A) -> BTreeMap<Self::Key, Self::Value>;
}

pub trait DoubleMapExtractor: MapExtractor {
    type A; type B;
    fn extract(source: &Source, a: &Self::A, b: &Self::B) -> BTreeMap<Self::Key, Self::Value>;
}

pub trait TripleMapExtractor: MapExtractor {
    type A; type B; type C;
    fn extract(source: &Source, a: &Self::A, b: &Self::B, c: &Self::C) -> BTreeMap<Self::Key, Self::Value>;
}

pub trait QuadrupleMapExtractor : MapExtractor {
    type A; type B; type C; type D;
    fn extract(source: &Source, a: &Self::A, b: &Self::B, c: &Self::C, d: &Self::D) -> BTreeMap<Self::Key, Self::Value>;
}

// pub trait QuadrupleMapExtractor: MapExtractor {
//     type A; type B; type C; type D;
//     fn extract(a: &Self::A, b: &Self::B, c: &Self::C, d: &Self::D) -> BTreeMap<Self::Key, Self::Value>;
// }

pub trait PersistentCollection {
    type Collection: Persistent + Countable + Weighed;

    fn setup_files<Sa,Sb>(name: Sa, dir: Sb) -> (PathBuf, PathBuf)
        where Sa: Into<String>, Sb: Into<String>  {

        let mut cache_dir = PathBuf::new();
        cache_dir.push(std::path::Path::new(dir.into().as_str()));

        let mut cache_path = cache_dir.clone();
        cache_path.push(std::path::Path::new(name.into().as_str()));
        cache_path.set_extension(PERSISTENT_EXTENSION);

        (cache_dir, cache_path)
    }

    fn name(&self) -> String;
    fn log(&self) -> &Log;
    fn cache_path(&self) -> &Option<PathBuf>;
    fn cache_dir(&self) -> &Option<PathBuf>;
    fn collection(&self) -> &Option<Self::Collection>;
    //fn weigh(&self) -> usize;
    fn set_collection(&mut self, collection: Self::Collection);

    fn grab_collection(&mut self) -> &Self::Collection {
        self.collection().as_ref().unwrap()
    }
    fn is_loaded(&self) -> bool {
        self.collection().is_some()
    }
    fn already_cached(&self) -> bool {
        self.cache_path().as_ref().map(|p| p.is_file()).unwrap_or(false)
    }
    fn skip_caching(&self) -> bool {
        self.cache_path().is_none()
    }

    fn load_from_cache(&mut self) -> Result<(), Box<dyn Error>> {
        let reader = File::open(&self.cache_path().as_ref().unwrap())?; // Probably ot the best solution to unwrap
        self.set_collection(serde_cbor::from_reader(reader)?);
        Ok(())
    }
    fn store_to_cache(&mut self) -> Result<(), Box<dyn Error>> {
        create_dir_all(&self.cache_dir().as_ref().unwrap())?; // Probably ot the best solution to unwrap
        let writer = File::create(&self.cache_path().as_ref().unwrap())?; // Probably ot the best solution to unwrap
        serde_cbor::to_writer(writer, &self.grab_collection())?;
        Ok(())
    }
    fn data_from_loader<F>(&mut self, mut load: F) -> &Self::Collection
        where F: FnMut() -> Self::Collection {

        if self.collection().is_none() {
            if self.already_cached() {
                let mut event = self.log().start(Verbosity::Log, format!("loading {} from cache {}", self.name(), self.cache_path().as_ref().unwrap().to_str().unwrap()));
                self.load_from_cache().unwrap();
                event.counted(self.collection().as_ref().map_or(0, |c| c.count_items()));
                event.weighed(self.collection().as_ref().unwrap());
                self.log().end(event);
            } else {
                let mut event = self.log().start(Verbosity::Log, format!("loading {} from source", self.name()));
                self.set_collection(load());
                event.counted(self.collection().as_ref().map_or(0, |c| c.count_items()));
                event.weighed(self.collection().as_ref().unwrap());
                self.log().end(event);

                if !self.skip_caching() {
                    let event = self.log().start(Verbosity::Log, format!("storing {} into cache at {}", self.name(), self.cache_path().as_ref().unwrap().to_str().unwrap()));
                    self.store_to_cache().unwrap();
                    self.log().end(event);
                }
            }
        }
        self.grab_collection()
    }
}

pub struct PersistentVector<E: VectorExtractor> {
    log: Log,
    name: String,
    cache_path: Option<PathBuf>,
    cache_dir: Option<PathBuf>,
    vector: Option<Vec<E::Value>>,
    extractor: PhantomData<E>,
}

impl<E> PersistentCollection for PersistentVector<E> where E: VectorExtractor {
    type Collection = Vec<E::Value>;
    //fn weigh(&self) -> usize { Self.weight_in_bytes() }
    fn name(&self) -> String { self.name.clone() }
    fn log(&self) -> &Log { &self.log }
    fn cache_path(&self) -> &Option<PathBuf> { &self.cache_path }
    fn cache_dir(&self) -> &Option<PathBuf> { &self.cache_dir }
    fn collection(&self) -> &Option<Self::Collection> { &self.vector }
    fn set_collection(&mut self, vector: Self::Collection) { self.vector = Some(vector) }
}

impl<E> PersistentVector<E> where E: VectorExtractor {
    pub fn new<Sa, Sb>(name: Sa, dir: Sb, log: &Log) -> Self where Sa: Into<String>, Sb: Into<String> {
        let name = name.into();
        let (cache_dir, cache_path) = Self::setup_files(name.clone(), dir);
        PersistentVector { name, log: log.clone(), cache_path: Some(cache_path), cache_dir: Some(cache_dir), vector: None, extractor: PhantomData }
    }
    pub fn new_without_cache<S>(name: S, log: &Log) -> Self where S: Into<String> {
        PersistentVector { name: name.into(), log: log.clone(), cache_path: None, cache_dir: None, vector: None, extractor: PhantomData }
    }
    pub fn without_cache(mut self) -> Self {
        self.cache_dir = None;
        self.cache_path = None;
        self
    }
    pub fn iter(&self) -> impl Iterator<Item=&E::Value> {
        self.vector.as_ref().map(|vector| vector.iter())
            .expect("Attempted to iterate over persistent vector before initializing it")
    }
}

impl<E> PersistentVector<E> where E: SourceVectorExtractor {
    pub fn load_from_source(&mut self, source: &Source) -> &Vec<E::Value> {
        self.data_from_loader(|| { E::extract(source) })
    }
}

impl<E,A> PersistentVector<E> where E: SingleVectorExtractor<A=A> {
    pub fn load_from_one(&mut self, source: &Source, input: &A) -> &Vec<E::Value> {
        self.data_from_loader(|| { E::extract(source, input) })
    }
}

impl<E,A,B> PersistentVector<E> where E: DoubleVectorExtractor<A=A, B=B> {
    pub fn load_from_two(&mut self, source: &Source, input_a: &A, input_b: &B) -> &Vec<E::Value> {
        self.data_from_loader(|| { E::extract(source, input_a, input_b) })
    }
}

impl<E,A,B,C> PersistentVector<E> where E: TripleVectorExtractor<A=A, B=B, C=C> {
    pub fn load_from_three(&mut self, source: &Source, input_a: &A, input_b: &B, input_c: &C) -> &Vec<E::Value> {
        self.data_from_loader(|| { E::extract(source, input_a, input_b, input_c) })
    }
}

pub struct PersistentMap<E: MapExtractor> {
    log: Log,
    name: String,
    cache_path: Option<PathBuf>,
    cache_dir: Option<PathBuf>,
    map: Option<BTreeMap<E::Key, E::Value>>,
    extractor: PhantomData<E>,
}

impl<E> PersistentCollection for PersistentMap<E> where E: MapExtractor {
    type Collection = BTreeMap<E::Key, E::Value>;
    //fn weigh(&self) -> usize { Self.weight_in_bytes() }
    fn name(&self) -> String { self.name.clone() }
    fn log(&self) -> &Log { &self.log }
    fn cache_path(&self) -> &Option<PathBuf> { &self.cache_path }
    fn cache_dir(&self) -> &Option<PathBuf> { &self.cache_dir }
    fn collection(&self) -> &Option<Self::Collection> { &self.map }
    fn set_collection(&mut self, map: Self::Collection) { self.map = Some(map) }
}

impl<E> PersistentMap<E> where E: MapExtractor {
    pub fn new<Sa, Sb>(name: Sa, log: Log, dir: Sb) -> Self where Sa: Into<String>, Sb: Into<String> {
        let name = name.into();
        let (cache_dir, cache_path) = Self::setup_files(name.clone(), dir);
        PersistentMap { name, log, cache_path: Some(cache_path), cache_dir: Some(cache_dir), map: None, extractor: PhantomData }
    }
    pub fn new_without_cache<S>(name: S, log: Log) -> Self where S: Into<String> {
        PersistentMap { name: name.into(), log, cache_path: None, cache_dir: None, map: None, extractor: PhantomData }
    }
    pub fn without_cache(mut self) -> Self {
        self.cache_dir = None;
        self.cache_path = None;
        self
    }
    pub fn iter(&self) -> impl Iterator<Item=(&E::Key, &E::Value)> {
        self.map.as_ref().map(|vector| vector.iter())
            .expect("Attempted to iterate over persistent map before initializing it")
    }
}

impl<E> PersistentMap<E> where E: SourceMapExtractor {
    pub fn load_from_source(&mut self, source: &Source) -> &BTreeMap<E::Key, E::Value> {
        self.data_from_loader(|| { E::extract(source) })
    }
}

impl<E,A> PersistentMap<E> where E: SingleMapExtractor<A=A> {
    pub fn load_from_one(&mut self, source: &Source, input: &A) -> &BTreeMap<E::Key, E::Value> {
        self.data_from_loader(|| { E::extract(source, input) })
    }
}

impl<E,A,B> PersistentMap<E> where E: DoubleMapExtractor<A=A,B=B> {
    pub fn load_from_two(&mut self, source: &Source, input_a: &A, input_b: &B) -> &BTreeMap<E::Key, E::Value> {
        self.data_from_loader(|| { E::extract(source, input_a, input_b) })
    }
}

impl<E,A,B,C> PersistentMap<E> where E: TripleMapExtractor<A=A,B=B,C=C> {
    pub fn load_from_three(&mut self, source: &Source, input_a: &A, input_b: &B, input_c: &C) -> &BTreeMap<E::Key, E::Value> {
        self.data_from_loader(|| { E::extract(source, input_a, input_b, input_c) })
    }
}

impl<E,A,B,C, D> PersistentMap<E> where E: QuadrupleMapExtractor<A=A,B=B,C=C,D=D> {
    pub fn load_from_four(&mut self, source: &Source, input_a: &A, input_b: &B, input_c: &C, input_d : &D) -> &BTreeMap<E::Key, E::Value> {
        self.data_from_loader(|| { E::extract(source, input_a, input_b, input_c, input_d) })
    }
}

// impl<E> IntoIterator for PersistentMap<E> where E: MapExtractor {
//     type Item = (E::Key, E::Value);
//     type IntoIter = ();
//
//     fn into_iter(self) -> Self::IntoIter {
//         self.map.as_ref().
//     }
// }

// impl<E,A,B,C,D> PersistentMap<E> where E: QuadrupleExtractor<A=A,B=B,C=C,D=D>{
//     #[allow(dead_code)] pub fn load_from_four(&mut self, input_a: &A, input_b: &B, input_c: &C, input_d: &D) -> &BTreeMap<E::Key, E::Value> {
//         self.data_from_loader(|| { E::extract(input_a, input_b, input_c, input_d) })
//     }
// }

// pub struct Created

