use std::collections::BTreeMap;
use std::path::PathBuf;
use std::marker::PhantomData;
use std::fs::{File, create_dir_all};
use std::error::Error;

use crate::log::Log;
use crate::weights_and_measures::*;

use super::persistent::{PERSISTENT_EXTENSION, Persistent, PersistentCollection};
use super::source::Source;

pub trait ItemExtractor {
    type Key: Clone + Ord + Persistent + Weighed;
    type Value: Clone + Persistent + Weighed;
}

pub trait SourceItemExtractor: ItemExtractor {
    fn extract(item_id: Self::Key, source: &Source) -> Self::Value;
}

pub trait SingleItemExtractor: ItemExtractor {
    type A;
    fn extract(item_id: Self::Key, source: &Source, a: &Self::A) -> Self::Value;
}

pub trait DoubleItemExtractor: ItemExtractor {
    type A; type B;
    fn extract(item_id: Self::Key, source: &Source, a: &Self::A, b: &Self::B) -> Self::Value;
}

pub trait TripleItemExtractor: ItemExtractor {
    type A; type B; type C;
    fn extract(item_id: Self::Key, source: &Source, a: &Self::A, b: &Self::B, c: &Self::C) -> Self::Value;
}

pub struct LazyMap<E: ItemExtractor> {
    pub log: Log,
    pub name: String,
    pub cache_path: Option<PathBuf>,
    pub cache_dir: Option<PathBuf>,
    map: BTreeMap<E::Key, E::Value>,
    extractor: PhantomData<E>,
    new_values: usize,
    loaded: bool,
}

// impl<E> PersistentCollection for LazyMap<E> where E: ItemExtractor {
//     type Collection = BTreeMap<E::Key, Option<E::Value>>;

//     fn name(&self) -> String { self.name.clone() }
//     fn log(&self) -> &Log { &self.log }
//     fn cache_path(&self) -> &Option<PathBuf> { &self.cache_path }
//     fn cache_dir(&self) -> &Option<PathBuf> { &self.cache_dir }
//     fn collection(&self) -> &Option<Self::Collection> { Some(self.vector) }
//     fn set_collection(&mut self, vector: Self::Collection) { self.vector = vector }
// }

impl<E> LazyMap<E> where E: ItemExtractor {
    pub fn new<Sa, Sb>(name: Sa, log: Log, dir: Sb) -> Self where Sa: Into<String>, Sb: Into<String> {
        let name = name.into();
        let (cache_dir, cache_path) = Self::setup_files(name.clone(), dir);
        LazyMap { 
            name, 
            log, 
            cache_path: Some(cache_path),
            cache_dir: Some(cache_dir),
            map: BTreeMap::new(),
            extractor: PhantomData,
            new_values: 0usize,
            loaded: false,
        }
    }

    pub fn new_without_cache<S>(name: S, log: Log) -> Self where S: Into<String> {
        LazyMap { 
            name: name.into(), 
            log, 
            cache_path: None, 
            cache_dir: None, 
            map: BTreeMap::new(), 
            extractor: PhantomData,
            new_values: 0usize, 
            loaded: false,
        }
    }

    pub fn without_cache(mut self) -> Self {
        self.cache_dir = None;
        self.cache_path = None;
        self
    }

    fn setup_files<Sa,Sb>(name: Sa, dir: Sb) -> (PathBuf, PathBuf)
        where Sa: Into<String>, Sb: Into<String>  {

        let mut cache_dir = PathBuf::new();
        cache_dir.push(std::path::Path::new(dir.into().as_str()));

        let mut cache_path = cache_dir.clone();
        cache_path.push(std::path::Path::new(name.into().as_str()));
        cache_path.set_extension(PERSISTENT_EXTENSION);

        (cache_dir, cache_path)
    }

    pub fn iter(&self) -> impl Iterator<Item=(&E::Key, &E::Value)> {
        self.map.iter()
    }

    fn get_or<'a, F>(&'a mut self, item_id: E:: Key, extract: F) -> Option<&'a E::Value> 
        where F: Fn(&mut Self, E:: Key) -> E::Value {

//        let value = self.map.get(item_id);

        let value_is_missing = !self.map.contains_key(&item_id);

        // Not retrieved yet  
        if value_is_missing && !self.loaded {
            self.load_from_cache().unwrap(); // Probably not the best thing to do here: unwrap
            return self.get_or(item_id, extract)
        }

        // Not retrieved yet, but cache exists
        if value_is_missing {            
            let value = extract(self, item_id.clone());
            self.new_values += 1;
            self.map.insert(item_id.clone(), value);
            // Continue to the next if.
        }

        // Already retrieved
        return self.map.get(&item_id)

        // note:ifs, not match to avoid double mutable borrow of self/self+value
    }

    fn load_from_cache(&mut self) -> Result<(), Box<dyn Error>> {
        let reader = File::open(&self.cache_path.as_ref().unwrap())?; // Probably not the best solution to unwrap
        self.map = serde_cbor::from_reader(reader)?;
        Ok(())
    }
    
    fn store_to_cache(&mut self) -> Result<(), Box<dyn Error>> {
        create_dir_all(&self.cache_dir.as_ref().unwrap())?; // Probably not the best solution to unwrap
        let writer = File::create(&self.cache_path.as_ref().unwrap())?; // Probably ot the best solution to unwrap
        serde_cbor::to_writer(writer, &self.map)?;
        Ok(())
    }
}

impl<E> Drop for LazyMap<E> where E: ItemExtractor {
    fn drop(&mut self) {
        if self.new_values > 0 {
            self.store_to_cache().unwrap()
        }
    }
}

impl<E> LazyMap<E> where E: SourceItemExtractor {
    pub fn get(&mut self, item_id: E:: Key, source: &Source) -> Option<&E::Value> {
        self.get_or(item_id, |this: &mut Self, item_id: E:: Key| { E::extract(item_id, source) })
    }
}

impl<E> LazyMap<E> where E: SingleItemExtractor {
    pub fn get_one(&mut self, item_id: E:: Key, source: &Source, a: &E::A) -> Option<&E::Value> {
        self.get_or(item_id, |this: &mut Self, item_id: E:: Key| { E::extract(item_id, source, a) })
    }
}

impl<E> LazyMap<E> where E: DoubleItemExtractor {
    pub fn get_two(&mut self, item_id: E:: Key, source: &Source, a: &E::A, b: &E::B) -> Option<&E::Value> {
        self.get_or(item_id, |this: &mut Self, item_id: E:: Key| { E::extract(item_id, source, a, b) })
    }
}

impl<E> LazyMap<E> where E: TripleItemExtractor {
    pub fn get_three(&mut self, item_id: E:: Key, source: &Source, a: &E::A, b: &E::B, c: &E::C) -> Option<&E::Value> {
        self.get_or(item_id, |this: &mut Self, item_id: E:: Key| { E::extract(item_id, source, a, b, c) })
    }
}

