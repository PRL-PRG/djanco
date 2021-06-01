use std::borrow::Borrow;
use std::cell::Ref;
use std::cell::RefCell;
use std::cell::RefMut;
use std::path::PathBuf;
use std::collections::{BTreeMap, HashMap, BTreeSet};
use std::error::Error;
use std::fs::{File, create_dir_all};
use std::rc::Rc;
use core::marker::PhantomData;

use serde_json::Value as JSON;
use chrono::DateTime;

use crate::persistent::*;
use crate::objects::*;
use crate::log::{Log, Verbosity, Warning};
use crate::weights_and_measures::{Weighed, Countable};
use crate::source::Source;

trait MetadataFieldExtractor {
    type Value: Persistent + Weighed;
    fn get(&self, value: &JSON) -> Option<Self::Value>;
}

struct BoolExtractor;
impl MetadataFieldExtractor for BoolExtractor {
    type Value = bool;
    fn get(&self, value: &JSON) -> Option<Self::Value> {
        match value {
            JSON::Bool(b) => Some(*b),
            JSON::Null => None,
            value => panic!("Expected Bool, found {:?}", value),
        }
    }
}

struct CountExtractor;
impl MetadataFieldExtractor for CountExtractor {
    type Value = usize;
    fn get(&self, value: &JSON) -> Option<Self::Value> {
        match value {
            JSON::Number(n) if n.is_u64() => {
                let number = n
                    .as_u64().map(|n| n as usize)
                    .expect(&format!("Could not parse JSON Number {} as usize", value));
                Some(number)
            }
            JSON::Number(n) => panic!("Expected Number >= 0, found {:?}", n),
            JSON::Null => None,
            value => panic!("Expected Number, found {:?}", value),
        }
    }
}

struct StringExtractor;
impl MetadataFieldExtractor for StringExtractor {
    type Value = String;
    fn get(&self, value: &JSON) -> Option<Self::Value> {
        match value {
            JSON::String(s) => Some(s.clone()),
            JSON::Null => None,
            value => panic!("Expected String or Null, found {:?}", value),
        }
    }
}

struct TimestampExtractor;
impl MetadataFieldExtractor for TimestampExtractor {
    type Value = i64;
    fn get(&self, value: &JSON) -> Option<Self::Value> {
        match value {
            JSON::String(s) => {
                let timestamp = DateTime::parse_from_rfc3339(s)
                    .expect(&format!("Could not parse JSON String {} as RFC3339 date", value)) // Should be there, right?
                    .timestamp();
                Some(timestamp)
            }
            JSON::Null => None,
            value => panic!("Expected String representing a timestamp, found {:?}", value),
        }
    }
}

struct LanguageExtractor;
impl MetadataFieldExtractor for LanguageExtractor {
    type Value = Language;
    fn get(&self, value: &JSON) -> Option<Self::Value> {
        println!("language extraction form {:?}", value);
        match value {
            JSON::String(s) => {
                Language::from_str(s)
                    .warn(&format!("Language {} is unknown, so it will be treated as None", s))
            },
            JSON::Null => { None }
            value => panic!("Expected String, found {:?}", value),
        }
    }
}
//
// struct FieldExtractor<M: MetadataFieldExtractor>(pub &'static str, pub M);
// impl<T, M> MetadataFieldExtractor for FieldExtractor<M>
//     where M: MetadataFieldExtractor<Value=T>, T: Persistent + Weighed {
//     type Value = T;
//     fn get(&self, value: &JSON) -> Option<Self::Value> {
//         match value {
//             JSON::Object(map) => {
//                 self.1.get(&map.get(&self.0.to_owned())
//                     .expect(&format!("Could not extract field {} from JSON Object {}: no such field", value, self.0)))
//             },
//             value => panic!("Expected Object for {} found {:?}", &self.0, value),
//         }
//     }
// }

struct FieldExtractor<M: MetadataFieldExtractor>(pub &'static str, pub M);
impl<T, M> MetadataFieldExtractor for FieldExtractor<M>
    where M: MetadataFieldExtractor<Value=T>, T: Persistent + Weighed {
    type Value = T;
    fn get(&self, value: &JSON) -> Option<Self::Value> {
        match value {
            JSON::Object(map) => {
                map.get(&self.0.to_owned())
                    .map(|value| { self.1.get(value) })
                    .flatten()
            },
            JSON::Null => None,
            value => panic!("Expected Object or Null for {} found {:?}", &self.0, value),
        }
    }
}

struct MetadataCacher<M: MetadataFieldExtractor> {
    name: String,
    log: Log,
    cache_path: PathBuf,
    cache_dir: PathBuf,
    extractor: M,
    // vector: Option<BTreeMap<ProjectId, M::Value>>,
}


impl<M> MetadataCacher<M> where M: MetadataFieldExtractor {
    pub fn new<Sa, Sb>(name: Sa, dir: Sb, log: &Log, extractor: M) -> Self
        where Sa: Into<String>, Sb: Into<String> {
        let name: String = name.into();

        let mut cache_dir = PathBuf::new();
        cache_dir.push(std::path::Path::new(dir.into().as_str()));

        let mut cache_path = cache_dir.clone();
        cache_path.push(std::path::Path::new(name.as_str()));
        cache_path.set_extension(PERSISTENT_EXTENSION);

        Self { name, extractor, cache_dir, cache_path, log: log.clone() }
    }

    pub fn cache_path(&self) -> &PathBuf { &self.cache_path }

    pub fn already_cached(&self) -> bool { self.cache_path.is_file() }

    fn load_from_store(&self, metadata: &HashMap<ProjectId, serde_json::Map<String, JSON>>) -> BTreeMap<ProjectId, M::Value> {
        let mut event = self.log.start(Verbosity::Log, format!("loading metadata ({}) from store", self.name));
        let vector: BTreeMap<ProjectId, <M as MetadataFieldExtractor>::Value> = 
            metadata.iter()
                .flat_map(|(id, properties)| {
                    let property = properties.get(&self.name);
                    match property {
                        Some(property) => {
                            self.extractor.get(property)
                                .map(|e| (id.clone(), e))
                        }
                        None => {
                            eprintln!("WARNING! Attempt to retrieve property {} for project {} from property map yielded None, available keys: {}",
                            &self.name, id, properties.iter().map(|(k, _)| k.to_string()).collect::<Vec<String>>().join(","));
                            None
                        }
                    }
                }).collect();

        event.weighed(&vector);
        event.counted(vector.len());
        self.log.end(event);

        vector
    }

    fn store_to_cache(&self, vector: &BTreeMap<ProjectId, <M as MetadataFieldExtractor>::Value>) -> Result<(), Box<dyn Error>> {
        let mut event = self.log.start(Verbosity::Log, format!("storing metadata ({}) to cache at {}", self.name, self.cache_path.to_str().unwrap()));
        create_dir_all(&self.cache_dir)?;
        let writer = File::create(&self.cache_path)?;
        serde_cbor::to_writer(writer, &vector)?;
        event.weighed(vector);
        event.counted(vector.len());
        self.log.end(event);
        Ok(())
    }

    pub(crate) fn convert_into_cache(&mut self, metadata: &HashMap<ProjectId, serde_json::Map<String, JSON>>) -> Result<(), Box<dyn Error>> {
        self.store_to_cache(&self.load_from_store(metadata))
    }
}



trait MetadataSource {
    fn load_metadata(&mut self, store: &Source) -> HashMap<ProjectId, serde_json::Map<String, JSON>> {
        store.project_github_metadata()
            .map(|(id, json)| match json {
                Ok(JSON::Object(map)) =>
                    (ProjectId::from(id), map),
                Ok(other) =>
                    panic!("Unexpected JSON value for project {} for metadata: {:?}", id, other),
                Err(error) =>
                    panic!("Failed to parse JSON for project {}: {}", id, error),
            }).collect()
    }

    fn convert_all_into_cache_from_store(&mut self, store: &Source) -> Result<(), Vec<Box<dyn Error>>> {
        let metadata = self.load_metadata(store);
        self.convert_all_into_cache(&metadata)
    }

    fn prepare_dir<Sa, Sb>(name: Sa, dir: Sb) -> String where Sa: Into<String>, Sb: Into<String> {
        let name = name.into();
        let dir = {
            let mut cache_subdir = PathBuf::new();
            cache_subdir.push(std::path::Path::new(dir.into().as_str()));
            cache_subdir.push(std::path::Path::new(name.as_str()));
            cache_subdir.set_extension(PERSISTENT_EXTENSION);
            cache_subdir.to_str().unwrap().to_owned()
        };
        dir
    }

    // fn load_all_from(&mut self, metadata: &HashMap<ProjectId, serde_json::Map<String, JSON>>);
    // fn store_all_to_cache(&mut self) -> Result<(), Vec<Box<dyn Error>>>;

    fn convert_all_into_cache(&mut self, metadata: &HashMap<ProjectId, serde_json::Map<String, JSON>>) -> Result<(), Vec<Box<dyn Error>>>;
}

// macro_rules! gimme {
//     ($self:expr, $vector:ident, $store:expr, $method:ident, $key:expr) => {{
//         if !$self.loaded && !$self.$vector.already_loaded() && !$self.$vector.already_cached() {
//             $self.load_all_from_store($store);
//             $self.store_all_to_cache().unwrap();
//             $self.loaded = true;
//         }
//         $self.$vector.$method($key)
//     }}
// }

// macro_rules! gimme_iter {
//     ($self:expr, $vector:ident, $store:expr) => {{
//         if !$self.loaded && !$self.$vector.already_loaded() && !$self.$vector.already_cached() {
//             $self.load_all_from_store($store);
//             $self.store_all_to_cache().unwrap();
//             $self.loaded = true;
//         }
//         $self.$vector.iter()
//     }}
// }

macro_rules! run_and_consolidate_errors {
    ($($statements:expr),*) => {{
        let mut outcomes = vec![];
        $(outcomes.push($statements);)*
        let errors: Vec<Box<dyn Error>> =
            outcomes.into_iter()
            .filter(|r| r.is_err())
            .map(|r| r.err().unwrap())
            .collect();
        if errors.is_empty() { Ok(()) } else { Err(errors) }
    }}
}

pub struct ProjectMetadataSource {
    loaded:           bool,
    //log:              Log,
    are_forks:        MetadataCacher<BoolExtractor>,
    are_archived:     MetadataCacher<BoolExtractor>,
    are_disabled:     MetadataCacher<BoolExtractor>,
    star_gazers:      MetadataCacher<CountExtractor>,
    watchers:         MetadataCacher<CountExtractor>,
    size:             MetadataCacher<CountExtractor>,
    open_issues:      MetadataCacher<CountExtractor>,
    forks:            MetadataCacher<CountExtractor>,
    subscribers:      MetadataCacher<CountExtractor>,
    licenses:         MetadataCacher<FieldExtractor<StringExtractor>>,
    languages:        MetadataCacher<LanguageExtractor>,
    descriptions:     MetadataCacher<StringExtractor>,
    homepages:        MetadataCacher<StringExtractor>,
    has_issues:       MetadataCacher<BoolExtractor>,
    has_downloads:    MetadataCacher<BoolExtractor>,
    has_wiki:         MetadataCacher<BoolExtractor>,
    has_pages:        MetadataCacher<BoolExtractor>,
    created:          MetadataCacher<TimestampExtractor>,
    updated:          MetadataCacher<TimestampExtractor>,
    pushed:           MetadataCacher<TimestampExtractor>,
    master:           MetadataCacher<StringExtractor>,
    issues:           MetadataCacher<CountExtractor>,
    buggy_issues:     MetadataCacher<CountExtractor>,
}

impl ProjectMetadataSource {
    pub fn new<Sa, Sb>(name: Sa, log: Log, dir: Sb) -> Self where Sa: Into<String>, Sb: Into<String> {
        let dir = Self::prepare_dir(name, dir);
        ProjectMetadataSource {
            are_forks:     MetadataCacher::new("fork",              dir.as_str(), &log, BoolExtractor),
            are_archived:  MetadataCacher::new("archived",          dir.as_str(), &log, BoolExtractor),
            are_disabled:  MetadataCacher::new("disabled",          dir.as_str(), &log, BoolExtractor),
            star_gazers:   MetadataCacher::new("stargazers_count",  dir.as_str(), &log, CountExtractor),
            watchers:      MetadataCacher::new("watchers_count",    dir.as_str(), &log, CountExtractor),
            size:          MetadataCacher::new("size",              dir.as_str(), &log, CountExtractor),
            open_issues:   MetadataCacher::new("open_issues_count", dir.as_str(), &log, CountExtractor),
            forks:         MetadataCacher::new("forks",             dir.as_str(), &log, CountExtractor),
            subscribers:   MetadataCacher::new("subscribers_count", dir.as_str(), &log, CountExtractor),
            languages:     MetadataCacher::new("language",          dir.as_str(), &log, LanguageExtractor),
            descriptions:  MetadataCacher::new("description",       dir.as_str(), &log, StringExtractor),
            homepages:     MetadataCacher::new("homepage",          dir.as_str(), &log, StringExtractor),
            licenses:      MetadataCacher::new("license",           dir.as_str(), &log, FieldExtractor("name", StringExtractor)),
            has_issues:    MetadataCacher::new("has_issues",        dir.as_str(), &log, BoolExtractor),
            has_downloads: MetadataCacher::new("has_downloads",     dir.as_str(), &log, BoolExtractor),
            has_wiki:      MetadataCacher::new("has_wiki",          dir.as_str(), &log, BoolExtractor),
            has_pages:     MetadataCacher::new("has_pages",         dir.as_str(), &log, BoolExtractor),
            created:       MetadataCacher::new("created_at",        dir.as_str(), &log, TimestampExtractor),
            updated:       MetadataCacher::new("updated_at",        dir.as_str(), &log, TimestampExtractor),
            pushed:        MetadataCacher::new("pushed_at",         dir.as_str(), &log, TimestampExtractor),
            master:        MetadataCacher::new("default_branch",    dir.as_str(), &log, StringExtractor),
            issues:        MetadataCacher::new("issues_count",       dir.as_str(), &log, CountExtractor),
            buggy_issues:  MetadataCacher::new("buggy_issues_count", dir.as_str(), &log, CountExtractor),

            loaded:        false,
            //log:           log.clone(),
        }
    }
}

impl ProjectMetadataSource {
    pub fn is_fork          (&mut self, store: &Source, key: &ProjectId) -> Option<bool>     { unimplemented!() }
    pub fn is_archived      (&mut self, store: &Source, key: &ProjectId) -> Option<bool>     { unimplemented!() }
    pub fn is_disabled      (&mut self, store: &Source, key: &ProjectId) -> Option<bool>     { unimplemented!() }
    pub fn star_gazers      (&mut self, store: &Source, key: &ProjectId) -> Option<usize>    { unimplemented!() }
    pub fn watchers         (&mut self, store: &Source, key: &ProjectId) -> Option<usize>    { unimplemented!() }
    pub fn size             (&mut self, store: &Source, key: &ProjectId) -> Option<usize>    { unimplemented!() }
    pub fn open_issues      (&mut self, store: &Source, key: &ProjectId) -> Option<usize>    { unimplemented!() }
    pub fn forks            (&mut self, store: &Source, key: &ProjectId) -> Option<usize>    { unimplemented!() }
    pub fn subscribers      (&mut self, store: &Source, key: &ProjectId) -> Option<usize>    { unimplemented!() }
    pub fn license          (&mut self, store: &Source, key: &ProjectId) -> Option<String>   { unimplemented!() }
    pub fn description      (&mut self, store: &Source, key: &ProjectId) -> Option<String>   { unimplemented!() }
    pub fn homepage         (&mut self, store: &Source, key: &ProjectId) -> Option<String>   { unimplemented!() }
    pub fn language         (&mut self, store: &Source, key: &ProjectId) -> Option<Language> { unimplemented!() }
    pub fn has_issues       (&mut self, store: &Source, key: &ProjectId) -> Option<bool>     { unimplemented!() }
    pub fn has_downloads    (&mut self, store: &Source, key: &ProjectId) -> Option<bool>     { unimplemented!() }
    pub fn has_wiki         (&mut self, store: &Source, key: &ProjectId) -> Option<bool>     { unimplemented!() }
    pub fn has_pages        (&mut self, store: &Source, key: &ProjectId) -> Option<bool>     { unimplemented!() }
    pub fn created          (&mut self, store: &Source, key: &ProjectId) -> Option<i64>      { unimplemented!() }
    pub fn updated          (&mut self, store: &Source, key: &ProjectId) -> Option<i64>      { unimplemented!() }
    pub fn pushed           (&mut self, store: &Source, key: &ProjectId) -> Option<i64>      { unimplemented!() }
    pub fn master           (&mut self, store: &Source, key: &ProjectId) -> Option<String>   { unimplemented!() }
    pub fn issues           (&mut self, store: &Source, key: &ProjectId) -> Option<usize>    { unimplemented!() }
    pub fn buggy_issues     (&mut self, store: &Source, key: &ProjectId) -> Option<usize>    { unimplemented!() }
}

// impl ProjectMetadataSource {
    //pub fn is_fork_map     (&mut self, store: &Source, key: &ProjectId) -> impl Iterator<Item=(ProjectId, bool)>     { gimme_iter!(self, are_forks, store).map(|e| e.pirate()) }
    // pub fn is_archived      (&mut self, store: &Source, key: &ProjectId) -> Option<bool>     { gimme_iter!(self, are_archived)           }
    // pub fn is_disabled      (&mut self, store: &Source, key: &ProjectId) -> Option<bool>     { gimme_iter!(self, are_disabled)           }
    // pub fn star_gazers      (&mut self, store: &Source, key: &ProjectId) -> Option<usize>    { gimme_iter!(self, star_gazers)           }
    // pub fn watchers         (&mut self, store: &Source, key: &ProjectId) -> Option<usize>    { gimme_iter!(self, watchers)           }
    // pub fn size             (&mut self, store: &Source, key: &ProjectId) -> Option<usize>    { gimme_iter!(self, size)           }
    // pub fn open_issues      (&mut self, store: &Source, key: &ProjectId) -> Option<usize>    { gimme_iter!(self, open_issues)           }
    // pub fn forks            (&mut self, store: &Source, key: &ProjectId) -> Option<usize>    { gimme_iter!(self, forks)           }
    // pub fn subscribers      (&mut self, store: &Source, key: &ProjectId) -> Option<usize>    { gimme_iter!(self, subscribers)           }
    // pub fn license          (&mut self, store: &Source, key: &ProjectId) -> Option<String>   { gimme_iter!(self, licenses)           }
    // pub fn description      (&mut self, store: &Source, key: &ProjectId) -> Option<String>   { gimme_iter!(self, descriptions)           }
    // pub fn homepage         (&mut self, store: &Source, key: &ProjectId) -> Option<String>   { gimme_iter!(self, homepages)           }
    // pub fn language         (&mut self, store: &Source, key: &ProjectId) -> Option<Language> { gimme_iter!(self, languages)           }
    // pub fn has_issues       (&mut self, store: &Source, key: &ProjectId) -> Option<bool>     { gimme_iter!(self, has_issues)           }
    // pub fn has_downloads    (&mut self, store: &Source, key: &ProjectId) -> Option<bool>     { gimme_iter!(self, has_downloads)           }
    // pub fn has_wiki         (&mut self, store: &Source, key: &ProjectId) -> Option<bool>     { gimme_iter!(self, has_wiki)           }
    // pub fn has_pages        (&mut self, store: &Source, key: &ProjectId) -> Option<bool>     { gimme_iter!(self, has_pages)           }
    //pub fn created_map<'a>   (&'a mut self, store: &Source) -> impl Iterator<Item=(ProjectId, i64)> + 'a    { gimme_iter!(self, created, store).map(|(id, value)| (id.clone(), value.clone())) }
    // pub fn updated          (&mut self, store: &Source, key: &ProjectId) -> Option<i64>      { gimme_iter!(self, updated)           }
    // pub fn pushed           (&mut self, store: &Source, key: &ProjectId) -> Option<i64>      { gimme_iter!(self, pushed)           }
    // pub fn master           (&mut self, store: &Source, key: &ProjectId) -> Option<String>   { gimme_iter!(self, master)           }
    // pub fn issues           (&mut self, store: &Source, key: &ProjectId) -> Option<usize>    { gimme_iter!(self, issues)           }
    // pub fn buggy_issues     (&mut self, store: &Source, key: &ProjectId) -> Option<usize>    { gimme_iter!(self, buggy_issues)         }
//     pub fn created_map<'a>   (&'a mut self, store: &Source) -> &BTreeMap<ProjectId, i64> { 
//         self.created.data()
//     }
// }

// A glorified tuple
#[derive(Hash, Clone, Debug)]
pub struct ProjectMetadata {
    pub id: ProjectId,
    pub is_fork: Option<bool>,
    pub is_archived: Option<bool>,
    pub is_disabled: Option<bool>,
    pub star_gazers: Option<usize>,
    pub watchers: Option<usize>,
    pub size: Option<usize>,
    pub open_issues: Option<usize>,
    pub forks: Option<usize>,
    pub subscribers: Option<usize>,
    pub license: Option<String>,
    pub description: Option<String>,
    pub homepage: Option<String>,
    pub language: Option<Language>,
    pub has_issues: Option<bool>,
    pub has_downloads: Option<bool>,
    pub has_wiki: Option<bool>,
    pub has_pages: Option<bool>,
    pub created: Option<i64>,
    pub updated: Option<i64>,
    pub pushed: Option<i64>,
    pub master: Option<String>,
    pub issues: Option<usize>,
    pub buggy_issues: Option<usize>,
}

impl ProjectMetadataSource {
    // pub fn keys(&mut self, store: &Source) -> impl Iterator<Item=ProjectId> {
    //     let mut keys = BTreeSet::new();
    //     keys.append(&mut gimme_iter!(self, are_forks,     store).map(|(id, _)| id.clone()).collect());
    //     keys.append(&mut gimme_iter!(self, are_archived,  store).map(|(id, _)| id.clone()).collect());
    //     keys.append(&mut gimme_iter!(self, are_disabled,  store).map(|(id, _)| id.clone()).collect());
    //     keys.append(&mut gimme_iter!(self, star_gazers,   store).map(|(id, _)| id.clone()).collect());
    //     keys.append(&mut gimme_iter!(self, watchers,      store).map(|(id, _)| id.clone()).collect());
    //     keys.append(&mut gimme_iter!(self, size,          store).map(|(id, _)| id.clone()).collect());
    //     keys.append(&mut gimme_iter!(self, open_issues,   store).map(|(id, _)| id.clone()).collect());
    //     keys.append(&mut gimme_iter!(self, forks,         store).map(|(id, _)| id.clone()).collect());
    //     keys.append(&mut gimme_iter!(self, subscribers,   store).map(|(id, _)| id.clone()).collect());
    //     keys.append(&mut gimme_iter!(self, languages,     store).map(|(id, _)| id.clone()).collect());
    //     keys.append(&mut gimme_iter!(self, descriptions,  store).map(|(id, _)| id.clone()).collect());
    //     keys.append(&mut gimme_iter!(self, homepages,     store).map(|(id, _)| id.clone()).collect());
    //     keys.append(&mut gimme_iter!(self, licenses,      store).map(|(id, _)| id.clone()).collect());
    //     keys.append(&mut gimme_iter!(self, has_issues,    store).map(|(id, _)| id.clone()).collect());
    //     keys.append(&mut gimme_iter!(self, has_downloads, store).map(|(id, _)| id.clone()).collect());
    //     keys.append(&mut gimme_iter!(self, has_wiki,      store).map(|(id, _)| id.clone()).collect());
    //     keys.append(&mut gimme_iter!(self, has_pages,     store).map(|(id, _)| id.clone()).collect());
    //     keys.append(&mut gimme_iter!(self, created,       store).map(|(id, _)| id.clone()).collect());
    //     keys.append(&mut gimme_iter!(self, updated,       store).map(|(id, _)| id.clone()).collect());
    //     keys.append(&mut gimme_iter!(self, pushed,        store).map(|(id, _)| id.clone()).collect());
    //     keys.append(&mut gimme_iter!(self, master,        store).map(|(id, _)| id.clone()).collect());
    //     keys.append(&mut gimme_iter!(self, issues,        store).map(|(id, _)| id.clone()).collect());
    //     keys.append(&mut gimme_iter!(self, buggy_issues,  store).map(|(id, _)| id.clone()).collect());
    //     keys.into_iter()
    // }

    pub fn all_metadata(&mut self, store: &Source, key: &ProjectId) -> ProjectMetadata {
        ProjectMetadata {
            id: key.clone(),
            is_fork: self.is_fork(store, key),
            is_archived: self.is_archived(store, key),
            is_disabled: self.is_disabled(store, key),
            star_gazers: self.star_gazers(store, key),
            watchers: self.watchers(store, key),
            size: self.size(store, key),
            open_issues: self.open_issues(store, key),
            forks: self.forks(store, key),
            subscribers: self.subscribers(store, key),
            license: self.license(store, key),
            description: self.description(store, key),
            homepage: self.homepage(store, key),
            language: self.language(store, key),
            has_issues: self.has_issues(store, key),
            has_downloads: self.has_downloads(store, key),
            has_wiki: self.has_wiki(store, key),
            has_pages: self.has_pages(store, key),
            created: self.created(store, key),
            updated: self.updated(store, key),
            pushed: self.pushed(store, key),
            master: self.master(store, key),
            issues: self.issues(store, key),
            buggy_issues: self.buggy_issues(store, key),
        }
    }

    // pub fn iter<'a>(&'a mut self, store: &'a Source) -> impl Iterator<Item=ProjectMetadata> + 'a {
    //     self.keys(store)
    //         .map(|project_id| self.all_metadata(store, &project_id))
    //         .collect::<Vec<ProjectMetadata>>()
    //         .into_iter()
    // }
}

impl MetadataSource for ProjectMetadataSource {
    // fn load_all_from(&mut self, metadata: &HashMap<ProjectId, serde_json::Map<String, JSON>>) {
    //     macro_rules! load_from_store {
    //         ($($id:ident),+) => {
    //             $( self.$id.load_from_store(metadata); )*
    //         }
    //     }
    //     load_from_store!(are_forks, are_archived, are_disabled, star_gazers, watchers, size,
    //                     open_issues, forks, subscribers, licenses, languages, descriptions,
    //                     homepages, has_issues, has_downloads, has_wiki, has_pages, created,
    //                     updated, pushed, master, issues, buggy_issues);
    // }

    // fn store_all_to_cache(&mut self) -> Result<(), Vec<Box<dyn Error>>> {
    //     macro_rules! save_to_store {
    //         ($($id:ident),+) => {
    //             run_and_consolidate_errors!(
    //                 $( self.$id.store_to_cache()  ),*
    //             )
    //         }
    //     }
    //     save_to_store! (are_forks, are_archived, are_disabled, star_gazers, watchers, size,
    //                     open_issues, forks, subscribers, licenses, languages, descriptions,
    //                     homepages, has_issues, has_downloads, has_wiki, has_pages, created,
    //                     updated, pushed, master, issues, buggy_issues)
    // }

    fn convert_all_into_cache(&mut self, metadata: &HashMap<ProjectId, serde_json::Map<String, JSON>>) -> Result<(), Vec<Box<dyn Error>>> {
        macro_rules! convert_into_store {
            ($($id:ident),+) => {
                run_and_consolidate_errors!(
                    $( self.$id.convert_into_cache(metadata)  ),*
                )
            }
        }
        convert_into_store! (are_forks, are_archived, are_disabled, star_gazers, watchers, size,
                            open_issues, forks, subscribers, licenses, languages, descriptions,
                            homepages, has_issues, has_downloads, has_wiki, has_pages, created,
                            updated, pushed, master, issues, buggy_issues)
    }
}

// trait MetaExtractor {
//     type Key:   Ord + Persistent + Weighed;
//     type Value: Clone + Persistent + Countable + Weighed;
// }

// pub struct PersistentMeta<E: MetaExtractor> {
//     log: Log,
//     name: String,
//     cache_path: Option<PathBuf>,
//     cache_dir: Option<PathBuf>,
//     //map: Option<BTreeMap<E::Key, E::Value>>,
//     extractor: PhantomData<E>, // TODO not needed
//     metadata: Rc<MetadataSource>,
    
// }

// impl<E> PersistentCollection for PersistentMeta<E> where E: MetaExtractor {
//     type Collection = BTreeMap<E::Key, E::Value>;
//     //fn weigh(&self) -> usize { Self.weight_in_bytes() }
//     fn name(&self) -> String { self.name.clone() }
//     fn log(&self) -> &Log { &self.log }
//     fn cache_path(&self) -> &Option<PathBuf> { &self.cache_path }
//     fn cache_dir(&self) -> &Option<PathBuf> { &self.cache_dir }
//     fn collection(&self) -> &Option<Self::Collection> { unimplemented!() }
//     fn set_collection(&mut self, map: Self::Collection) { unimplemented!() }
//     fn data_from_loader<F>(&mut self, mut load: F) -> &Self::Collection where F: FnMut() -> Self::Collection {
//         if self.collection().is_none() {
//             if self.already_cached() {
//                 let mut event = self.log().start(Verbosity::Log, format!("loading {} from cache {}", self.name(), self.cache_path().as_ref().unwrap().to_str().unwrap()));
//                 self.load_from_cache().unwrap();
//                 event.counted(self.collection().as_ref().map_or(0, |c| c.count_items()));
//                 event.weighed(self.collection().as_ref().unwrap());
//                 self.log().end(event);
//             } else {
//                 let mut event = self.log().start(Verbosity::Log, format!("loading {} from source", self.name()));
//                 self.set_collection(load());
//                 event.counted(self.collection().as_ref().map_or(0, |c| c.count_items()));
//                 event.weighed(self.collection().as_ref().unwrap());
//                 self.log().end(event);

//                 if !self.skip_caching() {
//                     let event = self.log().start(Verbosity::Log, format!("storing {} into cache at {}", self.name(), self.cache_path().as_ref().unwrap().to_str().unwrap()));
//                     self.store_to_cache().unwrap();
//                     self.log().end(event);
//                 }
//             }
//         }
//         self.grab_collection()
//     }
// }

// impl<E> PersistentMeta<E> where E: MetaExtractor {
//     pub fn new<Sa, Sb>(name: Sa, log: Log, dir: Sb) -> Self where Sa: Into<String>, Sb: Into<String> {
//         let name = name.into();
//         let (cache_dir, cache_path) = Self::setup_files(name.clone(), dir);
//         PersistentMeta { name, log, cache_path: Some(cache_path), cache_dir: Some(cache_dir), map: None, extractor: PhantomData }
//     }
//     pub fn new_without_cache<S>(name: S, log: Log) -> Self where S: Into<String> {
//         PersistentMeta { name: name.into(), log, cache_path: None, cache_dir: None, map: None, extractor: PhantomData }
//     }
//     pub fn without_cache(mut self) -> Self {
//         self.cache_dir = None;
//         self.cache_path = None;
//         self
//     }
//     pub fn iter(&self) -> impl Iterator<Item=(&E::Key, &E::Value)> {
//         self.map.as_ref().map(|vector| vector.iter())
//             .expect("Attempted to iterate over persistent map before initializing it")
//     }
//     pub fn load_from_one(&mut self, input: &A) -> &BTreeMap<E::Key, E::Value> {
//         self.data_from_loader(|| { E::extract(input) })
//     }
// }

//#[derive(Hash, Clone, Debug, PartialEq, Eq)]

// impl std::fmt::Display for MetadataKey {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
//             Self::Created => write!(f, "created"),
//         }
//     }
// }

pub struct PersistentMetaMap<S, K, V> 
where K: Ord + Persistent + Weighed, V: Clone + Persistent + Countable + Weighed {
    log: Log,
    cache_path: Option<PathBuf>,
    cache_dir: Option<PathBuf>,  
    name: String,
    metadata: Rc<RefCell<S>>,
    _type: PhantomData<(K, V)>,
}

impl<S, K, V> PersistentCollection for PersistentMetaMap<S, K, V> 
where K: Ord + Persistent + Weighed, V: Clone + Persistent + Countable + Weighed {
    type Collection = BTreeMap<K, V>;
    //fn weigh(&self) -> usize { Self.weight_in_bytes() }
    fn name(&self) -> String { self.name.clone() }
    fn log(&self) -> &Log { &self.log }
    fn cache_path(&self) -> &Option<PathBuf> { &self.cache_path }
    fn cache_dir(&self) -> &Option<PathBuf> { &self.cache_dir }
    fn collection(&self) -> &Option<Self::Collection> { unimplemented!() }
    fn set_collection(&mut self, map: Self::Collection) { 
        panic!("Collection is not settable for {:?}", self.name) 
    }
}

impl<S, K, V> PersistentMetaMap<S, K, V> 
where K: Ord + Persistent + Weighed, V: Clone + Persistent + Countable + Weighed {
    pub fn new<Sa, Sb>(name: Sa, log: Log, dir: Sb) -> Self where Sa: Into<String>, Sb: Into<String> {
        let name = name.into();
        let (cache_dir, cache_path) = Self::setup_files(name.clone(), dir);
        unimplemented!()
        //PersistentMetaMap { name, log, cache_path: Some(cache_path), cache_dir: Some(cache_dir), map: None, extractor: PhantomData }
    }
    // Always cache.
    // pub fn new_without_cache<S>(name: S, log: Log) -> Self where S: Into<String> {
    //     //PersistentMap { name: name.into(), log, cache_path: None, cache_dir: None, map: None, extractor: PhantomData }
    // }
    pub fn without_cache(mut self) -> Self {
        self.cache_dir = None;
        self.cache_path = None;
        self
    }
    // TODO
    // pub fn iter(&self) -> impl Iterator<Item=(&E::Key, &E::Value)> {
    //     self.map.as_ref().map(|vector| vector.iter())
    //         .expect("Attempted to iterate over persistent map before initializing it")
    // }
}

impl PersistentMetaMap<ProjectMetadataSource, ProjectId, String> {
    pub fn load_from_source(&mut self, source: &Source) -> RefMut<BTreeMap<ProjectId, i64>> {
        match self.name().as_str() {
            "created" => 
                //RefMut::map(self.metadata.as_ref().borrow_mut(), |metadata| &mut metadata.created_map(source)),
                unimplemented!(),                
            _ => unimplemented!(),
        }
    }
}
