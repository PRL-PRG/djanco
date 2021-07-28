use std::path::PathBuf;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fs::{File, create_dir_all};

use serde_json::Value as JSON;
use chrono::DateTime;

use crate::objects::*;

use crate::log::{Log, Verbosity, Warning};
use crate::weights_and_measures::{Weighed};

use super::cache::*;
use super::persistent::*;
use super::source::Source;

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
        //println!("language extraction form {:?}", value);
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
    // filename: String,
    field: String,
    log: Log,
    cache_path: PathBuf,
    cache_dir: PathBuf,
    extractor: M,
}


impl<M> MetadataCacher<M> where M: MetadataFieldExtractor {
    pub fn new<Sa, Sb, Sc>(field: Sc, filename: Sa, dir: Sb, log: &Log, extractor: M) -> Self
        where Sa: Into<String>, Sb: Into<String>, Sc: Into<String> {
        let filename: String = filename.into();
        let field: String = field.into();

        let mut cache_dir = PathBuf::new();
        cache_dir.push(std::path::Path::new(dir.into().as_str()));

        let mut cache_path = cache_dir.clone();
        cache_path.push(std::path::Path::new(filename.as_str()));
        cache_path.set_extension(PERSISTENT_EXTENSION);

        Self { field, extractor, cache_dir, cache_path, log: log.clone() }
    }

    // pub fn cache_path(&self) -> &PathBuf { &self.cache_path }

    pub fn already_cached(&self) -> bool { self.cache_path.is_file() }

    fn load_from_store(&self, metadata: &HashMap<ProjectId, serde_json::Map<String, JSON>>) -> BTreeMap<ProjectId, M::Value> {
        let mut event = self.log.start(Verbosity::Log, format!("loading metadata ({}) from store", self.field));
        let vector: BTreeMap<ProjectId, <M as MetadataFieldExtractor>::Value> = 
            metadata.iter()
                .flat_map(|(id, properties)| {
                    let property = properties.get(&self.field);
                    match property {
                        Some(property) => {
                            self.extractor.get(property)
                                .map(|e| (id.clone(), e))
                        }
                        None => {
                            eprintln!("WARNING! Attempt to retrieve property {} for project {} from property map yielded None, available keys: {}",
                            &self.field, id, properties.iter().map(|(k, _)| k.to_string()).collect::<Vec<String>>().join(","));
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
        let mut event = self.log.start(Verbosity::Log, format!("storing metadata ({}) to cache at {}", self.field, self.cache_path.to_str().unwrap()));
        create_dir_all(&self.cache_dir)?;
        let writer = File::create(&self.cache_path)?;
        serde_cbor::to_writer(writer, &vector)?;
        event.weighed(vector);
        event.counted(vector.len());
        self.log.end(event);
        Ok(())
    }

    fn load_from_cache(&self) -> Result<BTreeMap<ProjectId, M::Value>, Box<dyn Error>> {
        let mut event = self.log.start(Verbosity::Log, format!("loading metadata ({}) from cache at {}", self.field, self.cache_path.to_str().unwrap()));
        let reader = File::open(&self.cache_path)?;
        let vector: BTreeMap<ProjectId, M::Value> = serde_cbor::from_reader(reader)?;
        event.weighed(&vector);
        event.counted(vector.len());
        self.log.end(event);
        Ok(vector)
    }


    fn convert_into_cache(&self, metadata: &HashMap<ProjectId, serde_json::Map<String, JSON>>) -> Result<(), Box<dyn Error>> {
        self.store_to_cache(&self.load_from_store(metadata))
    }
}



pub trait MetadataSource {
    fn load_metadata(&self, store: &Source) -> HashMap<ProjectId, serde_json::Map<String, JSON>> {
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

    fn convert_all_into_cache_from_store(&self, store: &Source) -> Result<(), Vec<Box<dyn Error>>> {
        let metadata = self.load_metadata(store);
        self.convert_all_into_cache(&metadata)
    }

    // fn prepare_dir<Sa, Sb>(_name: Sa, dir: Sb) -> String where Sa: Into<String>, Sb: Into<String> {
    //     // let name = name.into();
    //     let dir = {
    //         let mut cache_subdir = PathBuf::new();
    //         cache_subdir.push(std::path::Path::new(dir.into().as_str()));
    //         //cache_subdir.push(std::path::Path::new(name.as_str()));
    //         //cache_subdir.set_extension(PERSISTENT_EXTENSION);
    //         cache_subdir.to_str().unwrap().to_owned()
    //     };
    //     dir
    // }

    fn convert_all_into_cache(&self, metadata: &HashMap<ProjectId, serde_json::Map<String, JSON>>) -> Result<(), Vec<Box<dyn Error>>>;
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
    pub fn new<S>(log: Log, dir: S) -> Self where S: Into<String> {
        let dir = dir.into();
        ProjectMetadataSource {
            are_forks:     MetadataCacher::new("fork",               CACHE_FILE_PROJECT_IS_FORK,           dir.as_str(), &log, BoolExtractor),
            are_archived:  MetadataCacher::new("archived",           CACHE_FILE_PROJECT_IS_ARCHIVED,       dir.as_str(), &log, BoolExtractor),
            are_disabled:  MetadataCacher::new("disabled",           CACHE_FILE_PROJECT_IS_DISABLED,       dir.as_str(), &log, BoolExtractor),
            star_gazers:   MetadataCacher::new("stargazers_count",   CACHE_FILE_PROJECT_STARGAZER_COUNT,   dir.as_str(), &log, CountExtractor),
            watchers:      MetadataCacher::new("watchers_count",     CACHE_FILE_PROJECT_WATCHER_COUNT,     dir.as_str(), &log, CountExtractor),
            size:          MetadataCacher::new("size",               CACHE_FILE_PROJECT_SIZE,              dir.as_str(), &log, CountExtractor),            
            forks:         MetadataCacher::new("forks",              CACHE_FILE_PROJECT_FORK_COUNT,        dir.as_str(), &log, CountExtractor),
            subscribers:   MetadataCacher::new("subscribers_count",  CACHE_FILE_PROJECT_SUBSCRIBER_COUNT,  dir.as_str(), &log, CountExtractor),
            languages:     MetadataCacher::new("language",           CACHE_FILE_PROJECT_LANGUAGE,          dir.as_str(), &log, LanguageExtractor),
            descriptions:  MetadataCacher::new("description",        CACHE_FILE_PROJECT_DESCRIPTION,       dir.as_str(), &log, StringExtractor),
            homepages:     MetadataCacher::new("homepage",           CACHE_FILE_PROJECT_HOMEPAGE,          dir.as_str(), &log, StringExtractor),
            licenses:      MetadataCacher::new("license",            CACHE_FILE_PROJECT_LICENSE,           dir.as_str(), &log, FieldExtractor("name", StringExtractor)),
            has_issues:    MetadataCacher::new("has_issues",         CACHE_FILE_PROJECT_HAS_ISSUES,        dir.as_str(), &log, BoolExtractor),
            has_downloads: MetadataCacher::new("has_downloads",      CACHE_FILE_PROJECT_HAS_DOWNLOADS,     dir.as_str(), &log, BoolExtractor),
            has_wiki:      MetadataCacher::new("has_wiki",           CACHE_FILE_PROJECT_HAS_WIKI,          dir.as_str(), &log, BoolExtractor),
            has_pages:     MetadataCacher::new("has_pages",          CACHE_FILE_PROJECT_HAS_PAGES,         dir.as_str(), &log, BoolExtractor),
            created:       MetadataCacher::new("created_at",         CACHE_FILE_PROJECT_CREATED,           dir.as_str(), &log, TimestampExtractor),
            updated:       MetadataCacher::new("updated_at",         CACHE_FILE_PROJECT_UPDATED,           dir.as_str(), &log, TimestampExtractor),
            pushed:        MetadataCacher::new("pushed_at",          CACHE_FILE_PROJECT_PUSHED,            dir.as_str(), &log, TimestampExtractor),
            master:        MetadataCacher::new("default_branch",     CACHE_FILE_PROJECT_DEFAULT_BRANCH,    dir.as_str(), &log, StringExtractor),
            issues:        MetadataCacher::new("issues_count",       CACHE_FILE_PROJECT_ISSUE_COUNT,       dir.as_str(), &log, CountExtractor),
            buggy_issues:  MetadataCacher::new("buggy_issues_count", CACHE_FILE_PROJECT_BUGGY_ISSUE_COUNT, dir.as_str(), &log, CountExtractor),
            open_issues:   MetadataCacher::new("open_issues_count",  CACHE_FILE_PROJECT_OPEN_ISSUE_COUNT,  dir.as_str(), &log, CountExtractor),
        }
    }
}

// impl ProjectMetadataSource {
//     pub fn is_fork          (&mut self, store: &Source, key: &ProjectId) -> Option<bool>     { unimplemented!() }
//     pub fn is_archived      (&mut self, store: &Source, key: &ProjectId) -> Option<bool>     { unimplemented!() }
//     pub fn is_disabled      (&mut self, store: &Source, key: &ProjectId) -> Option<bool>     { unimplemented!() }
//     pub fn star_gazers      (&mut self, store: &Source, key: &ProjectId) -> Option<usize>    { unimplemented!() }
//     pub fn watchers         (&mut self, store: &Source, key: &ProjectId) -> Option<usize>    { unimplemented!() }
//     pub fn size             (&mut self, store: &Source, key: &ProjectId) -> Option<usize>    { unimplemented!() }
//     pub fn open_issues      (&mut self, store: &Source, key: &ProjectId) -> Option<usize>    { unimplemented!() }
//     pub fn forks            (&mut self, store: &Source, key: &ProjectId) -> Option<usize>    { unimplemented!() }
//     pub fn subscribers      (&mut self, store: &Source, key: &ProjectId) -> Option<usize>    { unimplemented!() }
//     pub fn license          (&mut self, store: &Source, key: &ProjectId) -> Option<String>   { unimplemented!() }
//     pub fn description      (&mut self, store: &Source, key: &ProjectId) -> Option<String>   { unimplemented!() }
//     pub fn homepage         (&mut self, store: &Source, key: &ProjectId) -> Option<String>   { unimplemented!() }
//     pub fn language         (&mut self, store: &Source, key: &ProjectId) -> Option<Language> { unimplemented!() }
//     pub fn has_issues       (&mut self, store: &Source, key: &ProjectId) -> Option<bool>     { unimplemented!() }
//     pub fn has_downloads    (&mut self, store: &Source, key: &ProjectId) -> Option<bool>     { unimplemented!() }
//     pub fn has_wiki         (&mut self, store: &Source, key: &ProjectId) -> Option<bool>     { unimplemented!() }
//     pub fn has_pages        (&mut self, store: &Source, key: &ProjectId) -> Option<bool>     { unimplemented!() }
//     pub fn created          (&mut self, store: &Source, key: &ProjectId) -> Option<i64>      { unimplemented!() }
//     pub fn updated          (&mut self, store: &Source, key: &ProjectId) -> Option<i64>      { unimplemented!() }
//     pub fn pushed           (&mut self, store: &Source, key: &ProjectId) -> Option<i64>      { unimplemented!() }
//     pub fn master           (&mut self, store: &Source, key: &ProjectId) -> Option<String>   { unimplemented!() }
//     pub fn issues           (&mut self, store: &Source, key: &ProjectId) -> Option<usize>    { unimplemented!() }
//     pub fn buggy_issues     (&mut self, store: &Source, key: &ProjectId) -> Option<usize>    { unimplemented!() }
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
    pub default_branch: Option<String>,
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

    // pub fn all_metadata(&mut self, store: &Source, key: &ProjectId) -> ProjectMetadata {
    //     ProjectMetadata {
    //         id: key.clone(),
    //         is_fork: self.is_fork(store, key),
    //         is_archived: self.is_archived(store, key),
    //         is_disabled: self.is_disabled(store, key),
    //         star_gazers: self.star_gazers(store, key),
    //         watchers: self.watchers(store, key),
    //         size: self.size(store, key),
    //         open_issues: self.open_issues(store, key),
    //         forks: self.forks(store, key),
    //         subscribers: self.subscribers(store, key),
    //         license: self.license(store, key),
    //         description: self.description(store, key),
    //         homepage: self.homepage(store, key),
    //         language: self.language(store, key),
    //         has_issues: self.has_issues(store, key),
    //         has_downloads: self.has_downloads(store, key),
    //         has_wiki: self.has_wiki(store, key),
    //         has_pages: self.has_pages(store, key),
    //         created: self.created(store, key),
    //         updated: self.updated(store, key),
    //         pushed: self.pushed(store, key),
    //         master: self.master(store, key),
    //         issues: self.issues(store, key),
    //         buggy_issues: self.buggy_issues(store, key),
    //     }
    // }

    // pub fn iter<'a>(&'a mut self, store: &'a Source) -> impl Iterator<Item=ProjectMetadata> + 'a {
    //     self.keys(store)
    //         .map(|project_id| self.all_metadata(store, &project_id))
    //         .collect::<Vec<ProjectMetadata>>()
    //         .into_iter()
    // }
}

impl MetadataSource for ProjectMetadataSource {
    fn convert_all_into_cache(&self, metadata: &HashMap<ProjectId, serde_json::Map<String, JSON>>) -> Result<(), Vec<Box<dyn Error>>> {
        macro_rules! convert_into_store {
            ($($id:ident),+) => {
                run_and_consolidate_errors!(
                    $( self.$id.convert_into_cache(metadata)  ),*
                )
            }
        }
        convert_into_store!(are_forks, are_archived, are_disabled, star_gazers, watchers, size,
                            open_issues, forks, subscribers, licenses, languages, descriptions,
                            homepages, has_issues, has_downloads, has_wiki, has_pages, created,
                            updated, pushed, master, issues, buggy_issues)
    }
}



pub trait MetadataVectorExtractor<M>: MapExtractor where M: MetadataSource {
    fn extract(source: &Source, metadata: &M) -> BTreeMap<Self::Key, Self::Value>;
}

pub trait MetadataMapExtractor<M>: MapExtractor where M: MetadataSource {
    fn extract(source: &Source, metadata: &M) -> BTreeMap<Self::Key, Self::Value>;
}

impl<E> PersistentMap<E> where E: MetadataMapExtractor<ProjectMetadataSource> {
    pub fn load_from_metadata(&mut self, source: &Source, metadata: &ProjectMetadataSource) -> &BTreeMap<E::Key, E::Value> {
        self.data_from_loader(|| { E::extract(source, metadata) })
    }
}

macro_rules! precache_metadata {
    ($source:expr, $metadata:expr, $id:ident) => {{
        // If the one I need is not cached
        if !$metadata.$id.already_cached() {
            // Cache all
            $metadata.convert_all_into_cache_from_store($source).unwrap();
        }
        // Load just the one I need
        $metadata.$id.load_from_cache().unwrap()
    }}
}


pub(crate) struct ProjectBuggyIssuesExtractor {}
impl MapExtractor for ProjectBuggyIssuesExtractor {
    type Key = ProjectId;
    type Value = usize;
}
impl MetadataMapExtractor<ProjectMetadataSource> for ProjectBuggyIssuesExtractor {
    fn extract(source: &Source, metadata: &ProjectMetadataSource) -> BTreeMap<Self::Key, Self::Value> {
        precache_metadata!(source, metadata, buggy_issues)
    }
}


pub(crate) struct ProjectIsForkExtractor {}
impl MapExtractor for ProjectIsForkExtractor {
    type Key = ProjectId;
    type Value = bool;
}
impl MetadataMapExtractor<ProjectMetadataSource> for ProjectIsForkExtractor {
    fn extract(source: &Source, metadata: &ProjectMetadataSource) -> BTreeMap<Self::Key, Self::Value> {
        precache_metadata!(source, metadata, are_forks)
    }
}

pub(crate) struct ProjectIsArchivedExtractor {}
impl MapExtractor for ProjectIsArchivedExtractor {
    type Key = ProjectId;
    type Value = bool;
}
impl MetadataMapExtractor<ProjectMetadataSource> for ProjectIsArchivedExtractor {
    fn extract(source: &Source, metadata: &ProjectMetadataSource) -> BTreeMap<Self::Key, Self::Value> {
        precache_metadata!(source, metadata, are_archived)
    }
}

pub(crate) struct ProjectIsDisabledExtractor {}
impl MapExtractor for ProjectIsDisabledExtractor {
    type Key = ProjectId;
    type Value = bool;
}
impl MetadataMapExtractor<ProjectMetadataSource> for ProjectIsDisabledExtractor {
    fn extract(source: &Source, metadata: &ProjectMetadataSource) -> BTreeMap<Self::Key, Self::Value> {
        precache_metadata!(source, metadata, are_disabled)
    }
}

pub(crate) struct ProjectStargazersExtractor {}
impl MapExtractor for ProjectStargazersExtractor {
    type Key = ProjectId;
    type Value = usize;
}
impl MetadataMapExtractor<ProjectMetadataSource> for ProjectStargazersExtractor {
    fn extract(source: &Source, metadata: &ProjectMetadataSource) -> BTreeMap<Self::Key, Self::Value> {
        precache_metadata!(source, metadata, star_gazers)
    }
}

pub(crate) struct ProjectWatchersExtractor {}
impl MapExtractor for ProjectWatchersExtractor {
    type Key = ProjectId;
    type Value = usize;
}
impl MetadataMapExtractor<ProjectMetadataSource> for ProjectWatchersExtractor {
    fn extract(source: &Source, metadata: &ProjectMetadataSource) -> BTreeMap<Self::Key, Self::Value> {
        precache_metadata!(source, metadata, watchers)
    }
}

pub(crate) struct ProjectSizeExtractor {}
impl MapExtractor for ProjectSizeExtractor {
    type Key = ProjectId;
    type Value = usize;
}
impl MetadataMapExtractor<ProjectMetadataSource> for ProjectSizeExtractor {
    fn extract(source: &Source, metadata: &ProjectMetadataSource) -> BTreeMap<Self::Key, Self::Value> {
        precache_metadata!(source, metadata, size)
    }
}

pub(crate) struct ProjectOpenIssuesExtractor {}
impl MapExtractor for ProjectOpenIssuesExtractor {
    type Key = ProjectId;
    type Value = usize;
}
impl MetadataMapExtractor<ProjectMetadataSource> for ProjectOpenIssuesExtractor {
    fn extract(source: &Source, metadata: &ProjectMetadataSource) -> BTreeMap<Self::Key, Self::Value> {
        precache_metadata!(source, metadata, open_issues)
    }
}

pub(crate) struct ProjectIssuesExtractor {}
impl MapExtractor for ProjectIssuesExtractor {
    type Key = ProjectId;
    type Value = usize;
}
impl MetadataMapExtractor<ProjectMetadataSource> for ProjectIssuesExtractor {
    fn extract(source: &Source, metadata: &ProjectMetadataSource) -> BTreeMap<Self::Key, Self::Value> {
        precache_metadata!(source, metadata, issues)
    }
}

pub(crate) struct ProjectForksExtractor {}
impl MapExtractor for ProjectForksExtractor {
    type Key = ProjectId;
    type Value = usize;
}
impl MetadataMapExtractor<ProjectMetadataSource> for ProjectForksExtractor {
    fn extract(source: &Source, metadata: &ProjectMetadataSource) -> BTreeMap<Self::Key, Self::Value> {
        precache_metadata!(source, metadata, forks)
    }
}

pub(crate) struct ProjectSubscribersExtractor {}
impl MapExtractor for ProjectSubscribersExtractor {
    type Key = ProjectId;
    type Value = usize;
}
impl MetadataMapExtractor<ProjectMetadataSource> for ProjectSubscribersExtractor {
    fn extract(source: &Source, metadata: &ProjectMetadataSource) -> BTreeMap<Self::Key, Self::Value> {
        precache_metadata!(source, metadata, subscribers)
    }
}

pub(crate) struct ProjectLicenseExtractor {}
impl MapExtractor for ProjectLicenseExtractor {
    type Key = ProjectId;
    type Value = String;
}
impl MetadataMapExtractor<ProjectMetadataSource> for ProjectLicenseExtractor {
    fn extract(source: &Source, metadata: &ProjectMetadataSource) -> BTreeMap<Self::Key, Self::Value> {
        precache_metadata!(source, metadata, licenses)
    }
}

pub(crate) struct ProjectLanguageExtractor {}
impl MapExtractor for ProjectLanguageExtractor {
    type Key = ProjectId;
    type Value = Language;
}
impl MetadataMapExtractor<ProjectMetadataSource> for ProjectLanguageExtractor {
    fn extract(source: &Source, metadata: &ProjectMetadataSource) -> BTreeMap<Self::Key, Self::Value> {
        precache_metadata!(source, metadata, languages)
    }
}

pub(crate) struct ProjectDescriptionExtractor {}
impl MapExtractor for ProjectDescriptionExtractor {
    type Key = ProjectId;
    type Value = String;
}
impl MetadataMapExtractor<ProjectMetadataSource> for ProjectDescriptionExtractor {
    fn extract(source: &Source, metadata: &ProjectMetadataSource) -> BTreeMap<Self::Key, Self::Value> {
        precache_metadata!(source, metadata, descriptions)
    }
}

pub(crate) struct ProjectHomepageExtractor {}
impl MapExtractor for ProjectHomepageExtractor {
    type Key = ProjectId;
    type Value = String;
}
impl MetadataMapExtractor<ProjectMetadataSource> for ProjectHomepageExtractor {
    fn extract(source: &Source, metadata: &ProjectMetadataSource) -> BTreeMap<Self::Key, Self::Value> {
        precache_metadata!(source, metadata, homepages)
    }
}

pub(crate) struct ProjectHasIssuesExtractor {}
impl MapExtractor for ProjectHasIssuesExtractor {
    type Key = ProjectId;
    type Value = bool;
}
impl MetadataMapExtractor<ProjectMetadataSource> for ProjectHasIssuesExtractor {
    fn extract(source: &Source, metadata: &ProjectMetadataSource) -> BTreeMap<Self::Key, Self::Value> {
        precache_metadata!(source, metadata, has_issues)
    }
}

pub(crate) struct ProjectHasDownloadsExtractor {}
impl MapExtractor for ProjectHasDownloadsExtractor {
    type Key = ProjectId;
    type Value = bool;
}
impl MetadataMapExtractor<ProjectMetadataSource> for ProjectHasDownloadsExtractor {
    fn extract(source: &Source, metadata: &ProjectMetadataSource) -> BTreeMap<Self::Key, Self::Value> {
        precache_metadata!(source, metadata, has_downloads)
    }
}

pub(crate) struct ProjectHasWikiExtractor {}
impl MapExtractor for ProjectHasWikiExtractor {
    type Key = ProjectId;
    type Value = bool;
}
impl MetadataMapExtractor<ProjectMetadataSource> for ProjectHasWikiExtractor {
    fn extract(source: &Source, metadata: &ProjectMetadataSource) -> BTreeMap<Self::Key, Self::Value> {
        precache_metadata!(source, metadata, has_wiki)
    }
}

pub(crate) struct ProjectHasPagesExtractor {}
impl MapExtractor for ProjectHasPagesExtractor {
    type Key = ProjectId;
    type Value = bool;
}
impl MetadataMapExtractor<ProjectMetadataSource> for ProjectHasPagesExtractor {
    fn extract(source: &Source, metadata: &ProjectMetadataSource) -> BTreeMap<Self::Key, Self::Value> {
        precache_metadata!(source, metadata, has_pages)
    }
}

pub(crate) struct ProjectCreatedExtractor {}
impl MapExtractor for ProjectCreatedExtractor {
    type Key = ProjectId;
    type Value = i64;
}
impl MetadataMapExtractor<ProjectMetadataSource> for ProjectCreatedExtractor {
    fn extract(source: &Source, metadata: &ProjectMetadataSource) -> BTreeMap<Self::Key, Self::Value> {
        precache_metadata!(source, metadata, created)
    }
}

pub(crate) struct ProjectUpdatedExtractor {}
impl MapExtractor for ProjectUpdatedExtractor {
    type Key = ProjectId;
    type Value = i64;
}
impl MetadataMapExtractor<ProjectMetadataSource> for ProjectUpdatedExtractor {
    fn extract(source: &Source, metadata: &ProjectMetadataSource) -> BTreeMap<Self::Key, Self::Value> {
        precache_metadata!(source, metadata, updated)
    }
}

pub(crate) struct ProjectPushedExtractor {}
impl MapExtractor for ProjectPushedExtractor {
    type Key = ProjectId;
    type Value = i64;
}
impl MetadataMapExtractor<ProjectMetadataSource> for ProjectPushedExtractor {
    fn extract(source: &Source, metadata: &ProjectMetadataSource) -> BTreeMap<Self::Key, Self::Value> {
        precache_metadata!(source, metadata, pushed)
    }
}

pub(crate) struct ProjectDefaultBranchExtractor {}
impl MapExtractor for ProjectDefaultBranchExtractor {
    type Key = ProjectId;
    type Value = String;
}
impl MetadataMapExtractor<ProjectMetadataSource> for ProjectDefaultBranchExtractor {
    fn extract(source: &Source, metadata: &ProjectMetadataSource) -> BTreeMap<Self::Key, Self::Value> {
        precache_metadata!(source, metadata, master)
    }
}

