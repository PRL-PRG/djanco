use std::path::PathBuf;
use std::collections::{BTreeMap, HashMap, BTreeSet};
use std::error::Error;
use std::fs::{File, create_dir_all};

use serde_json::Value as JSON;
use chrono::DateTime;

use dcd::DatastoreView;

use crate::persistent::*;
use crate::objects::*;
use crate::log::{Log, Verbosity, Warning};
use crate::weights_and_measures::Weighed;
use bstr::ByteSlice;

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

struct MetadataVec<M: MetadataFieldExtractor> {
    name: String,
    log: Log,
    cache_path: PathBuf,
    cache_dir: PathBuf,
    extractor: M,
    vector: Option<BTreeMap<ProjectId, M::Value>>,
}

impl<M> MetadataVec<M> where M: MetadataFieldExtractor {
    pub fn new<Sa, Sb>(name: Sa, dir: Sb, log: &Log, extractor: M) -> Self
        where Sa: Into<String>, Sb: Into<String> {
        let name: String = name.into();

        let mut cache_dir = PathBuf::new();
        cache_dir.push(std::path::Path::new(dir.into().as_str()));

        let mut cache_path = cache_dir.clone();
        cache_path.push(std::path::Path::new(name.as_str()));
        cache_path.set_extension(PERSISTENT_EXTENSION);

        Self { name, extractor, vector: None, cache_dir, cache_path, log: log.clone() }
    }

    pub fn iter(&self) -> impl Iterator<Item=(&ProjectId, &M::Value)> {
        self.vector.as_ref().map(|vector| vector.iter())
            .expect("Attempted to iterate over metadata vector before initializing it")
    }

    pub fn already_loaded(&self) -> bool { self.vector.is_some() }
    pub fn already_cached(&self) -> bool { self.cache_path.is_file() }

    pub fn load_from_store(&mut self, metadata: &HashMap<ProjectId, serde_json::Map<String, JSON>>) {
        if !self.already_loaded() {
            let mut event = self.log.start(Verbosity::Log, format!("loading metadata ({}) from store", self.name));
            self.vector = Some(
                metadata.iter()
                    .flat_map(|(id, properties)| {
                        let property = properties.get(&self.name);
                        //println!("___ {:?} {:?}", property, properties);
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
                    }).collect()
            );
            event.weighed(&self.vector);
            event.counted(self.vector.as_ref().map_or(0, |v| v.len()));
            self.log.end(event)

        }
    }

    fn load_from_cache(&mut self) -> Result<(), Box<dyn Error>> {
        let mut event = self.log.start(Verbosity::Log, format!("loading metadata ({}) from cache at {}", self.name, self.cache_path.to_str().unwrap()));
        let reader = File::open(&self.cache_path)?;
        self.vector = Some(serde_cbor::from_reader(reader)?);
        event.weighed(&self.vector);
        event.counted(self.vector.as_ref().map_or(0, |v| v.len()));
        self.log.end(event);
        Ok(())
    }

    fn store_to_cache(&mut self) -> Result<(), Box<dyn Error>> {
        let mut event = self.log.start(Verbosity::Log, format!("storing metadata ({}) to cache at {}", self.name, self.cache_path.to_str().unwrap()));
        create_dir_all(&self.cache_dir)?;
        let writer = File::create(&self.cache_path)?;
        serde_cbor::to_writer(writer, &self.vector)?;
        event.weighed(&self.vector);
        event.counted(self.vector.as_ref().map_or(0, |v| v.len()));
        self.log.end(event);
        Ok(())
    }

    pub fn data(&mut self) -> &BTreeMap<ProjectId, M::Value> {
        if !self.already_loaded() {
            if self.already_cached() {
                self.load_from_cache()
                    .expect(&format!("Could not load data from data store at {} for {}",
                                     self.cache_dir.to_str().unwrap(), self.name))
            } else {
                panic!("Must preload data from data store before accessing!");
            }
        }
        self.vector.as_ref().unwrap() // guaranteed
    }

    pub fn get(&mut self, key: &ProjectId) -> Option<&M::Value> {
        self.data().get(key)
    }
}

impl<T,M> MetadataVec<M> where M: MetadataFieldExtractor<Value=T>, T: Clone + Persistent + Weighed {
    pub fn pirate(&mut self, key: &ProjectId) -> Option<M::Value> { // get owned
        self.get(key).map(|v| v.clone())
    }
}

trait MetadataSource {
    fn _load_metadata(&mut self, store: &DatastoreView) -> HashMap<ProjectId, serde_json::Map<String, JSON>> {
        let content_project_ids: HashMap<u64, u64> =
            store.projects_metadata()
                .filter(|(_, meta)| meta.key == "github_metadata")
                .map(|(id, metadata)| {
                    let value = metadata.value.parse::<u64>()
                        .expect(&format!("Could not parse {} as u64", metadata.value));
                    //eprintln!("metadata project_id={}->content_id={}", id, value); // FIXME! remove! hasty debug!
                    (id, value)
                })
                .map(|(project_id, content_id)| (content_id, project_id))
                .collect();
        // FIXME random access will probably work better
        store.contents_data()
            .filter(|(content_id, _)| content_project_ids.contains_key(content_id))
            .flat_map(|(content_id, contents)| {
                serde_json::from_slice(contents.as_slice())
                    .warn(&format!("Failed to parse JSON for content ID {} and content:\n>> {}\n",
                                   content_id, contents.to_str_lossy().replace("\n", "\n>> ")))
                    .map_or_else(|_| None, |value| Some(value))
                    .map(|json: JSON| {
                        content_project_ids.get(&content_id)
                            .warn(format!("No project ID found for content ID {}", content_id))
                            .map(|project_id| {
                                match json {
                                    JSON::Object(map) => {
                                        (ProjectId::from(project_id), map)
                                    },
                                    meta => {
                                        panic!("Unexpected JSON value for project ID {} for metadata: {:?}",
                                               project_id, meta)
                                    },
                                }
                            })
                    }).flatten()
            }).collect()
    }

    // Rewritten to use content_data instead of contents_data
    fn load_metadata(&mut self, store: &DatastoreView) -> HashMap<ProjectId, serde_json::Map<String, JSON>> {
        store.projects_metadata()
            .filter(|(_, meta)| meta.key == "github_metadata")
            .map(|(project_id, metadata)| {
                (project_id, metadata.value)
            })
            .map(|(project_id, content_id_as_string)| {
                let content_id = content_id_as_string.parse::<u64>()
                    .expect(&format!("Could not parse {} as u64", content_id_as_string));
                //eprintln!("metadata project_id={}->content_id={}", project_id, content_id);
                (project_id, content_id)
            })
            //.map(|(project_id, content_id)| {
            //    (ProjectId::from(project_id), SnapshotId::from(content_id))
            //})
            .flat_map(|(project_id, content_id)| {
                store.content_data(content_id).map(|content_data| {
                    (project_id, content_id, content_data)
                })
            })
            .flat_map(|(project_id, content_id, content_data)| {
                let json: Option<JSON> =
                    serde_json::from_slice(content_data.as_slice())
                        .warn(&format!("Failed to parse JSON for content ID {} and content:\n>> {}\n",
                                       content_id, content_data.to_str_lossy().replace("\n", "\n>> ")))
                        .map_or_else(|_| None, |value| Some(value));
                json.map(|json| (project_id, json))
            })
            .map(|(project_id, json)|
                match json {
                    JSON::Object(map) => (project_id, map),
                    value => panic!("Unexpected JSON value for project ID {} for metadata: {:?}", project_id, value),
                }
            )
            .map(|(project_id, map)|
                (ProjectId::from(project_id), map)
            )
            .collect()
    }

    fn load_all_from_store(&mut self, store: &DatastoreView) {
        let metadata = self.load_metadata(store);
        self.load_all_from(&metadata)
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

    fn load_all_from(&mut self, metadata: &HashMap<ProjectId, serde_json::Map<String, JSON>>);
    fn store_all_to_cache(&mut self) -> Result<(), Vec<Box<dyn Error>>>;
}

macro_rules! gimme {
    ($self:expr, $vector:ident, $store:expr, $method:ident, $key:expr) => {{
        if !$self.loaded && !$self.$vector.already_loaded() && !$self.$vector.already_cached() {
            $self.load_all_from_store($store);
            $self.store_all_to_cache().unwrap();
            $self.loaded = true;
        }
        $self.$vector.$method($key)
    }}
}

macro_rules! gimme_iter {
    ($self:expr, $vector:ident, $store:expr) => {{
        if !$self.loaded && !$self.$vector.already_loaded() && !$self.$vector.already_cached() {
            $self.load_all_from_store($store);
            $self.store_all_to_cache().unwrap();
            $self.loaded = true;
        }
        $self.$vector.iter()
    }}
}

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
    are_forks:        MetadataVec<BoolExtractor>,
    are_archived:     MetadataVec<BoolExtractor>,
    are_disabled:     MetadataVec<BoolExtractor>,
    star_gazers:      MetadataVec<CountExtractor>,
    watchers:         MetadataVec<CountExtractor>,
    size:             MetadataVec<CountExtractor>,
    open_issues:      MetadataVec<CountExtractor>,
    forks:            MetadataVec<CountExtractor>,
    subscribers:      MetadataVec<CountExtractor>,
    licenses:         MetadataVec<FieldExtractor<StringExtractor>>,
    languages:        MetadataVec<LanguageExtractor>,
    descriptions:     MetadataVec<StringExtractor>,
    homepages:        MetadataVec<StringExtractor>,
    has_issues:       MetadataVec<BoolExtractor>,
    has_downloads:    MetadataVec<BoolExtractor>,
    has_wiki:         MetadataVec<BoolExtractor>,
    has_pages:        MetadataVec<BoolExtractor>,
    created:          MetadataVec<TimestampExtractor>,
    updated:          MetadataVec<TimestampExtractor>,
    pushed:           MetadataVec<TimestampExtractor>,
    master:           MetadataVec<StringExtractor>,
}

impl ProjectMetadataSource {
    pub fn new<Sa, Sb>(name: Sa, log: &Log, dir: Sb) -> Self where Sa: Into<String>, Sb: Into<String> {
        let dir = Self::prepare_dir(name, dir);
        ProjectMetadataSource {
            are_forks:     MetadataVec::new("fork",              dir.as_str(), &log, BoolExtractor),
            are_archived:  MetadataVec::new("archived",          dir.as_str(), &log, BoolExtractor),
            are_disabled:  MetadataVec::new("disabled",          dir.as_str(), &log, BoolExtractor),
            star_gazers:   MetadataVec::new("stargazers_count",  dir.as_str(), &log, CountExtractor),
            watchers:      MetadataVec::new("watchers_count",    dir.as_str(), &log, CountExtractor),
            size:          MetadataVec::new("size",              dir.as_str(), &log, CountExtractor),
            open_issues:   MetadataVec::new("open_issues_count", dir.as_str(), &log, CountExtractor),
            forks:         MetadataVec::new("forks",             dir.as_str(), &log, CountExtractor),
            subscribers:   MetadataVec::new("subscribers_count", dir.as_str(), &log, CountExtractor),
            languages:     MetadataVec::new("language",          dir.as_str(), &log, LanguageExtractor),
            descriptions:  MetadataVec::new("description",       dir.as_str(), &log, StringExtractor),
            homepages:     MetadataVec::new("homepage",          dir.as_str(), &log, StringExtractor),
            licenses:      MetadataVec::new("license",           dir.as_str(), &log, FieldExtractor("name", StringExtractor)),
            has_issues:    MetadataVec::new("has_issues",        dir.as_str(), &log, BoolExtractor),
            has_downloads: MetadataVec::new("has_downloads",     dir.as_str(), &log, BoolExtractor),
            has_wiki:      MetadataVec::new("has_wiki",          dir.as_str(), &log, BoolExtractor),
            has_pages:     MetadataVec::new("has_pages",         dir.as_str(), &log, BoolExtractor),
            created:       MetadataVec::new("created_at",        dir.as_str(), &log, TimestampExtractor),
            updated:       MetadataVec::new("updated_at",        dir.as_str(), &log, TimestampExtractor),
            pushed:        MetadataVec::new("pushed_at",         dir.as_str(), &log, TimestampExtractor),
            master:        MetadataVec::new("default_branch",    dir.as_str(), &log, StringExtractor),

            loaded:        false,
            //log:           log.clone(),
        }
    }
}

impl ProjectMetadataSource {
    pub fn is_fork          (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<bool>     { gimme!(self, are_forks,     store, pirate, key)           }
    pub fn is_archived      (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<bool>     { gimme!(self, are_archived,  store, pirate, key)           }
    pub fn is_disabled      (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<bool>     { gimme!(self, are_disabled,  store, pirate, key)           }
    pub fn star_gazers      (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<usize>    { gimme!(self, star_gazers,   store, pirate, key)           }
    pub fn watchers         (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<usize>    { gimme!(self, watchers,      store, pirate, key)           }
    pub fn size             (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<usize>    { gimme!(self, size,          store, pirate, key)           }
    pub fn open_issues      (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<usize>    { gimme!(self, open_issues,   store, pirate, key)           }
    pub fn forks            (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<usize>    { gimme!(self, forks,         store, pirate, key)           }
    pub fn subscribers      (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<usize>    { gimme!(self, subscribers,   store, pirate, key)           }
    pub fn license          (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<String>   { gimme!(self, licenses,      store, pirate, key)           }
    pub fn description      (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<String>   { gimme!(self, descriptions,  store, pirate, key)           }
    pub fn homepage         (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<String>   { gimme!(self, homepages,     store, pirate, key)           }
    pub fn language         (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<Language> { gimme!(self, languages,     store, pirate, key)           }
    pub fn has_issues       (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<bool>     { gimme!(self, has_issues,    store, pirate, key)           }
    pub fn has_downloads    (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<bool>     { gimme!(self, has_downloads, store, pirate, key)           }
    pub fn has_wiki         (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<bool>     { gimme!(self, has_wiki,      store, pirate, key)           }
    pub fn has_pages        (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<bool>     { gimme!(self, has_pages,     store, pirate, key)           }
    pub fn created          (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<i64>      { gimme!(self, created,       store, pirate, key)           }
    pub fn updated          (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<i64>      { gimme!(self, updated,       store, pirate, key)           }
    pub fn pushed           (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<i64>      { gimme!(self, pushed,        store, pirate, key)           }
    pub fn master           (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<String>   { gimme!(self, master,        store, pirate, key)           }
}

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
}

impl ProjectMetadataSource {
    pub fn keys(&mut self, store: &DatastoreView) -> impl Iterator<Item=ProjectId> {
        let mut keys = BTreeSet::new();
        keys.append(&mut gimme_iter!(self, are_forks,     store).map(|(id, _)| id.clone()).collect());
        keys.append(&mut gimme_iter!(self, are_archived,  store).map(|(id, _)| id.clone()).collect());
        keys.append(&mut gimme_iter!(self, are_disabled,  store).map(|(id, _)| id.clone()).collect());
        keys.append(&mut gimme_iter!(self, star_gazers,   store).map(|(id, _)| id.clone()).collect());
        keys.append(&mut gimme_iter!(self, watchers,      store).map(|(id, _)| id.clone()).collect());
        keys.append(&mut gimme_iter!(self, size,          store).map(|(id, _)| id.clone()).collect());
        keys.append(&mut gimme_iter!(self, open_issues,   store).map(|(id, _)| id.clone()).collect());
        keys.append(&mut gimme_iter!(self, forks,         store).map(|(id, _)| id.clone()).collect());
        keys.append(&mut gimme_iter!(self, subscribers,   store).map(|(id, _)| id.clone()).collect());
        keys.append(&mut gimme_iter!(self, languages,     store).map(|(id, _)| id.clone()).collect());
        keys.append(&mut gimme_iter!(self, descriptions,  store).map(|(id, _)| id.clone()).collect());
        keys.append(&mut gimme_iter!(self, homepages,     store).map(|(id, _)| id.clone()).collect());
        keys.append(&mut gimme_iter!(self, licenses,      store).map(|(id, _)| id.clone()).collect());
        keys.append(&mut gimme_iter!(self, has_issues,    store).map(|(id, _)| id.clone()).collect());
        keys.append(&mut gimme_iter!(self, has_downloads, store).map(|(id, _)| id.clone()).collect());
        keys.append(&mut gimme_iter!(self, has_wiki,      store).map(|(id, _)| id.clone()).collect());
        keys.append(&mut gimme_iter!(self, has_pages,     store).map(|(id, _)| id.clone()).collect());
        keys.append(&mut gimme_iter!(self, created,       store).map(|(id, _)| id.clone()).collect());
        keys.append(&mut gimme_iter!(self, updated,       store).map(|(id, _)| id.clone()).collect());
        keys.append(&mut gimme_iter!(self, pushed,        store).map(|(id, _)| id.clone()).collect());
        keys.append(&mut gimme_iter!(self, master,        store).map(|(id, _)| id.clone()).collect());
        keys.into_iter()
    }

    pub fn all_metadata(&mut self, store: &DatastoreView, key: &ProjectId) -> ProjectMetadata {
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
        }
    }

    pub fn iter<'a>(&'a mut self, store: &'a DatastoreView) -> impl Iterator<Item=ProjectMetadata> + 'a {
        self.keys(store)
            .map(|project_id| self.all_metadata(store, &project_id))
            .collect::<Vec<ProjectMetadata>>()
            .into_iter()
    }
}

impl MetadataSource for ProjectMetadataSource {
    fn load_all_from(&mut self, metadata: &HashMap<ProjectId, serde_json::Map<String, JSON>>) {
        macro_rules! load_from_store {
            ($($id:ident),+) => {
                $( self.$id.load_from_store(metadata); )*
            }
        }
        load_from_store!(are_forks, are_archived, are_disabled, star_gazers, watchers, size,
                        open_issues, forks, subscribers, licenses, languages, descriptions,
                        homepages, has_issues, has_downloads, has_wiki, has_pages, created,
                        updated, pushed, master);
    }

    fn store_all_to_cache(&mut self) -> Result<(), Vec<Box<dyn Error>>> {
        macro_rules! save_to_store {
            ($($id:ident),+) => {
                run_and_consolidate_errors!(
                    $( self.$id.store_to_cache()  ),*
                )
            }
        }
        save_to_store! (are_forks, are_archived, are_disabled, star_gazers, watchers, size,
                        open_issues, forks, subscribers, licenses, languages, descriptions,
                        homepages, has_issues, has_downloads, has_wiki, has_pages, created,
                        updated, pushed, master)
    }
}