use std::path::PathBuf;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fs::{File, create_dir_all};

use serde_json::Value as JSON;
use chrono::DateTime;

use dcd::DatastoreView;

use crate::persistent::*;
use crate::objects::*;

trait MetadataFieldExtractor {
    type Value: Persistent;
    fn get(&self, value: &JSON) -> Self::Value;
}

struct BoolExtractor;
impl MetadataFieldExtractor for BoolExtractor {
    type Value = bool;
    fn get(&self, value: &JSON) -> Self::Value {
        match value {
            JSON::Bool(b) => *b,
            value => panic!("Expected Bool, found {:?}", value),
        }
    }
}

struct CountExtractor;
impl MetadataFieldExtractor for CountExtractor {
    type Value = usize;
    fn get(&self, value: &JSON) -> Self::Value {
        match value {
            JSON::Number(n) if n.is_u64() => n.as_u64().unwrap() as usize,
            JSON::Number(n) => panic!("Expected Number >= 0, found {:?}", n),
            value => panic!("Expected Number, found {:?}", value),
        }
    }
}

struct StringExtractor;
impl MetadataFieldExtractor for StringExtractor {
    type Value = String;
    fn get(&self, value: &JSON) -> Self::Value {
        match value {
            JSON::String(s) => s.clone(),
            value => panic!("Expected String, found {:?}", value),
        }
    }
}

struct TimestampExtractor;
impl MetadataFieldExtractor for TimestampExtractor {
    type Value = i64;
    fn get(&self, value: &JSON) -> Self::Value {
        match value {
            JSON::String(s) => {
                let date = DateTime::parse_from_rfc3339(s).unwrap(); // Should be there, right?
                date.timestamp()
            }
            value => panic!("Expected String, found {:?}", value),
        }
    }
}

struct LanguageExtractor;
impl MetadataFieldExtractor for LanguageExtractor {
    type Value = Option<Language>;
    fn get(&self, value: &JSON) -> Self::Value {
        match value {
            JSON::String(s) => {
                let language = Language::from_str(s);
                if language.is_none() {
                    eprintln!("WARNING: language {} is unknown, so it will be treated as None", s)
                }
                language
            },
            value => panic!("Expected String, found {:?}", value),
        }
    }
}

struct FieldExtractor<M: MetadataFieldExtractor>(pub &'static str, pub M);
impl<T, M> MetadataFieldExtractor for FieldExtractor<M>
    where M: MetadataFieldExtractor<Value=T>, T: Persistent {
    type Value = T;
    fn get(&self, value: &JSON) -> Self::Value {
        match value {
            JSON::Object(map) => {
                self.1.get(&map.get(&self.0.to_owned()).unwrap())
            },
            value => panic!("Expected String, found {:?}", value),
        }
    }
}

struct NullableStringExtractor;
impl MetadataFieldExtractor for NullableStringExtractor {
    type Value = Option<String>;
    fn get(&self, value: &JSON) -> Self::Value {
        match value {
            JSON::String(s) => Some(s.clone()),
            JSON::Null => None,
            value => panic!("Expected String or Null, found {:?}", value),
        }
    }
}

struct MetadataVec<M: MetadataFieldExtractor> {
    name: String,
    cache_path: PathBuf,
    cache_dir: PathBuf,
    extractor: M,
    vector: Option<BTreeMap<ProjectId, M::Value>>,
}

impl<M> MetadataVec<M> where M: MetadataFieldExtractor {
    pub fn new<Sa, Sb>(name: Sa, dir: Sb, extractor: M) -> Self
        where Sa: Into<String>, Sb: Into<String> {
        let name: String = name.into();

        let mut cache_dir = PathBuf::new();
        cache_dir.push(std::path::Path::new(dir.into().as_str()));

        let mut cache_path = cache_dir.clone();
        cache_path.push(std::path::Path::new(name.as_str()));
        cache_path.set_extension(PERSISTENT_EXTENSION);

        Self { name, extractor, vector: None, cache_dir, cache_path }
    }

    pub fn already_loaded(&self) -> bool { self.vector.is_some() }
    pub fn already_cached(&self) -> bool { self.cache_path.is_file() }

    pub fn load_from_store(&mut self, metadata: &HashMap<ProjectId, serde_json::Map<String, JSON>>) {
        if !self.already_loaded() {
            self.vector = Some(
                metadata.iter()
                    .map(|(id, properties)| {
                        let property = properties.get(&self.name).unwrap();
                        (id.clone(), self.extractor.get(property))
                    }).collect()
            )
        }
    }

    fn load_from_cache(&mut self) -> Result<(), Box<dyn Error>> {
        let reader = File::open(&self.cache_path)?;
        self.vector = Some(serde_cbor::from_reader(reader)?);
        Ok(())
    }

    fn store_to_cache(&mut self) -> Result<(), Box<dyn Error>> {
        create_dir_all(&self.cache_dir)?;
        let writer = File::create(&self.cache_path)?;
        serde_cbor::to_writer(writer, &self.vector)?;
        Ok(())
    }

    pub fn data(&mut self) -> &BTreeMap<ProjectId, M::Value> {
        if !self.already_loaded() {
            if self.already_cached() {
                self.load_from_cache().unwrap();
            } else {
                panic!("Must preload data from data store before accessing!");
            }
        }
        self.vector.as_ref().unwrap()
    }

    pub fn get(&mut self, key: &ProjectId) -> Option<&M::Value> {
        self.data().get(key)
    }
}

impl<T,M> MetadataVec<M> where M: MetadataFieldExtractor<Value=T>, T: Clone + Persistent {
    pub fn pirate(&mut self, key: &ProjectId) -> Option<M::Value> { // get owned
        self.get(key).map(|v| v.clone())
    }
}

trait MetadataSource {
    fn load_metadata(&mut self, store: &DatastoreView) -> HashMap<ProjectId, serde_json::Map<String, JSON>> {
        let content_project_ids: HashMap<u64, u64> =
            store.projects_metadata()
                .filter(|(_, meta)| meta.key == "github_metadata")
                .map(|(id, metadata)| (id, metadata.value.parse::<u64>().unwrap()))
                .map(|(project_id, content_id)| (content_id, project_id))
                .collect();

        store.contents()
            .filter(|(content_id, _)| content_project_ids.contains_key(content_id))
            .map(|(content_id, contents)| {
                let json: JSON = serde_json::from_slice(contents.as_slice()).unwrap();
                let project_id = content_project_ids.get(&content_id).unwrap();
                match json {
                    JSON::Object(map) => (ProjectId::from(project_id), map),
                    meta => panic!("Unexpected JSON value for metadata: {:?}", meta),
                }
            }).collect()
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

macro_rules! run_and_consolidate_errors {
    ($($statements:block),*) => {{
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
    pub fn new<Sa, Sb>(name: Sa, dir: Sb) -> Self where Sa: Into<String>, Sb: Into<String> {
        let dir = Self::prepare_dir(name, dir);
        ProjectMetadataSource {
            are_forks:     MetadataVec::new("fork",              dir.as_str(), BoolExtractor),
            are_archived:  MetadataVec::new("archived",          dir.as_str(), BoolExtractor),
            are_disabled:  MetadataVec::new("disabled",          dir.as_str(), BoolExtractor),
            star_gazers:   MetadataVec::new("star_gazers_count", dir.as_str(), CountExtractor),
            watchers:      MetadataVec::new("watchers_count",    dir.as_str(), CountExtractor),
            size:          MetadataVec::new("size",              dir.as_str(), CountExtractor),
            open_issues:   MetadataVec::new("open_issues_count", dir.as_str(), CountExtractor),
            forks:         MetadataVec::new("forks",             dir.as_str(), CountExtractor),
            subscribers:   MetadataVec::new("subscribers_count", dir.as_str(), CountExtractor),
            languages:     MetadataVec::new("language",          dir.as_str(), LanguageExtractor),
            descriptions:  MetadataVec::new("description",       dir.as_str(), StringExtractor),
            homepages:     MetadataVec::new("homepage",          dir.as_str(), StringExtractor),
            licenses:      MetadataVec::new("license",           dir.as_str(), FieldExtractor("name", StringExtractor)),
            has_issues:    MetadataVec::new("has_issues",        dir.as_str(), BoolExtractor),
            has_downloads: MetadataVec::new("has_downloads",     dir.as_str(), BoolExtractor),
            has_wiki:      MetadataVec::new("has_wiki",          dir.as_str(), BoolExtractor),
            has_pages:     MetadataVec::new("has_pages",         dir.as_str(), BoolExtractor),
            created:       MetadataVec::new("created_at",        dir.as_str(), TimestampExtractor),
            updated:       MetadataVec::new("updated_at",        dir.as_str(), TimestampExtractor),
            pushed:        MetadataVec::new("pushed_at",         dir.as_str(), TimestampExtractor),
            master:        MetadataVec::new("default_branch",    dir.as_str(), StringExtractor),

            loaded:        false,
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
    pub fn license_owned    (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<String>   { gimme!(self, licenses,      store, pirate, key)           }
    pub fn description_owned(&mut self, store: &DatastoreView, key: &ProjectId) -> Option<String>   { gimme!(self, descriptions,  store, pirate, key)           }
    pub fn homepage_owned   (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<String>   { gimme!(self, homepages,     store, pirate, key)           }
    pub fn license          (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<&String>  { gimme!(self, licenses,      store, get,    key)           }
    pub fn description      (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<&String>  { gimme!(self, descriptions,  store, get,    key)           }
    pub fn homepage         (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<&String>  { gimme!(self, homepages,     store, get,    key)           }
    pub fn language         (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<Language> { gimme!(self, languages,     store, pirate, key).flatten() }
    pub fn has_issues       (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<bool>     { gimme!(self, has_issues,    store, pirate, key)           }
    pub fn has_downloads    (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<bool>     { gimme!(self, has_downloads, store, pirate, key)           }
    pub fn has_wiki         (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<bool>     { gimme!(self, has_wiki,      store, pirate, key)           }
    pub fn has_pages        (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<bool>     { gimme!(self, has_pages,     store, pirate, key)           }
    pub fn created          (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<i64>      { gimme!(self, created,       store, pirate, key)           }
    pub fn updated          (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<i64>      { gimme!(self, updated,       store, pirate, key)           }
    pub fn pushed           (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<i64>      { gimme!(self, pushed,        store, pirate, key)           }
    pub fn master           (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<&String>  { gimme!(self, homepages,     store, get,    key)           }
    pub fn master_owned     (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<String>   { gimme!(self, homepages,     store, pirate, key)           }
}

impl MetadataSource for ProjectMetadataSource {
    fn load_all_from(&mut self, metadata: &HashMap<ProjectId, serde_json::Map<String, JSON>>) {
        self.are_forks.load_from_store(metadata);
        self.are_archived.load_from_store(metadata);
        self.are_disabled.load_from_store(metadata);
        self.star_gazers.load_from_store(metadata);
        self.watchers.load_from_store(metadata);
        self.size.load_from_store(metadata);
        self.open_issues.load_from_store(metadata);
        self.forks.load_from_store(metadata);
        self.subscribers.load_from_store(metadata);
        self.licenses.load_from_store(metadata);
        self.languages.load_from_store(metadata);
        self.descriptions.load_from_store(metadata);
        self.homepages.load_from_store(metadata);
    }

    fn store_all_to_cache(&mut self) -> Result<(), Vec<Box<dyn Error>>> {
        run_and_consolidate_errors!(
            { self.are_forks.store_to_cache()     },
            { self.are_archived.store_to_cache()  },
            { self.are_disabled.store_to_cache()  },
            { self.star_gazers.store_to_cache()   },
            { self.watchers.store_to_cache()      },
            { self.size.store_to_cache()          },
            { self.open_issues.store_to_cache()   },
            { self.forks.store_to_cache()         },
            { self.subscribers.store_to_cache()   },
            { self.licenses.store_to_cache()      },
            { self.languages.store_to_cache()     },
            { self.descriptions.store_to_cache()  },
            { self.homepages.store_to_cache()     },
            { self.has_issues.store_to_cache()    },
            { self.has_downloads.store_to_cache() },
            { self.has_wiki.store_to_cache()      },
            { self.has_pages.store_to_cache()     },
            { self.created.store_to_cache()       },
            { self.updated.store_to_cache()       },
            { self.pushed.store_to_cache()        },
            { self.master.store_to_cache()        })
    }
}