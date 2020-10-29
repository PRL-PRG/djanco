use std::rc::Rc;
use std::cell::RefCell;
use std::path::PathBuf;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fs::{File, create_dir_all};

use serde::Serialize;
use serde::de::DeserializeOwned;

use dcd::DatastoreView;

use crate::objects::*;
use serde_json::{Value, to_value};
use serde::export::PhantomData;
use serde_cbor;
use serde_json::value::Value::Number;
use itertools::Itertools;

pub type DataPtr = Rc<RefCell<Data>>;

trait Persistent: Serialize + DeserializeOwned {}
impl<T> Persistent for T where T: Serialize + DeserializeOwned {}

struct PersistentSource<K: Ord + Persistent, V: Clone + Persistent> {
    name: String,
    cache_path: PathBuf,
    cache_dir: PathBuf,
    map: Option<BTreeMap<K, V>>,
    extractor: Box<dyn Fn(&DatastoreView) -> BTreeMap<K, V>>
}

// impl<K,V> PersistentSource<K, V>
//     where K: Ord + Persistent, V: Clone + Persistent {
//     pub fn new<Sa,Sb,F>(name: Sa, cache_dir: Sb, extractor: F) -> PersistentSource<K, V>
//         where Sa: Into<String>, Sb: Into<String>,
//               F: Fn(&DatastoreView) -> BTreeMap<K, V> + 'static {
//
//         let name: String = name.into();
//
//         let mut cache_path = PathBuf::new();
//         cache_path.push(std::path::Path::new(cache_dir.into().as_str()));
//         cache_path.push(std::path::Path::new(name.as_str()));
//         cache_path.set_extension(".cbor");
//
//         PersistentSource { name, cache_path, map: None, extractor: Box::new(extractor) }
//     }
// }

impl<K,V> PersistentSource<K, Vec<V>>
    where K: Ord + Persistent, V: Clone + Persistent {
    pub fn new<Sa,Sb,F>(name: Sa, dir: Sb, extractor: F) -> PersistentSource<K, V>
        where Sa: Into<String>, Sb: Into<String>,
              F: Fn(&DatastoreView) -> BTreeMap<K, V> + 'static {

        let name: String = name.into();

        let mut cache_dir = PathBuf::new();
        cache_dir.push(std::path::Path::new(dir.into().as_str()));

        let mut cache_path = cache_dir.clone();
        cache_path.push(std::path::Path::new(name.as_str()));
        cache_path.set_extension(".cbor");

        let map = None; // Lazy.

        PersistentSource { name, cache_path, cache_dir, map, extractor: Box::new(extractor) }
    }
}

impl<K,V> PersistentSource<K, V>
    where K: Ord + Persistent, V: Clone + Persistent {
    fn already_cached(&self) -> bool {
        self.cache_path.is_file()
    }
    fn load_from_data_store(&mut self, data_store: &DatastoreView) {
        self.map = Some(self.extractor.as_ref()(data_store));
    }
    fn load_from_cache(&mut self) -> Result<(), Box<dyn Error>> {
        let reader = File::open(&self.cache_path)?;
        self.map = Some(serde_cbor::from_reader(reader)?);
        Ok(())
    }
    fn store_to_cache(&mut self) -> Result<(), Box<dyn Error>> {
        create_dir_all(&self.cache_dir)?;
        let writer = File::create(&self.cache_path)?;
        serde_cbor::to_writer(writer, &self.map)?;
        Ok(())
    }
}

impl<K,V> PersistentSource<K,V>
    where K: Ord + Persistent, V: Clone + Persistent {
    pub fn data(&mut self, data_store: &DatastoreView) -> &BTreeMap<K, V> {
        if self.map.is_none() {
            if self.already_cached() {
                self.load_from_cache().unwrap()
            } else {
                self.load_from_data_store(data_store);
                self.store_to_cache().unwrap()
            }
        }
        self.map.as_ref().unwrap()
    }
    pub fn get(&mut self, data_store: &DatastoreView, key: &K) -> Option<&V> {
        self.data(data_store).get(key)
    }
    pub fn pirate(&mut self, data_store: &DatastoreView, key: &K) -> Option<V> { // get owned
        self.get(data_store, key).map(|v| v.clone())
    }
}

trait MetadataFieldExtractor {
    type Value;
    fn get(&self, value: &serde_json::Value) -> Self::Value;
}

struct BoolExtractor;
impl MetadataFieldExtractor for BoolExtractor {
    type Value = bool;
    fn get(&self, value: &serde_json::Value) -> Self::Value {
        match value {
            serde_json::Value::Bool(b) => *b,
            value => panic!("Expected Bool, found {:?}", value),
        }
    }
}

struct CountExtractor;
impl MetadataFieldExtractor for CountExtractor {
    type Value = usize;
    fn get(&self, value: &serde_json::Value) -> Self::Value {
        match value {
            serde_json::Value::Number(n) if n.is_u64() => n.as_u64().unwrap() as usize,
            serde_json::Value::Number(n) => panic!("Expected Number >= 0, found {:?}", value),
            value => panic!("Expected Number, found {:?}", value),
        }
    }
}

struct StringExtractor;
impl MetadataFieldExtractor for StringExtractor {
    type Value = String;
    fn get(&self, value: &serde_json::Value) -> Self::Value {
        match value {
            serde_json::Value::String(s) => s.clone(),
            value => panic!("Expected String, found {:?}", value),
        }
    }
}

struct FieldExtractor<M: MetadataFieldExtractor>(pub &'static str, pub M);
// impl<T, M> FieldExtractor<M> where M: MetadataFieldExtractor<Value=T> {
//     pub fn new<S>(name: S, extractor: M) -> Self where S: Into<String> {
//         FieldExtractor { name: name.into(), extractor }
//     }
// }
impl<T, M> MetadataFieldExtractor for FieldExtractor<M> where M: MetadataFieldExtractor<Value=T>{
    type Value = T;
    fn get(&self, value: &serde_json::Value) -> Self::Value {
        match value {
            serde_json::Value::Object(map) => {
                self.1.get(&map.get(&self.0.to_owned()).unwrap())
            },
            value => panic!("Expected String, found {:?}", value),
        }
    }
}

struct NullableStringExtractor;
impl MetadataFieldExtractor for NullableStringExtractor {
    type Value = Option<String>;
    fn get(&self, value: &serde_json::Value) -> Self::Value {
        match value {
            serde_json::Value::String(s) => Some(s.clone()),
            serde_json::Value::Null => None,
            value => panic!("Expected String or Null, found {:?}", value),
        }
    }
}

struct MetadataVec<M: MetadataFieldExtractor> {
    name: String,
    extractor: M,
    vector: Option<BTreeMap<ProjectId, M::Value>>,
}

impl<M> MetadataVec<M> where M: MetadataFieldExtractor {
    pub fn new<S> (name: S, extractor: M) -> Self where S: Into<String> {
        Self { name: name.into(), extractor, vector: None }
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn already_loaded(&self) -> bool {
        self.vector.is_some()
    }

    pub fn load_from(&mut self, metadata: &HashMap<ProjectId, serde_json::Map<String, serde_json::Value>>) {
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
}

pub struct MetadataSource<'a> {
    data_store:   &'a DatastoreView,
    forks:            MetadataVec<BoolExtractor>,
    archived:         MetadataVec<BoolExtractor>,
    disabled:         MetadataVec<BoolExtractor>,
    star_gazers:      MetadataVec<CountExtractor>,
    watchers:         MetadataVec<CountExtractor>,
    size:             MetadataVec<CountExtractor>,
    open_issues:      MetadataVec<CountExtractor>,
    network:          MetadataVec<CountExtractor>,
    subscribers:      MetadataVec<CountExtractor>,
    licenses:         MetadataVec<FieldExtractor<StringExtractor>>,
    languages:        MetadataVec<StringExtractor>,
    descriptions:     MetadataVec<StringExtractor>,
    homepages:        MetadataVec<StringExtractor>,
}

impl<'a> MetadataSource<'a> {
    pub fn new(data_store: &'a DatastoreView) -> Self {
        MetadataSource {
            data_store,
            forks:        MetadataVec::new("fork",              BoolExtractor),
            archived:     MetadataVec::new("archived",          BoolExtractor),
            disabled:     MetadataVec::new("disabled",          BoolExtractor),
            star_gazers:  MetadataVec::new("star_gazers_count", CountExtractor),
            watchers:     MetadataVec::new("watchers_count",    CountExtractor),
            size:         MetadataVec::new("size",              CountExtractor),
            open_issues:  MetadataVec::new("open_issues_count", CountExtractor),
            network:      MetadataVec::new("network_count",     CountExtractor),
            subscribers:  MetadataVec::new("subscribers_count", CountExtractor),
            languages:    MetadataVec::new("language",          StringExtractor),
            descriptions: MetadataVec::new("description",       StringExtractor),
            homepages:    MetadataVec::new("homepage",          StringExtractor),
            licenses:     MetadataVec::new("license",           FieldExtractor("name", StringExtractor)),
        }
    }

    fn load_all_from(&mut self, metadata: &HashMap<ProjectId, serde_json::Map<String, Value>>) {
        self.forks.load_from(metadata);
        self.archived.load_from(metadata);
        self.disabled.load_from(metadata);
        self.star_gazers.load_from(metadata);
        self.watchers.load_from(metadata);
        self.size.load_from(metadata);
        self.open_issues.load_from(metadata);
        self.network.load_from(metadata);
        self.subscribers.load_from(metadata);
        self.licenses.load_from(metadata);
        self.languages.load_from(metadata);
        self.descriptions.load_from(metadata);
        self.homepages.load_from(metadata);
    }

    fn load_metadata(&mut self) -> HashMap<ProjectId, serde_json::Map<String, Value>> {
        let project_content_ids: HashMap<u64, u64> =
            self.data_store.projects_metadata()
                .filter(|(id, meta)| meta.key == "github_metadata")
                .map(|(id, metadata)| (id, metadata.value.parse::<u64>().unwrap()))
                .collect();

        let content_project_ids: HashMap<u64, u64> =
            project_content_ids.into_iter()
                .map(|(project_id, content_id)| (content_id, project_id))
                .collect();

        self.data_store.contents()
            .filter(|(content_id, _)| content_project_ids.contains_key(content_id))
            .map(|(content_id, contents)| {
                let json: Value = serde_json::from_slice(contents.as_slice()).unwrap();
                let project_id = content_project_ids.get(&content_id).unwrap();
                match json {
                    serde_json::Value::Object(map) => (ProjectId::from(project_id), map),
                    meta => panic!("Unexpected JSON value for metadata: {:?}", meta),
                }
            }).collect()
    }

    pub fn load_from_datastore(&mut self) {
        let metadata = self.load_metadata();
        self.load_all_from(&metadata)
    }
}

pub struct Data {
    data_store:              DatastoreView,

    // project_timestamps:      PersistentSource<ProjectId, i64>,
    project_languages:       PersistentSource<ProjectId, String>,
    project_stars:           PersistentSource<ProjectId, usize>,
    project_issues:          PersistentSource<ProjectId, usize>,
    project_buggy_issues:    PersistentSource<ProjectId, usize>,
    project_heads:           PersistentSource<ProjectId, Vec<(String, CommitId)>>,

    project_users:           PersistentSource<ProjectId, Vec<UserId>>,
    project_authors:         PersistentSource<ProjectId, Vec<UserId>>,
    project_committers:      PersistentSource<ProjectId, Vec<UserId>>,

    project_users_count:     PersistentSource<ProjectId, usize>,
    project_author_count:    PersistentSource<ProjectId, usize>,
    project_committer_count: PersistentSource<ProjectId, usize>,

    project_commits:         PersistentSource<ProjectId, Vec<CommitId>>,
    project_commit_count:    PersistentSource<ProjectId, usize>,

    users:                   PersistentSource<UserId, User>,
    paths:                   PersistentSource<PathId, Path>,

    commits:                 PersistentSource<CommitId, Commit>,
    commit_messages:         PersistentSource<CommitId, String>,
}

impl From<(u64, dcd::Commit)> for Commit {
    fn from((id, c): (u64, dcd::Commit)) -> Self {
        Commit {
            id: CommitId::from(id),
            committer: UserId::from(c.committer),
            author: UserId::from(c.author),
            parents: c.parents.into_iter().map(|id| CommitId::from(id)).collect(),
        }
    }
}

impl Data {
    pub fn from_data_store<S>(data_store: DatastoreView, cache_dir: S) -> Data where S: Into<String> {
        let dir = cache_dir.into();
        Data {
            data_store,
            // project_timestamps: PersistentSource::new("project_timestamps", dir.clone(), |ds| {
            //     ds.
            // }),
            // FIXME All metadata could potentially be loaded in in one go.
            project_languages: PersistentSource::new("project_languages", dir.clone(), |ds| {
                ds.projects_metadata()
                    .filter(|(_, metadata)| metadata.key == "")
                    .map(|(id, metadata)| (ProjectId::from(id), metadata.key)).collect()
            }),
            project_stars: PersistentSource::new("project_stars", dir.clone(), |ds| {
                unimplemented!()
            }),
            project_issues: PersistentSource::new("project_issues", dir.clone(), |ds| {
                unimplemented!()
            }),
            project_buggy_issues: PersistentSource::new("project_buggy_issues", dir.clone(), |ds| {
                unimplemented!()
            }),
            project_heads: PersistentSource::new("project_heads", dir.clone(), |ds| {
                unimplemented!()
            }),
            project_users: PersistentSource::new("project_users", dir.clone(), |ds| {
                unimplemented!()
            }),
            project_authors: PersistentSource::new("project_authors", dir.clone(), |ds| {
                unimplemented!()
            }),
            project_committers: PersistentSource::new("project_committers", dir.clone(), |ds| {
                unimplemented!()
            }),
            project_users_count: PersistentSource::new("project_user_count", dir.clone(), |ds| {
                unimplemented!()
            }),
            project_author_count: PersistentSource::new("project_author_count", dir.clone(), |ds| {
                unimplemented!()
            }),
            project_committer_count: PersistentSource::new("project_committer_count", dir.clone(), |ds| {
                unimplemented!()
            }),
            project_commits: PersistentSource::new("project_commits", dir.clone(), |ds| {
                unimplemented!()
            }),
            project_commit_count: PersistentSource::new("project_commit_count", dir.clone(), |ds| {
                unimplemented!()
            }),
            users: PersistentSource::new("users", dir.clone(), |ds| {
                ds.users().map(|(id, email)| (UserId::from(id), User::new(UserId::from(id), email))).collect()
            }),
            paths: PersistentSource::new("paths", dir.clone(), |ds| {
                ds.paths().map(|(id, location)| (PathId::from(id), Path::new(PathId::from(id), location))).collect()
            }),
            commits: PersistentSource::new("commits", dir.clone(), |ds| {
                ds.commits().map(|(id, commit)| { (CommitId::from(id), Commit::from((id, commit))) }).collect()
            }),
            commit_messages: PersistentSource::new("commit_messages", dir.clone(), |ds| {
                ds.commits().map(|(id, commit)| (CommitId::from(id), commit.message)).collect() // TODO maybe return iter?
            }),
        }
    }
}

impl Data {
    //pub fn project_timestamp      (&mut self, id: &ProjectId) -> i64                     { *self.project_timestamps.get(&self.data_store, id).unwrap()   } // Last update timestamps are obligatory
    pub fn project_language       (&mut self, id: &ProjectId) -> Option<String>          { self.project_languages.pirate(&self.data_store,id)           }
    pub fn project_stars          (&mut self, id: &ProjectId) -> Option<usize>           { self.project_stars.pirate(&self.data_store,id)               }
    pub fn project_issues         (&mut self, id: &ProjectId) -> Option<usize>           { self.project_issues.pirate(&self.data_store,id)              }
    pub fn project_buggy_issues   (&mut self, id: &ProjectId) -> Option<usize>           { self.project_buggy_issues.pirate(&self.data_store,id)        }
    pub fn project_heads          (&mut self, id: &ProjectId) -> Vec<(String, CommitId)> { self.project_heads.pirate(&self.data_store,id).unwrap()      } // Heads are obligatory

    pub fn project_users          (&mut self, id: &ProjectId) -> Vec<User>               { self.project_users.pirate(&self.data_store,id).unwrap().reify(self)      } // Obligatory, but can be 0 length
    pub fn project_authors        (&mut self, id: &ProjectId) -> Vec<User>               { self.project_authors.pirate(&self.data_store,id).unwrap().reify(self)    } // Obligatory, but can be 0 length
    pub fn project_committers     (&mut self, id: &ProjectId) -> Vec<User>               { self.project_committers.pirate(&self.data_store,id).unwrap().reify(self) } // Obligatory, but can be 0 length

    pub fn project_user_count     (&mut self, id: &ProjectId) -> usize                   { *self.project_users_count.get(&self.data_store,id).unwrap()     } // Obligatory
    pub fn project_author_count   (&mut self, id: &ProjectId) -> usize                   { *self.project_author_count.get(&self.data_store,id).unwrap()    } // Obligatory
    pub fn project_committer_count(&mut self, id: &ProjectId) -> usize                   { *self.project_committer_count.get(&self.data_store,id).unwrap() } // Obligatory

    pub fn project_commits        (&mut self, id: &ProjectId) -> usize                   { *self.project_committer_count.get(&self.data_store,id).unwrap() } // Obligatory
    pub fn project_commit_count   (&mut self, id: &ProjectId) -> usize                   { *self.project_committer_count.get(&self.data_store,id).unwrap() } // Obligatory

    pub fn user                   (&mut self, id: &UserId) -> Option<User>               { self.users.pirate(&self.data_store,id)                          }
    pub fn path                   (&mut self, id: &PathId) -> Option<Path>               { self.paths.pirate(&self.data_store,id)                          }

    pub fn commit                 (&mut self, id: &CommitId) -> Option<Commit>           { self.commits.pirate(&self.data_store,id)                        }
    pub fn commit_message         (&mut self, id: &CommitId) -> Option<String>           { self.commit_messages.pirate(&self.data_store,id)                }
}