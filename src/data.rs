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

use serde_json::Value as JSON;
use serde_cbor;
use serde::export::PhantomData;
use crate::piracy::Piracy;
use itertools::{Itertools, MinMaxResult};
use std::time::Duration;

pub type DataPtr = Rc<RefCell<Data>>;

static CACHE_EXTENSION: &str = ".cbor";
trait Persistent: Serialize + DeserializeOwned {}
impl<T> Persistent for T where T: Serialize + DeserializeOwned {}

trait Extractor {
    type Key:   Ord + Persistent;
    type Value: Clone + Persistent;
}

trait SingleExtractor: Extractor {
    type A;
    fn extract(a: &Self::A) -> BTreeMap<Self::Key, Self::Value>;
}

trait DoubleExtractor: Extractor {
    type A; type B;
    fn extract(a: &Self::A, b: &Self::B) -> BTreeMap<Self::Key, Self::Value>;
}

trait TripleExtractor: Extractor {
    type A; type B; type C;
    fn extract(a: &Self::A, b: &Self::B, c: &Self::C) -> BTreeMap<Self::Key, Self::Value>;
}

trait QuadrupleExtractor: Extractor {
    type A; type B; type C; type D;
    fn extract(a: &Self::A, b: &Self::B, c: &Self::C, d: &Self::D) -> BTreeMap<Self::Key, Self::Value>;
}

struct PersistentSource<E: Extractor> {
    //name: String,
    cache_path: PathBuf,
    cache_dir: PathBuf,
    map: Option<BTreeMap<E::Key, E::Value>>,
    extractor: PhantomData<E>,
}

impl<E> PersistentSource<E> where E: Extractor {
    pub fn new<Sa,Sb>(name: Sa, dir: Sb) -> PersistentSource<E>
        where Sa: Into<String>, Sb: Into<String> {

        let name: String = name.into();

        let mut cache_dir = PathBuf::new();
        cache_dir.push(std::path::Path::new(dir.into().as_str()));

        let mut cache_path = cache_dir.clone();
        cache_path.push(std::path::Path::new(name.as_str()));
        cache_path.set_extension(CACHE_EXTENSION);

        let map = None; // Lazy.

        PersistentSource { /*name,*/ cache_path, cache_dir, map, extractor: PhantomData }
    }

    fn is_loaded(&self) -> bool {
        self.map.is_some()
    }

    fn already_cached(&self) -> bool {
        self.cache_path.is_file()
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

    fn grab(&mut self) -> &BTreeMap<E::Key, E::Value> {
        self.map.as_ref().unwrap()
    }
}

impl<E> PersistentSource<E> where E: Extractor {
    pub fn data_from_loader<F>(&mut self, mut load: F) -> &BTreeMap<E::Key, E::Value>
        where F: FnMut() -> BTreeMap<E::Key, E::Value> {

        if self.map.is_none() {
            if self.already_cached() {
                self.load_from_cache().unwrap()
            } else {
                self.map = Some(load());
                self.store_to_cache().unwrap()
            }
        }

        self.map.as_ref().unwrap()
    }
}

impl<E,A> PersistentSource<E> where E: SingleExtractor<A=A> {
    pub fn load_from_one(&mut self, input: &A) -> &BTreeMap<E::Key, E::Value> {
        self.data_from_loader(|| { E::extract(input) })
    }
}

impl<E,A,B> PersistentSource<E> where E: DoubleExtractor<A=A,B=B> {
    pub fn load_from_two(&mut self, input_a: &A, input_b: &B) -> &BTreeMap<E::Key, E::Value> {
        self.data_from_loader(|| { E::extract(input_a, input_b) })
    }
}

impl<E,A,B,C> PersistentSource<E> where E: TripleExtractor<A=A,B=B,C=C> {
    pub fn load_from_three(&mut self, input_a: &A, input_b: &B, input_c: &C) -> &BTreeMap<E::Key, E::Value> {
        self.data_from_loader(|| { E::extract(input_a, input_b, input_c) })
    }
}

impl<E,A,B,C,D> PersistentSource<E> where E: QuadrupleExtractor<A=A,B=B,C=C,D=D>{
    #[allow(dead_code)] pub fn load_from_four(&mut self, input_a: &A, input_b: &B, input_c: &C, input_d: &D) -> &BTreeMap<E::Key, E::Value> {
        self.data_from_loader(|| { E::extract(input_a, input_b, input_c, input_d) })
    }
}

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
        cache_path.set_extension(CACHE_EXTENSION);

        Self { name, extractor, vector: None, cache_dir, cache_path }
    }

    //pub fn name(&self) -> &str { self.name.as_str() }
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
            cache_subdir.set_extension(CACHE_EXTENSION);
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
    languages:        MetadataVec<LanguageExtractor>,
    descriptions:     MetadataVec<StringExtractor>,
    homepages:        MetadataVec<StringExtractor>,
}

impl ProjectMetadataSource {
    pub fn new<Sa, Sb>(name: Sa, dir: Sb) -> Self where Sa: Into<String>, Sb: Into<String> {
        let dir = Self::prepare_dir(name, dir);
        ProjectMetadataSource {
            forks:        MetadataVec::new("fork",              dir.as_str(), BoolExtractor),
            archived:     MetadataVec::new("archived",          dir.as_str(), BoolExtractor),
            disabled:     MetadataVec::new("disabled",          dir.as_str(), BoolExtractor),
            star_gazers:  MetadataVec::new("star_gazers_count", dir.as_str(), CountExtractor),
            watchers:     MetadataVec::new("watchers_count",    dir.as_str(), CountExtractor),
            size:         MetadataVec::new("size",              dir.as_str(), CountExtractor),
            open_issues:  MetadataVec::new("open_issues_count", dir.as_str(), CountExtractor),
            network:      MetadataVec::new("network_count",     dir.as_str(), CountExtractor),
            subscribers:  MetadataVec::new("subscribers_count", dir.as_str(), CountExtractor),
            languages:    MetadataVec::new("language",          dir.as_str(), LanguageExtractor),
            descriptions: MetadataVec::new("description",       dir.as_str(), StringExtractor),
            homepages:    MetadataVec::new("homepage",          dir.as_str(), StringExtractor),
            licenses:     MetadataVec::new("license",           dir.as_str(), FieldExtractor("name", StringExtractor)),
            loaded:       false,
        }
    }
}

impl ProjectMetadataSource {
    pub fn fork             (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<bool>     { gimme!(self, forks,        store, pirate, key) }
    pub fn archived         (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<bool>     { gimme!(self, archived,     store, pirate, key) }
    pub fn disabled         (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<bool>     { gimme!(self, disabled,     store, pirate, key) }

    pub fn star_gazers      (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<usize>    { gimme!(self, star_gazers,  store, pirate, key) }
    pub fn watchers         (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<usize>    { gimme!(self, watchers,     store, pirate, key) }
    pub fn size             (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<usize>    { gimme!(self, size,         store, pirate, key) }
    pub fn open_issues      (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<usize>    { gimme!(self, open_issues,  store, pirate, key) }
    pub fn network          (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<usize>    { gimme!(self, network,      store, pirate, key) }
    pub fn subscribers      (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<usize>    { gimme!(self, subscribers,  store, pirate, key) }

    pub fn license_owned    (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<String>   { gimme!(self, licenses,     store, pirate, key) }
    pub fn description_owned(&mut self, store: &DatastoreView, key: &ProjectId) -> Option<String>   { gimme!(self, descriptions, store, pirate, key) }
    pub fn homepage_owned   (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<String>   { gimme!(self, homepages,    store, pirate, key) }

    pub fn license          (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<&String>  { gimme!(self, licenses,     store, get,    key) }
    pub fn description      (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<&String>  { gimme!(self, descriptions, store, get,    key) }
    pub fn homepage         (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<&String>  { gimme!(self, homepages,    store, get,    key) }

    pub fn language         (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<Language> { gimme!(self, languages,    store, pirate, key).flatten() }
}

impl MetadataSource for ProjectMetadataSource {
    fn load_all_from(&mut self, metadata: &HashMap<ProjectId, serde_json::Map<String, JSON>>) {
        self.forks.load_from_store(metadata);
        self.archived.load_from_store(metadata);
        self.disabled.load_from_store(metadata);
        self.star_gazers.load_from_store(metadata);
        self.watchers.load_from_store(metadata);
        self.size.load_from_store(metadata);
        self.open_issues.load_from_store(metadata);
        self.network.load_from_store(metadata);
        self.subscribers.load_from_store(metadata);
        self.licenses.load_from_store(metadata);
        self.languages.load_from_store(metadata);
        self.descriptions.load_from_store(metadata);
        self.homepages.load_from_store(metadata);
    }

    fn store_all_to_cache(&mut self) -> Result<(), Vec<Box<dyn Error>>> {
        run_and_consolidate_errors!(
            { self.forks.store_to_cache()        },
            { self.archived.store_to_cache()     },
            { self.disabled.store_to_cache()     },
            { self.star_gazers.store_to_cache()  },
            { self.watchers.store_to_cache()     },
            { self.size.store_to_cache()         },
            { self.open_issues.store_to_cache()  },
            { self.network.store_to_cache()      },
            { self.subscribers.store_to_cache()  },
            { self.licenses.store_to_cache()     },
            { self.languages.store_to_cache()    },
            { self.descriptions.store_to_cache() },
            { self.homepages.store_to_cache()    }
        )
    }
}

struct ProjectHeadsExtractor;
impl Extractor for ProjectHeadsExtractor {
    type Key = ProjectId;
    type Value = Vec<(String, CommitId)>;
}
impl SingleExtractor for ProjectHeadsExtractor {
    type A = DatastoreView;
    fn extract(store: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        store.project_heads().map(|(project_id, heads)| {
            (ProjectId::from(project_id), heads.into_iter().map(|(name, commit_id)| {
                (name, CommitId::from(commit_id))
            }).collect())
        }).collect()
    }
}

struct ProjectUsersExtractor {}
impl Extractor for ProjectUsersExtractor {
    type Key = ProjectId;
    type Value = Vec<UserId>;
}
impl DoubleExtractor for ProjectUsersExtractor {
    type A = BTreeMap<ProjectId, Vec<UserId>>;
    type B = BTreeMap<ProjectId, Vec<UserId>>;
    fn extract(project_authors: &Self::A, project_committers: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        project_authors.iter().map(|(project_id, authors)| {
            let mut users: Vec<UserId> = vec![];
            let committers = project_committers.get(project_id);
            if let Some(committers) = committers {
                users.extend(committers.iter().map(|user_id| user_id.clone()));
            }
            users.extend(authors.iter().map(|user_id| user_id.clone()));
            (project_id.clone(), users.into_iter().unique().collect())
        }).collect()
    }
}
struct ProjectAuthorsExtractor {}
impl Extractor for ProjectAuthorsExtractor {
    type Key = ProjectId;
    type Value = Vec<UserId>;
}
impl DoubleExtractor for ProjectAuthorsExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Commit>;
    fn extract(project_commits: &Self::A, commits: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        project_commits.iter().map(|(project_id, commit_ids)| {
            (project_id.clone(), commit_ids.iter().flat_map(|commit_id| {
                commits.get(commit_id).map(|c| c.author_id())
            }).unique().collect())
        }).collect()
    }
}
struct ProjectCommittersExtractor {}
impl Extractor for ProjectCommittersExtractor {
    type Key = ProjectId;
    type Value = Vec<UserId>;
}
impl DoubleExtractor for ProjectCommittersExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Commit>;
    fn extract(project_commits: &Self::A, commits: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        project_commits.iter().map(|(project_id, commit_ids)| {
            (project_id.clone(), commit_ids.iter().flat_map(|commit_id| {
                commits.get(commit_id).map(|c| c.committer_id())
            }).unique().collect())
        }).collect()
    }
}

struct CountPerKeyExtractor<K: Clone + Ord + Persistent, V>(PhantomData<(K, V)>);
impl<K, V> Extractor for CountPerKeyExtractor<K, V> where K: Clone + Ord + Persistent {
    type Key = K;
    type Value = usize;
}
impl<K, V> SingleExtractor for CountPerKeyExtractor<K, V> where K: Clone + Ord + Persistent {
    type A = BTreeMap<K, Vec<V>>;

    fn extract(primary: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        primary.iter().map(|(key, value)| (key.clone(), value.len())).collect()
    }
}

struct ProjectCommitsExtractor {}
impl ProjectCommitsExtractor {
    fn commits_from_head(commits: &BTreeMap<CommitId, Commit>, head: &CommitId) -> Vec<CommitId> {
        let mut commits_in_head: Vec<CommitId> = vec![];
        let mut stack = vec![head.clone()];
        while !stack.is_empty() {
            let commit_id = stack.pop().unwrap();
            commits_in_head.push(commit_id);
            let commit = commits.get(&commit_id).unwrap(); // Potentially explosive?
            let parents = commit.parent_ids();
            stack.extend(parents)
        }
        commits_in_head
    }
}
impl Extractor for ProjectCommitsExtractor {
    type Key = ProjectId;
    type Value = Vec<CommitId>;
}
impl DoubleExtractor for ProjectCommitsExtractor {
    type A = BTreeMap<ProjectId, Vec<(String, CommitId)>>;
    type B = BTreeMap<CommitId, Commit>;
    fn extract(heads: &Self::A, commits: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        heads.iter().map(|(project_id, heads)| {
            (project_id.clone(),
             heads.iter().flat_map(|(_, commit_id)| {
                 Self::commits_from_head(commits, commit_id)
             }).collect::<Vec<CommitId>>())
        }).collect()
    }
}

struct ProjectLifetimesExtractor {}
impl Extractor for ProjectLifetimesExtractor {
    type Key = ProjectId;
    type Value = u64;
}
impl TripleExtractor for ProjectLifetimesExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, i64>;
    type C = BTreeMap<CommitId, i64>;
    fn extract(project_commits: &Self::A, authored_timestamps: &Self::B, committed_timestamps: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
       project_commits.iter().flat_map(|(project_id, commit_ids)| {
           let min_max =
               commit_ids.iter()
                   .flat_map(|commit_id: &CommitId| {
                       let mut timestamps: Vec<i64> = Vec::new();
                       let author_timestamp = authored_timestamps.get(commit_id);
                       let committer_timestamp = committed_timestamps.get(commit_id);
                       if let Some(timestamp) = author_timestamp { timestamps.push(*timestamp) }
                       if let Some(timestamp) = committer_timestamp { timestamps.push(*timestamp) }
                       timestamps
                   })
                   .minmax();

           match min_max {
               MinMaxResult::NoElements => { None }
               MinMaxResult::OneElement(_) => { Some((project_id.clone(), 0)) }
               MinMaxResult::MinMax(min, max) => { Some((project_id.clone(), (max - min) as u64)) }
           }
       }).collect()
    }
}

struct UserExtractor {}
impl Extractor for UserExtractor {
    type Key = UserId;
    type Value = User;
}
impl SingleExtractor for UserExtractor {
    type A = DatastoreView;
    fn extract(store: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        store.users().map(|(id, email)| {
            (UserId::from(id), User::new(UserId::from(id), email))
        }).collect()
    }
}

struct UserAuthoredCommitsExtractor {}
impl Extractor for UserAuthoredCommitsExtractor {
    type Key = UserId;
    type Value = Vec<CommitId>;
}
impl SingleExtractor for UserAuthoredCommitsExtractor {
    type A = BTreeMap<CommitId, Commit>;
    fn extract(commits: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        commits.iter()
            .map(|(commit_id, commit)| {
                (commit.author_id().clone(), commit_id.clone(), )
            }).into_group_map()
            .into_iter()
            .collect()
    }
}

struct UserExperienceExtractor {}
impl Extractor for UserExperienceExtractor {
    type Key = UserId;
    type Value = u64;
}
impl DoubleExtractor for UserExperienceExtractor  {
    type A = BTreeMap<UserId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, i64>;
    fn extract(user_commits: &Self::A, timestamps: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        user_commits.iter()
        .flat_map(|(user_id, commit_ids)| {
            let min_max = commit_ids.iter()
                .flat_map(|commit_id| {
                    timestamps.get(commit_id).pirate()
                })
                .minmax();

            match min_max {
                MinMaxResult::NoElements => None,
                MinMaxResult::OneElement(_) => Some((user_id.clone(), 0)),
                MinMaxResult::MinMax(min, max) => Some((user_id.clone(), (max - min) as u64)),
            }
        }).collect()
    }
}

struct CombinedUserExperienceExtractor {}
impl Extractor for CombinedUserExperienceExtractor {
    type Key = UserId;
    type Value = u64;
}
impl TripleExtractor for CombinedUserExperienceExtractor  {
    type A = BTreeMap<UserId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, i64>;
    type C = BTreeMap<CommitId, i64>;
    fn extract(user_commits: &Self::A, authored_timestamps: &Self::B, committed_timestamps: &Self::C) -> BTreeMap<Self::Key, Self::Value> {
        user_commits.iter()
            .flat_map(|(user_id, commit_ids)| {
                let min_max = commit_ids.iter()
                    .flat_map(|commit_id| {
                        let mut timestamps: Vec<i64> = Vec::new();
                        let authored_timestamp = authored_timestamps.get(commit_id).pirate();
                        let committed_timestamp = committed_timestamps.get(commit_id).pirate();
                        if let Some(timestamp) = authored_timestamp { timestamps.push(timestamp) }
                        if let Some(timestamp) = committed_timestamp { timestamps.push(timestamp) }
                        timestamps
                    })
                    .minmax();

                match min_max {
                    MinMaxResult::NoElements => None,
                    MinMaxResult::OneElement(_) => Some((user_id.clone(), 0)),
                    MinMaxResult::MinMax(min, max) => Some((user_id.clone(), (max - min) as u64)),
                }
            }).collect()
    }
}

struct PathExtractor {}
impl Extractor for PathExtractor {
    type Key = PathId;
    type Value = Path;
}
impl SingleExtractor for PathExtractor {
    type A = DatastoreView;
    fn extract(store: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        store.paths().map(|(id, location)| {
            (PathId::from(id), Path::new(PathId::from(id), location))
        }).collect()
    }
}

struct SnapshotExtractor {}
impl Extractor for SnapshotExtractor {
    type Key = SnapshotId;
    type Value = Snapshot;
}
impl SingleExtractor for SnapshotExtractor {
    type A = DatastoreView;
    fn extract(store: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        store.contents().map(|(id, contents)| {
            (SnapshotId::from(id), Snapshot::new(SnapshotId::from(id), contents))
        }).collect()
    }
}

struct CommitExtractor {}
impl Extractor for CommitExtractor {
    type Key = CommitId;
    type Value = Commit;
}
impl SingleExtractor for CommitExtractor {
    type A = DatastoreView;
    fn extract(store: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        store.commits().map(|(id, commit)| {
            (CommitId::from(id), Commit::from((id, commit)))
        }).collect()
    }
}

struct CommitHashExtractor {}
impl Extractor for CommitHashExtractor {
    type Key = CommitId;
    type Value = String;
}
impl SingleExtractor for CommitHashExtractor {
    type A = DatastoreView;
    fn extract(store: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        store.commits().map(|(id, commit)| {
            (CommitId::from(id), commit.message)
        }).collect()
    }
}

struct CommitMessageExtractor {}
impl Extractor for CommitMessageExtractor {
    type Key = CommitId;
    type Value = String;
}
impl SingleExtractor for CommitMessageExtractor {
    type A = DatastoreView;
    fn extract(store: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        store.commits().map(|(id, commit)| {
            (CommitId::from(id), commit.message)
        }).collect() // TODO maybe return iter?
    }
}

struct CommitterTimestampExtractor {}
impl Extractor for CommitterTimestampExtractor {
    type Key = CommitId;
    type Value = i64;
}
impl SingleExtractor for CommitterTimestampExtractor {
    type A = DatastoreView;
    fn extract(store: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        store.commits().map(|(id, commit)| {
            (CommitId::from(id), commit.committer_time)
        }).collect() // TODO maybe return iter?
    }
}

struct CommitChangesExtractor {}
impl Extractor for CommitChangesExtractor {
    type Key = CommitId;
    type Value = Vec<(PathId, SnapshotId)>;
}
impl SingleExtractor for CommitChangesExtractor {
    type A = DatastoreView;
    fn extract(store: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        store.commits().map(|(id, commit)| {
            (CommitId::from(id), commit.changes.iter().map(|(path_id, snapshot_id)|
                (PathId::from(path_id), SnapshotId::from(snapshot_id))).collect())
        }).collect() // TODO maybe return iter?
    }
}

struct AuthorTimestampExtractor {}
impl Extractor for AuthorTimestampExtractor {
    type Key = CommitId;
    type Value = i64;
}
impl SingleExtractor for AuthorTimestampExtractor {
    type A = DatastoreView;
    fn extract(store: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        store.commits().map(|(id, commit)| {
            (CommitId::from(id), commit.author_time)
        }).collect() // TODO maybe return iter?
    }
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

pub struct Data {
    store:                   DatastoreView,

    project_metadata:            ProjectMetadataSource,
    project_heads:               PersistentSource<ProjectHeadsExtractor>,
    project_users:               PersistentSource<ProjectUsersExtractor>,
    project_authors:             PersistentSource<ProjectAuthorsExtractor>,
    project_committers:          PersistentSource<ProjectCommittersExtractor>,
    project_commits:             PersistentSource<ProjectCommitsExtractor>,
    project_lifetimes:           PersistentSource<ProjectLifetimesExtractor>,

    project_user_count:          PersistentSource<CountPerKeyExtractor<ProjectId, UserId>>,
    project_author_count:        PersistentSource<CountPerKeyExtractor<ProjectId, UserId>>,
    project_committer_count:     PersistentSource<CountPerKeyExtractor<ProjectId, UserId>>,
    project_commit_count:        PersistentSource<CountPerKeyExtractor<ProjectId, CommitId>>,

    users:                       PersistentSource<UserExtractor>,
    user_authored_commits:       PersistentSource<UserAuthoredCommitsExtractor>,
    user_committed_commits:      PersistentSource<UserAuthoredCommitsExtractor>,
    user_author_experience:      PersistentSource<UserExperienceExtractor>,
    user_committer_experience:   PersistentSource<UserExperienceExtractor>,
    user_experience:             PersistentSource<CombinedUserExperienceExtractor>,

    user_authored_commit_count:  PersistentSource<CountPerKeyExtractor<UserId, CommitId>>,
    user_committed_commit_count: PersistentSource<CountPerKeyExtractor<UserId, CommitId>>,

    paths:                       PersistentSource<PathExtractor>,
    snapshots:                   PersistentSource<SnapshotExtractor>,

    commits:                     PersistentSource<CommitExtractor>,
    commit_hashes:               PersistentSource<CommitHashExtractor>,
    commit_messages:             PersistentSource<CommitMessageExtractor>,
    commit_author_timestamps:    PersistentSource<AuthorTimestampExtractor>,
    commit_committer_timestamps: PersistentSource<CommitterTimestampExtractor>,
    commit_changes:              PersistentSource<CommitChangesExtractor>,

    commit_change_count:         PersistentSource<CountPerKeyExtractor<CommitId, (PathId, SnapshotId)>>,

    // TODO maybe some of these could be pre-cached all at once (eg all commit properties)
}

impl Data {
    pub fn from_store<S>(store: DatastoreView, cache_dir: S) -> Data where S: Into<String> {
        let dir = cache_dir.into();
        Data {
            store,
            project_metadata:            ProjectMetadataSource::new("project",               dir.clone()),
            project_heads:               PersistentSource::new("project_heads",              dir.clone()),
            project_users:               PersistentSource::new("project_users",              dir.clone()),
            project_user_count:          PersistentSource::new("project_user_count",         dir.clone()),
            project_authors:             PersistentSource::new("project_authors",            dir.clone(),),
            project_author_count:        PersistentSource::new("project_author_count",       dir.clone()),
            project_committers:          PersistentSource::new("project_committers",         dir.clone()),
            project_committer_count:     PersistentSource::new("project_committer_count",    dir.clone()),
            project_commits:             PersistentSource::new("project_commits",            dir.clone()),
            project_commit_count:        PersistentSource::new("project_commit_count",       dir.clone()),
            project_lifetimes:           PersistentSource::new("project_lifetimes",          dir.clone()),

            users:                       PersistentSource::new("users",                      dir.clone()),
            user_authored_commits:       PersistentSource::new("user_authored_commits",      dir.clone()),
            user_committed_commits:      PersistentSource::new("user_committed_commits",     dir.clone()),
            user_author_experience:      PersistentSource::new("user_author_experience",     dir.clone()),
            user_committer_experience:   PersistentSource::new("user_committer_experience",  dir.clone()),
            user_experience:             PersistentSource::new("user_experience",            dir.clone()),

            user_authored_commit_count:  PersistentSource::new("user_authored_commit_count", dir.clone()),
            user_committed_commit_count: PersistentSource::new("user_committed_commit_count",dir.clone()),

            paths:                       PersistentSource::new("paths",                      dir.clone()),
            snapshots:                   PersistentSource::new("snapshots",                  dir.clone()),

            commits:                     PersistentSource::new("commits",                    dir.clone()),
            commit_hashes:               PersistentSource::new("commit_hashes",              dir.clone()),
            commit_messages:             PersistentSource::new("commit_messages",            dir.clone()),
            commit_author_timestamps:    PersistentSource::new("commit_author_timestamps",   dir.clone()),
            commit_committer_timestamps: PersistentSource::new("commit_committer_timestamps",dir.clone()),
            commit_changes:              PersistentSource::new("commit_changes",             dir.clone()),
            commit_change_count:         PersistentSource::new("commit_change_count",        dir.clone()),
        }
    }
}

impl Data {
    // TODO streams

    pub fn project_issues(&mut self, _id: &ProjectId) -> Option<usize> { unimplemented!() }         // FIXME
    pub fn project_buggy_issues(&mut self, _id: &ProjectId) -> Option<usize> { unimplemented!() }

    pub fn project_is_fork(&mut self, id: &ProjectId) -> Option<bool> {
        self.project_metadata.fork(&self.store, id)
    }
    pub fn project_is_archived(&mut self, id: &ProjectId) -> Option<bool> {
        self.project_metadata.archived(&self.store, id)
    }
    pub fn project_is_disabled(&mut self, id: &ProjectId) -> Option<bool> {
        self.project_metadata.disabled(&self.store, id)
    }
    pub fn project_star_gazer_count(&mut self, id: &ProjectId) -> Option<usize> {
        self.project_metadata.star_gazers(&self.store, id)
    }
    pub fn project_watcher_count(&mut self, id: &ProjectId) -> Option<usize> {
        self.project_metadata.watchers(&self.store, id)
    }
    pub fn project_size(&mut self, id: &ProjectId) -> Option<usize> {
        self.project_metadata.size(&self.store, id)
    }
    pub fn project_open_issue_count(&mut self, id: &ProjectId) -> Option<usize> {
        self.project_metadata.open_issues(&self.store, id)
    }
    pub fn project_network_count(&mut self, id: &ProjectId) -> Option<usize> {
        self.project_metadata.network(&self.store, id)
    }
    pub fn project_subscriber_count(&mut self, id: &ProjectId) -> Option<usize> {
        self.project_metadata.subscribers(&self.store, id)
    }
    pub fn project_license(&mut self, id: &ProjectId) -> Option<&String> {
        self.project_metadata.license(&self.store, id)
    }
    pub fn project_language(&mut self, id: &ProjectId) -> Option<Language> {
        self.project_metadata.language(&self.store, id)
    }
    pub fn project_description(&mut self, id: &ProjectId) -> Option<&String> {
        self.project_metadata.description(&self.store, id)
    }
    pub fn project_homepages(&mut self, id: &ProjectId) -> Option<&String> {
        self.project_metadata.homepage(&self.store, id)
    }

    pub fn project_head_ids(&mut self, id: &ProjectId) -> Option<Vec<(String, CommitId)>> {
        self.smart_load_project_heads().get(id).pirate()
    }

    pub fn project_heads(&mut self, id: &ProjectId) -> Option<Vec<(String, Commit)>> {
        self.smart_load_project_heads().get(id).pirate().map(|v| {
            v.into_iter().map(|(name, commit_id)| {
                (name, commit_id.reify(self))
            }).collect()
        })
    }

    pub fn project_commit_ids(&mut self, id: &ProjectId) -> Option<&Vec<CommitId>> {
        self.smart_load_project_commits().get(id)
    }

    pub fn project_commits(&mut self, id: &ProjectId) -> Option<Vec<Commit>> {
        self.smart_load_project_commits().get(id).pirate().reify(self)
    }

    pub fn project_commit_count(&mut self, id: &ProjectId) -> Option<usize> {
        self.smart_load_project_commit_count().get(id).pirate()
    }

    pub fn project_author_ids(&mut self, id: &ProjectId) -> Option<&Vec<UserId>> {
        self.smart_load_project_authors().get(id)
    }

    pub fn project_authors(&mut self, id: &ProjectId) -> Option<Vec<User>> {
        self.smart_load_project_authors().get(id).pirate().reify(self)
    }

    pub fn project_author_count(&mut self, id: &ProjectId) -> Option<usize> {
        self.smart_load_project_author_count().get(id).pirate()
    }

    pub fn project_committer_ids(&mut self, id: &ProjectId) -> Option<&Vec<UserId>> {
        self.smart_load_project_committers().get(id)
    }

    pub fn project_committers(&mut self, id: &ProjectId) -> Option<Vec<User>> {
        self.smart_load_project_committers().get(id).pirate().reify(self)
    }

    pub fn project_committer_count(&mut self, id: &ProjectId) -> Option<usize> {
        self.smart_load_project_committer_count().get(id).pirate()
    }

    pub fn project_user_ids(&mut self, id: &ProjectId) -> Option<&Vec<UserId>> {
        self.smart_load_project_users().get(id)
    }

    pub fn project_users(&mut self, id: &ProjectId) -> Option<Vec<User>> {
        self.smart_load_project_users().get(id).pirate().reify(self)
    }

    pub fn project_user_count(&mut self, id: &ProjectId) -> Option<usize> {
        self.smart_load_project_user_count().get(id).pirate()
    }

    pub fn project_lifetime(&mut self, id: &ProjectId) -> Option<Duration> {
        self.smart_load_project_lifetimes().get(id)
            .pirate()
            .map(|seconds| Duration::from_secs(seconds))
    }

    pub fn user(&mut self, id: &UserId) -> Option<&User> {
        self.smart_load_users().get(id)
    }

    pub fn path(&mut self, id: &PathId) -> Option<&Path> {
        self.smart_load_paths().get(id)
    }
    pub fn snapshot(&mut self, id: &SnapshotId) -> Option<&Snapshot> {
        self.smart_load_snapshots().get(id)
    }

    pub fn commit(&mut self, id: &CommitId) -> Option<&Commit> {
        self.smart_load_commits().get(id)
    }
    pub fn commit_hash(&mut self, id: &CommitId) -> Option<&String> {
        self.smart_load_commit_hashes().get(id)
    }
    pub fn commit_message(&mut self, id: &CommitId) -> Option<&String> {
        self.smart_load_commit_messages().get(id)
    }
    pub fn commit_author_timestamp(&mut self, id: &CommitId) -> Option<i64> {
        self.smart_load_commit_author_timestamps().get(id).pirate()
    }
    pub fn commit_committer_timestamp(&mut self, id: &CommitId) -> Option<i64> {
        self.smart_load_commit_committer_timestamps().get(id).pirate()
    }
    pub fn commit_change_ids(&mut self, id: &CommitId) -> Option<&Vec<(PathId, SnapshotId)>> {
        self.smart_load_commit_changes().get(id)
    }
    pub fn commit_changes(&mut self, id: &CommitId) -> Option<Vec<(Path, Snapshot)>> {
        self.smart_load_commit_changes().get(id).pirate().reify(self)
    }

    pub fn commit_change_count(&mut self, id: &CommitId) -> Option<usize> {
        self.smart_load_commit_change_count().get(id).pirate()
    }

    pub fn user_committed_commit_ids(&mut self, id: &UserId) -> Option<&Vec<CommitId>> {
        self.smart_load_user_committed_commits().get(id)
    }

    pub fn user_authored_commits(&mut self, id: &UserId) -> Option<Vec<Commit>> {
        self.smart_load_user_authored_commits().get(id).pirate().reify(self)
    }

    pub fn user_authored_commit_ids(&mut self, id: &UserId) -> Option<&Vec<CommitId>> {
        self.smart_load_user_authored_commits().get(id)
    }

    pub fn user_committed_experience(&mut self, id: &UserId) -> Option<Duration> {
        self.smart_load_user_committer_experience()
            .get(id)
            .map(|seconds| Duration::from_secs(*seconds))
    }

    pub fn user_author_experience(&mut self, id: &UserId) -> Option<Duration> {
        self.smart_load_user_author_experience()
            .get(id)
            .map(|seconds| Duration::from_secs(*seconds))
    }

    pub fn user_experience(&mut self, id: &UserId) -> Option<Duration> {
        self.smart_load_user_experience()
            .get(id)
            .map(|seconds| Duration::from_secs(*seconds))
    }

    pub fn user_committed_commit_count(&mut self, id: &UserId) -> Option<usize> {
        self.smart_load_user_committed_commit_count().get(id).pirate()
    }

    pub fn user_authored_commit_count(&mut self, id: &UserId) -> Option<usize> {
        self.smart_load_user_authored_commit_count().get(id).pirate()
    }

    pub fn user_committed_commits(&mut self, id: &UserId) -> Option<Vec<Commit>> {
        self.smart_load_user_committed_commits().get(id).pirate().reify(self)
    }
}

macro_rules! load_from_store {
    ($self:ident, $vector:ident)  => {{
        if !$self.$vector.is_loaded() {
            $self.$vector.load_from_one(&$self.store);
        }
        $self.$vector.grab()
    }}
}

macro_rules! load_with_prerequisites {
    ($self:ident, $vector:ident, $n:ident, $($prereq:ident),*)  => {{
        mashup! {
            $( m["smart_load" $prereq] = smart_load_$prereq; )*
               m["load"] = load_from_$n;
        }
        if !$self.$vector.is_loaded() {
            m! { $(  $self."smart_load" $prereq(); )*              }
            m! { $self.$vector."load"($($self.$prereq.grab()), *); }
        }
        $self.$vector.grab()
    }}
}

impl Data {
    fn smart_load_project_heads(&mut self) -> &BTreeMap<ProjectId, Vec<(String, CommitId)>> {
        load_from_store!(self, project_heads)
    }

    fn smart_load_project_users(&mut self) -> &BTreeMap<ProjectId, Vec<UserId>> {
        load_with_prerequisites!(self, project_users, two, project_authors, project_committers)
    }

    fn smart_load_project_authors(&mut self) -> &BTreeMap<ProjectId, Vec<UserId>> {
        load_with_prerequisites!(self, project_authors, two, project_commits, commits)
    }

    fn smart_load_project_committers(&mut self) -> &BTreeMap<ProjectId, Vec<UserId>> {
        load_with_prerequisites!(self, project_committers, two, project_commits, commits)
    }

    fn smart_load_project_commits(&mut self) -> &BTreeMap<ProjectId, Vec<CommitId>> {
        load_with_prerequisites!(self, project_commits, two, project_heads, commits)
    }

    fn smart_load_project_user_count(&mut self) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_user_count, one, project_users)
    }

    fn smart_load_project_author_count(&mut self) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_author_count, one, project_authors)
    }

    fn smart_load_project_committer_count(&mut self) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_committer_count, one, project_committers)
    }

    fn smart_load_project_commit_count(&mut self) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_commit_count, one, project_commits)
    }

    fn smart_load_project_lifetimes(&mut self) -> &BTreeMap<ProjectId, u64> {
        load_with_prerequisites!(self, project_lifetimes, three, project_commits,
                                                                 commit_author_timestamps,
                                                                 commit_committer_timestamps)
    }

    fn smart_load_users(&mut self) -> &BTreeMap<UserId, User> {
        load_from_store!(self, users)
    }

    fn smart_load_user_authored_commits(&mut self) -> &BTreeMap<UserId, Vec<CommitId>> {
        load_with_prerequisites!(self, user_authored_commits, one, commits)
    }

    fn smart_load_user_committed_commits(&mut self) -> &BTreeMap<UserId, Vec<CommitId>> {
        load_with_prerequisites!(self, user_committed_commits, one, commits)
    }

    fn smart_load_user_author_experience(&mut self) -> &BTreeMap<UserId, u64> {
        load_with_prerequisites!(self, user_author_experience, two, user_authored_commits,
                                                                    commit_author_timestamps)
    }

    fn smart_load_user_committer_experience(&mut self) -> &BTreeMap<UserId, u64> {
        load_with_prerequisites!(self, user_committer_experience, two, user_committed_commits,
                                                                       commit_committer_timestamps)
    }

    fn smart_load_user_experience(&mut self) -> &BTreeMap<UserId, u64> {
        load_with_prerequisites!(self, user_experience, three, user_committed_commits,
                                                               commit_author_timestamps,
                                                               commit_committer_timestamps)
    }

    fn smart_load_user_committed_commit_count(&mut self) -> &BTreeMap<UserId, usize> {
        load_with_prerequisites!(self, user_committed_commit_count, one, user_committed_commits)
    }

    fn smart_load_user_authored_commit_count(&mut self) -> &BTreeMap<UserId, usize> {
        load_with_prerequisites!(self, user_authored_commit_count, one, user_authored_commits)
    }

    fn smart_load_paths(&mut self) -> &BTreeMap<PathId, Path> {
        load_from_store!(self, paths)
    }

    fn smart_load_snapshots(&mut self) -> &BTreeMap<SnapshotId, Snapshot> {
        load_from_store!(self, snapshots)
    }

    fn smart_load_commits(&mut self) -> &BTreeMap<CommitId, Commit> {
        load_from_store!(self, commits)
    }

    fn smart_load_commit_hashes(&mut self) -> &BTreeMap<CommitId, String> {
        load_from_store!(self, commit_hashes)
    }

    fn smart_load_commit_messages(&mut self) -> &BTreeMap<CommitId, String> {
        load_from_store!(self, commit_messages)
    }

    fn smart_load_commit_committer_timestamps(&mut self) -> &BTreeMap<CommitId, i64> {
        load_from_store!(self, commit_committer_timestamps)
    }

    fn smart_load_commit_author_timestamps(&mut self) -> &BTreeMap<CommitId, i64> {
        load_from_store!(self, commit_author_timestamps)
    }

    fn smart_load_commit_changes(&mut self) -> &BTreeMap<CommitId, Vec<(PathId, SnapshotId)>> {
        load_from_store!(self, commit_changes)
    }

    fn smart_load_commit_change_count(&mut self) -> &BTreeMap<CommitId, usize> {
        load_with_prerequisites!(self, commit_change_count, one, commit_changes)
    }
}