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

pub type DataPtr = Rc<RefCell<Data>>;

static CACHE_EXTENSION: &str = ".cbor";
trait Persistent: Serialize + DeserializeOwned {}
impl<T> Persistent for T where T: Serialize + DeserializeOwned {}

trait StoreExtractor {
    type Key: Ord + Persistent;
    type Value: Clone + Persistent;
    fn extract(store: &DatastoreView) -> BTreeMap<Self::Key, Self::Value>;
}

struct PersistentSource<E: StoreExtractor> {
    //name: String,
    cache_path: PathBuf,
    cache_dir: PathBuf,
    map: Option<BTreeMap<E::Key, E::Value>>,
    extractor: PhantomData<E>,
}

impl<E> PersistentSource<E> where E: StoreExtractor {
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
}

impl<E> PersistentSource<E> where E: StoreExtractor{
    fn already_cached(&self) -> bool {
        self.cache_path.is_file()
    }
    fn load_from_store(&mut self, store: &DatastoreView) {
        self.map = Some(E::extract(store));
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

impl<E> PersistentSource<E> where E: StoreExtractor {
    pub fn data(&mut self, store: &DatastoreView) -> &BTreeMap<E::Key, E::Value> {
        if self.map.is_none() {
            if self.already_cached() {
                self.load_from_cache().unwrap()
            } else {
                self.load_from_store(store);
                self.store_to_cache().unwrap()
            }
        }
        self.map.as_ref().unwrap()
    }
    pub fn get(&mut self, store: &DatastoreView, key: &E::Key) -> Option<&E::Value> {
        self.data(store).get(key)
    }
    pub fn pirate(&mut self, store: &DatastoreView, key: &E::Key) -> Option<E::Value> { // get owned
        self.get(store, key).map(|v| v.clone())
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
            JSON::Number(n) => panic!("Expected Number >= 0, found {:?}", value),
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

pub struct ProjectMetadataSource {
    //store:   &'a DatastoreView,
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
    languages:        MetadataVec<StringExtractor>,
    descriptions:     MetadataVec<StringExtractor>,
    homepages:        MetadataVec<StringExtractor>,
}

impl ProjectMetadataSource {
    pub fn new<Sa, Sb>(name: Sa, dir: Sb) -> Self where Sa: Into<String>, Sb: Into<String> {
        let name = name.into();
        let dir = {
            let mut cache_subdir = PathBuf::new();
            cache_subdir.push(std::path::Path::new(dir.into().as_str()));
            cache_subdir.push(std::path::Path::new(name.as_str()));
            cache_subdir.set_extension(CACHE_EXTENSION);
            cache_subdir.to_str().unwrap().to_owned()
        };
        ProjectMetadataSource {
            forks: MetadataVec::new("fork", dir.as_str(), BoolExtractor),
            archived: MetadataVec::new("archived", dir.as_str(), BoolExtractor),
            disabled: MetadataVec::new("disabled", dir.as_str(), BoolExtractor),
            star_gazers: MetadataVec::new("star_gazers_count", dir.as_str(), CountExtractor),
            watchers: MetadataVec::new("watchers_count", dir.as_str(), CountExtractor),
            size: MetadataVec::new("size", dir.as_str(), CountExtractor),
            open_issues: MetadataVec::new("open_issues_count", dir.as_str(), CountExtractor),
            network: MetadataVec::new("network_count", dir.as_str(), CountExtractor),
            subscribers: MetadataVec::new("subscribers_count", dir.as_str(), CountExtractor),
            languages: MetadataVec::new("language", dir.as_str(), StringExtractor),
            descriptions: MetadataVec::new("description", dir.as_str(), StringExtractor),
            homepages: MetadataVec::new("homepage", dir.as_str(), StringExtractor),
            licenses: MetadataVec::new("license", dir.as_str(), FieldExtractor("name", StringExtractor)),
            loaded: false,
        }
    }

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
        let mut outcomes = vec![];
        outcomes.push(self.forks.store_to_cache());
        outcomes.push(self.archived.store_to_cache());
        outcomes.push(self.disabled.store_to_cache());
        outcomes.push(self.star_gazers.store_to_cache());
        outcomes.push(self.watchers.store_to_cache());
        outcomes.push(self.size.store_to_cache());
        outcomes.push(self.open_issues.store_to_cache());
        outcomes.push(self.network.store_to_cache());
        outcomes.push(self.subscribers.store_to_cache());
        outcomes.push(self.licenses.store_to_cache());
        outcomes.push(self.languages.store_to_cache());
        outcomes.push(self.descriptions.store_to_cache());
        outcomes.push(self.homepages.store_to_cache());

        let errors: Vec<Box<dyn Error>> =
            outcomes.into_iter()
                .filter(|r| r.is_err())
                .map(|r| r.err().unwrap())
                .collect();
        if errors.is_empty() { Ok(()) } else { Err(errors) }
    }
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

impl ProjectMetadataSource {
    pub fn fork             (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<bool>    { gimme!(self, forks,        store, pirate, key) }
    pub fn archived         (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<bool>    { gimme!(self, archived,     store, pirate, key) }
    pub fn disabled         (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<bool>    { gimme!(self, disabled,     store, pirate, key) }

    pub fn star_gazer       (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<usize>   { gimme!(self, star_gazers,  store, pirate, key) }
    pub fn watcher          (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<usize>   { gimme!(self, watchers,     store, pirate, key) }
    pub fn size             (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<usize>   { gimme!(self, size,         store, pirate, key) }
    pub fn open_issue       (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<usize>   { gimme!(self, open_issues,  store, pirate, key) }
    pub fn network          (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<usize>   { gimme!(self, network,      store, pirate, key) }
    pub fn subscriber       (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<usize>   { gimme!(self, subscribers,  store, pirate, key) }

    pub fn license_owned    (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<String>  { gimme!(self, licenses,     store, pirate, key) }
    pub fn language_owned   (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<String>  { gimme!(self, languages,    store, pirate, key) }
    pub fn description_owned(&mut self, store: &DatastoreView, key: &ProjectId) -> Option<String>  { gimme!(self, descriptions, store, pirate, key) }
    pub fn homepage_owned   (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<String>  { gimme!(self, homepages,    store, pirate, key) }

    pub fn license          (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<&String> { gimme!(self, licenses,     store, get,    key) }
    pub fn language         (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<&String> { gimme!(self, languages,    store, get,    key) }
    pub fn description      (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<&String> { gimme!(self, descriptions, store, get,    key) }
    pub fn homepage         (&mut self, store: &DatastoreView, key: &ProjectId) -> Option<&String> { gimme!(self, homepages,    store, get,    key) }
}

impl ProjectMetadataSource {
    fn load_metadata(&mut self, store: &DatastoreView) -> HashMap<ProjectId, serde_json::Map<String, JSON>> {
        let content_project_ids: HashMap<u64, u64> =
            store.projects_metadata()
                .filter(|(id, meta)| meta.key == "github_metadata")
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

    pub fn load_all_from_store(&mut self, store: &DatastoreView) {
        let metadata = self.load_metadata(store);
        self.load_all_from(&metadata)
    }
}

pub struct Data {
    store:              DatastoreView,
    project_metadata:        ProjectMetadataSource,
    project_heads:           PersistentSource<ProjectHeadsExtractor>,
    project_users:           PersistentSource<ProjectUsersExtractor>,
    project_authors:         PersistentSource<ProjectAuthorsExtractor>,
    project_committers:      PersistentSource<ProjectCommittersExtractor>,
    project_commits:         PersistentSource<ProjectCommitsExtractor>,

    //project_users_count:     PersistentSource<ProjectId, usize>,
    //project_author_count:    PersistentSource<ProjectId, usize>,
    //project_committer_count: PersistentSource<ProjectId, usize>,
    //project_commit_count:    PersistentSource<ProjectId, usize>,

    users:                   PersistentSource<UserExtractor>,
    paths:                   PersistentSource<PathExtractor>,

    commits:                 PersistentSource<CommitExtractor>,
    commit_messages:         PersistentSource<CommitMessageExtractor>,
}

struct ProjectHeadsExtractor {}
impl StoreExtractor for ProjectHeadsExtractor {
    type Key = ProjectId;
    type Value = Vec<(String, CommitId)>;
    fn extract(store: &DatastoreView) -> BTreeMap<Self::Key, Self::Value> {
        unimplemented!()
    }
}
struct ProjectUsersExtractor {}
impl StoreExtractor for ProjectUsersExtractor {
    type Key = ProjectId;
    type Value = Vec<UserId>;
    fn extract(store: &DatastoreView) -> BTreeMap<Self::Key, Self::Value> {
        unimplemented!()
    }
}

struct ProjectAuthorsExtractor {}
impl StoreExtractor for ProjectAuthorsExtractor {
    type Key = ProjectId;
    type Value = Vec<UserId>;
    fn extract(store: &DatastoreView) -> BTreeMap<Self::Key, Self::Value> {
        unimplemented!()
    }
}

struct ProjectCommittersExtractor {}
impl StoreExtractor for ProjectCommittersExtractor {
    type Key = ProjectId;
    type Value = Vec<UserId>;
    fn extract(store: &DatastoreView) -> BTreeMap<Self::Key, Self::Value> {
        unimplemented!()
    }
}

struct ProjectCommitsExtractor {}
impl StoreExtractor for ProjectCommitsExtractor {
    type Key = ProjectId;
    type Value = Vec<CommitId>;
    fn extract(store: &DatastoreView) -> BTreeMap<Self::Key, Self::Value> {
        unimplemented!()
    }
}

struct UserExtractor {}
impl StoreExtractor for UserExtractor {
    type Key = UserId;
    type Value = User;
    fn extract(store: &DatastoreView) -> BTreeMap<Self::Key, Self::Value> {
        store.users().map(|(id, email)| (UserId::from(id), User::new(UserId::from(id), email))).collect()
    }
}

struct PathExtractor {}
impl StoreExtractor for PathExtractor {
    type Key = PathId;
    type Value = Path;
    fn extract(store: &DatastoreView) -> BTreeMap<Self::Key, Self::Value> {
        store.paths().map(|(id, location)| (PathId::from(id), Path::new(PathId::from(id), location))).collect()
    }
}

struct CommitExtractor {}
impl StoreExtractor for CommitExtractor {
    type Key = CommitId;
    type Value = Commit;
    fn extract(store: &DatastoreView) -> BTreeMap<Self::Key, Self::Value> {
        store.commits().map(|(id, commit)| { (CommitId::from(id), Commit::from((id, commit))) }).collect()
    }
}

struct CommitMessageExtractor {}
impl StoreExtractor for CommitMessageExtractor {
    type Key = CommitId;
    type Value = String;
    fn extract(store: &DatastoreView) -> BTreeMap<Self::Key, Self::Value> {
        store.commits().map(|(id, commit)| (CommitId::from(id), commit.message)).collect() // TODO maybe return iter?
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

impl Data {
    pub fn from_store<S>(store: DatastoreView, cache_dir: S) -> Data where S: Into<String> {
        let dir = cache_dir.into();
        Data {
            store,
            project_metadata: ProjectMetadataSource::new("project", dir.clone()),
            project_heads: PersistentSource::new("project_heads", dir.clone()),
            project_users: PersistentSource::new("project_users", dir.clone()),
            project_authors: PersistentSource::new("project_authors", dir.clone(),),
            project_committers: PersistentSource::new("project_committers", dir.clone()),
            project_commits: PersistentSource::new("project_commits", dir.clone()),
            users: PersistentSource::new("users", dir.clone()),
            paths: PersistentSource::new("paths", dir.clone()),
            commits: PersistentSource::new("commits", dir.clone()),
            commit_messages: PersistentSource::new("commit_messages", dir.clone()),
        }
    }
}

impl Data {
    //pub fn project_timestamp      (&mut self, id: &ProjectId) -> i64                     { *self.project_timestamps.get(&self.store, id).unwrap()   } // Last update timestamps are obligatory
    pub fn project_language       (&mut self, id: &ProjectId) -> Option<String>          { self.project_metadata.language_owned(&self.store,id) }
    pub fn project_stars          (&mut self, id: &ProjectId) -> Option<usize>           { self.project_metadata.star_gazer(&self.store,id)     }
    pub fn project_issues         (&mut self, id: &ProjectId) -> Option<usize>           { unimplemented!() }
    pub fn project_buggy_issues   (&mut self, id: &ProjectId) -> Option<usize>           { unimplemented!() }
    pub fn project_heads          (&mut self, id: &ProjectId) -> Vec<(String, CommitId)> { self.project_heads.pirate(&self.store,id).unwrap()      } // Heads are obligatory

    pub fn project_users          (&mut self, id: &ProjectId) -> Vec<User>               { self.project_users.pirate(&self.store,id).unwrap().reify(self)      } // Obligatory, but can be 0 length
    pub fn project_authors        (&mut self, id: &ProjectId) -> Vec<User>               { self.project_authors.pirate(&self.store,id).unwrap().reify(self)    } // Obligatory, but can be 0 length
    pub fn project_committers     (&mut self, id: &ProjectId) -> Vec<User>               { self.project_committers.pirate(&self.store,id).unwrap().reify(self) } // Obligatory, but can be 0 length

    pub fn project_user_count     (&mut self, id: &ProjectId) -> usize                   { unimplemented!() } // Obligatory
    pub fn project_author_count   (&mut self, id: &ProjectId) -> usize                   { unimplemented!() } // Obligatory
    pub fn project_committer_count(&mut self, id: &ProjectId) -> usize                   { unimplemented!() } // Obligatory

    pub fn project_commits        (&mut self, id: &ProjectId) -> Vec<Commit>             { self.project_commits.pirate(&self.store, id).unwrap().reify(self) } // Obligatory
    pub fn project_commit_count   (&mut self, id: &ProjectId) -> usize                   { unimplemented!() } // Obligatory

    pub fn user                   (&mut self, id: &UserId) -> Option<User>               { self.users.pirate(&self.store,id)                          }
    pub fn path                   (&mut self, id: &PathId) -> Option<Path>               { self.paths.pirate(&self.store,id)                          }

    pub fn commit                 (&mut self, id: &CommitId) -> Option<Commit>           { self.commits.pirate(&self.store,id)                        }
    pub fn commit_message         (&mut self, id: &CommitId) -> Option<String>           { self.commit_messages.pirate(&self.store,id)                }
}