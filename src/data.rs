use std::rc::Rc;
use std::cell::RefCell;
use std::path::PathBuf;
use std::collections::BTreeMap;
use std::error::Error;
use std::fs::File;

use serde::Serialize;
use serde::de::DeserializeOwned;

use dcd::DatastoreView;

use crate::objects::*;

pub type DataPtr = Rc<RefCell<Data>>;

trait Persistent: Serialize + DeserializeOwned {}
impl<T> Persistent for T where T: Serialize + DeserializeOwned {}

struct PersistentSource<K: Ord + Persistent, V: Clone + Persistent> {
    name: String,
    cache_path: PathBuf,
    map: Option<BTreeMap<K, V>>,
    extractor: Box<dyn Fn(&DatastoreView) -> BTreeMap<K, V>>
}

impl<K,V> PersistentSource<K, V>
    where K: Ord + Persistent, V: Clone + Persistent {
    pub fn new<Sa,Sb,F>(name: Sa, cache_dir: Sb, extractor: F) -> PersistentSource<K, V>
        where Sa: Into<String>, Sb: Into<String>,
              F: Fn(&DatastoreView) -> BTreeMap<K, V> + 'static {

        let name: String = name.into();

        let mut cache_path = PathBuf::new();
        cache_path.push(std::path::Path::new(cache_dir.into().as_str()));
        cache_path.push(std::path::Path::new(name.as_str()));
        cache_path.set_extension(".cbor");

        PersistentSource { name, cache_path, map: None, extractor: Box::new(extractor) }
    }
}

impl<K,V> PersistentSource<K, V>
    where K: Ord + Persistent, V: Clone + Persistent {
    fn already_cached(&self) -> bool {
        unimplemented!()
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

pub struct Data {
    data_store:              DatastoreView,

    project_timestamps:      PersistentSource<ProjectId, i64>,
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
    commit_messages:         PersistentSource<CommitId, Message>,
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
            project_timestamps: PersistentSource::new("project_timestamps", dir.clone(), |ds| {
                unimplemented!()
            }),
            project_languages: PersistentSource::new("project_languages", dir.clone(), |ds| {
                unimplemented!()
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
                ds.users().map(|(id, s)| { unimplemented!() }).collect()
            }),
            paths: PersistentSource::new("paths", dir.clone(), |ds| {
                unimplemented!()
            }),
            commits: PersistentSource::new("commits", dir.clone(), |ds| {
                ds.commits().map(|pair| (CommitId::from(&pair.0), Commit::from(pair))).collect()
            }),
            commit_messages: PersistentSource::new("commit_messages", dir.clone(), |ds| {
                unimplemented!()
            }),
        }
    }
}

impl Data {
    pub fn project_timestamp      (&mut self, id: &ProjectId) -> i64                     { *self.project_timestamps.get(&self.data_store, id).unwrap()   } // Last update timestamps are obligatory
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

            pub fn project_commits(&mut self, id: &ProjectId) -> usize                   { *self.project_committer_count.get(&self.data_store,id).unwrap() } // Obligatory
    pub fn project_commit_count(&mut self, id: &ProjectId) -> usize                      { *self.project_committer_count.get(&self.data_store,id).unwrap() } // Obligatory

    pub fn user                   (&mut self, id: &UserId) -> Option<User>               { self.users.pirate(&self.data_store,id)                          }
    pub fn path                   (&mut self, id: &PathId) -> Option<Path>               { self.paths.pirate(&self.data_store,id)                          }

    pub fn commit                 (&mut self, id: &CommitId) -> Option<Commit>           { self.commits.pirate(&self.data_store,id)                        }
    pub fn commit_message         (&mut self, id: &CommitId) -> Option<Message>          { self.commit_messages.pirate(&self.data_store,id)                }
}