use std::rc::Rc;
use std::cell::RefCell;
use crate::objects::{CommitId, ProjectId, Commit, User, UserId, Reifiable, PathId, Message, Path};
use std::path::PathBuf;
use std::hash::Hash;
use std::collections::BTreeMap;

pub type DataPtr = Rc<RefCell<Data>>;

struct PersistentSource<K: Ord, V: Clone> { name: String, cache_path: PathBuf, map: Option<BTreeMap<K, V>> }

impl<K,V> PersistentSource<K,V> where K: Ord, V: Clone {
    pub fn get(&mut self, key: &K) -> Option<&V> {
        match &self.map {
            Some(map) => map.get(key),
            None => unimplemented!(),
        }
    }
    pub fn pirate(&mut self, key: &K) -> Option<V> { // get owned
        self.get(key).map(|v| v.clone())
    }
}

pub struct Data {
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

    users:                   PersistentSource<UserId, User>,
    paths:                   PersistentSource<PathId, Path>,

    commits:                 PersistentSource<CommitId, Commit>,
    commit_messages:         PersistentSource<CommitId, Message>,
}

impl Data {
    pub fn project_timestamp      (&mut self, id: &ProjectId) -> i64                     { *self.project_timestamps.get(id).unwrap()   } // Last update timestamps are obligatory
    pub fn project_language       (&mut self, id: &ProjectId) -> Option<String>          { self.project_languages.pirate(id)           }
    pub fn project_stars          (&mut self, id: &ProjectId) -> Option<usize>           { self.project_stars.pirate(id)               }
    pub fn project_issues         (&mut self, id: &ProjectId) -> Option<usize>           { self.project_issues.pirate(id)              }
    pub fn project_buggy_issues   (&mut self, id: &ProjectId) -> Option<usize>           { self.project_buggy_issues.pirate(id)        }
    pub fn project_heads          (&mut self, id: &ProjectId) -> Vec<(String, CommitId)> { self.project_heads.pirate(id).unwrap()      } // Heads are obligatory

    pub fn project_users          (&mut self, id: &ProjectId) -> Vec<User>               { self.project_users.pirate(id).unwrap().reify(self)      } // Obligatory, but can be 0 length
    pub fn project_authors        (&mut self, id: &ProjectId) -> Vec<User>               { self.project_authors.pirate(id).unwrap().reify(self)    } // Obligatory, but can be 0 length
    pub fn project_committers     (&mut self, id: &ProjectId) -> Vec<User>               { self.project_committers.pirate(id).unwrap().reify(self) } // Obligatory, but can be 0 length

    pub fn project_user_count     (&mut self, id: &ProjectId) -> usize                   { *self.project_users_count.get(id).unwrap()     } // Obligatory
    pub fn project_author_count   (&mut self, id: &ProjectId) -> usize                   { *self.project_author_count.get(id).unwrap()    } // Obligatory
    pub fn project_committer_count(&mut self, id: &ProjectId) -> usize                   { *self.project_committer_count.get(id).unwrap() } // Obligatory

    pub fn user(&mut self, id: &UserId) -> Option<User>                                  { self.users.pirate(id) }
    pub fn path(&mut self, id: &PathId) -> Option<Path>                                  { self.paths.pirate(id) }

    pub fn commit(&mut self, id: &CommitId) -> Option<Commit>                            { self.commits.pirate(id) }
    pub fn commit_message(&mut self, id: &CommitId) -> Option<Message>                   { self.commit_messages.pirate(id) }
}