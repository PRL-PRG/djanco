use parasite;
use crate::{Store, objects};

use anyhow::*;
use crate::objects::SnapshotId;
use std::collections::HashMap;
use parasite::{Metadata, HashId};
use parasite::Table;
use serde_json::Value as JSON;
use std::str::FromStr;

macro_rules! convert {
    ($type:ident from $id:expr) => {
        objects::$type::from(Into::<u64>::into($id))
    };
    ($type:ident from $id:expr, except $exception:expr) => {
        if $id == $exception {
            None
        } else {
            Some(objects::$type::from(Into::<u64>::into($id)))
        }
    };
    (($type0:ident, $type1:ident) from $pair:expr) => {
        (convert!($type0 from $pair.0), convert!($type1 from $pair.1))
    };
    (($type0:ident, $type1:ident) from $pair:expr, except (_, $exception1:expr)) => {
        (convert!($type0 from $pair.0), convert!($type1 from $pair.1, except $exception1))
    };
    (($type0:ident, $type1:ident) from $pair:expr, except ($exception0:expr, _)) => {
        (convert!($type0 from $pair.0, except $exception0), convert!($type1 from $pair.1))
    };
    (Vec<$type:ident> from $collection:expr) => {
        $collection.into_iter().map(|id| convert!($type from id)).collect()
    };
    (HashMap<$key_type:ident, $value_type:ident> from $collection:expr) => {
        $collection.into_iter().map(|entry| convert!(($key_type, $value_type) from entry)).collect()
    };
    (HashMap<$key_type:ident, $value_type:ident> from $collection:expr, except (_, $value_exception:expr)) => {
        $collection.into_iter()
            .map(|entry| {
                convert!(($key_type, $value_type) from entry, except (_, $value_exception))
            }).collect()
    };
}

macro_rules! parse_github_json {
    ($entity_name:expr, $id:expr, $string:expr) => {
        JSON::from_str($string.as_str())
           .with_context(|| format!("Cannot read GitHub metadata for {} {}", $entity_name, $id));
    }
}

type URL = String;
type SHA = String;
type Path = String;
type Timestamp = i64;
type Bytes = Vec<u8>;

#[allow(dead_code)]
pub struct Source {
    store: parasite::DatastoreView,
    substore: parasite::StoreKind,
    savepoint: Timestamp,
}

impl Source {
    pub fn new<S>(dataset_path: S, savepoint: i64, substores: Vec<Store>) -> Result<Self> where S: Into<String> {
        if substores.len() != 1 {
            bail!("Currently only supporting queries on a single substore");
        }
        let substore = substores.into_iter().last().unwrap().kind();
        let store = parasite::DatastoreView::from(dataset_path.into().as_str());
        Ok(Source { store, savepoint, substore })
    }

    pub fn project_urls(&self) -> impl Iterator<Item=(objects::ProjectId, URL)> {
        self.store.project_urls()
            .into_iter()
            .map(|(id, url)| {
                (convert!(ProjectId from id), url.clone_url())
            })
    }

    pub fn project_credentials(&self) -> impl Iterator<Item=(objects::ProjectId, String)> {
        self.store.project_urls()
            .into_iter()
            .map(|(id, url)| {
                (convert!(ProjectId from id), url.name())
            })
    }

    pub fn project_heads(&self) -> impl Iterator<Item=(objects::ProjectId, HashMap<String, (objects::CommitId, SHA)>)> {
        self.store.project_heads()
            .into_iter()
            .map(|(project_id, heads)| {
                let heads = heads.into_iter()
                    .map(|(name, (commit_id, hash))| {
                        (name, (convert!(CommitId from commit_id), hash.to_string()))
                    })
                    .collect::<HashMap<String, (objects::CommitId, String)>>();
                (convert!(ProjectId from project_id), heads)
            })
    }

    pub fn project_github_metadata(&self) -> impl Iterator<Item=(objects::ProjectId, Result<JSON>)> {
        self.store.project_metadata()
            .into_iter()
            .filter(|(_, metadata)| metadata.key == Metadata::GITHUB_METADATA)
            .map(|(id, metadata)| {
                (convert!(ProjectId from id), parse_github_json!("Project", id, metadata.value))
            })
    }

    pub fn commit_hashes(&self) -> impl Iterator<Item=(objects::CommitId, SHA)> {
        self.store.commits(self.substore)
            .into_iter()
            .map(|(id, sha)| (convert!(CommitId from id), sha.to_string()))
    }

    pub fn commit_info(&self) -> impl Iterator<Item=(objects::CommitId, CommitBasics)> {
        self.store.commits_info(self.substore)
            .into_iter()
            .map(|(commit_id, info)| {
                let commit_basics = CommitBasics {
                    committer: convert!(UserId from info.committer),
                    committer_time: info.committer_time,
                    author: convert!(UserId from info.author),
                    author_time: info.author_time,
                    parents: convert!(Vec<CommitId> from info.parents),
                    changes: convert!(HashMap<PathId, SnapshotId> from info.changes,
                                                                  except (_, HashId::DELETED)),
                    message: info.message,
                };
                (convert!(CommitId from commit_id), commit_basics)
            })
    }

    pub fn commit_github_metadata(&self) -> impl Iterator<Item=(objects::CommitId, Result<JSON>)> {
        self.store.commits_metadata(self.substore)
            .into_iter()
            .filter(|(_, metadata)| metadata.key == Metadata::GITHUB_METADATA)
            .map(|(id, metadata)| {
                (convert!(CommitId from id), parse_github_json!("Commit", id, metadata.value))
            })
    }

    // TODO hashes?
    pub fn get_snapshot(&self, id: SnapshotId) -> Option<Bytes> {
        self.store.contents(self.substore)
            .get(parasite::HashId::from(Into::<u64>::into(id)))
            .map(|(_kind, hash)| hash)
    }

    pub fn snapshot_bytes(&self) -> impl Iterator<Item=(objects::SnapshotId, Bytes)> {
        self.store.contents(self.substore)
            .into_iter()
            .map(|(id, (_kind, contents))| {
                (convert!(SnapshotId from id), contents)
            })
        // Maybe I could do something with `kind` but I'm not sure how to handle it. On one hand it
        // really is a distinction of languages, so I could classify it like that, but there's a big
        // black hole of `small files` that don't make a lot of sense when filtering.
    }

    pub fn snapshot_metadata(&self) -> impl Iterator<Item=(objects::SnapshotId, Result<JSON>)> {
        self.store.contents_metadata(self.substore)
            .into_iter()
            .map(|(id, metadata)| {
                (convert!(SnapshotId from id), parse_github_json!("Snapshot", id, metadata.value))
            })
    }

    pub fn path_hashes(&self) -> impl Iterator<Item=(objects::PathId, SHA)> {
        self.store.paths(self.substore).into_iter().map(|(id, hash)| {
            (convert!(PathId from id), hash.to_string())
        })
    }

    pub fn paths(&self) -> impl Iterator<Item=(objects::PathId, Path)> {
        self.store.paths_strings(self.substore).into_iter().map(|(id, path)| {
            (convert!(PathId from id), path)
        })
    }

    pub fn user_emails(&self) -> impl Iterator<Item=(objects::UserId, String)> {
        self.store.users(self.substore).into_iter().map(|(id, email)| {
            (convert!(UserId from id), email)
        })
    }

    pub fn user_metadata(&self) -> impl Iterator<Item=(objects::UserId, Result<JSON>)> {
        self.store.users_metadata(self.substore).into_iter().map(|(id, metadata)| {
            (convert!(UserId from id), parse_github_json!("User", id, metadata.value))
        })
    }
}

pub struct CommitBasics {
    pub committer : objects::UserId,
    pub committer_time : Timestamp,
    pub author : objects::UserId,
    pub author_time : Timestamp,
    pub parents : Vec<objects::CommitId>,
    pub changes : Vec<(objects::PathId, Option<objects::SnapshotId>)>,
    pub message : String,
}