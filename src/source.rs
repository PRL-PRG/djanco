use parasite;
use crate::{Store, objects};

use anyhow::*;
use crate::objects::SnapshotId;
use std::collections::HashMap;
use parasite::{Metadata, HashId, StoreKind, ValidateAll};
use parasite::Table;
use serde_json::Value as JSON;
use std::str::FromStr;
use crate::CacheDir;
use std::fs::create_dir_all;
use itertools::Itertools;

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
    fn from_one_subset<Sd>(dataset_path: Sd, savepoint: i64, substore: Store) -> Result<Self> where Sd: Into<String> {
        let dataset_path = dataset_path.into();
        //println!("Store path: {}", s);
        let store = parasite::DatastoreView::from(dataset_path.as_str());
        Ok(Source { store, savepoint, substore: substore.kind() })
    }

    fn from_all_subsets<Sc,Sd>(dataset_path: Sd, cache_path: Sc, savepoint: i64) -> Result<Self> where Sd: Into<String>, Sc: Into<String> {
        let all_stores: Vec<Store> = StoreKind::all().map(|kind| Store::from(kind)).collect();
        Self::from_multiple_subsets(dataset_path, cache_path, savepoint, all_stores)
    }

    fn from_multiple_subsets<Sc,Sd>(dataset_path: Sd, cache_path: Sc, savepoint: i64, substores: Vec<Store>) -> Result<Self> where Sd: Into<String>, Sc: Into<String> {
        let cache_path = cache_path.into();
        let mut merged_store_path = CacheDir::from(cache_path, savepoint, substores.clone()).as_path();
        merged_store_path.push("merged_store");
        let merged_store_path_string = merged_store_path.as_os_str().to_str().unwrap();
        create_dir_all(merged_store_path_string)?;

        let mut merger = parasite::DatastoreMerger::new(merged_store_path_string,
                                                        dataset_path.into().as_str());
        for substore in substores {
            merger.merge_substore(StoreKind::Generic, substore.kind(), ValidateAll::new())
        }

        //println!("Merged store path: {}", merged_store_path_string);
        let store = parasite::DatastoreView::from(merged_store_path_string);
        Ok(Source { store, savepoint, substore: StoreKind::Generic })
    }

    pub fn new<Sc,Sd>(dataset_path: Sd, cache_path: Sc, savepoint: i64, substores: Vec<Store>) -> Result<Self> where Sd: Into<String>, Sc: Into<String> {
        match substores.len() {
            0 => Self::from_all_subsets(dataset_path, cache_path, savepoint),
            1 => Self::from_one_subset(dataset_path, savepoint, substores.into_iter().last().unwrap()),
            _ => Self::from_multiple_subsets(dataset_path, cache_path, savepoint, substores),
        }
    }

    pub fn project_logs(&self) -> impl Iterator<Item = (objects::ProjectId, Vec<parasite::ProjectLog>)> {
        self.store.project_updates()
            .map(|(id, log)| (convert!(ProjectId from id), log))
            .into_group_map()
            .into_iter()
            .map(|(project, logs)| (project, logs.into_iter().sorted_by_key(|log| -log.time()).collect() ))
    } 

    pub fn project_urls(&self) -> impl Iterator<Item=(objects::ProjectId, URL)> {
        self.store.project_urls()
            .into_iter()
            .map(|(id, url)| {
                (convert!(ProjectId from id), url.clone_url())
            })
    }

    pub fn project_substores(&self) -> impl Iterator<Item=(objects::ProjectId, Store)> {
        self.store.project_substores()
            .into_iter()
            .map(|(id, kind)| {
                (convert!(ProjectId from id), Store::from(kind))
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
