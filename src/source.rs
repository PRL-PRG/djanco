use parasite;
use crate::{Store, objects};
use crate::log::Log;
use anyhow::*;
use crate::objects::SnapshotId;
use std::collections::HashMap;
use parasite::Metadata;

pub struct Source {
    store: parasite::DatastoreView,
    substore: parasite::StoreKind,
    savepoint: i64,
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

    pub fn project_urls(&self) -> impl Iterator<Item=(objects::ProjectId, String)> {
        self.store.project_urls()
            .into_iter()
            .map(|(id, url)| {
                (objects::ProjectId::from(Into::<u64>::into(id)), url.clone_url())
            })
    }

    pub fn project_heads(&self) -> impl Iterator<Item=(objects::ProjectId, HashMap<String, (objects::CommitId, String)>)> {
        self.store.project_heads()
            .into_iter()
            .map(|(id, heads)| {
                let heads = heads.into_iter()
                    .map(|(name, (commit_id, hash))| {
                        let commit_id =
                            objects::CommitId::from(Into::<u64>::into(commit_id));
                        let hash = hash.to_string();
                        (name, (commit_id, hash))
                    })
                    .collect::<HashMap<String, (objects::CommitId, String)>>();
                (objects::ProjectId::from(Into::<u64>::into(id)), heads)
            })
    }

    pub fn project_github_metadata(&self) -> impl Iterator<Item=(objects::ProjectId, String)> {     // TODO JSON
        self.store.project_metadata()
            .into_iter()
            .filter(|(_, metadata)| metadata.key == Metadata::GITHUB_METADATA)
            .map(|(id, metadata)| {
                (objects::ProjectId::from(Into::<u64>::into(id)), metadata.value)
            })
    }

    pub fn commit_hashes(&self) -> impl Iterator<Item=(objects::CommitId, String)> {
        self.store.commits(self.substore)
            .into_iter()
            .map(|(id, sha)| {
                (objects::CommitId::from(Into::<u64>::into(id)), sha.to_string())
            })
    }

    pub fn commits_info(&self) -> impl Iterator<Item=(objects::CommitId, ())> {
        self.store.commits_info(self.substore)
            .into_iter()
            .map(|(id, info)| {
                (objects::CommitId::from(Into::<u64>::into(id)), ())                  // TODO
            })
    }

    pub fn commit_github_metadata(&self) -> impl Iterator<Item=(objects::CommitId, String)> {       // TODO JSON
        self.store.commits_metadata(self.substore)
            .into_iter()
            .filter(|(_, metadata)| metadata.key == Metadata::GITHUB_METADATA)
            .map(|(id, metadata)| {
                (objects::CommitId::from(Into::<u64>::into(id)), metadata.value)
            })
    }

    // FIXME
    pub fn snapshot_bytes(&self) -> impl Iterator<Item=(objects::SnapshotId, Vec<u8>)> {
        unimplemented!();
        self.store.contents(self.substore)
            .into_iter()
            .map(|(id, contents)| {
                (objects::SnapshotId::from(0u64), contents)
            })
    }
}
