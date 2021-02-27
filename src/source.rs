use parasite;
use anyhow::*;
use crate::Store;
//use parasite::db::StoreIterAll;
use std::hash::Hash;
use std::collections::HashMap;
use parasite::db::MappingIter;
use std::rc::Rc;
use std::borrow::Borrow;
use parasite::{SubstoreView, RandomAccessView};
use crate::objects::SnapshotId;
use crate::objects::Snapshot;

pub struct StoreSlice {
    store: parasite::DatastoreView,
    substores: Vec<parasite::StoreKind>,
    pub(crate) savepoint: parasite::Savepoint, // FIXME hide
}

impl StoreSlice {
    pub fn new<S>(dataset_path: S, savepoint: i64, substores: Vec<Store>) -> Result<Self> where S: Into<String> {
        let dataset_path = dataset_path.into();
        let store = parasite::DatastoreView::new(dataset_path.as_str());
        let savepoint = store.get_nearest_savepoint(savepoint)
            .with_context(|| {
                format!("Cannot find nearest savepoint to {} in store at path {}.",
                        savepoint, dataset_path)
            })?;
        let substores = substores.into_iter().map(|s| s.into()).collect();
        Ok(StoreSlice { store, savepoint, substores })
    }
    pub fn default_substore(&self) -> &SubstoreSlice {
        if self.substores.len() != 1 {
            panic!("Currently we only support loading data from a single substore")
        }
        let substore_kind = self.substores.get(0).unwrap().clone();
        let substore = self.store.get_substore(substore_kind);
        //SubstoreSlice { substore, savepoint: &self.savepoint }
        unimplemented!()
    }
}

pub struct SubstoreSlice<'a> {
    substore: parasite::SubstoreView<'a>,
    savepoint: &'a parasite::Savepoint,
}

impl<'a> SubstoreSlice<'a> {
    pub fn projects(&self) -> HashMap<parasite::ProjectId, parasite::Project> {
        self.substore.projects(&self.savepoint)
    }
    pub fn commits(&self) -> MappingSlice<parasite::MappingView<parasite::SHA, parasite::CommitId>> {
        MappingSlice { mapping: self.substore.commits(), savepoint: self.savepoint }
    }
    pub fn commits_info(& self) -> MappingSlice<parasite::StoreView<parasite::CommitInfo, parasite::CommitId>> {
        MappingSlice { mapping: self.substore.commits_info(), savepoint: self.savepoint }
    }
    pub fn commits_metadata(& self) -> MappingSlice<parasite::LinkedStoreView<parasite::Metadata, parasite::CommitId>> {
        MappingSlice { mapping: self.substore.commits_metadata(), savepoint: self.savepoint }
    }
    pub fn hashes(& self) -> MappingSlice<parasite::MappingView<parasite::SHA, parasite::HashId>> {
        MappingSlice { mapping: self.substore.hashes(), savepoint: self.savepoint }
    }
    pub fn contents(& self) -> MappingSlice<parasite::SplitStoreView<parasite::FileContents, parasite::ContentsKind, parasite::HashId>> {
        MappingSlice { mapping: self.substore.contents(), savepoint: self.savepoint }
    }
    pub fn content(& self, id: parasite::HashId) -> Option<Vec<u8>> {
        self.substore.contents().get(id)
    }

    pub fn contents_metadata(&self) -> MappingSlice<parasite::LinkedStoreView<parasite::Metadata, parasite::HashId>> {
        MappingSlice { mapping: self.substore.contents_metadata(), savepoint: self.savepoint }
    }
    // pub fn content_data(& self) -> parasite::SplitStoreView<parasite::FileContents, parasite::ContentsKind, parasite::HashId> {
    //     self.substore.content()
    // }
    pub fn paths(&self) -> MappingSlice<parasite::MappingView<parasite::SHA, parasite::PathId>> {
        MappingSlice { mapping: self.substore.paths(), savepoint: self.savepoint }
    }
    pub fn paths_strings(&self) -> MappingSlice<parasite::StoreView<String, parasite::PathId>> {
        MappingSlice { mapping: self.substore.paths_strings(), savepoint: self.savepoint }
    }
    pub fn users<'b>(&'b self) -> MappingSlice<'b, parasite::IndirectMappingView<String, parasite::UserId>> {
        MappingSlice { mapping: self.substore.users(), savepoint: self.savepoint }
    }
    pub fn users_metadata<'b>(&'b self) -> MappingSlice<'b, parasite::LinkedStoreView<parasite::Metadata, parasite::UserId>> {
        MappingSlice { mapping: self.substore.users_metadata(), savepoint: self.savepoint }
    }
}

pub struct MappingSlice<'a, M> {
    mapping: M,
    savepoint: &'a parasite::Savepoint,
}

impl<'b, I, E> MappingSlice<'b, parasite::MappingView<'b, E, I>>
    where E: parasite::db::FixedSizeSerializable<Item = E> + Eq + Hash + Clone,
          I : parasite::db::Id {
    pub fn iter<'a>(&'a mut self) -> impl Iterator<Item=(I, E)> + 'a {
        self.mapping.iter(&self.savepoint)
    }
    pub fn get(&mut self, id: I) -> Option<E> {
        self.mapping.get(id)
    }
}

impl<'b, I, E> MappingSlice<'b, parasite::IndirectMappingView<'b, E, I>>
    where E: parasite::db::Serializable<Item = E> + Eq + Hash + Clone,
          I : parasite::db::Id {
    pub fn iter<'a>(&'a mut self) -> impl Iterator<Item=(I, E)> + 'a {
        self.mapping.iter(&self.savepoint)
    }
    pub fn get(&mut self, id: I) -> Option<E> {
        self.mapping.get(id)
    }
}

impl<'b, I, E> MappingSlice<'b, parasite::StoreView<'b, E, I>>
    where E: parasite::db::Serializable<Item = E>,
          I : parasite::db::Id {
    pub fn iter<'a>(&'a mut self) -> impl Iterator<Item=(I, E)> + 'a {
        self.mapping.iter(&self.savepoint)
    }
}

impl<'b, I, E> MappingSlice<'b, parasite::LinkedStoreView<'b, E, I>>
    where E: parasite::db::Serializable<Item = E>,
          I : parasite::db::Id {
    pub fn iter<'a>(&'a mut self) -> impl Iterator<Item=(I, E)> + 'a {
        self.mapping.iter(&self.savepoint)
    }
}
//parasite::FileContents, parasite::ContentsKind, parasite::HashId>
// impl<'b, I, E, K> MappingSlice<'b, parasite::SplitStoreView<'b, E, K, I>>
//     where E: parasite::db::Serializable<Item = E>,
//           K: parasite::db::SplitKind<Item = K>,
//           I : parasite::db::Id {
//     pub fn iter<'a>(&'a mut self) -> impl Iterator<Item=(I, K, E)> + 'a {
//         self.mapping.iter(&self.savepoint)
//     }
// }
// Maping slice for contents
// FIXME we could separate off all of parasite's types in these iterators
// impl<'b> MappingSlice<'b, parasite::SplitStoreView<'b, parasite::FileContents, parasite::ContentsKind, parasite::HashId>> {
//     pub fn iter<'a>(&'a mut self) -> impl Iterator<Item=Snapshot> + 'a {
//         self.mapping.iter(&self.savepoint)
//             .map(|(hash_id, kind, contents)| {
//                 let hash: u64 = hash_id.into();
//                 let snapshot_id = SnapshotId::from(hash);
//                 let snapshot = Snapshot::new(snapshot_id, contents);
//                 snapshot
//             })
//     }
// }