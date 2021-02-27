use parasite;
use anyhow::*;
use crate::Store;
use std::hash::Hash;
use std::collections::HashMap;
use parasite::db::MappingIter;
use std::rc::Rc;
use std::borrow::Borrow;
use parasite::{SubstoreView, RandomAccessView, DatastoreView, StoreKind, Savepoint, CommitId, SHA};
use crate::objects::SnapshotId;
use crate::objects::Snapshot;
use std::path::Iter;
use std::marker::PhantomData;
use std::ops::{DerefMut, Deref};
use dereference::DereferenceMut;
use std::cell::RefCell;
use std::pin::Pin;
use chrono::DateTime;
use std::time::SystemTime;

pub fn commits_iter<'a>(store: &'a DatastoreView, substore_kind: StoreKind, sp: &Savepoint) -> impl Iterator<Item=(CommitId, SHA)> + 'a {
    let substore = store.get_substore(substore_kind);

    let view =
        DereferenceMut::new_mut(substore, |substore| substore.commits());

    let iter =
        DereferenceMut::map_mut(view, |view| RefCell::new(view.iter(sp)));

    //let mut view = substore.commits();
    //let iter = view.iter(sp);

    DerefMutIter { iter }
}


struct DerefMutIter<I, R>  {
    iter: Pin<Box<DereferenceMut<R, RefCell<I>>>>
}

impl<I, R, T> Iterator for DerefMutIter<I, R> where I: Iterator<Item=T>{
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        RefCell::borrow_mut(self.iter.deref()).next()
    }
}

#[test]
pub fn test() {
    let root = "";
    let store = DatastoreView::new(root);

    let savepoint = store.current_savepoint();

    let js_commits = commits_iter(&store,StoreKind::JavaScript, &savepoint);
    let c_commits = commits_iter(&store,StoreKind::C, &savepoint);

    println!("JS commits: {}", js_commits.count());
    println!("C commits: {}", c_commits.count());
}

