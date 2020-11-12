use std::marker::PhantomData;
use std::collections::VecDeque;
use std::time::Duration;
use std::borrow::Cow;

use crate::objects::*;
use crate::data::*;

pub struct IterWithData<'a, T, I: Iterator<Item=T> + 'a> { data: &'a Database, iterator: I/*, _t: PhantomData<&'a T>*/ }

impl<'a, T, I> Iterator for IterWithData<'a, T, I> where I: Iterator<Item=T> {
    type Item = ItemWithData<'a, T>;
    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.next().map(|e| ItemWithData::new(&self.data, e))
    }
}

pub struct ItemWithData<'a, T> { pub data: &'a Database, pub item: T }
impl<'a, T> ItemWithData<'a, T> {
    pub fn new(data: &'a Database, item: T) -> Self {
        ItemWithData { data, item }
    }
}
impl<'a> ItemWithData<'a, Project> {
    pub fn id               (&self)    -> ProjectId                       { self.item.id()                                     }
    pub fn url              (&self)    -> String                          { self.item.url().to_string()                        }
    pub fn issue_count      (&self)    -> Option<usize>                   { self.item.issue_count(&self.data)            }
    pub fn buggy_issue_count(&self)    -> Option<usize>                   { self.item.buggy_issue_count(&self.data)      }
    pub fn is_fork          (&self)    -> Option<bool>                    { self.item.is_fork(&self.data)                }
    pub fn is_archived      (&self)    -> Option<bool>                    { self.item.is_archived(&self.data)            }
    pub fn is_disabled      (&self)    -> Option<bool>                    { self.item.is_disabled(&self.data)            }
    pub fn star_count       (&self)    -> Option<usize>                   { self.item.star_count(&self.data)             }
    pub fn watcher_count    (&self)    -> Option<usize>                   { self.item.watcher_count(&self.data)          }
    pub fn size             (&self)    -> Option<usize>                   { self.item.size(&self.data)                   }
    pub fn open_issue_count (&self)    -> Option<usize>                   { self.item.open_issue_count(&self.data)       }
    pub fn fork_count       (&self)    -> Option<usize>                   { self.item.fork_count(&self.data)             }
    pub fn subscriber_count (&self)    -> Option<usize>                   { self.item.subscriber_count(&self.data)       }
    pub fn license          (&self)    -> Option<String>                  { self.item.license(&self.data)                }
    pub fn language         (&self)    -> Option<Language>                { self.item.language(&self.data)               }
    pub fn description      (&self)    -> Option<String>                  { self.item.description(&self.data)            }
    pub fn homepage         (&self)    -> Option<String>                  { self.item.homepage(&self.data)               }
    pub fn head_ids         (&self)    -> Option<Vec<(String, CommitId)>> { self.item.head_ids(&self.data)               }
    pub fn heads            (&self)    -> Option<Vec<(String, Commit)>>   { self.item.heads(&self.data)                  }
    pub fn head_count       (&self)    -> Option<usize>                   { self.item.head_count(&self.data)             }
    pub fn commit_ids       (&self)    -> Option<Vec<CommitId>>           { self.item.commit_ids(&self.data)             }
    pub fn commits          (&self)    -> Option<Vec<Commit>>             { self.item.commits(&self.data)                }
    pub fn commit_count     (&self)    -> Option<usize>                   { self.item.commit_count(&self.data)           }
    pub fn author_ids       (&self)    -> Option<Vec<UserId>>             { self.item.author_ids(&self.data)             }
    pub fn authors          (&self)    -> Option<Vec<User>>               { self.item.authors(&self.data)                }
    pub fn author_count     (&self)    -> Option<usize>                   { self.item.author_count(&self.data)           }
    pub fn paths            (&self)    -> Option<Vec<Path>>               { self.item.paths(&self.data)                  }
    pub fn path_count       (&self)    -> Option<usize>                   { self.item.path_count(&self.data)             }
    pub fn committer_ids    (&self)    -> Option<Vec<UserId>>             { self.item.committer_ids(&self.data)          }
    pub fn committers       (&self)    -> Option<Vec<User>>               { self.item.committers(&self.data)             }
    pub fn committer_count  (&self)    -> Option<usize>                   { self.item.committer_count(&self.data)        }
    pub fn user_ids         (&self)    -> Option<Vec<UserId>>             { self.item.user_ids(&self.data)               }
    pub fn users            (&self)    -> Option<Vec<User>>               { self.item.users(&self.data)                  }
    pub fn user_count       (&self)    -> Option<usize>                   { self.item.user_count(&self.data)             }
    pub fn lifetime         (&self)    -> Option<Duration>                { self.item.lifetime(&self.data)               }
    pub fn has_issues       (&self)    -> Option<bool>                    { self.item.has_issues(&self.data)             }
    pub fn has_downloads    (&self)    -> Option<bool>                    { self.item.has_downloads(&self.data)          }
    pub fn has_wiki         (&self)    -> Option<bool>                    { self.item.has_wiki(&self.data)               }
    pub fn has_pages        (&self)    -> Option<bool>                    { self.item.has_pages(&self.data)              }
    pub fn created          (&self)    -> Option<i64>                     { self.item.created(&self.data)                }
    pub fn updated          (&self)    -> Option<i64>                     { self.item.updated(&self.data)                }
    pub fn pushed           (&self)    -> Option<i64>                     { self.item.pushed(&self.data)                 }
    pub fn master_branch    (&self)    -> Option<String>                  { self.item.master_branch(&self.data)          }
}
impl<'a> ItemWithData<'a, Snapshot> {
    pub fn raw_contents(&self) -> &Vec<u8> { &self.item.raw_contents() }
    pub fn id(&self) -> SnapshotId { self.item.id() }
    pub fn contents(&self) -> Cow<str> { self.item.contents() }
    pub fn contains(&self, needle: &str) -> bool { self.item.contains(needle) }

}

impl<'a> Into<Project> for ItemWithData<'a, Project> { fn into(self) -> Project { self.item } }
impl<'a> Into<Commit> for ItemWithData<'a, Commit> { fn into(self) -> Commit { self.item } }
impl<'a> Into<User> for ItemWithData<'a, User> { fn into(self) -> User { self.item } }
impl<'a> Into<Path> for ItemWithData<'a, Path> { fn into(self) -> Path { self.item } }
impl<'a> Into<Snapshot> for ItemWithData<'a, Snapshot> { fn into(self) -> Snapshot { self.item } }

impl<'a> Into<ProjectId> for ItemWithData<'a, ProjectId> { fn into(self) -> ProjectId { self.item } }
impl<'a> Into<CommitId> for ItemWithData<'a, CommitId> { fn into(self) -> CommitId { self.item } }
impl<'a> Into<UserId> for ItemWithData<'a, UserId> { fn into(self) -> UserId { self.item } }
impl<'a> Into<PathId> for ItemWithData<'a, PathId> { fn into(self) -> PathId { self.item } }
impl<'a> Into<SnapshotId> for ItemWithData<'a, SnapshotId> { fn into(self) -> SnapshotId { self.item } }

impl<'a> Into<String> for ItemWithData<'a, String> { fn into(self) -> String { self.item } }
impl<'a> Into<u64> for ItemWithData<'a, u64> { fn into(self) -> u64 { self.item } }
impl<'a> Into<u32> for ItemWithData<'a, u32> { fn into(self) -> u32 { self.item } }
impl<'a> Into<i64> for ItemWithData<'a, i64> { fn into(self) -> i64 { self.item } }
impl<'a> Into<i32> for ItemWithData<'a, i32> { fn into(self) -> i32 { self.item } }
impl<'a> Into<f64> for ItemWithData<'a, f64> { fn into(self) -> f64 { self.item } }
impl<'a> Into<f32> for ItemWithData<'a, f32> { fn into(self) -> f32 { self.item } }
impl<'a> Into<usize> for ItemWithData<'a, usize> { fn into(self) -> usize { self.item } }

impl<'a,A,B> Into<(A,B)> for ItemWithData<'a, (A,B)> { fn into(self) -> (A,B) { self.item } }

pub struct QuincunxIter<'a, T: Identifiable> {
    data: &'a Database,
    ids: VecDeque<T::Identity>,
    _type: PhantomData<T>,
}

impl<'a> QuincunxIter<'a, Project> {
    pub fn new(data: &'a Database) -> Self {
        QuincunxIter { data, ids: VecDeque::from(data.all_project_ids()), _type: PhantomData }
    }
}

impl<'a> QuincunxIter<'a, Commit> {
    pub fn new(data: &'a Database) -> Self {
        QuincunxIter { data, ids: VecDeque::from(data.all_commit_ids()), _type: PhantomData }
    }
}

impl<'a> QuincunxIter<'a, User> {
    pub fn new(data: &'a Database) -> Self {
        QuincunxIter { data, ids: VecDeque::from(data.all_user_ids()), _type: PhantomData }
    }
}

// impl<'a> QuincunxIter<'a, Snapshot> {
//     pub fn new(data: &'a Database) -> Self {
//         QuincunxIter { data, ids: VecDeque::from(data.all_snapshot_ids()), _type: PhantomData }
//     }
// }

impl<'a> QuincunxIter<'a, Path> {
    pub fn new(data: &'a Database) -> Self {
        QuincunxIter { data, ids: VecDeque::from(data.all_path_ids()), _type: PhantomData }
    }
}

impl<'a> QuincunxIter<'a, Project> {
    fn reify(&'a self, id: &ProjectId) -> Option<Project> { self.data.project(id) }
}

impl<'a> QuincunxIter<'a, Commit> {
    fn reify(&'a self, id: &CommitId) -> Option<Commit> { self.data.commit(id) }
}

impl<'a> QuincunxIter<'a, User> {
    fn reify(&'a self, id: &UserId) -> Option<User> { self.data.user(id) }
}

impl<'a> QuincunxIter<'a, Path> {
    fn reify(&'a self, id: &PathId) -> Option<Path> { self.data.path(id) }
}

impl<'a> QuincunxIter<'a, Snapshot> {
    fn reify(&'a self, id: &SnapshotId) -> Option<Snapshot> { self.data.snapshot(id) }
}

macro_rules! get_next {
    ($self:expr) => {{
        loop {
            if $self.ids.is_empty() {
                return None
            }

            let id = $self.ids.pop_front().unwrap();
            let element = $self.reify(&id);

            if element.is_some() {
                return Some(ItemWithData::new(&$self.data, element.unwrap()));
            }
        }
    }}
}

impl<'a> Iterator for QuincunxIter<'a, Project> { // Ideally, make generic
    type Item = ItemWithData<'a, Project>;
    fn next(&mut self) -> Option<Self::Item> {
        get_next!(self)
    }
}

impl<'a> Iterator for QuincunxIter<'a, User> { // Ideally, make generic
type Item = ItemWithData<'a, User>;
    fn next(&mut self) -> Option<Self::Item> {
        get_next!(self)
    }
}

impl<'a> Iterator for QuincunxIter<'a, Commit> { // Ideally, make generic
type Item = ItemWithData<'a, Commit>;
    fn next(&mut self) -> Option<Self::Item> {
        get_next!(self)
    }
}

impl<'a> Iterator for QuincunxIter<'a, Path> { // Ideally, make generic
type Item = ItemWithData<'a, Path>;
    fn next(&mut self) -> Option<Self::Item> {
        get_next!(self)
    }
}

impl<'a> Iterator for QuincunxIter<'a, Snapshot> { // Ideally, make generic
type Item = ItemWithData<'a, Snapshot>;
    fn next(&mut self) -> Option<Self::Item> {
        get_next!(self)
    }
}

pub trait DropKey<K, V> {
    type Iterator;
    fn drop_key(self) -> std::iter::Map<Self::Iterator, Box<dyn FnMut((K, V)) -> V>>;
}

impl<K,V,I> DropKey<K,V> for I where I: Iterator<Item=(K, V)> {
    type Iterator = I;
    fn drop_key(self) -> std::iter::Map<Self::Iterator, Box<dyn FnMut((K, V)) -> V>> {
        self.map(Box::new(|(_,b)| b))
    }
}



