#![type_length_limit="1405002"]

             pub mod fraction;
             pub mod ordf64;
             pub mod commandline;
             pub mod weights_and_measures;
#[macro_use] pub mod log;
             pub mod csv;
#[macro_use] pub mod attrib;
             pub mod metadata;
             pub mod persistent;
             pub mod iterators;
             pub mod tuples;
             pub mod data;
             pub mod objects;
             pub mod receipt;
             pub mod spec;
             pub mod time;
             pub mod dump;
             mod piracy;
             mod product;

#[macro_use] extern crate mashup;

// TODO features
// CSV export
// receipts
// Git commit as version
// commit frequency
// fill in CSV-capable objects
// maybe length for all strings
// maybe non-empty precicate for vectors
// buckets
// Fraction vs f64
// unit tests
// print out fractions as decimals
// flat_map select
// explore parallelism
// prefiltering
// dump metadata, also make raw metadata accessible from objects::Project
// TODO rename Users to Contributors

use std::iter::{Sum, FromIterator};
use std::hash::{Hash, Hasher};
use std::collections::*;

use itertools::Itertools;
use serde::export::{PhantomData, Formatter};
use rand_pcg::Pcg64Mcg;
use rand::SeedableRng;
use rand::seq::IteratorRandom;

use crate::attrib::*;
use crate::fraction::*;
use crate::objects::ItemWithData;
use std::ops::Div;
use std::fmt::Display;

macro_rules! impl_attribute_definition {
    [$object:ty, $attribute:ident] => {
        #[derive(Eq, PartialEq, Copy, Clone, Hash, Debug)] pub struct $attribute;
        impl Attribute for $attribute { type Object = $object; }
    }
}

macro_rules! impl_attribute_getter {
    [! $object:ty, $attribute:ident] => {
        impl<'a> Getter<'a> for $attribute {
            type IntoItem = Self::Object;
            fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                object.item.clone()
            }
        }
        impl<'a> OptionGetter<'a> for $attribute {
            type IntoItem = Self::Object;
            fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                Some(object.item.clone())
            }
        }
    };
    [!+ $object:ty, $attribute:ident] => {
        impl<'a> Getter<'a> for $attribute {
            type IntoItem = objects::ItemWithData<'a, Self::Object>;
            fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                object.clone()
            }
        }
        impl<'a> OptionGetter<'a> for $attribute {
            type IntoItem = objects::ItemWithData<'a, Self::Object>;
            fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                Some(object.clone())
            }
        }
    };
    [! $object:ty, $attribute:ident, $small_type:ty, $getter:ident] => {
        impl<'a> Getter<'a> for $attribute {
            type IntoItem = $small_type;
            fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                object.$getter()
            }
        }
        impl<'a> OptionGetter<'a> for $attribute {
            type IntoItem = $small_type;
            fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                Some(object.$getter())
            }
        }
    };
    [? $object:ty, $attribute:ident, $small_type:ty, $getter:ident] => {
        impl<'a> Getter<'a> for $attribute {
            type IntoItem = Option<$small_type>;
            fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                object.$getter()
            }
        }
        impl<'a> OptionGetter<'a> for $attribute {
            type IntoItem = $small_type;
            fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                object.$getter()
            }
        }
    };
    [!+ $object:ty, $attribute:ident, $small_type:ty, $getter:ident] => {
        impl<'a> Getter<'a> for $attribute {
            type IntoItem = objects::ItemWithData<'a, $small_type>;
            fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                object.$getter()
            }
        }
        impl<'a> OptionGetter<'a> for $attribute {
            type IntoItem = objects::ItemWithData<'a, $small_type>;
            fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                Some(object.$getter())
            }
        }
    };
    [?+ $object:ty, $attribute:ident, $small_type:ty, $getter:ident] => {
        impl<'a> Getter<'a> for $attribute {
            type IntoItem = Option<objects::ItemWithData<'a, $small_type>>;
            fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                object.$getter()
            }
        }
        impl<'a> OptionGetter<'a> for $attribute {
            type IntoItem = objects::ItemWithData<'a, $small_type>;
            fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                object.$getter()
            }
        }
    };
    [!+.. $object:ty, $attribute:ident, $small_type:ty, $getter:ident] => {
        impl<'a> Getter<'a> for $attribute {
            type IntoItem = Vec<objects::ItemWithData<'a, $small_type>>;
            fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                object.$getter()
            }
        }
        impl<'a> OptionGetter<'a> for $attribute {
            type IntoItem = Vec<objects::ItemWithData<'a, $small_type>>;
            fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                Some(object.$getter())
            }
        }
    };
    [?+.. $object:ty, $attribute:ident, $small_type:ty, $getter:ident] => {
        impl<'a> Getter<'a> for $attribute {
            type IntoItem = Option<Vec<objects::ItemWithData<'a, $small_type>>>;
            fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                object.$getter()
            }
        }
        impl<'a> OptionGetter<'a> for $attribute {
            type IntoItem = Vec<objects::ItemWithData<'a, $small_type>>;
            fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                object.$getter()
            }
        }
    };
}

macro_rules! impl_attribute_count {
    [! $object:ty, $attribute:ident, $counter:ident] => {
        impl<'a> Countable<'a> for $attribute {
            fn count(&self, object: &objects::ItemWithData<'a, Self::Object>) -> usize {
                object.$counter()
            }
        }
        impl<'a> OptionCountable<'a> for $attribute {
            fn count(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<usize> {
                Some(object.$counter())
            }
        }
    };
    [? $object:ty, $attribute:ident, $counter:ident] => {
        impl<'a> OptionCountable<'a> for $attribute {
            fn count(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<usize> {
                object.$counter()
            }
        }
    }
}

macro_rules! impl_attribute_filter {
    [$object:ty, $attribute:ident] => {
        impl<'a> Filter<'a> for $attribute {
            type Item = $object;
            fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
                self.get(item_with_data).unwrap_or(false)
            }
        }
    }
}

macro_rules! impl_attribute {
    [! $object:ty, $attribute:ident] => {
        impl_attribute_definition![$object, $attribute];
        impl_attribute_getter![! $object, $attribute];
    };
    [!+ $object:ty, $attribute:ident] => {
        impl_attribute_definition![$object, $attribute];
        impl_attribute_getter![!+ $object, $attribute];
    };
    [! $object:ty, $attribute:ident, bool, $getter:ident] => {
        impl_attribute_definition![$object, $attribute];
        impl_attribute_getter![! $object, $attribute, bool, $getter];
        impl_attribute_filter![$object, $attribute];
    };
    [! $object:ty, $attribute:ident, $small_type:ty, $getter:ident] => {
        impl_attribute_definition![$object, $attribute];
        impl_attribute_getter![! $object, $attribute, $small_type, $getter];
    };
    [!+ $object:ty, $attribute:ident, $small_type:ty, $getter:ident] => {
        impl_attribute_definition![$object, $attribute];
        impl_attribute_getter![!+ $object, $attribute, $small_type, $getter];
    };
    [? $object:ty, $attribute:ident, bool, $getter:ident] => {
        impl_attribute_definition![$object, $attribute];
        impl_attribute_getter![? $object, $attribute, bool, $getter];
        impl_attribute_filter![$object, $attribute];
    };
    [? $object:ty, $attribute:ident, $small_type:ty, $getter:ident] => {
        impl_attribute_definition![$object, $attribute];
        impl_attribute_getter![? $object, $attribute, $small_type, $getter];
    };
    [?+ $object:ty, $attribute:ident, $small_type:ty, $getter:ident] => {
        impl_attribute_definition![$object, $attribute];
        impl_attribute_getter![?+ $object, $attribute, $small_type, $getter];
    };
    [!.. $object:ty, $attribute:ident, $small_type:ty, $getter:ident, $counter:ident] => {
        impl_attribute_definition![$object, $attribute];
        impl_attribute_getter![! $object, $attribute, Vec<$small_type>, $getter];
        impl_attribute_count![! $object, $attribute, $counter];
    };
    [!+.. $object:ty, $attribute:ident, $small_type:ty, $getter:ident, $counter:ident] => {
        impl_attribute_definition![$object, $attribute];
        impl_attribute_getter![!+.. $object, $attribute, $small_type, $getter];
        impl_attribute_count![! $object, $attribute, $counter];
    };
    [?.. $object:ty, $attribute:ident, $small_type:ty, $getter:ident, $counter:ident] => {
        impl_attribute_definition![$object, $attribute];
        impl_attribute_getter![? $object, $attribute, Vec<$small_type>, $getter];
        impl_attribute_count![? $object, $attribute, $counter];
    };
    [?+.. $object:ty, $attribute:ident, $small_type:ty, $getter:ident, $counter:ident] => {
        impl_attribute_definition![$object, $attribute];
        impl_attribute_getter![?+.. $object, $attribute, $small_type, $getter];
        impl_attribute_count![? $object, $attribute, $counter];
    };
}

pub mod project {
    use crate::objects;
    use crate::time;
    use crate::attrib::*;

    impl_attribute![!+    objects::Project, Itself];
    impl_attribute![!     objects::Project, Raw];
    impl_attribute![!     objects::Project, Id, objects::ProjectId, id];
    impl_attribute![!     objects::Project, URL, String, url];
    impl_attribute![?     objects::Project, Issues, usize, issue_count];
    impl_attribute![?     objects::Project, BuggyIssues, usize, buggy_issue_count];
    impl_attribute![?     objects::Project, IsFork, bool, is_fork];
    impl_attribute![?     objects::Project, IsArchived, bool, is_archived];
    impl_attribute![?     objects::Project, IsDisabled, bool, is_disabled];
    impl_attribute![?     objects::Project, Stars, usize, star_count];
    impl_attribute![?     objects::Project, Watchers, usize, watcher_count];
    impl_attribute![?     objects::Project, Size, usize, size];
    impl_attribute![?     objects::Project, OpenIssues, usize, open_issue_count];
    impl_attribute![?     objects::Project, Forks, usize, fork_count];
    impl_attribute![?     objects::Project, Subscribers, usize, subscriber_count];
    impl_attribute![?     objects::Project, License, String, license];
    impl_attribute![?     objects::Project, Language, objects::Language, language];
    impl_attribute![?     objects::Project, Description, String, description];
    impl_attribute![?     objects::Project, Homepage, String, homepage];
    impl_attribute![?     objects::Project, HasIssues, bool, has_issues];
    impl_attribute![?     objects::Project, HasDownloads, bool, has_downloads];
    impl_attribute![?     objects::Project, HasWiki, bool, has_wiki];
    impl_attribute![?     objects::Project, HasPages, bool, has_pages];
    impl_attribute![?     objects::Project, Created, i64, created];
    impl_attribute![?     objects::Project, Updated, i64, updated];
    impl_attribute![?     objects::Project, Pushed, i64, pushed];
    impl_attribute![?     objects::Project, DefaultBranch, String, default_branch];
    impl_attribute![?     objects::Project, Age, time::Duration, lifetime];
    impl_attribute![?+..  objects::Project, Heads, objects::Head, heads_with_data, head_count];
    impl_attribute![?..   objects::Project, CommitIds, objects::CommitId, commit_ids, commit_count];
    impl_attribute![?..   objects::Project, AuthorIds, objects::UserId, author_ids, author_count];
    impl_attribute![?..   objects::Project, CommitterIds, objects::UserId, committer_ids, committer_count];
    impl_attribute![?..   objects::Project, UserIds, objects::UserId, user_ids, user_count];
    impl_attribute![?..   objects::Project, PathIds, objects::PathId, path_ids, path_count];
    impl_attribute![?..   objects::Project, SnapshotIds, objects::SnapshotId, snapshot_ids, snapshot_count];
    impl_attribute![?+..  objects::Project, Commits, objects::Commit, commits_with_data, commit_count];
    impl_attribute![?+..  objects::Project, Authors, objects::User, authors_with_data, author_count];
    impl_attribute![?+..  objects::Project, Committers, objects::User, committers_with_data, committer_count];
    impl_attribute![?+..  objects::Project, Users, objects::User, users_with_data, user_count];
    impl_attribute![?+..  objects::Project, Paths, objects::Path, paths_with_data, path_count];
    impl_attribute![?+..  objects::Project, Snapshots, objects::Snapshot, snapshots_with_data, snapshot_count];
}

pub mod commit {
    use crate::objects;
    use crate::attrib::*;

    impl_attribute![!+   objects::Commit, Itself];
    impl_attribute![!    objects::Commit, Raw];
    impl_attribute![!    objects::Commit, Id, objects::CommitId, id];
    impl_attribute![!    objects::Commit, CommitterId, objects::UserId, committer_id];
    impl_attribute![!    objects::Commit, AuthorId, objects::UserId, author_id];
    impl_attribute![?+   objects::Commit, Committer, objects::User, committer_with_data];
    impl_attribute![?+   objects::Commit, Author, objects::User, author_with_data];
    impl_attribute![?    objects::Commit, Hash, String, hash];
    impl_attribute![?    objects::Commit, Message, String, message];
    impl_attribute![?    objects::Commit, MessageLength, usize, message_length];
    impl_attribute![?    objects::Commit, AuthoredTimestamp, i64, author_timestamp];
    impl_attribute![?    objects::Commit, CommittedTimestamp, i64, committer_timestamp];
    impl_attribute![?..  objects::Commit, PathIds, objects::PathId, changed_path_ids, changed_path_count];
    impl_attribute![?..  objects::Commit, SnapshotIds, objects::SnapshotId, changed_snapshot_ids, changed_snapshot_count];
    impl_attribute![!..  objects::Commit, ParentIds, objects::CommitId, parent_ids, parent_count];
    impl_attribute![?+.. objects::Commit, Paths, objects::Path, changed_paths_with_data, changed_path_count];
    impl_attribute![?+.. objects::Commit, Snapshots, objects::Snapshot, changed_snapshots_with_data, changed_snapshot_count];
    impl_attribute![!+.. objects::Commit, Parents, objects::Commit, parents_with_data, parent_count];
}

pub mod head {
    use crate::objects;
    use crate::attrib::*;

    impl_attribute![!+  objects::Head, Itself];
    impl_attribute![!   objects::Head, Raw];
    impl_attribute![!   objects::Head, Name, String, name];
    impl_attribute![!   objects::Head, CommitId, objects::CommitId, commit_id];
    impl_attribute![?+  objects::Head, Commit, objects::Commit, commit_with_data];
}

pub mod change {
    use crate::objects;
    use crate::attrib::*;

    impl_attribute![!+  objects::Change, Itself];
    impl_attribute![!   objects::Change, Raw];
    impl_attribute![!   objects::Change, PathId, objects::PathId, path_id];
    impl_attribute![?   objects::Change, SnapshotId, objects::SnapshotId, snapshot_id];
    impl_attribute![?+  objects::Change, Path, objects::Path, path_with_data];
    impl_attribute![?+  objects::Change, Snapshot, objects::Snapshot, snapshot_with_data];
}

pub mod user {
    use crate::objects;
    use crate::time;
    use crate::attrib::*;

    impl_attribute![!+   objects::User, Itself];
    impl_attribute![!    objects::User, Raw];
    impl_attribute![!    objects::User, Id, objects::UserId, id];
    impl_attribute![!    objects::User, Email, String, email];
    impl_attribute![?    objects::User, AuthorExperience, time::Duration, author_experience];
    impl_attribute![?    objects::User, CommitterExperience, time::Duration, committer_experience];
    impl_attribute![?    objects::User, Experience, time::Duration, experience];
    impl_attribute![?..  objects::User, AuthoredCommitIds, objects::CommitId, authored_commit_ids, authored_commit_count];
    impl_attribute![?..  objects::User, CommittedCommitIds, objects::CommitId, committed_commit_ids, committed_commit_count];
    impl_attribute![?+.. objects::User, AuthoredCommits, objects::Commit, authored_commits_with_data, authored_commit_count];
    impl_attribute![?+.. objects::User, CommittedCommits, objects::Commit, committed_commits_with_data, committed_commit_count];
}

pub mod path {
    use crate::objects;
    use crate::attrib::*;

    impl_attribute![!+  objects::Path, Itself];
    impl_attribute![!   objects::Path, Raw];
    impl_attribute![!   objects::Path, Id, objects::PathId, id];
    impl_attribute![!   objects::Path, Location, String, location];
    impl_attribute![?   objects::Path, Language, objects::Language, language];
}

pub mod snapshot {
    use crate::objects;
    use crate::attrib::*;

    impl_attribute![!+  objects::Snapshot, Itself];
    impl_attribute![!   objects::Snapshot, Raw];
    impl_attribute![!   objects::Snapshot, Id, objects::SnapshotId, id];
    impl_attribute![!   objects::Snapshot, Bytes, Vec<u8>, raw_contents_owned];
    impl_attribute![!   objects::Snapshot, Contents, String, contents_owned];
}

pub trait AttributeIterator<'a, T>: Sized + Iterator<Item=objects::ItemWithData<'a, T>> {
    fn filter_by_attrib<A>(self, attribute: A)
                           -> AttributeFilterIter<Self, A>
        where A: Filter<'a, Item=T> {
        AttributeFilterIter { iterator: self, attribute }
    }

    fn map_into_attrib<A, Ta, Tb>(self, attribute: A)
                                  -> AttributeMapIter<Self, A, Ta, Tb>
        where A: Select<'a, Ta, Tb> {
        AttributeMapIter { iterator: self, attribute, function: PhantomData }
    }

    fn sort_by_attrib<A: 'a, I>(self, attribute: A)
                                -> std::vec::IntoIter<objects::ItemWithData<'a, T>>
        where A: Sort<'a, T, I>, I: Ord {
        self.sort_by_attrib_with_direction(sort::Direction::Descending, attribute)
    }

    fn sort_by_attrib_with_direction<A: 'a, I>(self, direction: sort::Direction, attribute: A)
                                               -> std::vec::IntoIter<objects::ItemWithData<'a, T>>
        where A: Sort<'a, T, I>, I: Ord {
        let mut vector = Vec::from_iter(self);
        attribute.sort(direction, &mut vector);
        vector.into_iter()
    }

    fn sample<S>(self, sampler: S)
                 -> std::vec::IntoIter<objects::ItemWithData<'a, T>>
        where S: Sampler<'a, T> {
        sampler.sample(self).into_iter()
    }

    fn group_by_attrib<A, K>(self, attribute: A)
                             -> std::collections::hash_map::IntoIter<K, Vec<objects::ItemWithData<'a, T>>>
        where A: Group<'a, T, K>, K: Hash + Eq {
        self.map(|item_with_data| {
            let key = attribute.select_key(&item_with_data);
            (key, item_with_data)
        }).into_group_map().into_iter()
    }

    // TODO drop options
}

impl<'a, T, I> AttributeIterator<'a, T> for I
    where I: Sized + Iterator<Item=objects::ItemWithData<'a, T>> {}

pub trait AttributeGroupIterator<'a, K, T>: Sized + Iterator<Item=(K, Vec<objects::ItemWithData<'a, T>>)> {
    fn filter_by_attrib<A>(self, attribute: A)
                           -> AttributeGroupFilterIter<Self, A>
        where A: Filter<'a, Item=T> {
        AttributeGroupFilterIter { iterator: self, attribute }
    }
    // TODO filter_key

    fn map_into_attrib<A, Ta, Tb>(self, attribute: A)
                                  -> AttributeGroupMapIter<Self, A, Ta, Tb>
        where A: Select<'a, Ta, Tb> {
        AttributeGroupMapIter { iterator: self, attribute, function: PhantomData }
    }

    fn sort_by_attrib<A: 'a, I>(self, attribute: A)
                                -> std::vec::IntoIter<(K, Vec<objects::ItemWithData<'a, T>>)>
        where A: Sort<'a, T, I>, I: Ord {
        self.sort_by_attrib_with_direction(sort::Direction::Descending, attribute)
    }

    fn sort_by_attrib_with_direction<A: 'a, I>(self, direction: sort::Direction, attribute: A)
                                               -> std::vec::IntoIter<(K, Vec<objects::ItemWithData<'a, T>>)>
        where A: Sort<'a, T, I>, I: Ord {
        let vector: Vec<(K, Vec<objects::ItemWithData<'a, T>>)> =
            self.map(|(key, mut vector)| {
                attribute.sort(direction, &mut vector);
                (key, vector)
            }).collect();
        vector.into_iter()
    }
    // TODO sort_key, sort_key_by, sort_key_with, sort_values, sort_values_by, sort_values_with

    fn sample<S>(self, sampler: S)
                 -> std::vec::IntoIter<(K, Vec<objects::ItemWithData<'a, T>>)>
        where S: Sampler<'a, T> {
        let vector: Vec<(K, Vec<objects::ItemWithData<'a, T>>)> =
            self.map(|(key, vector)| {
                (key, sampler.sample_from(vector))
            }).collect();
        vector.into_iter()
    }
    // TODO sample_key

    fn ungroup(self) -> std::vec::IntoIter<objects::ItemWithData<'a, T>> {
        let vector: Vec<objects::ItemWithData<'a, T>> =
            self.flat_map(|(_, vector)| vector).collect();
        vector.into_iter()
    }
}

impl<'a, K, T, I> AttributeGroupIterator<'a, K, T> for I
    where I: Sized + Iterator<Item=(K, Vec<objects::ItemWithData<'a, T>>)> {}


macro_rules! impl_comparison {
        ($name:ident, $trait_limit:ident, $comparator:ident, $default:expr) => {
            pub struct $name<A, N>(pub A, pub N) where A: Attribute; // + OptionGetter<'a, IntoItem=N>;
            impl<'a, A, N, T> Filter<'a> for $name<A, N> where A: OptionGetter<'a, IntoItem=N> + Attribute<Object=T>, N: $trait_limit {
                type Item = T;
                fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
                    self.0.get_opt(item_with_data).map_or($default, |n| n.$comparator(&self.1))
                }
            }
        }
    }

impl_comparison!(LessThan, PartialOrd, lt, false);
impl_comparison!(AtMost,   PartialOrd, le, false);
impl_comparison!(Equal,    Eq,         eq, false);
impl_comparison!(AtLeast,  PartialOrd, ge, true);
impl_comparison!(MoreThan, PartialOrd, gt, true);

macro_rules! impl_binary {
        ($name:ident, $comparator:expr) => {
            pub struct $name<A, B>(pub A, pub B); // where A: Attribute, B: Attribute;
            impl<'a, A, B, T> Filter<'a> for $name<A, B> where A: Filter<'a, Item=T>, B: Filter<'a, Item=T> {
                type Item = T;
                fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
                    $comparator(self.0.accept(item_with_data),
                                self.1.accept(item_with_data))
                }
            }
        }
    }

impl_binary!(And, |a, b| a && b); // TODO Effectively does not short circuit.
impl_binary!(Or,  |a, b| a || b);

macro_rules! impl_unary {
        ($name:ident, $comparator:expr) => {
            pub struct $name<A>(pub A); // where A: Attribute;
            impl<'a, A, T> Filter<'a> for $name<A> where A: Filter<'a, Item=T> {
                type Item = T;
                fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
                    $comparator(self.0.accept(item_with_data))
                }
            }
        }
    }

impl_unary!(Not,  |a: bool| !a);

macro_rules! impl_existential {
        ($name:ident, $method:ident) => {
            pub struct $name<A>(pub A) where A: Attribute; // + OptionGetter<'a>;
            impl<'a, A, T> Filter<'a> for $name<A> where A: OptionGetter<'a>, A: Attribute<Object=T> {
                type Item = T;
                fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
                    self.0.get_opt(item_with_data).$method()
                }
            }
        }
    }

impl_existential!(Exists,  is_some);
impl_existential!(Missing, is_none);

pub struct Same<'a, A>(pub A, pub &'a str) where A: OptionGetter<'a>;
impl<'a, A, T> Filter<'a> for Same<'a, A> where A: OptionGetter<'a, IntoItem=String>, A: Attribute<Object=T> {
    type Item = T;
    fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
        self.0.get_opt(item_with_data).map_or(false, |e| e.as_str() == self.1)
    }
}

pub struct Contains<'a, A>(pub A, pub &'a str) where A: OptionGetter<'a>;
impl<'a, A, T> Filter<'a> for Contains<'a, A> where A: OptionGetter<'a, IntoItem=String>, A: Attribute<Object=T> {
    type Item = T;
    fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
        self.0.get_opt(item_with_data).map_or(false, |e| e.contains(self.1))
    }
}

#[macro_export] macro_rules! regex { ($str:expr) => { regex::Regex::new($str).unwrap() }}
pub struct Matches<A>(pub A, pub regex::Regex) where A: Attribute;
impl<'a, A, T> Filter<'a> for  Matches<A> where A: OptionGetter<'a, IntoItem=String>, A: Attribute<Object=T> {
    type Item = T;
    fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
        self.0.get_opt(item_with_data).map_or(false, |e| self.1.is_match(&e))
    }
}

macro_rules! impl_collection_membership {
        ($collection_type:tt<I> where I: $($requirements:tt),+) => {
            impl<'a, A, T, I> Filter<'a> for Member<A, $collection_type<I>>
                where A: OptionGetter<'a, IntoItem=I>,
                A: Attribute<Object=T>,
                I: $($requirements+)+ {
                type Item = T;
                fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
                    self.0.get_opt(item_with_data).map_or(false, |e| self.1.contains(&e))
                }
            }
            impl<'a, A, T, I> Filter<'a> for AnyIn<A, $collection_type<I>>
                where A: OptionGetter<'a, IntoItem=Vec<I>>,
                      A: Attribute<Object=T>,
                      I: $($requirements+)+ {
                type Item = T;
                fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
                    self.0.get_opt(item_with_data).map_or(false, |vector| {
                        vector.iter().any(|e| self.1.contains(e))
                    })
                }
            }
            impl<'a, A, T, I> Filter<'a> for AllIn<A, $collection_type<I>>
                where A: OptionGetter<'a, IntoItem=Vec<I>>,
                      A: Attribute<Object=T>,
                      I: $($requirements+)+ {
                type Item = T;
                fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
                    self.0.get_opt(item_with_data).map_or(false, |vector| {
                        vector.iter().all(|e| self.1.contains(e))
                    })
                }
            }
        }
    }

pub struct Member<A, C>(pub A, pub C);
pub struct AnyIn<A, C>(pub A, pub C);
pub struct AllIn<A, C>(pub A, pub C);
impl_collection_membership!(Vec<I> where I: Eq);
impl_collection_membership!(BTreeSet<I> where I: Ord);
impl_collection_membership!(HashSet<I> where I: Hash, Eq);

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)] pub struct Top(pub usize);
impl<'a, T> Sampler<'a, T> for Top {
    fn sample<I>(&self, iter: I) -> Vec<objects::ItemWithData<'a, T>>
        where I: Iterator<Item=objects::ItemWithData<'a, T>> {

        iter.take(self.0).collect()
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)] pub struct Seed(pub u128);
impl Seed {
    pub fn to_be_bytes(&self) -> [u8; 16] { self.0.to_be_bytes() }
    pub fn to_le_bytes(&self) -> [u8; 16] { self.0.to_le_bytes() }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)] pub struct Random(pub usize, pub Seed);
impl<'a, T> Sampler<'a, T> for Random {
    fn sample<I>(&self, iter: I) -> Vec<objects::ItemWithData<'a, T>>
        where I: Iterator<Item=objects::ItemWithData<'a, T>> {

        let mut rng = Pcg64Mcg::from_seed(self.1.to_be_bytes());
        iter.choose_multiple(&mut rng, self.0)
    }
}

pub trait SimilarityCriterion<'a> {
    type Item;
    type IntoItem;
    type Similarity: Similarity<Self::IntoItem>;
    fn from(&self, object: &objects::ItemWithData<'a, Self::Item>) -> Self::Similarity;
}
pub trait Similarity<T>: Eq + Hash { }

// TODO hide
pub struct _MinRatio<T> { min_ratio: f64, items: Option<BTreeSet<T>> }
impl<T> Hash for _MinRatio<T> {
    // Everything needs to be compared explicitly.
    fn hash<H: Hasher>(&self, state: &mut H) { state.write_u64(42) }
}
impl<T> Eq for _MinRatio<T> where T: Ord {}
impl<T> PartialEq for _MinRatio<T> where T: Ord {
    fn eq(&self, other: &Self) -> bool {
        match (&self.items, &other.items) {
            (None, None) => true,
            (Some(me), Some(them)) => {
                let mine: f64 = me.len() as f64;
                let same: f64 = me.intersection(&them).count() as f64;
                same / mine > self.min_ratio
            }
            _ => false,
        }
    }
}
impl<T> Similarity<T> for _MinRatio<T> where T: Ord {}

#[derive(Debug, Clone, Copy)] pub struct MinRatio<A: Attribute>(pub A, pub f64);
impl<'a, A, T, I> SimilarityCriterion<'a> for MinRatio<A>
    where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<I>>, I: Ord {
    type Item = T;
    type IntoItem = I;
    type Similarity = _MinRatio<Self::IntoItem>;

    fn from(&self, object: &objects::ItemWithData<'a, Self::Item>) -> Self::Similarity {
        let items = self.0.get_opt(object).map(|e| {
            BTreeSet::from_iter(e.into_iter())
        });
        _MinRatio { min_ratio: self.1, items }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)] pub struct Distinct<S, C>(pub S, pub C);
impl<'a, T, S, C> Sampler<'a, T> for Distinct<S, C> where S: Sampler<'a, T>, C: SimilarityCriterion<'a, Item=T> {
    fn sample<I>(&self, iter: I) -> Vec<objects::ItemWithData<'a, T>>
        where I: Iterator<Item=objects::ItemWithData<'a, T>> {
        let filtered_iter = iter.unique_by(|object| {
            self.1.from(object)
        });
        self.0.sample(filtered_iter)
    }
}

pub struct Length<A: Attribute>(pub A);
impl<A, T> Attribute for Length<A> where A: Attribute<Object=T> {
    type Object = T;
}
impl<'a, A, T> Getter<'a> for Length<A> where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=String> {
    type IntoItem = Option<usize>;
    fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
        self.0.get_opt(object).map(|s| s.len())
    }
}
impl<'a, A, T> OptionGetter<'a> for Length<A> where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=String> {
    type IntoItem = usize;
    fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
        self.0.get_opt(object).map(|s| s.len())
    }
}

pub struct Count<A: Attribute>(pub A);
impl<A, T> Attribute for Count<A> where A: Attribute<Object=T> {
    type Object = T;
}
impl<'a, A, T> Getter<'a> for Count<A> where A: Attribute<Object=T> + OptionCountable<'a> {
    type IntoItem = usize;
    fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
        self.0.count(object).unwrap_or(0)
    }
}
impl<'a, A, T> OptionGetter<'a> for Count<A> where A: Attribute<Object=T> + OptionCountable<'a> {
    type IntoItem = usize;
    fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
        self.0.count(object)
    }
}

trait CalculateStat<N, T>{ fn calculate(vector: Vec<N>) -> T; }
macro_rules! impl_calculator {
        ($name:ident -> $result:ty where N: $($requirements:path),+; $calculate:item) => {
            pub struct $name<A: Attribute>(pub A);
            impl<A, T> Attribute for $name<A> where A: Attribute<Object=T> {
                type Object = T;
            }
            impl<'a, A, N, T> Getter<'a> for $name<A>
                where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<N>>, N: $($requirements +)+ {
                type IntoItem = Option<$result>;
                fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                    self.0.get_opt(object).map(|object| Self::calculate(object)).flatten()
                }
            }
            impl<'a, A, N, T> OptionGetter<'a> for $name<A>
                where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<N>>, N: $($requirements +)+  { //$n: $(as_item!($requirements) +)+ {
                type IntoItem = $result;
                fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                    self.0.get_opt(object).map(|object| Self::calculate(object)).flatten()
                }
            }
            impl<A, N, T> CalculateStat<N, Option<$result>> for $name<A> where A: Attribute<Object=T>, N: $($requirements +)+  {
                $calculate
            }
        }
    }

// trait Unwrap<T,I> { fn unwrap(&self) -> I; }
// impl Unwrap<usize, f64> for std::result::Result<f64, <usize as TryInto<f64>>::Err> {
//     fn unwrap(&self) -> f64 {
//         self.unwrap()
//     }
// }

//TODO min_by/max_by/minmax_by
impl_calculator!(Min -> N where N: Ord, Clone;
        fn calculate(vector: Vec<N>) -> Option<N> { vector.into_iter().min() }
    );
impl_calculator!(Max -> N where N: Ord, Clone;
        fn calculate(vector: Vec<N>) -> Option<N> { vector.into_iter().max() }
    );
impl_calculator!(MinMax -> (N, N) where N: Ord, Clone;
        fn calculate(vector: Vec<N>) -> Option<(N,N)> { vector.into_iter().minmax().into_option() }
    );
impl_calculator!(Mean -> Fraction<N> where N: Sum;
        fn calculate(vector: Vec<N>) -> Option<Fraction<N>> {
            let length = vector.len();
            let sum = vector.into_iter().sum::<N>();
            if length == 0 {
                None
            } else {
                Some(Fraction::new(sum, length))
            }
        }
    );
impl_calculator!(Median -> Fraction<N> where N: Ord, Clone, Sum;
        fn calculate(mut items: Vec<N>) -> Option<Fraction<N>> {
            items.sort();
            let length = items.len();
            if length == 0 {
                None
            } else {
                let value: Fraction<N> =
                    if length == 1 {
                        Fraction::new(items[0].clone(), 1)
                    } else if length % 2 != 0usize {
                        Fraction::new(items[length / 2].clone(), 1)
                    } else {
                        let left: N = items[(length / 2) - 1].clone();
                        let right: N = items[(length / 2)].clone();
                        Fraction::new(vec![left, right].into_iter().sum(), 2)
                    };
                Some(value)
            }
        }
    );
//
// type BinKey = Option<i64>;
// pub trait BinningFunction {
//     type From;
//     fn calculate_key(&self, value: Self::From) -> BinKey;
//     fn calculate_limits(&self, key: BinKey) -> (Self::From, Self::From);
//     fn bin(value: Self::From) -> Bin<Self> {
//         Bin { value, binning_function: PhantomData }
//     }
// }
//
// pub struct Bin<F: BinningFunction> { value: F::From, binning_function: PhantomData<F> }
// impl<F> Bin<F> where F: BinningFunction {
//     // pub fn from(value: F::From) -> Self {
//     //
//     // }
// }
//
// impl<F, T> std::convert::From<T> for Bin<F> where F: BinningFunction<From=T>, T: Div /*TODO*/ {
//     fn from(value: T) -> Self {
//         Bin { value, binning_function: PhantomData }
//     }
// }
//
// #[derive(Clone, Debug)]
// pub struct Interval(pub usize);
// impl BinningFunction for Interval {
//     type From = f64;
//     fn calculate_key(&self, value: Self::From) -> BinKey {
//
//         let bin = (value / self.0 as f64).to_i64();
//
//     }
//     fn calculate_limits(&self, key: BinKey) -> (Self::From, Self::From) {
//
//     }
// }

// impl<T, F> Display for Bin<T, F> where F: BinningFunction<T> {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         write!(f, "{}..{}", self.binning_function.minimum(), self.binning_function.maximum())
//     }
// }

// pub trait BinningFunction<T>: Clone {
//     type Into;
//     fn bin(&self, value: T) -> Bin<Self::Into, Self>;
//
//     fn minimum(&self, bin: Self::Into) -> T;
//     fn maximum(&self, bin: Self::Into) -> T;
//     // fn minimum(bin: Bin<Self::Into, Self>) -> T {
//     //     bin.binning_function.minimum_value(bin.bin)
//     // }
//     // fn maximum(bin: Bin<Self::Into, Self>) -> T {
//     //     bin.binning_function.minimum_value(bin.bin)
//     // }
// }

// macro_rules! impl_interval_binning_function {
//     ($from:tt -> $to:tt) => {
//         impl BinningFunction for Interval<usize> {
//             type From = $from;
//             type Into = $to;
//             fn bin(&self, value: Self::From) -> Bin<Self> {
//                 let binning_function = self.clone();
//                 let bin = value / self.0;
//                 Bin { value , bin, binning_function }
//             }
//         }
//     }
// }

// pub trait DivInto<T> {
//     fn div_by(&self, other: &T) -> T;
//     fn convert_into(&self) -> T;
//     fn convert_from(other: &T) -> Self;
// }
//
// impl<T> BinningFunction<T> for Interval<i64> where T: DivInto<i64> {
//     type Into = i64;
//     fn bin(&self, value: T) -> Bin<T, Self> {
//         let binning_function = self.clone();
//         let bin = value.div_by(&self.0);
//         Bin { value , bin, binning_function }
//     }
//     fn minimum(&self, bin: Self::Into) -> T {
//         T::convert_from(&(bin * self.0))
//     }
//     fn maximum(&self, bin: Self::Into) -> T {
//         T::convert_from(&(((bin + 1) * self.0) - 1))
//     }
// }

//         100
// 0 ->      0  0..99         (n) * N..( (n+1) * N )-1
// 10->      0  0..99
// 100->     1  100..199
// 101->     1  100..199
// 1000     10  1000..1099
// -1 ->     0

pub struct Bucket<A: Attribute, F>(pub A, pub F);
impl<A, F, T> Attribute for Bucket<A, F> where A: Attribute<Object=T> {
    type Object = T;
}
// impl<'a, A, F, T, I> OptionGetter<'a> for Bucket<A, F>
//     where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=I>, F: BinningFunction<I> {
//     type IntoItem = Bin<I, F>;
//     fn get_opt(&self, object: &ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
//         self.0.get_opt(object).map(|item| self.1.bin(item))
//     }
// }
// impl<'a, A, F, T, I> Getter<'a> for Bucket<A, F>
//     where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=I>, F: BinningFunction<I> {
//     type IntoItem = Option<Bin<I, F>>;
//     fn get(&self, object: &ItemWithData<'a, Self::Object>) -> Self::IntoItem {
//         self.0.get_opt(object).map(|item| self.1.bin(item))
//     }
// }

pub struct Ratio<A: Attribute<Object=T>, P: Attribute<Object=T>, T>(pub A, pub P);
impl<A, P, T> Attribute for Ratio<A, P, T>
    where A: Attribute<Object=T>,
          P: Attribute<Object=T> {

    type Object = T;
}
impl<'a, A, P, T> OptionGetter<'a> for Ratio<A, P, T>
    where A: Attribute<Object=T> + OptionCountable<'a>,
          P: Attribute<Object=T> + OptionCountable<'a> {
    type IntoItem = Fraction<usize>;
    fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
        match (self.0.count(object), self.1.count(object)) {
            (Some(n), Some(m)) => Some(Fraction::new(n, m)),
            _ => None,
        }
    }
}

impl<'a, A, P, T> Getter<'a> for Ratio<A, P, T>
    where A: Attribute<Object=T> + OptionCountable<'a>,
          P: Attribute<Object=T> + OptionCountable<'a> {
    type IntoItem = Option<Fraction<usize>>;
    fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
        match (self.0.count(object), self.1.count(object)) {
            (Some(n), Some(m)) => Some(Fraction::new(n, m)),
            _ => None,
        }
    }
}

/// Get an attribute's attribute.
pub struct From<O: Attribute, A: Attribute> (pub O, pub A);

impl<'a, O, A, T, I> Attribute for From<O, A>
    where O: Attribute<Object=T>, A: Attribute<Object=I> {
    type Object = T;
}

impl<'a, O, A, T, I, E> Getter<'a> for From<O, A>
    where O: Attribute<Object=T> + OptionGetter<'a, IntoItem=objects::ItemWithData<'a, I>>,
          A: Attribute<Object=I> + Getter<'a, IntoItem=E> {
    type IntoItem = Option<E>;
    fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
        self.0.get_opt(object).map(|object| self.1.get(&object))
    }
}

impl<'a, O, A, T, I, E> OptionGetter<'a> for From<O, A>
    where O: Attribute<Object=T> + OptionGetter<'a, IntoItem=objects::ItemWithData<'a, I>>,
          A: Attribute<Object=I> + OptionGetter<'a, IntoItem=E> {
    type IntoItem = E;
    fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
        self.0.get_opt(object).map(|object| self.1.get_opt(&object)).flatten()
    }
}

/// Get an attribute from each of a sequence of attributes.
pub struct FromEach<O: Attribute, A: Attribute> (pub O, pub A);

impl<'a, O, A, T> Attribute for FromEach<O, A>
    where O: Attribute<Object=T> /*+ OptionGetter<'a, IntoItem=Vec<I>>)*/, A: Attribute {
    //<Object=I>*/ {
    type Object = T;
}

impl<'a, O, A, T, I, E> Getter<'a> for FromEach<O, A>
    where O: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<objects::ItemWithData<'a, I>>>,
          A: Attribute<Object=I> + Getter<'a, IntoItem=E> {
    type IntoItem = Option<Vec<E>>;
    fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
        self.0.get_opt(object).map(|v| {
            v.iter().map(|object| { self.1.get(object) }).collect()
        })
    }
}

impl<'a, O, A, T, I, E> OptionGetter<'a> for FromEach<O, A>
    where O: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<objects::ItemWithData<'a, I>>>,
          A: Attribute<Object=I> + OptionGetter<'a, IntoItem=E> {
    type IntoItem = Vec<E>;
    fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
        self.0.get_opt(object).map(|v| {
            v.iter().flat_map(|object| { self.1.get_opt(object) }).collect()
        })
    }
}

// Get an attribute from each of a sequence of attributes buy only if a specific condition was met.
pub struct FromEachIf<A: Attribute, P> (pub A, pub P);

impl<'a, A, P, T> Attribute for FromEachIf<A, P>
    where A: Attribute<Object=T> {
    type Object = T;
}

impl<'a, A, P, T, I> OptionGetter<'a> for FromEachIf<A, P>
    where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<objects::ItemWithData<'a, I>>>,
          P: Filter<'a, Item=I> {
    type IntoItem = Vec<objects::ItemWithData<'a, I>>;
    fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
        self.0.get_opt(object).map(|items| {
            items.into_iter()
                .filter(|item| self.1.accept(item))
                .collect()
        })
    }
}

impl<'a, A, P, T, I> Getter<'a> for FromEachIf<A, P>
    where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<objects::ItemWithData<'a, I>>>,
          P: Filter<'a, Item=I> {
    type IntoItem = Option<Vec<objects::ItemWithData<'a, I>>>;
    fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
        self.0.get_opt(object).map(|items| {
            items.into_iter()
                .filter(|item| self.1.accept(item))
                .collect()
        })
    }
}

impl<'a, A, P, T, I> Countable<'a> for FromEachIf<A, P>
    where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<objects::ItemWithData<'a, I>>>,
          P: Filter<'a, Item=I> {
    fn count(&self, object: &objects::ItemWithData<'a, Self::Object>) -> usize {
        self.get_opt(object).map_or(0, |vector| vector.len())
        // Could potentially count straight from iter, but would have to reimplement all of
        // get_opt. It would save allocating the vector.
    }
}

impl<'a, A, P, T, I> OptionCountable<'a> for FromEachIf<A, P>
    where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<objects::ItemWithData<'a, I>>>,
          P: Filter<'a, Item=I> {
    fn count(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<usize> {
        self.get_opt(object).map(|vector| vector.len())
    }
}

macro_rules! impl_select {
        ($n:ident, $($ti:ident -> $i:tt),+) => {
            pub struct $n<$($ti: Attribute,)+> ($(pub $ti,)+);
            impl<T, $($ti,)+> Attribute for $n<$($ti,)+>
                where $($ti: Attribute<Object=T>,)+ {
                type Object = T;
            }
            impl<'a, T, $($ti,)+> OptionGetter<'a> for $n<$($ti,)+>
                where $($ti: Attribute<Object=T> + OptionGetter<'a>,)+ {
                type IntoItem = ($(Option<$ti::IntoItem>,)+);
                fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                    Some(($(self.$i.get_opt(object),)+))
                }
            }
            impl<'a, T, $($ti,)+> Getter<'a> for $n<$($ti,)+>
                where $($ti: Attribute<Object=T> + Getter<'a>,)+ {
                type IntoItem = ($($ti::IntoItem,)+);

                fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                    ($(self.$i.get(object),)+)
                }
            }
        }
    }

impl_select!(Select1,  Ta -> 0);
impl_select!(Select2,  Ta -> 0, Tb -> 1);
impl_select!(Select3,  Ta -> 0, Tb -> 1, Tc -> 2);
impl_select!(Select4,  Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3);
impl_select!(Select5,  Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4);
impl_select!(Select6,  Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4, Tf -> 5);
impl_select!(Select7,  Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4, Tf -> 5, Tg -> 6);
impl_select!(Select8,  Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4, Tf -> 5, Tg -> 6, Th -> 7);
impl_select!(Select9,  Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4, Tf -> 5, Tg -> 6, Th -> 7, Ti -> 8);
impl_select!(Select10, Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4, Tf -> 5, Tg -> 6, Th -> 7, Ti -> 8, Tj -> 9);

#[macro_export]
macro_rules! Select {
    ($ta:expr) => {
        Select1($ta)
    };
    ($ta:expr, $tb:expr) => {
        Select2($ta, $tb)
    };
    ($ta:expr, $tb:expr, $tc:expr) => {
        Select3($ta, $tb, $tc)
    };
    ($ta:expr, $tb:expr, $tc:expr, $td:expr) => {
        Select4($ta, $tb, $tc, $td)
    };
    ($ta:expr, $tb:expr, $tc:expr, $td:expr, $te:expr) => {
        Select5($ta, $tb, $tc, $td, $te)
    };
    ($ta:expr, $tb:expr, $tc:expr, $td:expr, $te:expr, $tf:expr) => {
        Select6($ta, $tb, $tc, $td, $te, $tf)
    };
    ($ta:expr, $tb:expr, $tc:expr, $td:expr, $te:expr, $tf:expr, $tg:expr) => {
        Select7($ta, $tb, $tc, $td, $te, $tf, $tg)
    };
    ($ta:expr, $tb:expr, $tc:expr, $td:expr, $te:expr, $tf:expr, $tg:expr, $th:expr) => {
        Select8($ta, $tb, $tc, $td, $te, $tf, $tg, $th)
    };
    ($ta:expr, $tb:expr, $tc:expr, $td:expr, $te:expr, $tf:expr, $tg:expr, $th:expr, $ti:expr) => {
        Select9($ta, $tb, $tc, $td, $te, $tf, $tg, $th, $ti)
    };
    ($ta:expr, $tb:expr, $tc:expr, $td:expr, $te:expr, $tf:expr, $tg:expr, $th:expr, $ti:expr, $tj:expr) => {
        Select10($ta, $tb, $tc, $td, $te, $tf, $tg, $th, $ti, $tj)
    };
}

#[cfg(test)]
mod testing {
    use dcd::DatastoreView;

    use crate::time;
    use crate::log::*;
    use crate::data::*;
    use std::collections::*;
    use std::iter::*;
    use crate::objects::*;
    use crate::project::License;
    use chrono::DateTime;

    fn database() -> Database {
        let now = time::Month::December(2020);
        let log = Log::new(Verbosity::Debug);
        let store = DatastoreView::new("/dejacode/testing/10b", now.into());
        let database =  Database::from_store(store, "/dejacode/testing/10b", log);

        database
    }

    macro_rules! check_project_attrib_value {
        ($method:ident -> $type:ty, $converter:expr, $($values:expr),+) => {{
            let database = database();

            let expected: BTreeMap<ProjectId, $type> = BTreeMap::from_iter(vec![
                $($values,)+
            ].into_iter().map(|(i, e) | (ProjectId::from(i as usize), $converter(e))));

            let actual: BTreeMap<ProjectId, $type> =
                BTreeMap::from_iter(database.projects().map(|p| (p.id(), p.$method())));

            assert_eq!(expected, actual);
        }}
    }

    #[test] fn project_urls () {
        check_project_attrib_value!(url -> String, |e: &str| e.to_owned(),
            (0, "https://github.com/nodejs/node.git"),
            (1, "https://github.com/pixijs/pixi.js.git"),
            (2, "https://github.com/angular/angular.git"),
            (3, "https://github.com/apache/airflow.git"),
            (4, "https://github.com/facebook/react.git"),
            (5, "https://github.com/vuejs/vue.git"),
            (6, "https://github.com/xonsh/xonsh.git"),
            (7, "https://github.com/meteor/meteor.git"),
            (8, "https://github.com/3b1b/manim.git"),
            (9, "https://github.com/s0md3v/photon.git")
        );
    }

    #[test] fn project_languages() {
        check_project_attrib_value!(language -> Option<Language>, |e: Language| Some(e),
            (0, Language::JavaScript), //https://github.com/nodejs/node.git
            (1, Language::TypeScript), //https://github.com/pixijs/pixi.js.git
            (2, Language::TypeScript), //https://github.com/angular/angular.git
            (3, Language::Python),     //https://github.com/apache/airflow.git
            (4, Language::JavaScript), //https://github.com/facebook/react.git
            (5, Language::JavaScript), //https://github.com/vuejs/vue.git
            (6, Language::Python),     //https://github.com/xonsh/xonsh.git
            (7, Language::JavaScript), //https://github.com/meteor/meteor.git
            (8, Language::Python),     //https://github.com/3b1b/manim.git
            (9, Language::Python)      //https://github.com/s0md3v/photon.git
        );
    }

    #[test] fn project_stars() {
        check_project_attrib_value!(star_count -> Option<usize>, |e: usize| Some(e),
            (0, 75415),     //https://github.com/nodejs/node.git
            (1, 31403),     //https://github.com/pixijs/pixi.js.git
            (2, 68799),     //https://github.com/angular/angular.git
            (3, 19612),     //https://github.com/apache/airflow.git
            (4, 160740),    //https://github.com/facebook/react.git
            (5, 176839),    //https://github.com/vuejs/vue.git
            (6, 4117),      //https://github.com/xonsh/xonsh.git
            (7, 42118),     //https://github.com/meteor/meteor.git
            (8, 29000),     //https://github.com/3b1b/manim.git
            (9, 7398)      //https://github.com/s0md3v/photon.git
        );
    }

    #[test] fn project_watchers() {
        check_project_attrib_value!(watcher_count -> Option<usize>, |e: usize| Some(e),
            (0, 75415),     //https://github.com/nodejs/node.git
            (1, 31403),     //https://github.com/pixijs/pixi.js.git
            (2, 68799),     //https://github.com/angular/angular.git
            (3, 19612),     //https://github.com/apache/airflow.git
            (4, 160740),    //https://github.com/facebook/react.git
            (5, 176839),    //https://github.com/vuejs/vue.git
            (6, 4117),      //https://github.com/xonsh/xonsh.git
            (7, 42118),     //https://github.com/meteor/meteor.git
            (8, 29000),     //https://github.com/3b1b/manim.git
            (9, 7398)       //https://github.com/s0md3v/photon.git
        );
    }

    #[test] fn project_subscribers() {
        check_project_attrib_value!(subscriber_count -> Option<usize>, |e: usize| Some(e),
            (0, 2985),    //https://github.com/nodejs/node.git
            (1, 1056),    //https://github.com/pixijs/pixi.js.git
            (2, 3212),    //https://github.com/angular/angular.git
            (3, 737),     //https://github.com/apache/airflow.git
            (4, 6749),    //https://github.com/facebook/react.git
            (5, 6350),    //https://github.com/vuejs/vue.git
            (6, 99),      //https://github.com/xonsh/xonsh.git
            (7, 1715),    //https://github.com/meteor/meteor.git
            (8, 791),     //https://github.com/3b1b/manim.git
            (9, 291)      //https://github.com/s0md3v/photon.git
        );
    }

    #[test] fn project_forks() {
        check_project_attrib_value!(fork_count -> Option<usize>, |e: usize| Some(e),
            (0, 18814),    //https://github.com/nodejs/node.git
            (1, 4218),     //https://github.com/pixijs/pixi.js.git
            (2, 18195),    //https://github.com/angular/angular.git
            (3, 7630),     //https://github.com/apache/airflow.git
            (4, 32006),    //https://github.com/facebook/react.git
            (5, 27556),    //https://github.com/vuejs/vue.git
            (6, 446),      //https://github.com/xonsh/xonsh.git
            (7, 5154),     //https://github.com/meteor/meteor.git
            (8, 3699),     //https://github.com/3b1b/manim.git
            (9, 1029)      //https://github.com/s0md3v/photon.git
        );
    }

    #[test] fn project_open_issues() {
        check_project_attrib_value!(open_issue_count -> Option<usize>, |e: usize| Some(e),
            (0, 1237),    //https://github.com/nodejs/node.git
            (1, 66),      //https://github.com/pixijs/pixi.js.git
            (2, 2838),    //https://github.com/angular/angular.git
            (3, 923),     //https://github.com/apache/airflow.git
            (4, 682),     //https://github.com/facebook/react.git
            (5, 551),     //https://github.com/vuejs/vue.git
            (6, 418),     //https://github.com/xonsh/xonsh.git
            (7, 131),     //https://github.com/meteor/meteor.git
            (8, 331),     //https://github.com/3b1b/manim.git
            (9, 36)       //https://github.com/s0md3v/photon.git
        );
    }

    #[test] fn project_sizes() {
        check_project_attrib_value!(size -> Option<usize>, |e: usize| Some(e),
            (0, 639607),    //https://github.com/nodejs/node.git
            (1, 72165),     //https://github.com/pixijs/pixi.js.git
            (2, 271123),    //https://github.com/angular/angular.git
            (3, 97275),     //https://github.com/apache/airflow.git
            (4, 161343),    //https://github.com/facebook/react.git
            (5, 27591),     //https://github.com/vuejs/vue.git
            (6, 23304),     //https://github.com/xonsh/xonsh.git
            (7, 80487),     //https://github.com/meteor/meteor.git
            (8, 17759),     //https://github.com/3b1b/manim.git
            (9, 356)        //https://github.com/s0md3v/photon.git
        );
    }

    #[test] fn project_is_fork() {
        check_project_attrib_value!(is_fork -> Option<bool>, |e: bool| Some(e),
            (0, false),     //https://github.com/nodejs/node.git
            (1, false),     //https://github.com/pixijs/pixi.js.git
            (2, false),     //https://github.com/angular/angular.git
            (3, false),     //https://github.com/apache/airflow.git
            (4, false),     //https://github.com/facebook/react.git
            (5, false),     //https://github.com/vuejs/vue.git
            (6, false),     //https://github.com/xonsh/xonsh.git
            (7, false),     //https://github.com/meteor/meteor.git
            (8, false),     //https://github.com/3b1b/manim.git
            (9, false)      //https://github.com/s0md3v/photon.git
        );
    }

    #[test] fn project_is_archived() {
        check_project_attrib_value!(is_archived -> Option<bool>, |e: bool| Some(e),
            (0, false),     //https://github.com/nodejs/node.git
            (1, false),     //https://github.com/pixijs/pixi.js.git
            (2, false),     //https://github.com/angular/angular.git
            (3, false),     //https://github.com/apache/airflow.git
            (4, false),     //https://github.com/facebook/react.git
            (5, false),     //https://github.com/vuejs/vue.git
            (6, false),     //https://github.com/xonsh/xonsh.git
            (7, false),     //https://github.com/meteor/meteor.git
            (8, false),     //https://github.com/3b1b/manim.git
            (9, false)      //https://github.com/s0md3v/photon.git
        );
    }

    #[test] fn project_is_disabled() {
        check_project_attrib_value!(is_disabled -> Option<bool>, |e: bool| Some(e),
            (0, false),     //https://github.com/nodejs/node.git
            (1, false),     //https://github.com/pixijs/pixi.js.git
            (2, false),     //https://github.com/angular/angular.git
            (3, false),     //https://github.com/apache/airflow.git
            (4, false),     //https://github.com/facebook/react.git
            (5, false),     //https://github.com/vuejs/vue.git
            (6, false),     //https://github.com/xonsh/xonsh.git
            (7, false),     //https://github.com/meteor/meteor.git
            (8, false),     //https://github.com/3b1b/manim.git
            (9, false)      //https://github.com/s0md3v/photon.git
        );
    }

    #[test] fn project_has_pages() {
        check_project_attrib_value!(has_pages -> Option<bool>, |e: bool| Some(e),
            (0, false),     //https://github.com/nodejs/node.git
            (1, false),     //https://github.com/pixijs/pixi.js.git
            (2, false),     //https://github.com/angular/angular.git
            (3, false),     //https://github.com/apache/airflow.git
            (4, true),      //https://github.com/facebook/react.git
            (5, false),     //https://github.com/vuejs/vue.git
            (6, false),     //https://github.com/xonsh/xonsh.git
            (7, false),     //https://github.com/meteor/meteor.git
            (8, false),     //https://github.com/3b1b/manim.git
            (9, false)      //https://github.com/s0md3v/photon.git
        );
    }

    #[test] fn project_has_downloads() {
        check_project_attrib_value!(has_downloads -> Option<bool>, |e: bool| Some(e),
            (0, true),     //https://github.com/nodejs/node.git
            (1, true),     //https://github.com/pixijs/pixi.js.git
            (2, true),     //https://github.com/angular/angular.git
            (3, true),     //https://github.com/apache/airflow.git
            (4, true),      //https://github.com/facebook/react.git
            (5, true),     //https://github.com/vuejs/vue.git
            (6, true),     //https://github.com/xonsh/xonsh.git
            (7, true),     //https://github.com/meteor/meteor.git
            (8, true),     //https://github.com/3b1b/manim.git
            (9, true)      //https://github.com/s0md3v/photon.git
        );
    }

    #[test] fn project_has_wiki() {
        check_project_attrib_value!(has_wiki -> Option<bool>, |e: bool| Some(e),
            (0, false),     //https://github.com/nodejs/node.git
            (1, true),      //https://github.com/pixijs/pixi.js.git
            (2, false),     //https://github.com/angular/angular.git
            (3, false),     //https://github.com/apache/airflow.git
            (4, true),      //https://github.com/facebook/react.git
            (5, true),      //https://github.com/vuejs/vue.git
            (6, true),      //https://github.com/xonsh/xonsh.git
            (7, true),      //https://github.com/meteor/meteor.git
            (8, true),      //https://github.com/3b1b/manim.git
            (9, true)       //https://github.com/s0md3v/photon.git
        );
    }

    #[test] fn project_has_issues() {
        check_project_attrib_value!(has_issues -> Option<bool>, |e: bool| Some(e),
            (0, true),     //https://github.com/nodejs/node.git
            (1, true),     //https://github.com/pixijs/pixi.js.git
            (2, true),     //https://github.com/angular/angular.git
            (3, true),     //https://github.com/apache/airflow.git
            (4, true),     //https://github.com/facebook/react.git
            (5, true),     //https://github.com/vuejs/vue.git
            (6, true),     //https://github.com/xonsh/xonsh.git
            (7, true),     //https://github.com/meteor/meteor.git
            (8, true),     //https://github.com/3b1b/manim.git
            (9, true)      //https://github.com/s0md3v/photon.git
        );
    }

    #[test] fn project_default_branch() {
        check_project_attrib_value!(default_branch -> Option<String>, |e: &str| Some(e.to_owned()),
            (0, "master"),     //https://github.com/nodejs/node.git
            (1, "dev"),        //https://github.com/pixijs/pixi.js.git
            (2, "master"),     //https://github.com/angular/angular.git
            (3, "master"),     //https://github.com/apache/airflow.git
            (4, "master"),     //https://github.com/facebook/react.git
            (5, "dev"),        //https://github.com/vuejs/vue.git
            (6, "master"),     //https://github.com/xonsh/xonsh.git
            (7, "devel"),      //https://github.com/meteor/meteor.git
            (8, "master"),     //https://github.com/3b1b/manim.git
            (9, "master")      //https://github.com/s0md3v/photon.git
        );
    }

    #[test] fn project_license() {
        check_project_attrib_value!(license -> Option<String>, |e: &str| Some(e.to_owned()),
            (0, "Other"),                               //https://github.com/nodejs/node.git
            (1, "MIT License"),                         //https://github.com/pixijs/pixi.js.git
            (2, "MIT License"),                         //https://github.com/angular/angular.git
            (3, "Apache License 2.0"),                  //https://github.com/apache/airflow.git
            (4, "MIT License"),                         //https://github.com/facebook/react.git
            (5, "MIT License"),                         //https://github.com/vuejs/vue.git
            (6, "Other"),                               //https://github.com/xonsh/xonsh.git
            (7, "Other"),                               //https://github.com/meteor/meteor.git
            (8, "Other"),                               //https://github.com/3b1b/manim.git
            (9, "GNU General Public License v3.0")      //https://github.com/s0md3v/photon.git
        );
    }

    #[test] fn project_description() {
        check_project_attrib_value!(description -> Option<String>, |e: &str| Some(e.to_owned()),
            (0, "Node.js JavaScript runtime :sparkles::turtle::rocket::sparkles:"),
            (1, "The HTML5 Creation Engine: Create beautiful digital content with the fastest, most flexible 2D WebGL renderer."),
            (2, "One framework. Mobile & desktop."),
            (3, "Apache Airflow - A platform to programmatically author, schedule, and monitor workflows"),
            (4, "A declarative, efficient, and flexible JavaScript library for building user interfaces."),
            (5, "🖖 Vue.js is a progressive, incrementally-adoptable JavaScript framework for building UI on the web."),
            (6, ":shell: Python-powered, cross-platform, Unix-gazing shell"),
            (7, "Meteor, the JavaScript App Platform"),
            (8, "Animation engine for explanatory math videos"),
            (9, "Incredibly fast crawler designed for OSINT.")
        );
    }

    #[test] fn project_homepage() {
        check_project_attrib_value!(homepage -> Option<String>, |e: Option<&str>| e.map(|e: &str| e.to_owned()),
            (0, Some("https://nodejs.org/")),
            (1, Some("http://pixijs.com")),
            (2, Some("https://angular.io")),
            (3, Some("https://airflow.apache.org/")),
            (4, Some("https://reactjs.org")),
            (5, Some("http://vuejs.org")),
            (6, Some("http://xon.sh")),
            (7, Some("https://www.meteor.com")),
            (8, None),
            (9, Some(""))
        );
    }

    #[test] fn project_created() {
        check_project_attrib_value!(created -> Option<i64>, |e: &str| Some(DateTime::parse_from_rfc3339(e).unwrap().timestamp()),
            (8, "2015-03-22T18:50:58Z"),
            (6, "2015-01-21T22:05:27Z"),
            (1, "2013-01-21T22:40:50Z"),
            (9, "2018-03-30T19:38:22Z"),
            (3, "2015-04-13T18:04:58Z"),
            (4, "2013-05-24T16:15:54Z"),
            (2, "2014-09-18T16:12:01Z"),
            (7, "2012-01-19T01:58:17Z"),
            (0, "2014-11-26T19:57:11Z"),
            (5, "2013-07-29T03:24:51Z")
        );
    }

    #[test] fn project_updated() {
        check_project_attrib_value!(updated -> Option<i64>, |e: &str| Some(DateTime::parse_from_rfc3339(e).unwrap().timestamp()),
            (8, "2020-12-17T17:12:22Z"),
            (6, "2020-12-17T16:53:36Z"),
            (1, "2020-12-17T17:20:36Z"),
            (9, "2020-12-17T15:01:36Z"),
            (3, "2020-12-17T15:01:21Z"),
            (4, "2020-12-17T17:17:19Z"),
            (2, "2020-12-17T17:00:56Z"),
            (7, "2020-12-17T11:08:27Z"),
            (0, "2020-12-17T17:12:19Z"),
            (5, "2020-12-17T17:17:25Z")
        );
    }

    #[test] fn project_pushed() {
        check_project_attrib_value!(pushed -> Option<i64>, |e: &str| Some(DateTime::parse_from_rfc3339(e).unwrap().timestamp()),
            (8, "2020-12-04T16:26:20Z"),
            (6, "2020-12-17T16:19:16Z"),
            (1, "2020-12-17T17:10:44Z"),
            (9, "2020-10-28T14:01:02Z"),
            (3, "2020-12-17T17:19:24Z"),
            (4, "2020-12-17T17:05:15Z"),
            (2, "2020-12-17T17:01:19Z"),
            (7, "2020-12-11T12:26:58Z"),
            (0, "2020-12-17T16:03:55Z"),
            (5, "2020-12-17T13:27:21Z")
        );
    }

    #[test] fn project_head_count() {
        check_project_attrib_value!(head_count -> Option<usize>, |e: usize| Some(e),
            (0, 33),
            (1, 29),
            (2, 57),
            (3, 15),
            (4, 102),
            (5, 61),
            (6, 42),
            (7, 1110),
            (8, 16),
            (9, 2)
        );
    }

    // #[test] fn project_heads() {
    //     check_project_attrib_value!(head_count -> Option<Vec<Head>>, |e: usize| Some(e),
    //         (0, vec![]),
    //         (1, vec![]),
    //         (2, vec![]),
    //         (3, vec![]),
    //         (4, vec![]),
    //         (5, vec![]),
    //         (6, vec![]),
    //         (7, vec![]),
    //         (8, vec![]),
    //         (9, vec!["master", "2.0-beta"])
    //     );
    // }
}