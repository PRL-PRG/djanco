use crate::attrib::*;
use crate::objects;
use crate::objects::Duration;
use crate::iterators::ItemWithData;

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
            fn get(&self, object: &ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                object.item.clone()
            }
        }
        impl<'a> OptionGetter<'a> for $attribute {
            type IntoItem = Self::Object;
            fn get_opt(&self, object: &ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                Some(object.item.clone())
            }
        }
    };
    [!+ $object:ty, $attribute:ident] => {
        impl<'a> Getter<'a> for $attribute {
            type IntoItem = ItemWithData<'a, Self::Object>;
            fn get(&self, object: &ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                object.clone()
            }
        }
        impl<'a> OptionGetter<'a> for $attribute {
            type IntoItem = ItemWithData<'a, Self::Object>;
            fn get_opt(&self, object: &ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                Some(object.clone())
            }
        }
    };
    [! $object:ty, $attribute:ident, $small_type:ty, $getter:ident] => {
        impl<'a> Getter<'a> for $attribute {
            type IntoItem = $small_type;
            fn get(&self, object: &ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                object.$getter()
            }
        }
        impl<'a> OptionGetter<'a> for $attribute {
            type IntoItem = $small_type;
            fn get_opt(&self, object: &ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                Some(object.$getter())
            }
        }
    };
    [? $object:ty, $attribute:ident, $small_type:ty, $getter:ident] => {
        impl<'a> Getter<'a> for $attribute {
            type IntoItem = Option<$small_type>;
            fn get(&self, object: &ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                object.$getter()
            }
        }
        impl<'a> OptionGetter<'a> for $attribute {
            type IntoItem = $small_type;
            fn get_opt(&self, object: &ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                object.$getter()
            }
        }
    };
    [!+ $object:ty, $attribute:ident, $small_type:ty, $getter:ident] => {
        impl<'a> Getter<'a> for $attribute {
            type IntoItem = ItemWithData<'a, $small_type>;
            fn get(&self, object: &ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                object.$getter()
            }
        }
        impl<'a> OptionGetter<'a> for $attribute {
            type IntoItem = ItemWithData<'a, $small_type>;
            fn get_opt(&self, object: &ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                Some(object.$getter())
            }
        }
    };
    [?+ $object:ty, $attribute:ident, $small_type:ty, $getter:ident] => {
        impl<'a> Getter<'a> for $attribute {
            type IntoItem = Option<ItemWithData<'a, $small_type>>;
            fn get(&self, object: &ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                object.$getter()
            }
        }
        impl<'a> OptionGetter<'a> for $attribute {
            type IntoItem = ItemWithData<'a, $small_type>;
            fn get_opt(&self, object: &ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                object.$getter()
            }
        }
    };
    [!+.. $object:ty, $attribute:ident, $small_type:ty, $getter:ident] => {
        impl<'a> Getter<'a> for $attribute {
            type IntoItem = Vec<ItemWithData<'a, $small_type>>;
            fn get(&self, object: &ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                object.$getter()
            }
        }
        impl<'a> OptionGetter<'a> for $attribute {
            type IntoItem = Vec<ItemWithData<'a, $small_type>>;
            fn get_opt(&self, object: &ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                Some(object.$getter())
            }
        }
    };
    [?+.. $object:ty, $attribute:ident, $small_type:ty, $getter:ident] => {
        impl<'a> Getter<'a> for $attribute {
            type IntoItem = Option<Vec<ItemWithData<'a, $small_type>>>;
            fn get(&self, object: &ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                object.$getter()
            }
        }
        impl<'a> OptionGetter<'a> for $attribute {
            type IntoItem = Vec<ItemWithData<'a, $small_type>>;
            fn get_opt(&self, object: &ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                object.$getter()
            }
        }
    };
}

macro_rules! impl_attribute_count {
    [! $object:ty, $attribute:ident, $counter:ident] => {
        impl<'a> Countable<'a> for $attribute {
            fn count(&self, object: &ItemWithData<'a, Self::Object>) -> usize {
                object.$counter()
            }
        }
        impl<'a> OptionCountable<'a> for $attribute {
            fn count(&self, object: &ItemWithData<'a, Self::Object>) -> Option<usize> {
                Some(object.$counter())
            }
        }
    };
    [? $object:ty, $attribute:ident, $counter:ident] => {
        impl<'a> OptionCountable<'a> for $attribute {
            fn count(&self, object: &ItemWithData<'a, Self::Object>) -> Option<usize> {
                object.$counter()
            }
        }
    }
}

macro_rules! impl_attribute_filter {
    [$object:ty, $attribute:ident] => {
        impl<'a> Filter<'a> for $attribute {
            type Item = $object;
            fn accept(&self, item_with_data: &ItemWithData<'a, Self::Item>) -> bool {
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
    use crate::query::*;
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
    impl_attribute![?     objects::Project, Age, Duration, lifetime];
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
    use crate::query::*;
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
    use crate::query::*;
    impl_attribute![!+  objects::Head, Itself];
    impl_attribute![!   objects::Head, Raw];
    impl_attribute![!   objects::Head, Name, String, name];
    impl_attribute![!   objects::Head, CommitId, objects::CommitId, commit_id];
    impl_attribute![?+  objects::Head, Commit, objects::Commit, commit_with_data];
}

pub mod change {
    use crate::query::*;
    impl_attribute![!+  objects::Change, Itself];
    impl_attribute![!   objects::Change, Raw];
    impl_attribute![!   objects::Change, PathId, objects::PathId, path_id];
    impl_attribute![?   objects::Change, SnapshotId, objects::SnapshotId, snapshot_id];
    impl_attribute![?+  objects::Change, Path, objects::Path, path_with_data];
    impl_attribute![?+  objects::Change, Snapshot, objects::Snapshot, snapshot_with_data];
}

pub mod user {
    use crate::query::*;
    impl_attribute![!+   objects::User, Itself];
    impl_attribute![!    objects::User, Raw];
    impl_attribute![!    objects::User, Id, objects::UserId, id];
    impl_attribute![!    objects::User, Email, String, email];
    impl_attribute![?    objects::User, AuthorExperience, Duration, author_experience];
    impl_attribute![?    objects::User, CommitterExperience, Duration, committer_experience];
    impl_attribute![?    objects::User, Experience, Duration, experience];
    impl_attribute![?..  objects::User, AuthoredCommitIds, objects::CommitId, authored_commit_ids, authored_commit_count];
    impl_attribute![?..  objects::User, CommittedCommitIds, objects::CommitId, committed_commit_ids, committed_commit_count];
    impl_attribute![?+.. objects::User, AuthoredCommits, objects::Commit, authored_commits_with_data, authored_commit_count];
    impl_attribute![?+.. objects::User, CommittedCommits, objects::Commit, committed_commits_with_data, committed_commit_count];
}

pub mod path {
    use crate::query::*;
    impl_attribute![!+  objects::Path, Itself];
    impl_attribute![!   objects::Path, Raw];
    impl_attribute![!   objects::Path, Id, objects::PathId, id];
    impl_attribute![!   objects::Path, Location, String, location];
    impl_attribute![?   objects::Path, Language, objects::Language, language];
}

pub mod snapshot {
    use crate::query::*;
    impl_attribute![!+  objects::Snapshot, Itself];
    impl_attribute![!   objects::Snapshot, Raw];
    impl_attribute![!   objects::Snapshot, Id, objects::SnapshotId, id];
    impl_attribute![!   objects::Snapshot, Bytes, Vec<u8>, raw_contents_owned];
    impl_attribute![!   objects::Snapshot, Contents, String, contents_owned];
}

pub mod require {
    use crate::query::*;
    use crate::iterators::ItemWithData;
    use std::collections::*;
    use std::hash::Hash;

    // pub struct True;
    // impl<'a, T> Filter<'a> for True {
    //     type Item = T;
    //     fn accept(&self, _: &ItemWithData<'a, Self::Item>) -> bool { true }
    // }
    //
    // pub struct False;
    // impl<'a, T> Filter<'a> for True {
    //     type Item = T;
    //     fn accept(&self, _: &ItemWithData<'a, Self::Item>) -> bool { false }
    // }

    macro_rules! impl_comparison {
        ($name:ident, $trait_limit:ident, $comparator:ident, $default:expr) => {
            pub struct $name<A, N>(pub A, pub N) where A: Attribute; // + OptionGetter<'a, IntoItem=N>;
            impl<'a, A, N, T> Filter<'a> for $name<A, N> where A: OptionGetter<'a, IntoItem=N> + Attribute<Object=T>, N: $trait_limit {
                type Item = T;
                fn accept(&self, item_with_data: &ItemWithData<'a, Self::Item>) -> bool {
                    self.0.get_opt(item_with_data).map_or($default, |n| n.$comparator(&self.1))
                }
            }
        }
    }
                                               /* vs None */
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
                fn accept(&self, item_with_data: &ItemWithData<'a, Self::Item>) -> bool {
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
                fn accept(&self, item_with_data: &ItemWithData<'a, Self::Item>) -> bool {
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
                fn accept(&self, item_with_data: &ItemWithData<'a, Self::Item>) -> bool {
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
        fn accept(&self, item_with_data: &ItemWithData<'a, Self::Item>) -> bool {
            self.0.get_opt(item_with_data).map_or(false, |e| e.as_str() == self.1)
        }
    }

    pub struct Contains<'a, A>(pub A, pub &'a str) where A: OptionGetter<'a>;
    impl<'a, A, T> Filter<'a> for Contains<'a, A> where A: OptionGetter<'a, IntoItem=String>, A: Attribute<Object=T> {
        type Item = T;
        fn accept(&self, item_with_data: &ItemWithData<'a, Self::Item>) -> bool {
            self.0.get_opt(item_with_data).map_or(false, |e| e.contains(self.1))
        }
    }

    #[macro_export] macro_rules! regex { ($str:expr) => { regex::Regex::new($str).unwrap() }}
    pub struct Matches<A>(pub A, pub regex::Regex) where A: Attribute;
    impl<'a, A, T> Filter<'a> for  Matches<A> where A: OptionGetter<'a, IntoItem=String>, A: Attribute<Object=T> {
        type Item = T;
        fn accept(&self, item_with_data: &ItemWithData<'a, Self::Item>) -> bool {
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
                fn accept(&self, item_with_data: &ItemWithData<'a, Self::Item>) -> bool {
                    self.0.get_opt(item_with_data).map_or(false, |e| self.1.contains(&e))
                }
            }
            impl<'a, A, T, I> Filter<'a> for AnyIn<A, $collection_type<I>>
                where A: OptionGetter<'a, IntoItem=Vec<I>>,
                      A: Attribute<Object=T>,
                      I: $($requirements+)+ {
                type Item = T;
                fn accept(&self, item_with_data: &ItemWithData<'a, Self::Item>) -> bool {
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
                fn accept(&self, item_with_data: &ItemWithData<'a, Self::Item>) -> bool {
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
}

pub mod sample {
    use crate::attrib::{Sampler, Attribute, OptionGetter};
    use crate::iterators::ItemWithData;
    use rand_pcg::Pcg64Mcg;
    use rand::SeedableRng;
    use rand::seq::IteratorRandom;
    use std::hash::{Hash, Hasher};
    use std::collections::BTreeSet;
    use std::iter::FromIterator;
    use itertools::Itertools;
    // #[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)] pub struct Distinct<S, C>(pub S, pub C);


    #[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)] pub struct Top(pub usize);
    impl<'a, T> Sampler<'a, T> for Top {
        fn sample<I>(&self, iter: I) -> Vec<ItemWithData<'a, T>>
            where I: Iterator<Item=ItemWithData<'a, T>> {

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
        fn sample<I>(&self, iter: I) -> Vec<ItemWithData<'a, T>>
            where I: Iterator<Item=ItemWithData<'a, T>> {

            let mut rng = Pcg64Mcg::from_seed(self.1.to_be_bytes());
            iter.choose_multiple(&mut rng, self.0)
        }
    }

    pub trait SimilarityCriterion<'a> {
        type Item;
        type IntoItem;
        type Similarity: Similarity<Self::IntoItem>;
        fn from(&self, object: &ItemWithData<'a, Self::Item>) -> Self::Similarity;
    }
    pub trait Similarity<T>: Eq + Hash { }

    pub struct MinRatio<T> { min_ratio: f64, items: Option<BTreeSet<T>> }
    impl<T> Hash for MinRatio<T> {
        // Everything needs to be compared explicitly.
        fn hash<H: Hasher>(&self, state: &mut H) { state.write_u64(42) }
    }
    impl<T> Eq for MinRatio<T> where T: Ord {}
    impl<T> PartialEq for MinRatio<T> where T: Ord {
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
    impl<T> Similarity<T> for MinRatio<T> where T: Ord {}

    #[derive(Debug, Clone, Copy)] pub struct Ratio<A: Attribute>(pub A, pub f64);
    impl<'a, A, T, I> SimilarityCriterion<'a> for Ratio<A> where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<I>>, I: Ord {
        type Item = T;
        type IntoItem = I;
        type Similarity = MinRatio<Self::IntoItem>;

        fn from(&self, object: &ItemWithData<'a, Self::Item>) -> Self::Similarity {
            let items = self.0.get_opt(object).map(|e| {
                BTreeSet::from_iter(e.into_iter())
            });
            MinRatio { min_ratio: self.1, items }
        }
    }

    //#[derive(Debug, Clone, Copy)] pub struct All<A: Attribute>(pub A, pub f64); Seems superfluous
    #[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)] pub struct Distinct<S, C>(pub S, pub C);
    impl<'a, T, S, C> Sampler<'a, T> for Distinct<S, C> where S: Sampler<'a, T>, C: SimilarityCriterion<'a, Item=T> {
        fn sample<I>(&self, iter: I) -> Vec<ItemWithData<'a, T>>
            where I: Iterator<Item=ItemWithData<'a, T>> {
            let filtered_iter = iter.unique_by(|object| {
                self.1.from(object)
            });
            self.0.sample(filtered_iter)
        }
    }
}