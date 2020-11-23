use chrono::Duration;

use crate::attrib::*;
use crate::objects;
use crate::iterators::ItemWithData;

// macro_rules! impl_sort_by_key {
//     ($object:ty, $attribute:ident) => {
//
//     }
// }

macro_rules! impl_surefire_attribute {
    ($object:ty, $attribute:ident, $small_type:ty, $getter:ident) => {
        #[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct $attribute;
        impl Attribute for $attribute { type Object = $object; }
        impl Getter<$small_type> for $attribute {
            fn get(object: &ItemWithData<Self::Object>) -> Option<$small_type> {
                Some(object.$getter())
            }
        }
        impl Sort for $attribute {
            type Item = $object;
            fn sort(&self, vector: &mut Vec<ItemWithData<Self::Item>>) {
                vector.sort_by_key(|item_with_data| Self::get(item_with_data).unwrap())
            }
        }
        impl Select for $attribute {
            type Item = $object;
            type IntoItem = $small_type;
            fn select(&self, item_with_data: &ItemWithData<Self::Item>) -> Self::IntoItem {
                Self::get(item_with_data).unwrap()
            }
        }
        impl Group for $attribute {
            type Key = $small_type;
            type Item = $object;
            fn select_key(&self, item_with_data: &ItemWithData<Self::Item>) -> Self::Key {
                Self::get(item_with_data).unwrap()
            }
        }
    }
}

macro_rules! impl_optional_attribute {
    ($object:ty, $attribute:ident, $small_type:ty, $getter:ident) => {
        #[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct $attribute;
        impl Attribute for $attribute { type Object = $object; }
        impl Getter<$small_type> for $attribute {
            fn get(object: &ItemWithData<Self::Object>) -> Option<$small_type> {
                object.$getter()
            }
        }
        impl Sort for $attribute {
            type Item = $object;
            fn sort(&self, vector: &mut Vec<ItemWithData<Self::Item>>) {
                vector.sort_by_key(|e| Self::get(e))
            }
        }
        impl Select for $attribute {
            type Item = $object;
            type IntoItem = Option<$small_type>;
            fn select(&self, item_with_data: &ItemWithData<Self::Item>) -> Self::IntoItem {
                Self::get(item_with_data)
            }
        }
        impl Group for $attribute {
            type Key = Option<$small_type>;
            type Item = $object;
            fn select_key(&self, item_with_data: &ItemWithData<Self::Item>) -> Self::Key {
                Self::get(item_with_data)
            }
        }
    }
}

macro_rules! impl_collection_attribute {
    ($object:ty, $attribute:ident, $small_type:ty, $getter:ident, $counter:ident) => {
        #[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct $attribute;
        impl Attribute for $attribute { type Object = $object; }
        impl Getter<Vec<$small_type>> for $attribute {
            fn get(object: &ItemWithData<Self::Object>) -> Option<Vec<$small_type>> {
                object.$getter()
            }
        }
        impl Counter for $attribute {
            fn count(object: &ItemWithData<Self::Object>) -> Option<usize> {
                object.$counter()
            }
        }
        // Not sorting by collection attributes.
        impl Select for $attribute {
            type Item = $object;
            type IntoItem = Option<Vec<$small_type>>;
            fn select(&self, item_with_data: &ItemWithData<Self::Item>) -> Self::IntoItem {
                Self::get(item_with_data)
            }
        }
        // No grouping by collection attributes (use counts and buckets).// FIXME impl counts and buckets
    }
}

macro_rules! impl_surefire_collection_attribute {
    ($object:ty, $attribute:ident, $small_type:ty, $getter:ident, $id_getter:ident) => {
        #[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct $attribute;
        impl Attribute for $attribute { type Object = $object; }
        impl Getter<Vec<$small_type>> for $attribute {
            fn get(object: &ItemWithData<Self::Object>) -> Option<Vec<$small_type>> {
                Some(object.$getter())
            }
        }
        impl Counter for $attribute {
            fn count(object: &ItemWithData<Self::Object>) -> Option<usize> {
                Some(object.$id_getter())
            }
        }
        // Not sorting by collection attributes.
        impl Select for $attribute {
            type Item = $object;
            type IntoItem = Vec<$small_type>;
            fn select(&self, item_with_data: &ItemWithData<Self::Item>) -> Self::IntoItem {
                Self::get(item_with_data).unwrap()
            }
        }
        // No grouping by collection attributes (use counts and buckets).
    }
}

pub mod project {
    use crate::query::*;
    impl_surefire_attribute!(objects::Project, Id, objects::ProjectId, id);
    impl_surefire_attribute!(objects::Project, URL, String, url);
    impl_optional_attribute!(objects::Project, Issues, usize, issue_count);
    impl_optional_attribute!(objects::Project, BuggyIssues, usize, buggy_issue_count);
    impl_optional_attribute!(objects::Project, IsFork, bool, is_fork);
    impl_optional_attribute!(objects::Project, IsArchived, bool, is_archived);
    impl_optional_attribute!(objects::Project, IsDisabled, bool, is_disabled);
    impl_optional_attribute!(objects::Project, Stars, usize, star_count);
    impl_optional_attribute!(objects::Project, Watchers, usize, watcher_count);
    impl_optional_attribute!(objects::Project, Size, usize, size);
    impl_optional_attribute!(objects::Project, OpenIssues, usize, open_issue_count);
    impl_optional_attribute!(objects::Project, Forks, usize, fork_count);
    impl_optional_attribute!(objects::Project, Subscribers, usize, subscriber_count);
    impl_optional_attribute!(objects::Project, License, String, license);
    impl_optional_attribute!(objects::Project, Language, objects::Language, language);
    impl_optional_attribute!(objects::Project, Description, String, description);
    impl_optional_attribute!(objects::Project, Homepage, String, homepage);
    impl_optional_attribute!(objects::Project, HasIssues, bool, has_issues);
    impl_optional_attribute!(objects::Project, HasDownloads, bool, has_downloads);
    impl_optional_attribute!(objects::Project, HasWiki, bool, has_wiki);
    impl_optional_attribute!(objects::Project, HasPages, bool, has_pages);
    impl_optional_attribute!(objects::Project, Created, i64, created);
    impl_optional_attribute!(objects::Project, Updated, i64, updated);
    impl_optional_attribute!(objects::Project, Pushed, i64, pushed);
    impl_optional_attribute!(objects::Project, DefaultBranch, String, default_branch);
    impl_optional_attribute!(objects::Project, Age, Duration, lifetime);
    impl_collection_attribute!(objects::Project, Heads, objects::Head, heads, head_count);
    impl_collection_attribute!(objects::Project, Commits, objects::Commit, commits, commit_count);
    impl_collection_attribute!(objects::Project, Authors, objects::User, authors, author_count);
    impl_collection_attribute!(objects::Project, Committers, objects::User, committers, committer_count);
    impl_collection_attribute!(objects::Project, Users, objects::User, users, user_count);
    impl_collection_attribute!(objects::Project, Paths, objects::Path, paths, path_count);
    impl_collection_attribute!(objects::Project, Snapshots, objects::Snapshot, snapshots, snapshot_count);
}

pub mod commit {
    use crate::query::*;
    impl_surefire_attribute!(objects::Commit, Id, objects::CommitId, id);
    impl_surefire_attribute!(objects::Commit, Committer, objects::User, committer);
    impl_surefire_attribute!(objects::Commit, Author, objects::User, author);
    impl_optional_attribute!(objects::Commit, Hash, String, hash);
    impl_optional_attribute!(objects::Commit, Message, String, message);
    impl_optional_attribute!(objects::Commit, AuthoredTimestamp, i64, author_timestamp);
    impl_optional_attribute!(objects::Commit, CommittedTimestamp, i64, committer_timestamp);
    impl_collection_attribute!(objects::Commit, Paths, objects::Path, changed_paths, changed_path_count);
    impl_collection_attribute!(objects::Commit, Snapshots, objects::Snapshot, changed_snapshots, changed_snapshot_count);
    impl_surefire_collection_attribute!(objects::Commit, Parents, objects::Commit, parents, parent_count);
}

pub mod head {
    use crate::query::*;
    impl_surefire_attribute!(objects::Head, Name, String, name);
    impl_surefire_attribute!(objects::Head, Commit, objects::Commit, commit);
}

pub mod user {
    use crate::query::*;
    impl_surefire_attribute!(objects::User, Id, objects::UserId, id);
    impl_surefire_attribute!(objects::User, Email, String, email);
    impl_optional_attribute!(objects::User, AuthorExperience, Duration, author_experience);
    impl_optional_attribute!(objects::User, CommitterExperience, Duration, committer_experience);
    impl_optional_attribute!(objects::User, Experience, Duration, experience);
    impl_collection_attribute!(objects::User, AuthoredCommits, objects::Commit, authored_commits, authored_commit_count);
    impl_collection_attribute!(objects::User, CommittedCommits, objects::Commit, committed_commits, committed_commit_count);
}

pub mod path {
    use crate::query::*;
    impl_surefire_attribute!(objects::Path, Id, objects::PathId, id);
    impl_surefire_attribute!(objects::Path, Location, String, location);
    impl_optional_attribute!(objects::Path, Language, objects::Language, language);
}

pub mod snapshot {
    use crate::query::*;
    impl_surefire_attribute!(objects::Snapshot, Id, objects::SnapshotId, id);
    impl_surefire_attribute!(objects::Snapshot, Bytes, Vec<u8>, raw_contents_owned);
    impl_surefire_attribute!(objects::Snapshot, Contents, String, contents_owned);
}

pub mod require {
    use crate::query::*;
    use crate::attrib::*;
    use crate::iterators::ItemWithData;

    pub struct MoreThan<A, N>(pub A, pub N) where A: Getter<N>;
    impl<A, N, T> Filter for MoreThan<A, N> where A: Getter<N> + Attribute<Object=T>, N: PartialOrd {
        type Item = T;
        fn accept(&self, item_with_data: &ItemWithData<Self::Item>) -> bool {
            A::get(item_with_data).map_or(false, |n| n > self.1)
        }
    }
    pub struct AtLeast<A, N>(pub A, pub N) where A: Getter<N>;
    impl<A, N, T> Filter for AtLeast<A, N> where A: Getter<N> + Attribute<Object=T>, N: PartialOrd {
        type Item = T;
        fn accept(&self, item_with_data: &ItemWithData<Self::Item>) -> bool {
            A::get(item_with_data).map_or(false, |n| n >= self.1)
        }
    }
    pub struct Exactly<A, N>(pub A, pub N) where A: Getter<N>;
    impl<A, N, T> Filter for Exactly<A, N> where A: Getter<N> + Attribute<Object=T>, N: Eq {
        type Item = T;
        fn accept(&self, item_with_data: &ItemWithData<Self::Item>) -> bool {
            A::get(item_with_data).map_or(false, |n| n == self.1)
        }
    }
    pub struct AtMost<A, N>(pub A, pub N) where A: Getter<N>;
    impl<A, N, T> Filter for AtMost<A, N> where A: Getter<N> + Attribute<Object=T>, N: PartialOrd {
        type Item = T;
        fn accept(&self, item_with_data: &ItemWithData<Self::Item>) -> bool {
            A::get(item_with_data).map_or(true, |n| n <= self.1)
        }
    }
    pub struct LessThan<A, N>(pub A, pub N) where A: Getter<N>;
    impl<A, N, T> Filter for LessThan<A, N> where A: Getter<N> + Attribute<Object=T>, N: PartialOrd {
        type Item = T;
        fn accept(&self, item_with_data: &ItemWithData<Self::Item>) -> bool {
            A::get(item_with_data).map_or(true, |n| n < self.1)
        }
    }
}