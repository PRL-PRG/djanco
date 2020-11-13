//use std::borrow::Cow;

use chrono::Duration;

use crate::attrib::*;
//use crate::iterators::ItemWithData;
use crate::objects;
use crate::iterators::ItemWithData;

macro_rules! impl_surefire_attribute {
    ($object:ty, $attribute:ident, $small_type:ty, $getter:ident) => {
        #[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct $attribute;
        impl Attribute for $attribute { type Object = $object; }
        impl Getter<$small_type> for $attribute {
            fn get(object: &ItemWithData<Self::Object>) -> Option<$small_type> {
                Some(object.$getter())
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
    }
}

mod project {
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

mod commit {
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

mod head {
    use crate::query::*;
    impl_surefire_attribute!(objects::Head, Name, String, name);
    impl_surefire_attribute!(objects::Head, Commit, objects::Commit, commit);
}

mod user {
    use crate::query::*;
    impl_surefire_attribute!(objects::User, Id, objects::UserId, id);
    impl_surefire_attribute!(objects::User, Email, String, email);
    impl_optional_attribute!(objects::User, AuthorExperience, Duration, author_experience);
    impl_optional_attribute!(objects::User, CommitterExperience, Duration, committer_experience);
    impl_optional_attribute!(objects::User, Experience, Duration, experience);
    impl_collection_attribute!(objects::User, AuthoredCommits, objects::Commit, authored_commits, authored_commit_count);
    impl_collection_attribute!(objects::User, CommittedCommits, objects::Commit, committed_commits, committed_commit_count);
}

mod path {
    use crate::query::*;
    impl_surefire_attribute!(objects::Path, Id, objects::PathId, id);
    impl_surefire_attribute!(objects::Path, Location, String, location);
    impl_optional_attribute!(objects::Path, Language, objects::Language, language);
}

mod snapshot {
    use crate::query::*;
    impl_surefire_attribute!(objects::Snapshot, Id, objects::SnapshotId, id);
    impl_surefire_attribute!(objects::Snapshot, Bytes, Vec<u8>, raw_contents_owned);
    impl_surefire_attribute!(objects::Snapshot, Contents, String, contents_owned);
}

/*
impl_sort_by_key_sans_db!(Project, Id,  id);
impl_sort_by_key_sans_db!(Project, URL, url);
impl_sort_by_key_with_db!(Project, Issues, issue_count);
impl_sort_by_key_with_db!(Project, BuggyIssues, buggy_issue_count);
impl_sort_by_key_with_db!(Project, IsFork, is_fork);
impl_sort_by_key_with_db!(Project, IsArchived, is_archived);
impl_sort_by_key_with_db!(Project, IsDisabled, is_disabled);
impl_sort_by_key_with_db!(Project, Stars, star_count);
impl_sort_by_key_with_db!(Project, Watchers, watcher_count);
impl_sort_by_key_with_db!(Project, Size, size);
impl_sort_by_key_with_db!(Project, OpenIssues, open_issue_count);
impl_sort_by_key_with_db!(Project, Forks, fork_count);
impl_sort_by_key_with_db!(Project, Subscribers, subscriber_count);
impl_sort_by_key_with_db!(Project, License, license);
impl_sort_by_key_with_db!(Project, Language, language);
impl_sort_by_key_with_db!(Project, Description, description);
impl_sort_by_key_with_db!(Project, Homepage, homepage);
impl_sort_by_key_with_db!(Project, Heads, head_count);
impl_sort_by_key_with_db!(Project, Commits, commit_count);
impl_sort_by_key_with_db!(Project, Authors, author_count);
impl_sort_by_key_with_db!(Project, Committers, committer_count);
impl_sort_by_key_with_db!(Project, Users, user_count);
impl_sort_by_key_with_db!(Project, Paths, path_count);
impl_sort_by_key_with_db!(Project, HasIssues, has_issues);
impl_sort_by_key_with_db!(Project, HasDownloads, has_downloads);
impl_sort_by_key_with_db!(Project, HasWiki, has_wiki);
impl_sort_by_key_with_db!(Project, HasPages, has_pages);
impl_sort_by_key_with_db!(Project, Created, created);
impl_sort_by_key_with_db!(Project, Updated, updated);
impl_sort_by_key_with_db!(Project, Pushed, pushed);
impl_sort_by_key_with_db!(Project, DefaultBranch, master_branch);
impl_sort_by_key_with_db!(Project, Age, lifetime);
*/


