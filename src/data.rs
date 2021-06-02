use std::collections::{BTreeMap, BTreeSet};
use std::collections::btree_map::Entry;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::iter::FromIterator;

use anyhow::*;
use itertools::{Itertools, MinMaxResult};

use crate::objects::*;
use crate::piracy::*;
use crate::persistent::*;
use crate::iterators::*;
use crate::metadata::*;
use crate::log::*;
use crate::weights_and_measures::{Weighed};
use crate::time::Duration;
use crate::source::Source;
use crate::{CacheDir, Store};
use chrono::{Utc};

pub mod cache_filenames {
    pub static CACHE_FILE_PROJECT_IS_FORK:                &'static str = "project_is_fork";
    pub static CACHE_FILE_PROJECT_IS_ARCHIVED:            &'static str = "project_is_archived";
    pub static CACHE_FILE_PROJECT_IS_DISABLED:            &'static str = "project_is_disabled";
    pub static CACHE_FILE_PROJECT_STARGAZER_COUNT:        &'static str = "project_stargazer_count";
    pub static CACHE_FILE_PROJECT_WATCHER_COUNT:          &'static str = "project_watcher_count";
    pub static CACHE_FILE_PROJECT_SIZE:                   &'static str = "project_size";
    pub static CACHE_FILE_PROJECT_ISSUE_COUNT:            &'static str = "project_issue_count";
    pub static CACHE_FILE_PROJECT_BUGGY_ISSUE_COUNT:      &'static str = "project_buggy_issue_count";
    pub static CACHE_FILE_PROJECT_OPEN_ISSUE_COUNT:       &'static str = "project_open_issue_count";
    pub static CACHE_FILE_PROJECT_FORK_COUNT:             &'static str = "project_fork_count";
    pub static CACHE_FILE_PROJECT_SUBSCRIBER_COUNT:       &'static str = "project_subscriber_count";
    pub static CACHE_FILE_PROJECT_LANGUAGE:               &'static str = "project_language";
    pub static CACHE_FILE_PROJECT_DESCRIPTION:            &'static str = "project_description";
    pub static CACHE_FILE_PROJECT_HOMEPAGE:               &'static str = "project_homepage";
    pub static CACHE_FILE_PROJECT_LICENSE:                &'static str = "project_license";
    pub static CACHE_FILE_PROJECT_HAS_ISSUES:             &'static str = "project_has_issues";
    pub static CACHE_FILE_PROJECT_HAS_DOWNLOADS:          &'static str = "project_has_downloads";
    pub static CACHE_FILE_PROJECT_HAS_WIKI:               &'static str = "project_has_wiki";
    pub static CACHE_FILE_PROJECT_HAS_PAGES:              &'static str = "project_has_pages";
    pub static CACHE_FILE_PROJECT_CREATED:                &'static str = "project_created";
    pub static CACHE_FILE_PROJECT_UPDATED:                &'static str = "project_updated";
    pub static CACHE_FILE_PROJECT_PUSHED:                 &'static str = "project_pushed";
    pub static CACHE_FILE_PROJECT_DEFAULT_BRANCH:         &'static str = "project_default_branch";
    pub static CACHE_FILE_PROJECT_URL:                    &'static str = "project_url";
    pub static CACHE_FILE_PROJECT_SUBSTORE:               &'static str = "project_substore";
    pub static CACHE_FILE_PROJECT_HEADS:                  &'static str = "project_heads";
    pub static CACHE_FILE_PROJECT_PATHS:                  &'static str = "project_paths";
    pub static CACHE_FILE_PROJECT_PATH_COUNT:             &'static str = "project_path_count";
    pub static CACHE_FILE_PROJECT_SNAPSHOTS:              &'static str = "project_snapshots";
    pub static CACHE_FILE_PROJECT_SNAPSHOT_COUNT:         &'static str = "project_snapshot_count";
    pub static CACHE_FILE_PROJECT_USERS:                  &'static str = "project_users";
    pub static CACHE_FILE_PROJECT_USER_COUNT:             &'static str = "project_user_count";
    pub static CACHE_FILE_PROJECT_AUTHORS:                &'static str = "project_authors";
    pub static CACHE_FILE_PROJECT_AUTHOR_COUNT:           &'static str = "project_author_count";
    pub static CACHE_FILE_PROJECT_COMMITTERS:             &'static str = "project_committers";
    pub static CACHE_FILE_PROJECT_COMMITTER_COUNT:        &'static str = "project_committer_count";
    pub static CACHE_FILE_PROJECT_COMMITS:                &'static str = "project_commits";
    pub static CACHE_FILE_PROJECT_COMMIT_COUNT:           &'static str = "project_commit_count";
    pub static CACHE_FILE_PROJECT_LIFETIME:               &'static str = "project_lifetime";
    pub static CACHE_FILE_PROJECT_UNIQUE_FILES:           &'static str = "project_unique_files";
    pub static CACHE_FILE_PROJECT_ORIGINAL_FILES:         &'static str = "project_original_files";
    pub static CACHE_FILE_PROJECT_IMPACT:                 &'static str = "project_impact";
    pub static CACHE_FILE_PROJECT_FILES:                  &'static str = "project_files";
    pub static CACHE_FILE_PROJECT_LANGUAGES:              &'static str = "project_languages";
    pub static CACHE_FILE_PROJECT_LANGUAGES_COUNT:        &'static str = "project_languages_count";
    pub static CACHE_FILE_PROJECT_MAJOR_LANGUAGE:         &'static str = "project_major_language";
    pub static CACHE_FILE_PROJECT_MAJOR_LANGUAGE_RATIO:   &'static str = "project_major_language_ratio";
    pub static CACHE_FILE_PROJECT_MAJOR_LANGUAGE_CHANGES: &'static str = "project_major_language_changes";
    pub static CACHE_FILE_USERS:                          &'static str = "users";
    pub static CACHE_FILE_USER_AUTHORED_COMMITS:          &'static str = "user_authored_commits";
    pub static CACHE_FILE_USER_COMMITTED_COMMITS:         &'static str = "user_committed_commits";
    pub static CACHE_FILE_USER_AUTHOR_EXPERIENCE:         &'static str = "user_author_experience";
    pub static CACHE_FILE_USER_COMMITTER_EXPERIENCE:      &'static str = "user_committer_experience";
    pub static CACHE_FILE_USER_EXPERIENCE:                &'static str = "user_experience";
    pub static CACHE_FILE_USER_AUTHORED_COMMIT_COUNT:     &'static str = "user_authored_commit_count";
    pub static CACHE_FILE_USER_COMMITTED_COMMIT_COUNT:    &'static str = "user_committed_commit_count";
    pub static CACHE_FILE_PATHS:                          &'static str = "paths";
    pub static CACHE_FILE_COMMITS:                        &'static str = "commits";
    pub static CACHE_FILE_COMMIT_HASHES:                  &'static str = "commit_hashes";
    pub static CACHE_FILE_COMMIT_MESSAGES:                &'static str = "commit_messages";
    pub static CACHE_FILE_COMMIT_AUTHOR_TIMESTAMPS:       &'static str = "commit_author_timestamps";
    pub static CACHE_FILE_COMMIT_COMMITTER_TIMESTAMPS:    &'static str = "commit_committer_timestamps";
    pub static CACHE_FILE_COMMIT_CHANGES:                 &'static str = "commit_changes";
    pub static CACHE_FILE_COMMIT_CHANGE_COUNT:            &'static str = "commit_change_count";
    pub static CACHE_FILE_COMMIT_PROJECTS:                &'static str = "commit_projects";
    pub static CACHE_FILE_COMMIT_PROJECTS_COUNT:          &'static str = "commit_projects_count";
    pub static CACHE_FILE_SNAPSHOT_PROJECTS:              &'static str = "snapshot_projects";
    pub static CACHE_FILE_LONGEST_INACTIVITTY_STREAK:     &'static str = "longest_inactivity_streak";
    pub static CACHE_FILE_AVG_COMMIT_RATE:                &'static str = "avg_commit_rate";  
    pub static CACHE_FILE_TIME_SINCE_LAST_COMMIT:         &'static str = "time_since_last_commit";  
    pub static CACHE_FILE_IS_ABANDONED:                   &'static str = "is_abandoned";  
    pub static CACHE_FILE_SNAPSHOT_LOCS:                  &'static str = "snapshot_locs";  
    pub static CACHE_FILE_PROJECT_LOCS:                   &'static str = "project_locs";  
}

use cache_filenames::*;

// Internally Mutable Data
pub struct Database {
    data: RefCell<Data>,
    source: Source,
    log: Log,
}

// Constructors
impl Database {
    // pub fn from_source<S>(source: DataSource, cache_dir: S) -> Database where S: Into<String> {
    //     let log: Log = Log::new(Verbosity::Log);
    //     Database { data: RefCell::new(Data::new(cache_dir, log.clone())), source, log }
    // }
    // pub fn from<S>(source: DataSource, cache_dir: S, log: Log) -> Database where S: Into<String> {
    //     Database { data: RefCell::new(Data::new(cache_dir, log.clone())), source, log }
    // }
    //pub fn from_spec<Sd, Sc>(dataset_path: Sd, cache_path: Sc, savepoint: i64, subsources: Vec<source>) -> anyhow::Result<Database> where Sd: Into<String>, Sc: Into<String> {
    pub fn new(source: Source, cache_dir: CacheDir, log: Log) -> Self {
        let data = RefCell::new(Data::new(cache_dir, log.clone()));
        Database { data, source, log }
    }
}

// Prequincunx
impl Database {
    pub fn all_project_ids(&self) -> Vec<ProjectId> { self.data.borrow_mut().all_project_ids(&self.source) }
    pub fn all_user_ids(&self)    -> Vec<UserId>    { self.data.borrow_mut().all_user_ids(&self.source)    }
    pub fn all_path_ids(&self)    -> Vec<PathId>    { self.data.borrow_mut().all_path_ids(&self.source)    }
    pub fn all_commit_ids(&self)  -> Vec<CommitId>  { self.data.borrow_mut().all_commit_ids(&self.source)  }
}

pub struct OptionIter<I> where I: Iterator {
    pub iter: Option<I>
}

impl<I> OptionIter<I> where I: Iterator {
    pub fn new() -> Self {
        OptionIter { iter: None }
    }
}

impl<I> Iterator for OptionIter<I> where I: Iterator {
    type Item = I::Item;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.as_mut().map(|iter| iter.next()).flatten()
    }
}

// Quincunx
impl Database {
    pub fn projects(&self)  -> QuincunxIter<Project>  { QuincunxIter::<Project>::new(&self)  }
    pub fn commits(&self)   -> QuincunxIter<Commit>   { QuincunxIter::<Commit>::new(&self)   }
    pub fn users(&self)     -> QuincunxIter<User>     { QuincunxIter::<User>::new(&self)     }
    pub fn paths(&self)     -> QuincunxIter<Path>     { QuincunxIter::<Path>::new(&self)     }
}

// Uncached stuff
impl Database {
    pub fn snapshot(&self, id: &SnapshotId) -> Option<Snapshot> {
        self.source.get_snapshot(id.clone()).map(|bytes| Snapshot::new(id.clone(), bytes))
    }
    pub fn snapshots<'a>(&'a self) -> impl Iterator<Item=Snapshot> + 'a {
        LogIter::new("reading snapshots", &self.log,Verbosity::Log,
                     self.source.snapshot_bytes()
                         .map(|(id, bytes)| Snapshot::new(id, bytes)))
    }
    pub fn snapshots_with_data<'a>(&'a self) -> impl Iterator<Item=ItemWithData<'a, Snapshot>> + 'a {
        self.snapshots().attach_data_to_each(self)
    }
}

impl Database {
    pub fn project(&self, id: &ProjectId) -> Option<Project> {
        self.data.borrow_mut().project(&self.source, id)
    }
    pub fn project_issues(&self, id: &ProjectId) -> Option<usize> {
        self.data.borrow_mut().project_issues(&self.source, id)
    }
    pub fn project_buggy_issues(&self, id: &ProjectId) -> Option<usize> {
        self.data.borrow_mut().project_buggy_issues(&self.source, id)
    }
    pub fn project_is_fork(&self, id: &ProjectId) -> Option<bool> {
        self.data.borrow_mut().project_is_fork(&self.source, id)
    }
    pub fn project_is_archived(&self, id: &ProjectId) -> Option<bool> {
        self.data.borrow_mut().project_is_archived(&self.source, id)
    }
    pub fn project_is_disabled(&self, id: &ProjectId) -> Option<bool> {
        self.data.borrow_mut().project_is_disabled(&self.source, id)
    }
    pub fn project_star_gazer_count(&self, id: &ProjectId) -> Option<usize> {
        self.data.borrow_mut().project_star_gazer_count(&self.source, id)
    }
    pub fn project_watcher_count(&self, id: &ProjectId) -> Option<usize> {
        self.data.borrow_mut().project_watcher_count(&self.source, id)
    }
    pub fn project_size(&self, id: &ProjectId) -> Option<usize> {
        self.data.borrow_mut().project_size(&self.source, id)
    }
    pub fn project_open_issue_count(&self, id: &ProjectId) -> Option<usize> {
        self.data.borrow_mut().project_open_issue_count(&self.source, id)
    }
    pub fn project_fork_count(&self, id: &ProjectId) -> Option<usize> {
        self.data.borrow_mut().project_fork_count(&self.source, id)
    }
    pub fn project_subscriber_count(&self, id: &ProjectId) -> Option<usize> {
        self.data.borrow_mut().project_subscriber_count(&self.source, id)
    }
    pub fn project_license(&self, id: &ProjectId) -> Option<String> {
        self.data.borrow_mut().project_license(&self.source, id)
    }
    pub fn project_language(&self, id: &ProjectId) -> Option<Language> {
        self.data.borrow_mut().project_language(&self.source, id)
    }
    pub fn project_substore(&self, id: &ProjectId) -> Option<Store> {
        self.data.borrow_mut().project_substore(&self.source, id)
    }
    pub fn project_description(&self, id: &ProjectId) -> Option<String> {
        self.data.borrow_mut().project_description(&self.source, id)
    }
    pub fn project_homepage(&self, id: &ProjectId) -> Option<String> {
        self.data.borrow_mut().project_homepage(&self.source, id)
    }
    pub fn project_has_issues(&self, id: &ProjectId) -> Option<bool> {
        self.data.borrow_mut().project_has_issues(&self.source, id)
    }
    pub fn project_has_downloads(&self, id: &ProjectId) -> Option<bool> {
        self.data.borrow_mut().project_has_downloads(&self.source, id)
    }
    pub fn project_has_wiki(&self, id: &ProjectId) -> Option<bool> {
        self.data.borrow_mut().project_has_wiki(&self.source, id)
    }
    pub fn project_has_pages(&self, id: &ProjectId) -> Option<bool> {
        self.data.borrow_mut().project_has_pages(&self.source, id)
    }
    pub fn project_created(&self, id: &ProjectId) -> Option<i64> {
        self.data.borrow_mut().project_created(&self.source, id)
    }
    pub fn project_updated(&self, id: &ProjectId) -> Option<i64> {
        self.data.borrow_mut().project_updated(&self.source, id)
    }
    pub fn project_pushed(&self, id: &ProjectId) -> Option<i64> {
        self.data.borrow_mut().project_pushed(&self.source, id)
    }
    pub fn project_default_branch(&self, id: &ProjectId) -> Option<String> {
        self.data.borrow_mut().project_default_branch(&self.source, id)
    }
    pub fn project_url(&self, id: &ProjectId) -> Option<String> {
        self.data.borrow_mut().project_url(&self.source, id)
    }
    // pub fn project_head_ids(&self, id: &ProjectId) -> Option<Vec<(String, CommitId)>> {
    //     self.data.borrow_mut().project_head_ids(&self.source, id)
    // }
    pub fn project_heads(&self, id: &ProjectId) -> Option<Vec<Head>> {
        self.data.borrow_mut().project_heads(&self.source, id)
    }
    pub fn project_commit_ids(&self, id: &ProjectId) -> Option<Vec<CommitId>> {
        self.data.borrow_mut().project_commit_ids(&self.source, id).pirate()
    }
    pub fn project_commits(&self, id: &ProjectId) -> Option<Vec<Commit>> {
        self.data.borrow_mut().project_commits(&self.source, id)
    }
    pub fn project_commit_count(&self, id: &ProjectId) -> Option<usize> {
        self.data.borrow_mut().project_commit_count(&self.source, id)
    }
    pub fn project_author_ids(&self, id: &ProjectId) -> Option<Vec<UserId>> {
        self.data.borrow_mut().project_author_ids(&self.source, id).pirate()
    }
    pub fn project_authors(&self, id: &ProjectId) -> Option<Vec<User>> {
        self.data.borrow_mut().project_authors(&self.source, id)
    }
    pub fn project_author_count(&self, id: &ProjectId) -> Option<usize> {
        self.data.borrow_mut().project_author_count(&self.source, id)
    }
    pub fn project_path_ids(&self, id: &ProjectId) -> Option<Vec<PathId>> {
        self.data.borrow_mut().project_path_ids(&self.source, id).pirate()
    }
    pub fn project_paths(&self, id: &ProjectId) -> Option<Vec<Path>> {
        self.data.borrow_mut().project_paths(&self.source, id)
    }
    pub fn project_path_count(&self, id: &ProjectId) -> Option<usize> {
        self.data.borrow_mut().project_path_count(&self.source, id)
    }
    pub fn project_snapshot_ids(&self, id: &ProjectId) -> Option<Vec<SnapshotId>> {
        self.data.borrow_mut().project_snapshot_ids(&self.source, id).pirate()
    }
    pub fn project_snapshots(&self, id: &ProjectId) -> Option<Vec<Snapshot>> {
        self.project_snapshot_ids(id).map(|vector| {
            vector.into_iter()
                .flat_map(|id| self.snapshot(&id))
                .collect::<Vec<Snapshot>>()
        })
    }
    pub fn project_snapshot_count(&self, id: &ProjectId) -> Option<usize> {
        self.data.borrow_mut().project_snapshot_count(&self.source, id)
    }
    pub fn project_committer_ids(&self, id: &ProjectId) -> Option<Vec<UserId>> {
        self.data.borrow_mut().project_committer_ids(&self.source, id).pirate()
    }
    pub fn project_committers(&self, id: &ProjectId) -> Option<Vec<User>> {
        self.data.borrow_mut().project_committers(&self.source, id)
    }
    pub fn project_committer_count(&self, id: &ProjectId) -> Option<usize> {
        self.data.borrow_mut().project_committer_count(&self.source, id)
    }
    pub fn project_user_ids(&self, id: &ProjectId) -> Option<Vec<UserId>> {
        self.data.borrow_mut().project_user_ids(&self.source, id).pirate()
    }
    pub fn project_users(&self, id: &ProjectId) -> Option<Vec<User>> {
        self.data.borrow_mut().project_users(&self.source, id)
    }
    pub fn project_user_count(&self, id: &ProjectId) -> Option<usize> {
        self.data.borrow_mut().project_user_count(&self.source, id)
    }
    pub fn project_lifetime(&self, id: &ProjectId) -> Option<Duration> {
        self.data.borrow_mut().project_lifetime(&self.source, id)
    }
    pub fn project_unique_files(&self, id: &ProjectId) -> Option<usize> {
        self.data.borrow_mut().project_unique_files(&self.source, id)
    }
    pub fn project_original_files(&self, id: &ProjectId) -> Option<usize> {
        self.data.borrow_mut().project_original_files(&self.source, id)
    }
    pub fn project_impact(&self, id: &ProjectId) -> Option<usize> {
        self.data.borrow_mut().project_impact(&self.source, id)
    }
    pub fn project_files(&self, id: &ProjectId) -> Option<usize> {
        self.data.borrow_mut().project_files(&self.source, id)
    }
    pub fn project_languages(&self, id: & ProjectId) -> Option<Vec<(Language,usize)>> {
        self.data.borrow_mut().project_languages(&self.source, id)
    }
    pub fn project_languages_count(&self, id: & ProjectId) -> Option<usize> {
        self.data.borrow_mut().project_languages_count(&self.source, id)
    }
    pub fn project_major_language(&self, id: &ProjectId) -> Option<Language> {
        self.data.borrow_mut().project_major_language(&self.source, id)
    }
    pub fn project_major_language_ratio(&self, id: &ProjectId) -> Option<f64> {
        self.data.borrow_mut().project_major_language_ratio(&self.source, id)
    }
    pub fn project_major_language_changes(&self, id: &ProjectId) -> Option<usize> {
        self.data.borrow_mut().project_major_language_changes(&self.source, id)
    }
    pub fn user(&self, id: &UserId) -> Option<User> {
        self.data.borrow_mut().user(&self.source, id).pirate()
    }
    pub fn path(&self, id: &PathId) -> Option<Path> {
        self.data.borrow_mut().path(&self.source, id).pirate()
    }
    pub fn commit(&self, id: &CommitId) -> Option<Commit> {
        self.data.borrow_mut().commit(&self.source, id).pirate()
    }
    pub fn commit_hash(&self, id: &CommitId) -> Option<String> {
        self.data.borrow_mut().commit_hash(&self.source, id).pirate()
    }
    pub fn commit_message(&self, id: &CommitId) -> Option<String> {
        self.data.borrow_mut().commit_message(&self.source, id).pirate()
    }
    pub fn commit_author_timestamp(&self, id: &CommitId) -> Option<i64> {
        self.data.borrow_mut().commit_author_timestamp(&self.source, id)
    }
    pub fn commit_committer_timestamp(&self, id: &CommitId) -> Option<i64> {
        self.data.borrow_mut().commit_committer_timestamp(&self.source, id)
    }
    pub fn commit_changes(&self, id: &CommitId) -> Option<Vec<Change>> {
        self.data.borrow_mut().commit_changes(&self.source, id)
    }
    pub fn commit_changed_paths(&self, id: &CommitId) -> Option<Vec<Path>> {
        self.data.borrow_mut().commit_changed_paths(&self.source, id)
    }
    pub fn commit_change_count(&self, id: &CommitId) -> Option<usize> {
        self.data.borrow_mut().commit_change_count(&self.source, id)
    }
    pub fn commit_changed_path_count(&self, id: &CommitId) -> Option<usize> {
        self.data.borrow_mut().commit_changed_path_count(&self.source, id)
    }
    pub fn commit_projects(&self, id : &CommitId) -> Option<Vec<Project>> {
        self.data.borrow_mut().commit_projects(&self.source, id)
    }
    pub fn commit_projects_count(&self, id: &CommitId) -> Option<usize> {
        self.data.borrow_mut().commit_projects_count(&self.source, id)
    }
    pub fn user_committed_commit_ids(&self, id: &UserId) -> Option<Vec<CommitId>> {
        self.data.borrow_mut().user_committed_commit_ids(&self.source, id).pirate()
    }
    pub fn user_authored_commits(&self, id: &UserId) -> Option<Vec<Commit>> {
        self.data.borrow_mut().user_authored_commits(&self.source, id)
    }
    pub fn user_authored_commit_ids(&self, id: &UserId) -> Option<Vec<CommitId>> {
        self.data.borrow_mut().user_authored_commit_ids(&self.source, id).pirate()
    }
    pub fn user_committed_experience(&self, id: &UserId) -> Option<Duration> {
        self.data.borrow_mut().user_committed_experience(&self.source, id)
    }
    pub fn user_author_experience(&self, id: &UserId) -> Option<Duration> {
        self.data.borrow_mut().user_author_experience(&self.source, id)
    }
    pub fn user_experience(&self, id: &UserId) -> Option<Duration> {
        self.data.borrow_mut().user_experience(&self.source, id)
    }
    pub fn user_committed_commit_count(&self, id: &UserId) -> Option<usize> {
        self.data.borrow_mut().user_committed_commit_count(&self.source, id)
    }
    pub fn user_authored_commit_count(&self, id: &UserId) -> Option<usize> {
        self.data.borrow_mut().user_authored_commit_count(&self.source, id)
    }
    pub fn user_committed_commits(&self, id: &UserId) -> Option<Vec<Commit>> {
        self.data.borrow_mut().user_committed_commits(&self.source, id)
    }
    pub fn project_longest_inactivity_streak(&self, id: &ProjectId) -> Option<i64> {
        self.data.borrow_mut().longest_inactivity_streak(&self.source, id)
    }
    pub fn avg_commit_rate(&self, id: &ProjectId) -> Option<i64> {
        self.data.borrow_mut().avg_commit_rate(&self.source, id)
    }
    pub fn project_time_since_last_commit(&self, id: &ProjectId) -> Option<i64> {
        self.data.borrow_mut().time_since_last_commit(&self.source, id)
    }
    pub fn is_abandoned(&self, id: &ProjectId) -> Option<bool> {
        self.data.borrow_mut().is_abandoned(&self.source, id)
    }
    pub fn snapshot_locs(&self, id: &SnapshotId) -> Option<usize> {
        self.data.borrow_mut().snapshot_locs(&self.source, id)
    }
    pub fn project_locs(&self, id: &ProjectId) -> Option<usize> {
        self.data.borrow_mut().project_locs(&self.source, id)
    }

    pub fn snapshot_unique_projects(&self, id : &SnapshotId) -> usize {
        self.data.borrow_mut().snapshot_unique_projects(&self.source, id)
    }
    pub fn snapshot_original_project(&self, id : &SnapshotId) -> ProjectId {
        self.data.borrow_mut().snapshot_original_project(&self.source, id)
    }
}

struct IdExtractor<Id: Identity + Persistent> { _type: PhantomData<Id> }
impl<Id> IdExtractor<Id> where Id: Identity + Persistent {
    pub fn _new() -> IdExtractor<Id> {
        IdExtractor { _type: PhantomData }
    }
}
impl<Id> VectorExtractor for IdExtractor<Id> where Id: Identity + Persistent {
    type Value = Id;
}
impl<Id> SingleVectorExtractor for IdExtractor<Id> where Id: Identity + Persistent  {
    type A = BTreeMap<Id, String>;
    fn extract(_: &Source, whatever: &Self::A) -> Vec<Self::Value> {
        whatever.keys().collect::<Vec<&Id>>().pirate()
    }
}

struct ProjectUrlExtractor;
impl MapExtractor for ProjectUrlExtractor {
    type Key = ProjectId;
    type Value = String;
}
impl SourceMapExtractor for ProjectUrlExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.project_urls().collect()
    }
}

struct LongestInactivityStreakExtractor {}
impl MapExtractor for LongestInactivityStreakExtractor {
    type Key = ProjectId;
    type Value = i64;
}
impl DoubleMapExtractor for LongestInactivityStreakExtractor  {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, i64>;
    fn extract(_: &Source, project_commits: &Self::A, committed_timestamps: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        
        project_commits.iter().flat_map(|(project_id, commit_ids)| {
            let mut timestamps: Vec<i64> = Vec::new();

            for i in 0..commit_ids.len(){
                let committer_timestamp = committed_timestamps.get(&commit_ids[i]);
                if let Some(timestamp) = committer_timestamp { timestamps.push(*timestamp) };
            }

            if timestamps.clone().len() == 0 {
                Some((project_id.clone(), 0))
            }else{
                timestamps.sort();
                let mut ans: i64 = 0;
                let mut previous: i64 = timestamps[0];
                
                for i in 1..timestamps.len() {
                    
                    if (timestamps[i] - previous) > ans {
                        
                        ans = timestamps[i] - previous;
                        
                    }
                    previous = timestamps[i];
                    
                }

                Some((project_id.clone(), ans))
            }
            
        }).collect()
    }
}

struct AvgCommitRateExtractor {}
impl MapExtractor for AvgCommitRateExtractor {
    type Key = ProjectId;
    type Value = i64;
}
impl DoubleMapExtractor for AvgCommitRateExtractor  {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, i64>;
    fn extract(_: &Source, project_commits: &Self::A, committed_timestamps: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        
        project_commits.iter().flat_map(|(project_id, commit_ids)| {
            let mut timestamps: Vec<i64> = Vec::new();

            for i in 0..commit_ids.len(){
                let committer_timestamp = committed_timestamps.get(&commit_ids[i]);
                if let Some(timestamp) = committer_timestamp { timestamps.push(*timestamp) };
            }

            if timestamps.clone().len() == 0 {
                Some((project_id.clone(), 0))
            }else{
                timestamps.sort();
                let mut ans: f64 = timestamps[0] as f64;
                let mut previous: i64 = timestamps[0];
                
                for i in 1..timestamps.len() {

                    ans += (timestamps[i] - previous) as f64;
                    previous = timestamps[i];
                
                }

                if timestamps.len() > 2 {
                    ans /= (timestamps.len()-1) as f64;
                }

                Some((project_id.clone(), ans.round() as i64))
            }
            
        }).collect()
    }
}

struct TimeSinceLastCommitExtractor {}
impl MapExtractor for TimeSinceLastCommitExtractor {
    type Key = ProjectId;
    type Value = i64;
}
impl DoubleMapExtractor for TimeSinceLastCommitExtractor  {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, i64>;
    fn extract(_: &Source, project_commits: &Self::A, committed_timestamps: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        
        project_commits.iter().flat_map(|(project_id, commit_ids)| {
            let mut timestamps: Vec<i64> = Vec::new();

            for i in 0..commit_ids.len() {
                let committer_timestamp = committed_timestamps.get(&commit_ids[i]);
                if let Some(timestamp) = committer_timestamp { timestamps.push(*timestamp) };
            }

            if timestamps.clone().len() == 0 {

                Some((project_id.clone(), 0))

            }else{
                
                timestamps.sort();

                let now: i64 = Utc::now().timestamp();

                Some((project_id.clone(), now - timestamps[timestamps.len()-1]))
            }
            
        }).collect()
    }
}

struct IsAbandonedExtractor {}
impl MapExtractor for IsAbandonedExtractor {
    type Key = ProjectId;
    type Value = bool;
}
impl DoubleMapExtractor for IsAbandonedExtractor  {
    type A = BTreeMap<ProjectId, i64>;
    type B = BTreeMap<ProjectId, i64>;
    fn extract(_: &Source, longest_inactivity_streak: &Self::A, time_since_last_commit: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        
        longest_inactivity_streak.iter().flat_map(|(project_id, inactivity_streak)| {
            let option_last_commit = time_since_last_commit.get(&project_id);
            if let Some(last_commit) = option_last_commit { 
                return Some((project_id.clone(), *last_commit > *inactivity_streak));
            }

            Some((project_id.clone(), false))

        }).collect()
    }
}

struct ProjectSubstoreExtractor;
impl MapExtractor for ProjectSubstoreExtractor {
    type Key = ProjectId;
    type Value = Store;
}
impl SourceMapExtractor for ProjectSubstoreExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.project_substores().collect()
    }
}

struct ProjectCredentialsExtractor; // TODO plug in
impl MapExtractor for ProjectCredentialsExtractor {
    type Key = ProjectId;
    type Value = String;
}
impl SourceMapExtractor for ProjectCredentialsExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.project_credentials().collect()
    }
}

struct ProjectHeadsExtractor;
impl MapExtractor for ProjectHeadsExtractor {
    type Key = ProjectId;
    type Value = Vec<Head>;
}
impl SourceMapExtractor for ProjectHeadsExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.project_heads().map(|(project_id, map)| {
            let heads = map.into_iter()
                .map(|(branch_name, (commit_id, _hash))| {
                    Head::new(branch_name, commit_id)
                }).collect::<Vec<Head>>();

            (project_id, heads)
        }).collect()
    }
}

struct ProjectSnapshotsExtractor {}
impl MapExtractor for ProjectSnapshotsExtractor {
    type Key = ProjectId;
    type Value = Vec<SnapshotId>;
}
impl DoubleMapExtractor for ProjectSnapshotsExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Vec<ChangeTuple>>;

    fn extract(_: &Source, project_commit_ids: &Self::A, commit_change_ids: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        project_commit_ids.iter().map(|(project_id, commit_ids)| {
            let path_ids /* Iterator equivalent of Vec<Vec<PathId>>*/ =
                commit_ids.iter().flat_map(|commit_id| {
                    let path_ids_option =
                        commit_change_ids.get(commit_id).map(|changes| {
                            let vector: Vec<SnapshotId> =
                                changes.iter().flat_map(|change| {
                                    change.1/*snapshot_id()*/
                                }).collect();
                            vector
                        });
                    path_ids_option
                });
            (project_id.clone(), path_ids.flatten().unique().collect())
        }).collect()
    }
}

struct ProjectPathsExtractor {}
impl MapExtractor for ProjectPathsExtractor {
    type Key = ProjectId;
    type Value = Vec<PathId>;
}
impl DoubleMapExtractor for ProjectPathsExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Vec<ChangeTuple>>;

    fn extract(_: &Source, project_commit_ids: &Self::A, commit_change_ids: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        project_commit_ids.iter().map(|(project_id, commit_ids)| {
            let path_ids /* Iterator equivalent of Vec<Vec<PathId>>*/ =
                commit_ids.iter().flat_map(|commit_id| {
                    let path_ids_option =
                        commit_change_ids.get(commit_id).map(|changes| {
                            let vector: Vec<PathId> =
                                changes.iter().map(|change| {
                                    change.0//path_id()
                                }).collect();
                            vector
                        });
                    path_ids_option
                });
            (project_id.clone(), path_ids.flatten().unique().collect())
        }).collect()
    }
}

struct ProjectUsersExtractor {}
impl MapExtractor for ProjectUsersExtractor {
    type Key = ProjectId;
    type Value = Vec<UserId>;
}
impl DoubleMapExtractor for ProjectUsersExtractor {
    type A = BTreeMap<ProjectId, Vec<UserId>>;
    type B = BTreeMap<ProjectId, Vec<UserId>>;
    fn extract(_: &Source, project_authors: &Self::A, project_committers: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        project_authors.iter().map(|(project_id, authors)| {
            let mut users: Vec<UserId> = vec![];
            let committers = project_committers.get(project_id);
            if let Some(committers) = committers {
                users.extend(committers.iter().map(|user_id| user_id.clone()));
            }
            users.extend(authors.iter().map(|user_id| user_id.clone()));
            (project_id.clone(), users.into_iter().unique().collect())
        }).collect()
    }
}

struct ProjectAuthorsExtractor {}
impl MapExtractor for ProjectAuthorsExtractor {
    type Key = ProjectId;
    type Value = Vec<UserId>;
}
impl DoubleMapExtractor for ProjectAuthorsExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Commit>;
    fn extract(_: &Source, project_commits: &Self::A, commits: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        project_commits.iter().map(|(project_id, commit_ids)| {
            (project_id.clone(), commit_ids.iter().flat_map(|commit_id| {
                commits.get(commit_id).map(|c| c.author_id())
            }).unique().collect())
        }).collect()
    }
}

struct ProjectCommittersExtractor {}
impl MapExtractor for ProjectCommittersExtractor {
    type Key = ProjectId;
    type Value = Vec<UserId>;
}
impl DoubleMapExtractor for ProjectCommittersExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Commit>;
    fn extract(_: &Source, project_commits: &Self::A, commits: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        project_commits.iter().map(|(project_id, commit_ids)| {
            (project_id.clone(), commit_ids.iter().flat_map(|commit_id| {
                commits.get(commit_id).map(|c| c.committer_id())
            }).unique().collect())
        }).collect()
    }
}

struct CountPerKeyExtractor<K: Clone + Ord + Persistent, V>(PhantomData<(K, V)>);
impl<K, V> MapExtractor for CountPerKeyExtractor<K, V> where K: Clone + Ord + Persistent + Weighed {
    type Key = K;
    type Value = usize;
}
impl<K, V> SingleMapExtractor for CountPerKeyExtractor<K, V> where K: Clone + Ord + Persistent + Weighed {
    type A = BTreeMap<K, Vec<V>>;

    fn extract(_: &Source, primary: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        primary.iter().map(|(key, value)| (key.clone(), value.len())).collect()
    }
}

struct ProjectCommitsExtractor {}
impl ProjectCommitsExtractor {
    fn commits_from_head(commits: &BTreeMap<CommitId, Commit>, head: &CommitId) -> BTreeSet<CommitId> {
        let mut commits_in_head: BTreeSet<CommitId> = BTreeSet::new();
        let mut stack = vec![head.clone()];
        let mut visited: BTreeSet<CommitId> = BTreeSet::new();
        while !stack.is_empty() {
            let commit_id = stack.pop().unwrap();
            if !visited.insert(commit_id) { continue } // If the set **did have** this value present, `false` is returned.
            commits_in_head.insert(commit_id);
            if let Some(commit) = commits.get(&commit_id) {// Potentially explosive?
                let parents = commit.parent_ids();
                stack.extend(parents)
            } else {
                eprintln!("WARNING: commit id {} was found as a parent of another commit, but it \
                           does not have a commit associated with it", commit_id)
            }
        }
        commits_in_head
    }
}
impl MapExtractor for ProjectCommitsExtractor {
    type Key = ProjectId;
    type Value = Vec<CommitId>;
}
impl DoubleMapExtractor for ProjectCommitsExtractor {
    type A = BTreeMap<ProjectId, Vec<Head>>;
    type B = BTreeMap<CommitId, Commit>;
    fn extract(_: &Source, heads: &Self::A, commits: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        heads.iter().map(|(project_id, heads)| {
            (project_id.clone(),
             heads.iter().flat_map(|head| {
                 Self::commits_from_head(commits, &head.commit_id())
             }).collect::<BTreeSet<CommitId>>())
        }).map(|(project_id, commits)| {
            (project_id, Vec::from_iter(commits.into_iter()))
        }).collect()
    }
}

struct ProjectLifetimesExtractor {}
impl MapExtractor for ProjectLifetimesExtractor {
    type Key = ProjectId;
    type Value = u64;
}
impl TripleMapExtractor for ProjectLifetimesExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, i64>;
    type C = BTreeMap<CommitId, i64>;
    fn extract(_: &Source, 
               project_commits: &Self::A,
               authored_timestamps: &Self::B,
               committed_timestamps: &Self::B) -> BTreeMap<Self::Key, Self::Value> {

       project_commits.iter().flat_map(|(project_id, commit_ids)| {
           let min_max =
               commit_ids.iter()
                   .flat_map(|commit_id: &CommitId| {
                       let mut timestamps: Vec<i64> = Vec::new();
                       let author_timestamp = authored_timestamps.get(commit_id);
                       let committer_timestamp = committed_timestamps.get(commit_id);
                       if let Some(timestamp) = author_timestamp { timestamps.push(*timestamp) }
                       if let Some(timestamp) = committer_timestamp { timestamps.push(*timestamp) }
                       timestamps
                   })
                   .minmax();

           match min_max {
               MinMaxResult::NoElements => { None }
               MinMaxResult::OneElement(_) => { Some((project_id.clone(), 0)) }
               MinMaxResult::MinMax(min, max) => { Some((project_id.clone(), (max - min) as u64)) }
           }
       }).collect()
    }
}

struct UserExtractor {}
impl MapExtractor for UserExtractor {
    type Key = UserId;
    type Value = User;
}
impl SourceMapExtractor for UserExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.user_emails().map(|(id, email)| {
            (UserId::from(id), User::new(UserId::from(id), email))
        }).collect()
    }
}

struct UserAuthoredCommitsExtractor {}
impl MapExtractor for UserAuthoredCommitsExtractor {
    type Key = UserId;
    type Value = Vec<CommitId>;
}
impl SingleMapExtractor for UserAuthoredCommitsExtractor {
    type A = BTreeMap<CommitId, Commit>;
    fn extract(_: &Source, commits: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        commits.iter()
            .map(|(commit_id, commit)| {
                (commit.author_id().clone(), commit_id.clone(), )
            }).into_group_map()
            .into_iter()
            .collect()
    }
}

struct UserExperienceExtractor {}
impl MapExtractor for UserExperienceExtractor {
    type Key = UserId;
    type Value = u64;
}
impl DoubleMapExtractor for UserExperienceExtractor  {
    type A = BTreeMap<UserId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, i64>;
    fn extract(_: &Source, user_commits: &Self::A, timestamps: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        user_commits.iter()
        .flat_map(|(user_id, commit_ids)| {
            let min_max = commit_ids.iter()
                .flat_map(|commit_id| {
                    timestamps.get(commit_id).pirate()
                })
                .minmax();

            match min_max {
                MinMaxResult::NoElements => None,
                MinMaxResult::OneElement(_) => Some((user_id.clone(), 0)),
                MinMaxResult::MinMax(min, max) => Some((user_id.clone(), (max - min) as u64)),
            }
        }).collect()
    }
}

struct CombinedUserExperienceExtractor {}
impl MapExtractor for CombinedUserExperienceExtractor {
    type Key = UserId;
    type Value = u64;
}
impl TripleMapExtractor for CombinedUserExperienceExtractor  {
    type A = BTreeMap<UserId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, i64>;
    type C = BTreeMap<CommitId, i64>;
    fn extract(_: &Source, user_commits: &Self::A, authored_timestamps: &Self::B, committed_timestamps: &Self::C) -> BTreeMap<Self::Key, Self::Value> {
        user_commits.iter()
            .flat_map(|(user_id, commit_ids)| {
                let min_max = commit_ids.iter()
                    .flat_map(|commit_id| {
                        let mut timestamps: Vec<i64> = Vec::new();
                        let authored_timestamp = authored_timestamps.get(commit_id).pirate();
                        let committed_timestamp = committed_timestamps.get(commit_id).pirate();
                        if let Some(timestamp) = authored_timestamp { timestamps.push(timestamp) }
                        if let Some(timestamp) = committed_timestamp { timestamps.push(timestamp) }
                        timestamps
                    })
                    .minmax();

                match min_max {
                    MinMaxResult::NoElements => None,
                    MinMaxResult::OneElement(_) => Some((user_id.clone(), 0)),
                    MinMaxResult::MinMax(min, max) => Some((user_id.clone(), (max - min) as u64)),
                }
            }).collect()
    }
}

struct PathExtractor {}
impl MapExtractor for PathExtractor {
    type Key = PathId;
    type Value = Path;
}
impl SourceMapExtractor for PathExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.paths().map(|(id, path)| {
            (id.clone(), Path::new(id, path))
        }).collect()
    }
}

// FIXME impl path_shas

struct SnapshotExtractor {}
impl MapExtractor for SnapshotExtractor {
    type Key = SnapshotId;
    type Value = Snapshot;
}
impl SourceMapExtractor for SnapshotExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.snapshot_bytes().map(|(id, contents)| {
             (id.clone(), Snapshot::new(id, contents))
        }).collect()
        // FIXME snapshots this shouldn't exist, right?
    }
}

struct CommitExtractor {}
impl MapExtractor for CommitExtractor {
    type Key = CommitId;
    type Value = Commit;
}
impl SourceMapExtractor for CommitExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.commit_info().map(|(id, basics)| {
            (id, Commit::new(id, basics.committer, basics.author, basics.parents))
        }).collect()
    }
}

struct CommitHashExtractor {}
impl MapExtractor for CommitHashExtractor {
    type Key = CommitId;
    type Value = String;
}
impl SourceMapExtractor for CommitHashExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.commit_hashes().collect()
    }
}

struct CommitMessageExtractor {}
impl MapExtractor for CommitMessageExtractor {
    type Key = CommitId;
    type Value = String;
}
impl SourceMapExtractor for CommitMessageExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.commit_info()
            .map(|(id, basics)| (id, basics.message))
            .collect()
    }
}

struct CommitterTimestampExtractor {}
impl MapExtractor for CommitterTimestampExtractor {
    type Key = CommitId;
    type Value = i64;
}
impl SourceMapExtractor for CommitterTimestampExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.commit_info().map(|(id, commit)| {
            (id, commit.committer_time)
        }).collect()
    }
}

pub type ChangeTuple = (PathId, Option<SnapshotId>); // This is a tuple and not a struct for performance reasons.
struct CommitChangesExtractor {}
impl MapExtractor for CommitChangesExtractor {
    type Key = CommitId;
    type Value = Vec<ChangeTuple>;
}
impl SourceMapExtractor for CommitChangesExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.commit_info()
            .map(|(commit_id,info)| (commit_id, info.changes))
            .collect()
    }
}

struct AuthorTimestampExtractor {}
impl MapExtractor for AuthorTimestampExtractor {
    type Key = CommitId;
    type Value = i64; // TODO wrap
}
impl SourceMapExtractor for AuthorTimestampExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.commit_info().map(|(id, commit)| {
            (id, commit.author_time)
        }).collect()
    }
}

struct SnapshotLocsExtractor{}
impl MapExtractor for SnapshotLocsExtractor {
    type Key = SnapshotId;
    type Value = usize;
}
impl SourceMapExtractor for SnapshotLocsExtractor {

    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.snapshot_bytes().map(|(id, contents)| {

            let snapshot = Snapshot::new(id, contents);
            let contents = snapshot.contents_owned();
            
            (id.clone(), contents.matches("\n").count())
       }).collect()
    }
}

struct ProjectLocsExtractor{} 
impl MapExtractor for ProjectLocsExtractor{
    type Key = ProjectId;
    type Value = usize;
}
impl QuadrupleMapExtractor for ProjectLocsExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, i64>;
    type C = BTreeMap<CommitId, Vec<ChangeTuple>>;
    type D = BTreeMap<SnapshotId, usize>;
    fn extract(_: &Source, project_commits: &Self::A, commit_timestamps: &Self::B, commit_changes: &Self::C, snapshot_locs: &Self::D) -> BTreeMap<Self::Key, Self::Value> {
        // TODO: We should look after parent commits rather than timestamps. 
        project_commits.iter().map(|(project_id, commit_ids)| {
            let mut last_state_files : BTreeMap<PathId, usize> = BTreeMap::new();
            let mut last_timestamp : BTreeMap<PathId, i64> = BTreeMap::new();
            
            for commit_i in 0..commit_ids.len() {
                
                let commit = &commit_ids[commit_i];
                let changes = commit_changes.get(commit).unwrap();

                for change_i in 0..changes.len() {
                    let path = &changes[change_i].0;
                    let current_timestamp = commit_timestamps.get(commit).unwrap();
                    if !last_state_files.contains_key(path) ||  *current_timestamp > *last_timestamp.get(path).unwrap(){
                        
                        let snapshot_id = changes[change_i].1;

                        if !snapshot_id.is_none() {
                            let count_locs = snapshot_locs.get(&(snapshot_id).unwrap());
                            if !count_locs.is_none() {
                                last_timestamp.insert(*path, *current_timestamp);
                                last_state_files.insert(*path, *count_locs.unwrap());
                            }
                        }
                        
                        
                    }
                }
                
            }

            let vec_locs : Vec<usize> = last_state_files.values().cloned().collect();

            (project_id.clone(), vec_locs.iter().sum())

        }).collect()
    }
}
            
struct CommitProjectsExtractor {}
impl MapExtractor for CommitProjectsExtractor {
    type Key = CommitId;
    type Value = Vec<ProjectId>;
}

impl SingleMapExtractor for CommitProjectsExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    fn extract(_: &Source, project_commits: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        let mut result = BTreeMap::<CommitId, Vec<ProjectId>>::new();
        project_commits.iter().for_each(|(pid, commits)| {
            commits.iter().for_each(|cid|{
                match result.entry(*cid) {
                    Entry::Vacant(e) => { e.insert(vec!{*pid}); },
                    Entry::Occupied(mut e) => { e.get_mut().push(*pid); },
                }
            });
        });
        return result;
    }
}

struct SnapshotCloneInfo {
    original : ProjectId,
    oldest_commit_time : i64,
    projects : BTreeSet<ProjectId>,
}

impl SnapshotCloneInfo {
    pub fn new() -> SnapshotCloneInfo {
        return SnapshotCloneInfo{
            original : ProjectId(0),
            oldest_commit_time: i64::MAX,
            projects : BTreeSet::new(),
        };
    }
}

struct SnapshotProjectsExtractor {}
impl MapExtractor for SnapshotProjectsExtractor {
    type Key = SnapshotId;
    type Value = (usize, ProjectId);
}
impl TripleMapExtractor for SnapshotProjectsExtractor {
    type A = BTreeMap<CommitId, Vec<ChangeTuple>>;
    type B = BTreeMap<CommitId, Vec<ProjectId>>;
    type C = BTreeMap<CommitId, i64>;
    //type D = ProjectMetadataSource;

    fn extract (_: &Source, commit_changes : &Self::A, commit_projects : &Self::B, commit_author_timestamps : &Self::C) -> BTreeMap<SnapshotId, (usize, ProjectId)> {
        // first for each snapshot get projects and 
        let mut snapshot_projects = BTreeMap::<SnapshotId, SnapshotCloneInfo>::new();
        // for each commit
        commit_changes.iter().for_each(|(cid, changes)| {
            let commit_time = *commit_author_timestamps.get(cid).unwrap();
            // for each snapshot
            changes.iter().for_each(|(_path_id, sid_option)| {
                // if it is actually a snapshot (i.e. we have its code)
                // TODO we might want to extend this to *all* snapshots even those we do not have the code for
                if let Some(sid) = sid_option {
                    // if it is not a delete TODO: can it be a delete, or are deletes deleted
                    if *sid != SnapshotId(0) {
                        let ref mut sinfo = snapshot_projects.entry(*sid).or_insert_with(|| { SnapshotCloneInfo::new() });
                        // add the projects of the commit to the projects of the snapshot 
                        if let Some(pids) = commit_projects.get(cid) {
                            pids.iter().for_each(|pid| { sinfo.projects.insert(*pid); });
                            // if the commit is older than the current time associated with the snapshot, determine the oldest project 
                            if sinfo.oldest_commit_time > commit_time {
                                // TODO use oldest project really once we know how to get it, for now I am just using the first project
                                if let Some(pid) = pids.get(0) {
                                    sinfo.original = *pid;
                                }
                            }
                        }
                    }
                }
            });
        });
        // once we have a vector of projects for each snapshot, we want to collapse these to the number of unique projects and the original project. To calculate the original project, we get projects and select the one with smallest creation time
        return snapshot_projects.iter().map(|(sid, sinfo)| {
            return (*sid, (sinfo.projects.len(), sinfo.original));
        }).collect();
    }
}

struct ProjectUniqueFilesExtractor {}
impl MapExtractor for ProjectUniqueFilesExtractor {
    type Key = ProjectId;
    type Value = usize;
}

impl TripleMapExtractor for ProjectUniqueFilesExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Vec<ChangeTuple>>;
    type C = BTreeMap<SnapshotId, (usize, ProjectId)>;

    fn extract (_: &Source, project_commits : &Self::A, commit_changes : &Self::B, snapshot_projects : &Self::C) -> BTreeMap<ProjectId, usize> {
        // visited snapshots so that we only add each snapshot once (original & unique snapshots can be cloned within project too)
        let mut visited = BTreeSet::<SnapshotId>::new();
        return project_commits.iter().map(|(pid, commits)| {
            // for all commits
            let unique_files = commits.iter().map(|cid| {
                // for all changes with snapshots
                if let Some(changes) = commit_changes.get(cid) {
                    changes.iter().map(|(_path_id, snapshot)| {
                        if let Some(snapshot_id) = snapshot {
                            if visited.insert(*snapshot_id) {
                                return match snapshot_projects.get(snapshot_id) {
                                    Some((1, _)) => 1,
                                    _ => 0,
                                }
                            }
                        }
                        0
                    }).sum::<usize>()
                } else {
                    println!("No commit changes for commit : {}", cid);
                    0
                }
            }).sum();
            return (*pid, unique_files);
        }).collect();        
    }
}

struct ProjectOriginalFilesExtractor {}
impl MapExtractor for ProjectOriginalFilesExtractor {
    type Key = ProjectId;
    type Value = usize;
}

impl TripleMapExtractor for ProjectOriginalFilesExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Vec<ChangeTuple>>;
    type C = BTreeMap<SnapshotId, (usize, ProjectId)>;

    fn extract (_: &Source, project_commits : &Self::A, commit_changes : &Self::B, snapshot_projects : &Self::C) -> BTreeMap<ProjectId, usize> {
        // visited snapshots so that we only add each snapshot once (original & unique snapshots can be cloned within project too)
        let mut visited = BTreeSet::<SnapshotId>::new();
        return project_commits.iter().map(|(pid, commits)| {
            // for all commits
            let unique_files = commits.iter().map(|cid| {
                // for all changes with snapshots
                if let Some(changes) = commit_changes.get(cid) {
                    changes.iter().map(|(_path_id, snapshot)| {
                        if let Some(snapshot_id) = snapshot {
                            if visited.insert(*snapshot_id) {
                                if let Some((copies, original)) = snapshot_projects.get(snapshot_id) {
                                    if original == pid && *copies > 1 {
                                        return 1;
                                    }
                                }
                            }
                        } 
                        0
                    }).sum::<usize>()
                } else {
                    println!("No commit changes for commit : {}", cid);
                    0
                }
            }).sum();
            return (*pid, unique_files);
        }).collect();        
    }
}

struct ProjectImpactExtractor {}
impl MapExtractor for ProjectImpactExtractor {
    type Key = ProjectId;
    type Value = usize;
}

impl TripleMapExtractor for ProjectImpactExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Vec<ChangeTuple>>;
    type C = BTreeMap<SnapshotId, (usize, ProjectId)>;

    fn extract (_: &Source, project_commits : &Self::A, commit_changes : &Self::B, snapshot_projects : &Self::C) -> BTreeMap<ProjectId, usize> {
        // visited snapshots so that we only add each snapshot once (original & unique snapshots can be cloned within project too)
        let mut visited = BTreeSet::<SnapshotId>::new();
        return project_commits.iter().map(|(pid, commits)| {
            // for all commits
            let unique_files = commits.iter().map(|cid| {
                // for all changes with snapshots
                if let Some(changes) = commit_changes.get(cid) {
                    changes.iter().map(|(_path_id, snapshot)| {
                        if let Some(snapshot_id) = snapshot {
                            if visited.insert(*snapshot_id) {
                                // so if we are unique, then by definition we are original as well
                                if let Some((copies, original)) = snapshot_projects.get(snapshot_id) {
                                    if original == pid {
                                        return *copies;
                                    }
                                }
                            }
                        } 
                        0
                    }).sum::<usize>()
                } else {
                    println!("No commit changes for commit : {}", cid);
                    0
                }
            }).sum();
            return (*pid, unique_files);
        }).collect();        
    }
}

struct ProjectFilesExtractor {}
impl MapExtractor for ProjectFilesExtractor {
    type Key = ProjectId;
    type Value = usize;
}
impl DoubleMapExtractor for ProjectFilesExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Vec<ChangeTuple>>;

    fn extract (_: &Source, project_commits : &Self::A, commit_changes : &Self::B) -> BTreeMap<ProjectId, usize> {
        project_commits.iter().map(|(pid, commits)| {
            let mut paths = BTreeSet::<PathId>::new();
            for cid in commits {
                if let Some(commits) = commit_changes.get(cid) {
                    for (path_id, _hash) in commits {
                        paths.insert(*path_id);
                    }
                }
            }
            (*pid, paths.len())            
        }).collect()
    }
}

struct ProjectLanguagesExtractor {}
impl MapExtractor for ProjectLanguagesExtractor {
    type Key = ProjectId;
    type Value = Vec<(Language,usize)>;
}
impl TripleMapExtractor for ProjectLanguagesExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Vec<ChangeTuple>>;
    type C = BTreeMap<PathId, Path>;

    fn extract (_: &Source, project_commits : &Self::A, commit_changes : &Self::B, paths : &Self::C) -> BTreeMap<ProjectId, Vec<(Language,usize)>> {
        let mut cached_paths = BTreeMap::<PathId, Language>::new();
        project_commits.iter().map(|(pid, commits)| {
            let mut languages = BTreeMap::<Language, usize>::new();
            for cid in commits {
                if let Some(commits) = commit_changes.get(cid) {
                    for (path_id, hash) in commits {
                        if let Some(_) = hash {
                            let lang = cached_paths.entry(*path_id).or_insert_with(|| {
                                if let Some(language) = paths.get(path_id).unwrap().language() {
                                    language
                                } else {
                                    Language::Other
                                }
                            });
                            match languages.entry(*lang) {
                                Entry::Occupied(mut e) => { *e.get_mut() += 1; },
                                Entry::Vacant(e) => { e.insert(1); },
                            }
                        }
                    }
                }
            }
            let mut langs : Vec<(Language, usize)> = languages.into_iter().collect();
            langs.sort_by(|a, b| b.1.cmp(&a.1) ); // reversed
            (*pid, langs)            
        }).collect()
    }
}

struct ProjectMajorLanguageExtractor {}
impl MapExtractor for ProjectMajorLanguageExtractor {
    type Key = ProjectId;
    type Value = Language;
}
impl SingleMapExtractor for ProjectMajorLanguageExtractor {
    type A = BTreeMap<ProjectId, Vec<(Language, usize)>>;

    fn extract (_: &Source, project_languages : &Self::A) -> BTreeMap<ProjectId, Language> {
        project_languages.iter()
            .filter(|(_pid, langs)| langs.len() > 0)
            .map(|(pid, langs)| (*pid, langs.get(0).unwrap().0))
            .collect()
    }
}

struct ProjectMajorLanguageRatioExtractor {}
impl MapExtractor for ProjectMajorLanguageRatioExtractor {
    type Key = ProjectId;
    type Value = f64;
}
impl SingleMapExtractor for ProjectMajorLanguageRatioExtractor {
    type A = BTreeMap<ProjectId, Vec<(Language, usize)>>;

    fn extract (_: &Source, project_languages : &Self::A) -> BTreeMap<ProjectId, f64> {
        project_languages.iter()
            .filter(|(_pid, langs)| langs.len() > 0)
            .map(|(pid, langs)| (*pid, langs.get(0).unwrap().1 as f64 / langs.iter().map(|(_, count)| *count).sum::<usize>() as f64))
            .collect()
    }
}

struct ProjectMajorLanguageChangesExtractor {}
impl MapExtractor for ProjectMajorLanguageChangesExtractor {
    type Key = ProjectId;
    type Value = usize;
}
impl SingleMapExtractor for ProjectMajorLanguageChangesExtractor {
    type A = BTreeMap<ProjectId, Vec<(Language, usize)>>;

    fn extract (_: &Source, project_languages : &Self::A) -> BTreeMap<ProjectId, usize> {
        project_languages.iter()
            .filter(|(_pid, langs)| langs.len() > 0)
            .map(|(pid, langs)| (*pid, langs.get(0).unwrap().1))
            .collect()
    }
}



pub(crate) struct Data {
    project_metadata:            ProjectMetadataSource,
    project_substores:           PersistentMap<ProjectSubstoreExtractor>,
    project_urls:                PersistentMap<ProjectUrlExtractor>,
    project_heads:               PersistentMap<ProjectHeadsExtractor>,
    project_paths:               PersistentMap<ProjectPathsExtractor>,
    project_snapshots:           PersistentMap<ProjectSnapshotsExtractor>,
    project_users:               PersistentMap<ProjectUsersExtractor>,
    project_authors:             PersistentMap<ProjectAuthorsExtractor>,
    project_committers:          PersistentMap<ProjectCommittersExtractor>,
    project_commits:             PersistentMap<ProjectCommitsExtractor>,
    project_lifetimes:           PersistentMap<ProjectLifetimesExtractor>,

    project_path_count:          PersistentMap<CountPerKeyExtractor<ProjectId, PathId>>,
    project_snapshot_count:      PersistentMap<CountPerKeyExtractor<ProjectId, SnapshotId>>,
    project_user_count:          PersistentMap<CountPerKeyExtractor<ProjectId, UserId>>,
    project_author_count:        PersistentMap<CountPerKeyExtractor<ProjectId, UserId>>,
    project_committer_count:     PersistentMap<CountPerKeyExtractor<ProjectId, UserId>>,
    project_commit_count:        PersistentMap<CountPerKeyExtractor<ProjectId, CommitId>>,

    project_unique_files:        PersistentMap<ProjectUniqueFilesExtractor>,
    project_original_files:      PersistentMap<ProjectOriginalFilesExtractor>,
    project_impact:              PersistentMap<ProjectImpactExtractor>,
    project_files:               PersistentMap<ProjectFilesExtractor>,
    project_languages:           PersistentMap<ProjectLanguagesExtractor>,
    project_languages_count:     PersistentMap<CountPerKeyExtractor<ProjectId, (Language,usize)>>,
    project_major_language:      PersistentMap<ProjectMajorLanguageExtractor>,
    project_major_language_ratio: PersistentMap<ProjectMajorLanguageRatioExtractor>,
    project_major_language_changes: PersistentMap<ProjectMajorLanguageChangesExtractor>,

    project_buggy_issue_count:   PersistentMap<ProjectBuggyIssuesExtractor>,
    project_issue_count:         PersistentMap<ProjectBuggyIssuesExtractor>,
    project_is_fork:             PersistentMap<ProjectIsForkExtractor>,
    project_is_archived:         PersistentMap<ProjectIsArchivedExtractor>,
    project_is_disabled:         PersistentMap<ProjectIsDisabledExtractor>,
    project_star_gazer_count:    PersistentMap<ProjectStargazersExtractor>,
    project_watcher_count:       PersistentMap<ProjectWatchersExtractor>,
    project_project_size:        PersistentMap<ProjectSizeExtractor>,
    project_open_issue_count:    PersistentMap<ProjectIssuesExtractor>,
    project_fork_count:          PersistentMap<ProjectForksExtractor>,
    project_subscriber_count:    PersistentMap<ProjectSubscribersExtractor>,
    project_license:             PersistentMap<ProjectLicenseExtractor>,
    project_language:            PersistentMap<ProjectLanguageExtractor>,
    project_description:         PersistentMap<ProjectDescriptionExtractor>,
    project_homepage:            PersistentMap<ProjectHomepageExtractor>,
    project_has_issues:          PersistentMap<ProjectHasIssuesExtractor>,
    project_has_downloads:       PersistentMap<ProjectHasDownloadsExtractor>,
    project_has_wiki:            PersistentMap<ProjectHasWikiExtractor>,
    project_has_pages:           PersistentMap<ProjectHasPagesExtractor>,
    project_created:             PersistentMap<ProjectCreatedExtractor>,
    project_updated:             PersistentMap<ProjectUpdatedExtractor>,
    project_pushed:              PersistentMap<ProjectPushedExtractor>,
    project_default_branch:      PersistentMap<ProjectDefaultBranchExtractor>,

    users:                       PersistentMap<UserExtractor>,
    user_authored_commits:       PersistentMap<UserAuthoredCommitsExtractor>,
    user_committed_commits:      PersistentMap<UserAuthoredCommitsExtractor>,
    user_author_experience:      PersistentMap<UserExperienceExtractor>,
    user_committer_experience:   PersistentMap<UserExperienceExtractor>,
    user_experience:             PersistentMap<CombinedUserExperienceExtractor>,

    user_authored_commit_count:  PersistentMap<CountPerKeyExtractor<UserId, CommitId>>,
    user_committed_commit_count: PersistentMap<CountPerKeyExtractor<UserId, CommitId>>,

    paths:                       PersistentMap<PathExtractor>,
    //snapshots:                   PersistentMap<SnapshotExtractor>,

    commits:                     PersistentMap<CommitExtractor>,
    commit_hashes:               PersistentMap<CommitHashExtractor>,
    commit_messages:             PersistentMap<CommitMessageExtractor>,
    commit_author_timestamps:    PersistentMap<AuthorTimestampExtractor>,
    commit_committer_timestamps: PersistentMap<CommitterTimestampExtractor>,
    commit_changes:              PersistentMap<CommitChangesExtractor>,

    commit_change_count:         PersistentMap<CountPerKeyExtractor<CommitId, ChangeTuple>>,

    commit_projects:             PersistentMap<CommitProjectsExtractor>,
    commit_projects_count:       PersistentMap<CountPerKeyExtractor<CommitId, ProjectId>>,
    snapshot_projects :          PersistentMap<SnapshotProjectsExtractor>,

    // TODO frequency of commits/regularity of commits
    // TODO maybe some of these could be pre-cached all at once (eg all commit properties)

    project_longest_inactivity_streak:    PersistentMap<LongestInactivityStreakExtractor>,
    avg_commit_rate:              PersistentMap<AvgCommitRateExtractor>,
    project_time_since_last_commit:       PersistentMap<TimeSinceLastCommitExtractor>,
    is_abandoned:                 PersistentMap<IsAbandonedExtractor>,
    snapshot_locs:                 PersistentMap<SnapshotLocsExtractor>,
    project_locs:                 PersistentMap<ProjectLocsExtractor>
}

impl Data {
    pub fn new(/*source: DataSource,*/ cache_dir: CacheDir, log: Log) -> Data {
        let dir = cache_dir.as_string();
        Data {
            project_metadata:               ProjectMetadataSource::new(log.clone(),dir.clone()),
   
            project_urls:                   PersistentMap::new(CACHE_FILE_PROJECT_URL,                    log.clone(),dir.clone()).without_cache(),
            project_substores:              PersistentMap::new(CACHE_FILE_PROJECT_SUBSTORE,               log.clone(),dir.clone()).without_cache(),
            project_heads:                  PersistentMap::new(CACHE_FILE_PROJECT_HEADS,                  log.clone(),dir.clone()),
            project_paths:                  PersistentMap::new(CACHE_FILE_PROJECT_PATHS,                  log.clone(),dir.clone()),
            project_path_count:             PersistentMap::new(CACHE_FILE_PROJECT_PATH_COUNT,             log.clone(),dir.clone()),
            project_snapshots:              PersistentMap::new(CACHE_FILE_PROJECT_SNAPSHOTS,              log.clone(),dir.clone()),
            project_snapshot_count:         PersistentMap::new(CACHE_FILE_PROJECT_SNAPSHOT_COUNT,         log.clone(),dir.clone()),
            project_users:                  PersistentMap::new(CACHE_FILE_PROJECT_USERS,                  log.clone(),dir.clone()),
            project_user_count:             PersistentMap::new(CACHE_FILE_PROJECT_USER_COUNT,             log.clone(),dir.clone()),
            project_authors:                PersistentMap::new(CACHE_FILE_PROJECT_AUTHORS,                log.clone(),dir.clone()),
            project_author_count:           PersistentMap::new(CACHE_FILE_PROJECT_AUTHOR_COUNT,           log.clone(),dir.clone()),
            project_committers:             PersistentMap::new(CACHE_FILE_PROJECT_COMMITTERS,             log.clone(),dir.clone()),
            project_committer_count:        PersistentMap::new(CACHE_FILE_PROJECT_COMMITTER_COUNT,        log.clone(),dir.clone()),
            project_commits:                PersistentMap::new(CACHE_FILE_PROJECT_COMMITS,                log.clone(),dir.clone()),
            project_commit_count:           PersistentMap::new(CACHE_FILE_PROJECT_COMMIT_COUNT,           log.clone(),dir.clone()),
            project_lifetimes:              PersistentMap::new(CACHE_FILE_PROJECT_LIFETIME,               log.clone(),dir.clone()),
            project_issue_count:            PersistentMap::new(CACHE_FILE_PROJECT_ISSUE_COUNT,            log.clone(),dir.clone()),
            project_buggy_issue_count:      PersistentMap::new(CACHE_FILE_PROJECT_BUGGY_ISSUE_COUNT,      log.clone(),dir.clone()),
            project_open_issue_count:       PersistentMap::new(CACHE_FILE_PROJECT_OPEN_ISSUE_COUNT,       log.clone(),dir.clone()),
            project_is_fork:                PersistentMap::new(CACHE_FILE_PROJECT_IS_FORK,                log.clone(),dir.clone()),
            project_is_archived:            PersistentMap::new(CACHE_FILE_PROJECT_IS_ARCHIVED,            log.clone(),dir.clone()),
            project_is_disabled:            PersistentMap::new(CACHE_FILE_PROJECT_IS_DISABLED,            log.clone(),dir.clone()),
            project_star_gazer_count:       PersistentMap::new(CACHE_FILE_PROJECT_STARGAZER_COUNT,        log.clone(),dir.clone()),
            project_watcher_count:          PersistentMap::new(CACHE_FILE_PROJECT_WATCHER_COUNT,          log.clone(),dir.clone()),
            project_project_size:           PersistentMap::new(CACHE_FILE_PROJECT_SIZE,                   log.clone(),dir.clone()),
            project_fork_count:             PersistentMap::new(CACHE_FILE_PROJECT_FORK_COUNT,             log.clone(),dir.clone()),
            project_subscriber_count:       PersistentMap::new(CACHE_FILE_PROJECT_SUBSCRIBER_COUNT,       log.clone(),dir.clone()),
            project_license:                PersistentMap::new(CACHE_FILE_PROJECT_LICENSE,                log.clone(),dir.clone()),
            project_language:               PersistentMap::new(CACHE_FILE_PROJECT_LANGUAGE,               log.clone(),dir.clone()),
            project_description:            PersistentMap::new(CACHE_FILE_PROJECT_DESCRIPTION,            log.clone(),dir.clone()),
            project_homepage:               PersistentMap::new(CACHE_FILE_PROJECT_HOMEPAGE,               log.clone(),dir.clone()),
            project_has_issues:             PersistentMap::new(CACHE_FILE_PROJECT_HAS_ISSUES,             log.clone(),dir.clone()),
            project_has_downloads:          PersistentMap::new(CACHE_FILE_PROJECT_HAS_DOWNLOADS,          log.clone(),dir.clone()),
            project_has_wiki:               PersistentMap::new(CACHE_FILE_PROJECT_HAS_WIKI,               log.clone(),dir.clone()),
            project_has_pages:              PersistentMap::new(CACHE_FILE_PROJECT_HAS_PAGES,              log.clone(),dir.clone()),
            project_created:                PersistentMap::new(CACHE_FILE_PROJECT_CREATED,                log.clone(),dir.clone()),
            project_updated:                PersistentMap::new(CACHE_FILE_PROJECT_UPDATED,                log.clone(),dir.clone()),
            project_pushed:                 PersistentMap::new(CACHE_FILE_PROJECT_PUSHED,                 log.clone(),dir.clone()),
            project_default_branch:         PersistentMap::new(CACHE_FILE_PROJECT_DEFAULT_BRANCH,         log.clone(),dir.clone()),
            project_unique_files:           PersistentMap::new(CACHE_FILE_PROJECT_UNIQUE_FILES,           log.clone(),dir.clone()),
            project_original_files:         PersistentMap::new(CACHE_FILE_PROJECT_ORIGINAL_FILES,         log.clone(),dir.clone()),
            project_impact:                 PersistentMap::new(CACHE_FILE_PROJECT_IMPACT,                 log.clone(),dir.clone()),
            project_files:                  PersistentMap::new(CACHE_FILE_PROJECT_FILES,                  log.clone(), dir.clone()),
            project_languages:              PersistentMap::new(CACHE_FILE_PROJECT_LANGUAGES,              log.clone(), dir.clone()),
            project_languages_count:        PersistentMap::new(CACHE_FILE_PROJECT_LANGUAGES_COUNT,        log.clone(), dir.clone()),
            project_major_language:         PersistentMap::new(CACHE_FILE_PROJECT_MAJOR_LANGUAGE,         log.clone(), dir.clone()),
            project_major_language_ratio:   PersistentMap::new(CACHE_FILE_PROJECT_MAJOR_LANGUAGE_RATIO,   log.clone(), dir.clone()),
            project_major_language_changes: PersistentMap::new(CACHE_FILE_PROJECT_MAJOR_LANGUAGE_CHANGES, log.clone(), dir.clone()),
            users:                          PersistentMap::new(CACHE_FILE_USERS,                          log.clone(),dir.clone()).without_cache(),
            user_authored_commits:          PersistentMap::new(CACHE_FILE_USER_AUTHORED_COMMITS,          log.clone(),dir.clone()),
            user_committed_commits:         PersistentMap::new(CACHE_FILE_USER_COMMITTED_COMMITS,         log.clone(),dir.clone()),
            user_author_experience:         PersistentMap::new(CACHE_FILE_USER_AUTHOR_EXPERIENCE,         log.clone(),dir.clone()),
            user_committer_experience:      PersistentMap::new(CACHE_FILE_USER_COMMITTER_EXPERIENCE,      log.clone(),dir.clone()),
            user_experience:                PersistentMap::new(CACHE_FILE_USER_EXPERIENCE,                log.clone(),dir.clone()),
            user_authored_commit_count:     PersistentMap::new(CACHE_FILE_USER_AUTHORED_COMMIT_COUNT,     log.clone(),dir.clone()),
            user_committed_commit_count:    PersistentMap::new(CACHE_FILE_USER_COMMITTED_COMMIT_COUNT,    log.clone(),dir.clone()),
            paths:                          PersistentMap::new(CACHE_FILE_PATHS,                          log.clone(),dir.clone()).without_cache(),
            commits:                        PersistentMap::new(CACHE_FILE_COMMITS,                        log.clone(),dir.clone()),
            commit_hashes:                  PersistentMap::new(CACHE_FILE_COMMIT_HASHES,                  log.clone(),dir.clone()).without_cache(),
            commit_messages:                PersistentMap::new(CACHE_FILE_COMMIT_MESSAGES,                log.clone(),dir.clone()).without_cache(),
            commit_author_timestamps:       PersistentMap::new(CACHE_FILE_COMMIT_AUTHOR_TIMESTAMPS,       log.clone(),dir.clone()),
            commit_committer_timestamps:    PersistentMap::new(CACHE_FILE_COMMIT_COMMITTER_TIMESTAMPS,    log.clone(),dir.clone()),
            commit_changes:                 PersistentMap::new(CACHE_FILE_COMMIT_CHANGES,                 log.clone(),dir.clone()).without_cache(),
            commit_change_count:            PersistentMap::new(CACHE_FILE_COMMIT_CHANGE_COUNT,            log.clone(),dir.clone()),
            commit_projects:                PersistentMap::new(CACHE_FILE_COMMIT_PROJECTS,                log.clone(),dir.clone()),
            commit_projects_count:          PersistentMap::new(CACHE_FILE_COMMIT_PROJECTS_COUNT,          log.clone(),dir.clone()),
            snapshot_projects:              PersistentMap::new(CACHE_FILE_SNAPSHOT_PROJECTS,              log.clone(),dir.clone()),
            project_longest_inactivity_streak:   PersistentMap::new(CACHE_FILE_LONGEST_INACTIVITTY_STREAK, log.clone(), dir.clone()),
            avg_commit_rate:                PersistentMap::new(CACHE_FILE_AVG_COMMIT_RATE, log.clone(), dir.clone()),
            project_time_since_last_commit:      PersistentMap::new(CACHE_FILE_TIME_SINCE_LAST_COMMIT, log.clone(), dir.clone()),
            is_abandoned:                   PersistentMap::new(CACHE_FILE_IS_ABANDONED, log.clone(), dir.clone()),
            snapshot_locs:                  PersistentMap::new(CACHE_FILE_SNAPSHOT_LOCS, log.clone(), dir.clone()),
            project_locs:                   PersistentMap::new(CACHE_FILE_PROJECT_LOCS, log.clone(), dir.clone()),
        }
    }
}

impl Data { // Prequincunx, sort of
    pub fn all_project_ids(&mut self, source: &Source) -> Vec<ProjectId> {
        self.smart_load_project_urls(source).keys().collect::<Vec<&ProjectId>>().pirate()
    }
    pub fn all_user_ids(&mut self, source: &Source) -> Vec<UserId> {
        self.smart_load_users(source).keys().collect::<Vec<&UserId>>().pirate()
    }
    pub fn all_path_ids(&mut self, source: &Source) -> Vec<PathId> {
        self.smart_load_paths(source).keys().collect::<Vec<&PathId>>().pirate()
    }
    pub fn all_commit_ids(&mut self, source: &Source) -> Vec<CommitId> {
        self.smart_load_commits(source).keys().collect::<Vec<&CommitId>>().pirate()
    }
}

impl Data { // Quincunx, sort of
    #[allow(dead_code)] pub fn projects<'a>(&'a mut self, source: &Source) -> impl Iterator<Item=Project> + 'a {
        self.smart_load_project_urls(source).iter().map(|(id, url)| Project::new(id.clone(), url.clone()))
    }

    #[allow(dead_code)] pub fn users<'a>(&'a mut self, source: &Source) -> impl Iterator<Item=&'a User> + 'a {
        self.smart_load_users(source).iter().map(|(_, user)| user)
    }

    #[allow(dead_code)] pub fn paths<'a>(&'a mut self, source: &Source) -> impl Iterator<Item=&'a Path> + 'a {
        self.smart_load_paths(source).iter().map(|(_, path)| path)
    }

    #[allow(dead_code)] pub fn commits<'a>(&'a mut self, source: &Source) -> impl Iterator<Item=&'a Commit> + 'a {
        self.smart_load_commits(source).iter().map(|(_, commit)| commit)
    }
}

impl Data {
    pub fn project(&mut self, source: &Source, id: &ProjectId) -> Option<Project> {
        self.smart_load_project_urls(source).get(id)
            .map(|url| Project::new(id.clone(), url.clone()))
    }
    pub fn project_issues(&mut self, source: &Source, id: &ProjectId) -> Option<usize> {
        self.smart_load_project_issues(source).get(id).pirate()
    }
    pub fn project_buggy_issues(&mut self, source: &Source, id: &ProjectId) -> Option<usize> {
        self.smart_load_project_buggy_issues(source).get(id).pirate()
    }
    pub fn project_is_fork(&mut self, source: &Source, id: &ProjectId) -> Option<bool> {
        self.smart_load_project_is_fork(source).get(id).pirate()
    }
    pub fn project_is_archived(&mut self, source: &Source, id: &ProjectId) -> Option<bool> {
        self.smart_load_project_is_archived(source).get(id).pirate()
    }
    pub fn project_is_disabled(&mut self, source: &Source, id: &ProjectId) -> Option<bool> {
        self.smart_load_project_is_disabled(source).get(id).pirate()
    }
    pub fn project_star_gazer_count(&mut self, source: &Source, id: &ProjectId) -> Option<usize> {
        self.smart_load_project_star_gazer_count(source).get(id).pirate()
    }

    pub fn project_watcher_count(&mut self, source: &Source, id: &ProjectId) -> Option<usize> {
        self.smart_load_project_watcher_count(source).get(id).pirate()
    }
    pub fn project_size(&mut self, source: &Source, id: &ProjectId) -> Option<usize> {
        self.smart_load_project_size(source).get(id).pirate()
    }
    pub fn project_open_issue_count(&mut self, source: &Source, id: &ProjectId) -> Option<usize> {
        self.smart_load_project_open_issue_count(source).get(id).pirate()
    }
    pub fn project_fork_count(&mut self, source: &Source, id: &ProjectId) -> Option<usize> {
        self.smart_load_project_fork_count(source).get(id).pirate()
    }
    pub fn project_subscriber_count(&mut self, source: &Source, id: &ProjectId) -> Option<usize> {
        self.smart_load_project_subscriber_count(source).get(id).pirate()
    }
    pub fn project_license(&mut self, source: &Source, id: &ProjectId) -> Option<String> {
        self.smart_load_project_license(source).get(id).pirate()
    }
    pub fn project_language(&mut self, source: &Source, id: &ProjectId) -> Option<Language> {
        self.smart_load_project_language(source).get(id).pirate()
    }
    pub fn project_description(&mut self, source: &Source, id: &ProjectId) -> Option<String> {
        self.smart_load_project_description(source).get(id).pirate()
    }
    pub fn project_homepage(&mut self, source: &Source, id: &ProjectId) -> Option<String> {
        self.smart_load_project_homepage(source).get(id).pirate()
    }
    pub fn project_has_issues(&mut self, source: &Source, id: &ProjectId) -> Option<bool> {
        self.smart_load_project_has_issues(source).get(id).pirate()
    }
    pub fn project_has_downloads(&mut self, source: &Source, id: &ProjectId) -> Option<bool> {
        self.smart_load_project_has_downloads(source).get(id).pirate()
    }
    pub fn project_has_wiki(&mut self, source: &Source, id: &ProjectId) -> Option<bool> {
        self.smart_load_project_has_wiki(source).get(id).pirate()
    }
    pub fn project_has_pages(&mut self, source: &Source, id: &ProjectId) -> Option<bool> {
        self.smart_load_project_has_pages(source).get(id).pirate()
    }
    pub fn project_created(&mut self, source: &Source, id: &ProjectId) -> Option<i64> {
        self.smart_load_project_created(source).get(id).pirate()        
    }
    pub fn project_updated(&mut self, source: &Source, id: &ProjectId) -> Option<i64> {
        self.smart_load_project_updated(source).get(id).pirate()
    }
    pub fn project_pushed(&mut self, source: &Source, id: &ProjectId) -> Option<i64> {
        self.smart_load_project_pushed(source).get(id).pirate()
    }
    pub fn project_default_branch(&mut self, source: &Source, id: &ProjectId) -> Option<String> {
        self.smart_load_project_default_branch(source).get(id).pirate()
    }
    pub fn project_url(&mut self, source: &Source, id: &ProjectId) -> Option<String> {
        self.smart_load_project_urls(source).get(id).pirate()
    }
    pub fn project_heads(&mut self, source: &Source, id: &ProjectId) -> Option<Vec<Head>> {
        self.smart_load_project_heads(source).get(id).pirate()
    }
    // pub fn project_heads(&mut self, source: &DataSource, id: &ProjectId) -> Option<Vec<(String, Commit)>> {
    //     self.smart_load_project_heads(source).get(id).pirate().map(|v| {
    //         v.into_iter().flat_map(|(name, commit_id)| {
    //             self.commit(source, &commit_id).map(|commit| {
    //                 Head::new(name, commit.clone())
    //             })
    //         }).collect()
    //     })
    // }
    pub fn project_commit_ids(&mut self, source: &Source, id: &ProjectId) -> Option<&Vec<CommitId>> {
        self.smart_load_project_commits(source).get(id)
    }
    pub fn project_commits(&mut self, source: &Source, id: &ProjectId) -> Option<Vec<Commit>> {
        self.smart_load_project_commits(source).get(id).pirate().map(|ids| {
            ids.iter().flat_map(|id| self.commit(source, id).pirate()).collect()
            // FIXME issue warnings in situations like these (when self.commit(id) fails etc.)
        })
    }
    pub fn project_commit_count(&mut self, source: &Source, id: &ProjectId) -> Option<usize> {
        self.smart_load_project_commit_count(source).get(id).pirate()
    }
    pub fn project_path_ids(&mut self, source: &Source, id: &ProjectId) -> Option<&Vec<PathId>> {
        self.smart_load_project_paths(source).get(id)
    }
    pub fn project_paths(&mut self, source: &Source, id: &ProjectId) -> Option<Vec<Path>> {
        self.smart_load_project_paths(source).get(id).pirate().map(|ids| {
            ids.iter().flat_map(|id| self.path(source, id).pirate()).collect()
        })
    }
    pub fn project_path_count(&mut self, source: &Source, id: &ProjectId) -> Option<usize> {
        self.smart_load_project_path_count(source).get(id).pirate()
    }
    pub fn project_snapshot_ids(&mut self, source: &Source, id: &ProjectId) -> Option<&Vec<SnapshotId>> {
        self.smart_load_project_snapshots(source).get(id)
    }
    pub fn project_snapshot_count(&mut self, source: &Source, id: &ProjectId) -> Option<usize> {
        self.smart_load_project_snapshot_count(source).get(id).pirate()
    }
    pub fn project_author_ids(&mut self, source: &Source, id: &ProjectId) -> Option<&Vec<UserId>> {
        self.smart_load_project_authors(source).get(id)
    }
    pub fn project_authors(&mut self, source: &Source, id: &ProjectId) -> Option<Vec<User>> {
        self.smart_load_project_authors(source).get(id).pirate().map(|ids| {
            ids.iter().flat_map(|id| self.user(source, id).pirate()).collect()
        })
    }
    pub fn project_author_count(&mut self, source: &Source, id: &ProjectId) -> Option<usize> {
        self.smart_load_project_author_count(source).get(id).pirate()
    }
    pub fn project_committer_ids(&mut self, source: &Source, id: &ProjectId) -> Option<&Vec<UserId>> {
        self.smart_load_project_committers(source).get(id)
    }
    pub fn project_committers(&mut self, source: &Source, id: &ProjectId) -> Option<Vec<User>> {
        self.smart_load_project_committers(source).get(id).pirate().map(|ids| {
            ids.iter().flat_map(|id| self.user(source, id).pirate()).collect()
        })
    }
    pub fn project_committer_count(&mut self, source: &Source, id: &ProjectId) -> Option<usize> {
        self.smart_load_project_committer_count(source).get(id).pirate()
    }
    pub fn project_user_ids(&mut self, source: &Source, id: &ProjectId) -> Option<&Vec<UserId>> {
        self.smart_load_project_users(source).get(id)
    }
    pub fn project_users(&mut self, source: &Source, id: &ProjectId) -> Option<Vec<User>> {
        self.smart_load_project_users(source).get(id).pirate().map(|ids| {
            ids.iter().flat_map(|id| self.user(source, id).pirate()).collect()
        })
    }
    pub fn project_user_count(&mut self, source: &Source, id: &ProjectId) -> Option<usize> {
        self.smart_load_project_user_count(source).get(id).pirate()
    }
    pub fn project_lifetime(&mut self, source: &Source, id: &ProjectId) -> Option<Duration> {
        self.smart_load_project_lifetimes(source).get(id)
            .pirate()
            .map(|seconds| Duration::from(seconds))
    }
    pub fn project_substore(&mut self, source: &Source, id: &ProjectId) -> Option<Store> {
        self.smart_load_project_substore(source).get(id)
            .pirate()
    }
    pub fn project_unique_files(& mut self, source: &Source, id:&ProjectId) -> Option<usize> {
        self.smart_load_project_unique_files(source).get(id)
            .pirate()
    }
    pub fn project_original_files(& mut self, source: &Source, id:&ProjectId) -> Option<usize> {
        self.smart_load_project_original_files(source).get(id)
            .pirate()
    }
    pub fn project_impact(& mut self, source: &Source, id:&ProjectId) -> Option<usize> {
        self.smart_load_project_impact(source).get(id)
            .pirate()
    }
    pub fn project_files(& mut self, source: &Source, id:&ProjectId) -> Option<usize> {
        self.smart_load_project_files(source).get(id)
            .pirate()
    }
    pub fn project_languages(& mut self, source: &Source, id:&ProjectId) -> Option<Vec<(Language,usize)>> {
        self.smart_load_project_languages(source).get(id)
            .pirate()
    }
    pub fn project_languages_count(& mut self, source: &Source, id:&ProjectId) -> Option<usize> {
        self.smart_load_project_languages_count(source).get(id)
            .pirate()
    }
    pub fn project_major_language(& mut self, source: &Source, id:&ProjectId) -> Option<Language> {
        self.smart_load_project_major_language(source).get(id)
            .pirate()
    }
    pub fn project_major_language_ratio(& mut self, source: &Source, id:&ProjectId) -> Option<f64> {
        self.smart_load_project_major_language_ratio(source).get(id)
            .pirate()
    }
    pub fn project_major_language_changes(& mut self, source: &Source, id:&ProjectId) -> Option<usize> {
        self.smart_load_project_major_language_changes(source).get(id)
            .pirate()
    }
    pub fn user(&mut self, source: &Source, id: &UserId) -> Option<&User> {
        self.smart_load_users(source).get(id)
    }
    pub fn path(&mut self, source: &Source, id: &PathId) -> Option<&Path> {
        self.smart_load_paths(source).get(id)
    }
    pub fn commit(&mut self, source: &Source, id: &CommitId) -> Option<&Commit> {
        self.smart_load_commits(source).get(id)
    }
    pub fn commit_hash(&mut self, source: &Source, id: &CommitId) -> Option<&String> {
        self.smart_load_commit_hashes(source).get(id)
    }
    pub fn commit_message(&mut self, source: &Source, id: &CommitId) -> Option<&String> {
        self.smart_load_commit_messages(source).get(id)
    }
    pub fn commit_author_timestamp(&mut self, source: &Source, id: &CommitId) -> Option<i64> {
        self.smart_load_commit_author_timestamps(source).get(id).pirate()
    }
    pub fn commit_committer_timestamp(&mut self, source: &Source, id: &CommitId) -> Option<i64> {
        self.smart_load_commit_committer_timestamps(source).get(id).pirate()
    }
    pub fn commit_changes(&mut self, source: &Source, id: &CommitId) -> Option<Vec<Change>> {
        self.smart_load_commit_changes(source).get(id).map(|vector| {
            vector.iter().map(|(path_id, snapshot_id)| {
                Change::new(path_id.clone(), snapshot_id.clone())
            }).collect()
        })
    }
    pub fn commit_changed_paths(&mut self, source: &Source, id: &CommitId) -> Option<Vec<Path>> {
        self.smart_load_commit_changes(source).get(id).pirate().map(|ids| {
            ids.iter().flat_map(|change| self.path(source, &change.0/*path_id()*/).pirate()).collect()
        })
    }
    pub fn commit_change_count(&mut self, source: &Source, id: &CommitId) -> Option<usize> {
        self.smart_load_commit_change_count(source).get(id).pirate()
    }
    pub fn commit_changed_path_count(&mut self, source: &Source, id: &CommitId) -> Option<usize> {
        self.smart_load_commit_change_count(source).get(id).pirate()
    }
    pub fn commit_projects(&mut self, source: &Source, id : &CommitId) -> Option<Vec<Project>> {
        self.smart_load_commit_projects(source).get(id).pirate().map(|ids| {
            ids.iter().flat_map(|id| self.project(source, id)).collect()
        })   
    }
    pub fn commit_projects_count(&mut self, source: &Source, id : &CommitId) -> Option<usize> {
        self.smart_load_commit_projects_count(source).get(id).pirate()
    }
    pub fn user_committed_commit_ids(&mut self, source: &Source, id: &UserId) -> Option<&Vec<CommitId>> {
        self.smart_load_user_committed_commits(source).get(id)
    }
    pub fn user_authored_commits(&mut self, source: &Source, id: &UserId) -> Option<Vec<Commit>> {
        self.smart_load_user_authored_commits(source).get(id).pirate().map(|ids| {
            ids.iter().flat_map(|id| self.commit(source, id).pirate()).collect()
        })
    }
    pub fn user_authored_commit_ids(&mut self, source: &Source, id: &UserId) -> Option<&Vec<CommitId>> {
        self.smart_load_user_authored_commits(source).get(id)
    }
    pub fn user_committed_experience(&mut self, source: &Source, id: &UserId) -> Option<Duration> {
        self.smart_load_user_committer_experience(source)
            .get(id)
            .map(|seconds| Duration::from(*seconds))
    }
    pub fn user_author_experience(&mut self, source: &Source, id: &UserId) -> Option<Duration> {
        self.smart_load_user_author_experience(source)
            .get(id)
            .map(|seconds| Duration::from(*seconds))
    }
    pub fn user_experience(&mut self, source: &Source, id: &UserId) -> Option<Duration> {
        self.smart_load_user_experience(source)
            .get(id)
            .map(|seconds| Duration::from(*seconds))
    }
    pub fn user_committed_commit_count(&mut self, source: &Source, id: &UserId) -> Option<usize> {
        self.smart_load_user_committed_commit_count(source).get(id).pirate()
    }
    pub fn user_authored_commit_count(&mut self, source: &Source, id: &UserId) -> Option<usize> {
        self.smart_load_user_authored_commit_count(source).get(id).pirate()
    }
    pub fn user_committed_commits(&mut self, source: &Source, id: &UserId) -> Option<Vec<Commit>> {
        self.smart_load_user_committed_commits(source).get(id).pirate().map(|ids| {
            ids.iter().flat_map(|id| self.commit(source, id).pirate()).collect()
        })
    }
    pub fn longest_inactivity_streak(&mut self, source: &Source, id: &ProjectId) -> Option<i64> {
        self.smart_load_project_longest_inactivity_streak(source).get(id).pirate()
    }
    pub fn avg_commit_rate(&mut self, source: &Source, id: &ProjectId) -> Option<i64> {
        self.smart_load_project_avg_commit_rate(source).get(id).pirate()
    }
    pub fn time_since_last_commit(&mut self, source: &Source, id: &ProjectId) -> Option<i64> {
        self.smart_load_project_time_since_last_commit(source).get(id).pirate()
    }
    pub fn is_abandoned(&mut self, source: &Source, id: &ProjectId) -> Option<bool> {
        self.smart_load_project_is_abandoned(source).get(id).pirate()
    }
    pub fn snapshot_locs(&mut self, source: &Source, id: &SnapshotId) -> Option<usize> {
        self.smart_load_snapshot_locs(source).get(id).pirate()
    }
    pub fn project_locs(&mut self, source: &Source, id: &ProjectId) -> Option<usize> {
        self.smart_load_project_locs(source).get(id).pirate()
    }
    pub fn snapshot_unique_projects(&mut self, source: &Source, id : &SnapshotId) -> usize {
        // TODO I am sure rust frowns upon this, but how do I return ! attributes that are cached in the datastore? 
        self.smart_load_snapshot_projects(source).get(id).unwrap().0
    }
    pub fn snapshot_original_project(&mut self, source: &Source, id : &SnapshotId) -> ProjectId {
        // TODO I am sure rust frowns upon this, but how do I return ! attributes that are cached in the datastore? 
        self.smart_load_snapshot_projects(source).get(id).unwrap().1
    }
}

macro_rules! load_from_source {
    ($self:ident, $vector:ident, $source:expr)  => {{
        if !$self.$vector.is_loaded() {
            $self.$vector.load_from_source($source);
        }
        $self.$vector.grab_collection()
    }}
}

macro_rules! load_from_metadata {
    ($self:ident, $vector:ident, $source:expr)  => {{
        if !$self.$vector.is_loaded() {
            $self.$vector.load_from_metadata($source, &$self.project_metadata);
        }
        $self.$vector.grab_collection()
    }}
}

macro_rules! load_with_prerequisites {
    ($self:ident, $vector:ident, $source:expr, $n:ident, $($prereq:ident),*)  => {{
        mashup! {
            $( m["smart_load" $prereq] = smart_load_$prereq; )*
               m["load"] = load_from_$n;
        }
        if !$self.$vector.is_loaded() {
            m! { $(  $self."smart_load" $prereq($source); )*              }
            m! { $self.$vector."load"($source, $($self.$prereq.grab_collection()), *); }
        }
        $self.$vector.grab_collection()
    }}
}

impl Data {
    fn smart_load_project_substore(&mut self, source: &Source) -> &BTreeMap<ProjectId, Store> {
        load_from_source!(self, project_substores, source)
    }
    fn smart_load_project_urls(&mut self, source: &Source) -> &BTreeMap<ProjectId, String> {
        load_from_source!(self, project_urls, source)
    }
    fn smart_load_project_heads(&mut self, source: &Source) -> &BTreeMap<ProjectId, Vec<Head>> {
        load_from_source!(self, project_heads, source)
    }
    fn smart_load_project_users(&mut self, source: &Source) -> &BTreeMap<ProjectId, Vec<UserId>> {
        load_with_prerequisites!(self, project_users, source, two, project_authors, project_committers)
    }
    fn smart_load_project_authors(&mut self, source: &Source) -> &BTreeMap<ProjectId, Vec<UserId>> {
        load_with_prerequisites!(self, project_authors, source, two, project_commits, commits)
    }
    fn smart_load_project_committers(&mut self, source: &Source) -> &BTreeMap<ProjectId, Vec<UserId>> {
        load_with_prerequisites!(self, project_committers, source, two, project_commits, commits)
    }
    fn smart_load_project_commits(&mut self, source: &Source) -> &BTreeMap<ProjectId, Vec<CommitId>> {
        load_with_prerequisites!(self, project_commits, source, two, project_heads, commits)
    }
    fn smart_load_project_paths(&mut self, source: &Source) -> &BTreeMap<ProjectId, Vec<PathId>> {
        load_with_prerequisites!(self, project_paths, source, two, project_commits, commit_changes)
    }
    fn smart_load_project_snapshots(&mut self, source: &Source) -> &BTreeMap<ProjectId, Vec<SnapshotId>> {
        load_with_prerequisites!(self, project_snapshots, source, two, project_commits, commit_changes)
    }
    fn smart_load_project_user_count(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_user_count, source, one, project_users)
    }
    fn smart_load_project_author_count(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_author_count, source, one, project_authors)
    }
    fn smart_load_project_path_count(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_path_count, source, one, project_paths)
    }
    fn smart_load_project_snapshot_count(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_snapshot_count, source, one, project_snapshots)
    }
    fn smart_load_project_committer_count(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_committer_count, source, one, project_committers)
    }
    fn smart_load_project_commit_count(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_commit_count, source, one, project_commits)
    }
    fn smart_load_project_lifetimes(&mut self, source: &Source) -> &BTreeMap<ProjectId, u64> {
        load_with_prerequisites!(self, project_lifetimes, source, three, project_commits,
                                                                        commit_author_timestamps,
                                                                        commit_committer_timestamps)
    }
    fn smart_load_project_unique_files(& mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_unique_files, source, three, project_commits, commit_changes, snapshot_projects)
    }
    fn smart_load_project_original_files(& mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_original_files, source, three, project_commits, commit_changes, snapshot_projects)
    }
    fn smart_load_project_impact(& mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_impact, source, three, project_commits, commit_changes, snapshot_projects)
    }
    fn smart_load_project_files(& mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_files, source, two, project_commits, commit_changes)
    }
    fn smart_load_project_languages(& mut self, source: &Source) -> &BTreeMap<ProjectId, Vec<(Language,usize)>> {
        load_with_prerequisites!(self, project_languages, source, three, project_commits, commit_changes, paths)
    }
    fn smart_load_project_languages_count(& mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_languages_count, source, one, project_languages)
    }
    fn smart_load_project_major_language(& mut self, source: &Source) -> &BTreeMap<ProjectId, Language> {
        load_with_prerequisites!(self, project_major_language, source, one, project_languages)
    }
    fn smart_load_project_major_language_ratio(& mut self, source: &Source) -> &BTreeMap<ProjectId, f64> {
        load_with_prerequisites!(self, project_major_language_ratio, source, one, project_languages)
    }
    fn smart_load_project_major_language_changes(& mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_major_language_changes, source, one, project_languages)
    }
    fn smart_load_users(&mut self, source: &Source) -> &BTreeMap<UserId, User> {
        load_from_source!(self, users, source)
    }
    fn smart_load_user_authored_commits(&mut self, source: &Source) -> &BTreeMap<UserId, Vec<CommitId>> {
        load_with_prerequisites!(self, user_authored_commits, source, one, commits)
    }
    fn smart_load_user_committed_commits(&mut self, source: &Source) -> &BTreeMap<UserId, Vec<CommitId>> {
        load_with_prerequisites!(self, user_committed_commits, source, one, commits)
    }
    fn smart_load_user_author_experience(&mut self, source: &Source) -> &BTreeMap<UserId, u64> {
        load_with_prerequisites!(self, user_author_experience, source, two, user_authored_commits,
                                                                           commit_author_timestamps)
    }
    fn smart_load_user_committer_experience(&mut self, source: &Source) -> &BTreeMap<UserId, u64> {
        load_with_prerequisites!(self, user_committer_experience, source, two, user_committed_commits,
                                                                              commit_committer_timestamps)
    }
    fn smart_load_user_experience(&mut self, source: &Source) -> &BTreeMap<UserId, u64> {
        load_with_prerequisites!(self, user_experience, source, three, user_committed_commits,
                                                                      commit_author_timestamps,
                                                                      commit_committer_timestamps)
    }
    fn smart_load_user_committed_commit_count(&mut self, source: &Source) -> &BTreeMap<UserId, usize> {
        load_with_prerequisites!(self, user_committed_commit_count, source, one, user_committed_commits)
    }
    fn smart_load_user_authored_commit_count(&mut self, source: &Source) -> &BTreeMap<UserId, usize> {
        load_with_prerequisites!(self, user_authored_commit_count, source, one, user_authored_commits)
    }
    fn smart_load_paths(&mut self, source: &Source) -> &BTreeMap<PathId, Path> {
        load_from_source!(self, paths, source)
    }
    // fn smart_load_snapshots(&mut self, source: &DataSource) -> &BTreeMap<SnapshotId, Snapshot> {
    //     load_from_source!(self, snapshots, source)
    // }
    fn smart_load_commits(&mut self, source: &Source) -> &BTreeMap<CommitId, Commit> {
        load_from_source!(self, commits, source)
    }
    fn smart_load_commit_hashes(&mut self, source: &Source) -> &BTreeMap<CommitId, String> {
        load_from_source!(self, commit_hashes, source)
    }
    fn smart_load_commit_messages(&mut self, source: &Source) -> &BTreeMap<CommitId, String> {
        load_from_source!(self, commit_messages, source)
    }
    fn smart_load_commit_committer_timestamps(&mut self, source: &Source) -> &BTreeMap<CommitId, i64> {
        load_from_source!(self, commit_committer_timestamps, source)
    }
    fn smart_load_commit_author_timestamps(&mut self, source: &Source) -> &BTreeMap<CommitId, i64> {
        load_from_source!(self, commit_author_timestamps, source)
    }
    fn smart_load_commit_changes(&mut self, source: &Source) -> &BTreeMap<CommitId, Vec<ChangeTuple>> {
        load_from_source!(self, commit_changes, source)
    }
    fn smart_load_commit_change_count(&mut self, source: &Source) -> &BTreeMap<CommitId, usize> {
        load_with_prerequisites!(self, commit_change_count, source, one, commit_changes)
    }
    fn smart_load_project_longest_inactivity_streak(&mut self, source: &Source) -> &BTreeMap<ProjectId, i64> {
        load_with_prerequisites!(self, project_longest_inactivity_streak, source, two, project_commits, commit_committer_timestamps)
    }
    fn smart_load_project_avg_commit_rate(&mut self, source: &Source) -> &BTreeMap<ProjectId, i64> {
        load_with_prerequisites!(self, avg_commit_rate, source, two, project_commits, commit_committer_timestamps)
    }
    fn smart_load_project_time_since_last_commit(&mut self, source: &Source) -> &BTreeMap<ProjectId, i64> {
        load_with_prerequisites!(self, project_time_since_last_commit, source, two, project_commits, commit_committer_timestamps)
    }
    fn smart_load_project_is_abandoned(&mut self, source: &Source) -> &BTreeMap<ProjectId, bool> {
        load_with_prerequisites!(self, is_abandoned, source, two, project_longest_inactivity_streak, project_time_since_last_commit)
    }
    fn smart_load_snapshot_locs(&mut self, source: &Source) -> &BTreeMap<SnapshotId, usize> {
        load_from_source!(self, snapshot_locs, source)
        //load_with_prerequisites!(self, is_abandoned, source, one, project_snapshots)
    }
    fn smart_load_project_locs(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_locs, source, four, project_commits,  commit_committer_timestamps, commit_changes, snapshot_locs)
    }

    fn smart_load_commit_projects(&mut self, source: &Source) -> &BTreeMap<CommitId, Vec<ProjectId>> {
        load_with_prerequisites!(self, commit_projects, source, one, project_commits)
    }
    fn smart_load_commit_projects_count(&mut self, source: &Source) -> &BTreeMap<CommitId, usize> {
        load_with_prerequisites!(self, commit_projects_count, source, one, commit_projects)
    }
    fn smart_load_snapshot_projects(& mut self, source: &Source) -> &BTreeMap<SnapshotId,(usize, ProjectId)> {
        load_with_prerequisites!(self, snapshot_projects, source, three, commit_changes, commit_projects, commit_author_timestamps)
    }

    pub fn smart_load_project_issues(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_from_metadata!(self, project_issue_count, source)
    }
    pub fn smart_load_project_buggy_issues(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_from_metadata!(self, project_buggy_issue_count, source)
    }
    pub fn smart_load_project_is_fork(&mut self, source: &Source) -> &BTreeMap<ProjectId, bool> {
        load_from_metadata!(self, project_is_fork, source)
    }
    pub fn smart_load_project_is_archived(&mut self, source: &Source) -> &BTreeMap<ProjectId, bool> {
        load_from_metadata!(self, project_is_archived, source)
    }
    pub fn smart_load_project_is_disabled(&mut self, source: &Source) -> &BTreeMap<ProjectId, bool> {
        load_from_metadata!(self, project_is_disabled, source)
    }
    pub fn smart_load_project_star_gazer_count(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_from_metadata!(self, project_star_gazer_count, source)
    }
    pub fn smart_load_project_watcher_count(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_from_metadata!(self, project_watcher_count, source)
    }
    pub fn smart_load_project_size(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_from_metadata!(self, project_project_size, source)
    }
    pub fn smart_load_project_open_issue_count(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_from_metadata!(self, project_open_issue_count, source)
    }
    pub fn smart_load_project_fork_count(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_from_metadata!(self, project_fork_count, source)
    }
    pub fn smart_load_project_subscriber_count(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_from_metadata!(self, project_subscriber_count, source)
    }
    pub fn smart_load_project_license(&mut self, source: &Source) -> &BTreeMap<ProjectId, String> {
        load_from_metadata!(self, project_license, source)
    }
    pub fn smart_load_project_language(&mut self, source: &Source) -> &BTreeMap<ProjectId, Language> {
        load_from_metadata!(self, project_language, source)
    }
    pub fn smart_load_project_description(&mut self, source: &Source) -> &BTreeMap<ProjectId, String> {
        load_from_metadata!(self, project_description, source)
    }
    pub fn smart_load_project_homepage(&mut self, source: &Source) -> &BTreeMap<ProjectId, String> {
        load_from_metadata!(self, project_homepage, source)
    }
    pub fn smart_load_project_has_issues(&mut self, source: &Source) -> &BTreeMap<ProjectId, bool> {
        load_from_metadata!(self, project_has_issues, source)
    }
    pub fn smart_load_project_has_downloads(&mut self, source: &Source) -> &BTreeMap<ProjectId, bool> {
        load_from_metadata!(self, project_has_downloads, source)
    }
    pub fn smart_load_project_has_wiki(&mut self, source: &Source) -> &BTreeMap<ProjectId, bool> {
        load_from_metadata!(self, project_has_wiki, source)
    }
    pub fn smart_load_project_has_pages(&mut self, source: &Source) -> &BTreeMap<ProjectId, bool> {
        load_from_metadata!(self, project_has_pages, source)
    }
    fn smart_load_project_created(&mut self, source: &Source) -> &BTreeMap<ProjectId, i64> {
        load_from_metadata!(self, project_created, source)
    }
    pub fn smart_load_project_updated(&mut self, source: &Source) -> &BTreeMap<ProjectId, i64> {
        load_from_metadata!(self, project_updated, source)
    }
    pub fn smart_load_project_pushed(&mut self, source: &Source) -> &BTreeMap<ProjectId, i64> {
        load_from_metadata!(self, project_pushed, source)
    }
    pub fn smart_load_project_default_branch(&mut self, source: &Source) -> &BTreeMap<ProjectId, String> {
        load_from_metadata!(self, project_default_branch, source)
    }
}

impl Data {
    pub fn export_to_csv<S>(&mut self, _: &Source, _dir: S) -> Result<(), std::io::Error> where S: Into<String> {
        // let dir = dir.into();
        // std::fs::create_dir_all(&dir)?;
        // macro_rules! path {
        //     ($filename:expr) => {
        //         format!("{}/{}.csv", dir, $filename)
        //     }
        // }

        // self.project_metadata.iter(source).into_csv(path!("project_metadata"))?;
        // FIXME add 
        todo!()

        // self.smart_load_project_urls(source).iter().into_csv(path!("project_urls"))?;
        // self.smart_load_project_heads(source).iter().into_csv(path!("project_heads"))?;
        // self.smart_load_users(source).iter().into_csv(path!("users"))?;
        // self.smart_load_paths(source).iter().into_csv(path!("paths"))?;
        // self.smart_load_commits(source).iter().into_csv(path!("commits"))?;
        // self.smart_load_commit_hashes(source).iter().into_csv(path!("commit_hashes"))?;
        // self.smart_load_commit_messages(source).iter().into_csv(path!("commit_messages"))?;
        // self.smart_load_commit_committer_timestamps(source).iter().into_csv(path!("commit_committer_timestamps"))?;
        // self.smart_load_commit_author_timestamps(source).iter().into_csv(path!("commit_author_timestamps"))?;
        // self.smart_load_commit_changes(source).iter().into_csv(path!("commit_changes"))?;

        // source.snapshot_bytes()
        //      .map(|(id, content)| {
        //          Snapshot::new(id, content)
        //      }).into_csv(path!("snapshots"))?;

        // Ok(())
    }
}

impl Database {
    pub fn export_to_csv<S>(&self, dir: S) -> Result<(), std::io::Error> where S: Into<String> {
        self.data.borrow_mut().export_to_csv(&self.source, dir)
    }
}
