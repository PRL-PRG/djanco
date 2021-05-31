use std::collections::{BTreeMap, BTreeSet};
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
use crate::csv::*;
use crate::source::Source;
use crate::{CacheDir, Store};

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
    pub fn project_master(&self, id: &ProjectId) -> Option<String> {
        self.data.borrow_mut().project_master(&self.source, id)
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
    fn extract(whatever: &Self::A) -> Vec<Self::Value> {
        whatever.keys().collect::<Vec<&Id>>().pirate()
    }
}

struct ProjectUrlExtractor;
impl MapExtractor for ProjectUrlExtractor {
    type Key = ProjectId;
    type Value = String;
}
impl SingleMapExtractor for ProjectUrlExtractor {
    type A = Source;
    fn extract(source: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        source.project_urls().collect()
    }
}

struct ProjectSubstoreExtractor;
impl MapExtractor for ProjectSubstoreExtractor {
    type Key = ProjectId;
    type Value = Store;
}
impl SingleMapExtractor for ProjectSubstoreExtractor {
    type A = Source;
    fn extract(source: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        source.project_substores().collect()
    }
}

struct ProjectCredentialsExtractor; // TODO plug in
impl MapExtractor for ProjectCredentialsExtractor {
    type Key = ProjectId;
    type Value = String;
}
impl SingleMapExtractor for ProjectCredentialsExtractor {
    type A = Source;
    fn extract(source: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        source.project_credentials().collect()
    }
}

struct ProjectHeadsExtractor;
impl MapExtractor for ProjectHeadsExtractor {
    type Key = ProjectId;
    type Value = Vec<Head>;
}
impl SingleMapExtractor for ProjectHeadsExtractor {
    type A = Source;
    fn extract(source: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
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

    fn extract(project_commit_ids: &Self::A, commit_change_ids: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
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

    fn extract(project_commit_ids: &Self::A, commit_change_ids: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
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
    fn extract(project_authors: &Self::A, project_committers: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
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
    fn extract(project_commits: &Self::A, commits: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
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
    fn extract(project_commits: &Self::A, commits: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
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

    fn extract(primary: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
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
    fn extract(heads: &Self::A, commits: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
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
    fn extract(project_commits: &Self::A,
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
impl SingleMapExtractor for UserExtractor {
    type A = Source;
    fn extract(source: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
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
    fn extract(commits: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
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
    fn extract(user_commits: &Self::A, timestamps: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
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
    fn extract(user_commits: &Self::A, authored_timestamps: &Self::B, committed_timestamps: &Self::C) -> BTreeMap<Self::Key, Self::Value> {
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
impl SingleMapExtractor for PathExtractor {
    type A = Source;
    fn extract(source: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
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
impl SingleMapExtractor for SnapshotExtractor {
    type A = Source;
    fn extract(source: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
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
impl SingleMapExtractor for CommitExtractor {
    type A = Source;
    fn extract(source: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
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
impl SingleMapExtractor for CommitHashExtractor {
    type A = Source;
    fn extract(source: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        source.commit_hashes().collect()
    }
}

struct CommitMessageExtractor {}
impl MapExtractor for CommitMessageExtractor {
    type Key = CommitId;
    type Value = String;
}
impl SingleMapExtractor for CommitMessageExtractor {
    type A = Source;
    fn extract(source: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
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
impl SingleMapExtractor for CommitterTimestampExtractor {
    type A = Source;
    fn extract(source: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
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
impl SingleMapExtractor for CommitChangesExtractor {
    type A = Source;
    fn extract(source: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
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
impl SingleMapExtractor for AuthorTimestampExtractor {
    type A = Source;
    fn extract(source: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        source.commit_info().map(|(id, commit)| {
            (id, commit.author_time)
        }).collect()
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

    project_created:             Rybka,

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

    // TODO frequency of commits/regularity of commits
    // TODO maybe some of these could be pre-cached all at once (eg all commit properties)
}

struct Rybka {

}

impl Data {
    pub fn new(/*source: DataSource,*/ cache_dir: CacheDir, log: Log) -> Data {
        let dir = cache_dir.as_string();
        Data {
            project_urls:                PersistentMap::new("project_urls",                log.clone(),dir.clone()).without_cache(),
            project_substores:           PersistentMap::new("project_substores",           log.clone(),dir.clone()).without_cache(),
            project_heads:               PersistentMap::new("project_heads",               log.clone(),dir.clone()),
            project_paths:               PersistentMap::new("project_paths",               log.clone(),dir.clone()),
            project_path_count:          PersistentMap::new("project_path_count",          log.clone(),dir.clone()),
            project_snapshots:           PersistentMap::new("project_snapshots",           log.clone(),dir.clone()),
            project_snapshot_count:      PersistentMap::new("project_snapshot_count",      log.clone(),dir.clone()),
            project_users:               PersistentMap::new("project_users",               log.clone(),dir.clone()),
            project_user_count:          PersistentMap::new("project_user_count",          log.clone(),dir.clone()),
            project_authors:             PersistentMap::new("project_authors",             log.clone(),dir.clone()),
            project_author_count:        PersistentMap::new("project_author_count",        log.clone(),dir.clone()),
            project_committers:          PersistentMap::new("project_committers",          log.clone(),dir.clone()),
            project_committer_count:     PersistentMap::new("project_committer_count",     log.clone(),dir.clone()),
            project_commits:             PersistentMap::new("project_commits",             log.clone(),dir.clone()),
            project_commit_count:        PersistentMap::new("project_commit_count",        log.clone(),dir.clone()),
            project_lifetimes:           PersistentMap::new("project_lifetimes",           log.clone(),dir.clone()),

            project_metadata:            ProjectMetadataSource::new("project",             log.clone(),dir.clone()),
            project_created:             Rybka{},

            users:                       PersistentMap::new("users",                       log.clone(),dir.clone()).without_cache(),
            user_authored_commits:       PersistentMap::new("user_authored_commits",       log.clone(),dir.clone()),
            user_committed_commits:      PersistentMap::new("user_committed_commits",      log.clone(),dir.clone()),
            user_author_experience:      PersistentMap::new("user_author_experience",      log.clone(),dir.clone()),
            user_committer_experience:   PersistentMap::new("user_committer_experience",   log.clone(),dir.clone()),
            user_experience:             PersistentMap::new("user_experience",             log.clone(),dir.clone()),

            user_authored_commit_count:  PersistentMap::new("user_authored_commit_count",  log.clone(),dir.clone()),
            user_committed_commit_count: PersistentMap::new("user_committed_commit_count", log.clone(),dir.clone()),

            paths:                       PersistentMap::new("paths",                       log.clone(),dir.clone()).without_cache(),
            //snapshots:                   PersistentMap::new("snapshots",                   dir.clone()),

            commits:                     PersistentMap::new("commits",                     log.clone(),dir.clone()),
            commit_hashes:               PersistentMap::new("commit_hashes",               log.clone(),dir.clone()).without_cache(),
            commit_messages:             PersistentMap::new("commit_messages",             log.clone(),dir.clone()).without_cache(),
            commit_author_timestamps:    PersistentMap::new("commit_author_timestamps",    log.clone(),dir.clone()),
            commit_committer_timestamps: PersistentMap::new("commit_committer_timestamps", log.clone(),dir.clone()),
            commit_changes:              PersistentMap::new("commit_changes",              log.clone(),dir.clone()).without_cache(),
            commit_change_count:         PersistentMap::new("commit_change_count",         log, dir.clone()),
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
        self.project_metadata.issues(source, id)
    }
    pub fn project_buggy_issues(&mut self, source: &Source, id: &ProjectId) -> Option<usize> {
        self.project_metadata.issues(source, id)
    }
    pub fn project_is_fork(&mut self, source: &Source, id: &ProjectId) -> Option<bool> {
        self.project_metadata.is_fork(source, id)
    }
    pub fn project_is_archived(&mut self, source: &Source, id: &ProjectId) -> Option<bool> {
        self.project_metadata.is_archived(source, id)
    }
    pub fn project_is_disabled(&mut self, source: &Source, id: &ProjectId) -> Option<bool> {
        self.project_metadata.is_disabled(source, id)
    }
    pub fn project_star_gazer_count(&mut self, source: &Source, id: &ProjectId) -> Option<usize> {
        self.project_metadata.star_gazers(source, id)
    }
    pub fn project_watcher_count(&mut self, source: &Source, id: &ProjectId) -> Option<usize> {
        self.project_metadata.watchers(source, id)
    }
    pub fn project_size(&mut self, source: &Source, id: &ProjectId) -> Option<usize> {
        self.project_metadata.size(source, id)
    }
    pub fn project_open_issue_count(&mut self, source: &Source, id: &ProjectId) -> Option<usize> {
        self.project_metadata.open_issues(source, id)
    }
    pub fn project_fork_count(&mut self, source: &Source, id: &ProjectId) -> Option<usize> {
        self.project_metadata.forks(source, id)
    }
    pub fn project_subscriber_count(&mut self, source: &Source, id: &ProjectId) -> Option<usize> {
        self.project_metadata.subscribers(source, id)
    }
    pub fn project_license(&mut self, source: &Source, id: &ProjectId) -> Option<String> {
        self.project_metadata.license(source, id)
    }
    pub fn project_language(&mut self, source: &Source, id: &ProjectId) -> Option<Language> {
        self.project_metadata.language(source, id)
    }
    pub fn project_description(&mut self, source: &Source, id: &ProjectId) -> Option<String> {
        self.project_metadata.description(source, id)
    }
    pub fn project_homepage(&mut self, source: &Source, id: &ProjectId) -> Option<String> {
        self.project_metadata.homepage(source, id)
    }
    pub fn project_has_issues(&mut self, source: &Source, id: &ProjectId) -> Option<bool> {
        self.project_metadata.has_issues(source, id)
    }
    pub fn project_has_downloads(&mut self, source: &Source, id: &ProjectId) -> Option<bool> {
        self.project_metadata.has_downloads(source, id)
    }
    pub fn project_has_wiki(&mut self, source: &Source, id: &ProjectId) -> Option<bool> {
        self.project_metadata.has_wiki(source, id)
    }
    pub fn project_has_pages(&mut self, source: &Source, id: &ProjectId) -> Option<bool> {
        self.project_metadata.has_pages(source, id)
    }
    pub fn project_created(&mut self, source: &Source, id: &ProjectId) -> Option<i64> {
        //self.smart_load_project_created(source).get(id).pirate()
        unimplemented!()
    }
    pub fn project_updated(&mut self, source: &Source, id: &ProjectId) -> Option<i64> {
        self.project_metadata.updated(source, id)
    }
    pub fn project_pushed(&mut self, source: &Source, id: &ProjectId) -> Option<i64> {
        self.project_metadata.pushed(source, id)
    }
    pub fn project_master(&mut self, source: &Source, id: &ProjectId) -> Option<String> {
        self.project_metadata.master(source, id)
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
}

macro_rules! load_from_source {
    ($self:ident, $vector:ident, $source:expr)  => {{
        if !$self.$vector.is_loaded() {
            $self.$vector.load_from_one($source);
        }
        $self.$vector.grab_collection()
    }}
}

macro_rules! load_from_metadata {
    ($self:ident, $vector:ident, $source:expr)  => {{
        if !$self.$vector.is_loaded() {
            $self.$vector.load_from_one($source);
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
            m! { $self.$vector."load"($($self.$prereq.grab_collection()), *); }
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
    fn smart_load_project_created(&mut self, source: &Source) -> &BTreeMap<ProjectId, Vec<PathId>> {
        unimplemented!()
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
}

impl Data {
    pub fn export_to_csv<S>(&mut self, source: &Source, dir: S) -> Result<(), std::io::Error> where S: Into<String> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        macro_rules! path {
            ($filename:expr) => {
                format!("{}/{}.csv", dir, $filename)
            }
        }

        self.project_metadata.iter(source).into_csv(path!("project_metadata"))?;

        self.smart_load_project_urls(source).iter().into_csv(path!("project_urls"))?;
        self.smart_load_project_heads(source).iter().into_csv(path!("project_heads"))?;
        self.smart_load_users(source).iter().into_csv(path!("users"))?;
        self.smart_load_paths(source).iter().into_csv(path!("paths"))?;
        self.smart_load_commits(source).iter().into_csv(path!("commits"))?;
        self.smart_load_commit_hashes(source).iter().into_csv(path!("commit_hashes"))?;
        self.smart_load_commit_messages(source).iter().into_csv(path!("commit_messages"))?;
        self.smart_load_commit_committer_timestamps(source).iter().into_csv(path!("commit_committer_timestamps"))?;
        self.smart_load_commit_author_timestamps(source).iter().into_csv(path!("commit_author_timestamps"))?;
        self.smart_load_commit_changes(source).iter().into_csv(path!("commit_changes"))?;

        source.snapshot_bytes()
             .map(|(id, content)| {
                 Snapshot::new(id, content)
             }).into_csv(path!("snapshots"))?;

        Ok(())
    }
}

impl Database {
    pub fn export_to_csv<S>(&self, dir: S) -> Result<(), std::io::Error> where S: Into<String> {
        self.data.borrow_mut().export_to_csv(&self.source, dir)
    }
}

#[cfg(test)]
mod data {
    use std::collections::HashMap;
    use std::fs::{remove_dir_all, metadata};

    use crate::stores;
    use crate::data::Database;
    use crate::objects::{ProjectId, Project, ItemWithData};
    use crate::Djanco;

    const DATASET_DIR: &'static str = "/dejacode/tiny-mk2";
    const CACHE_DIR:   &'static str = "/dejacode/cache-mk2";
    const TIME:                 i64 = 1607952032i64;

    fn exists(path: &'static str) -> bool {
        metadata(std::path::Path::new(path)).map_or(false, |_| true)
    }

    // fn dir_exists(path: &Path) -> bool {
    //     metadata(path).map_or(false, |metadata| metadata.is_dir())
    // }

    fn setup_database(precached: bool) -> Database {
        if !precached && exists(CACHE_DIR) {
            remove_dir_all(CACHE_DIR)
                .expect(&format!("Could not delete directory {}", CACHE_DIR));
        }

        Djanco::from_store(DATASET_DIR, TIME, stores!(All)).expect("Could not create database")
    }

    #[test]
    fn projects_against_expected() {
        let database = setup_database(false);

        let expected: HashMap<ProjectId, String> = vec![
            (0, "https://github.com/tosch/ruote-kit.git"),
            (1, "https://github.com/kennethkalmer/ruote-kit.git"),
            (2, "https://github.com/matplotlib/basemap.git"),
            (3, "https://github.com/jswhit/basemap.git"),
            (4, "https://github.com/rolandoam/cocos2d-x.git"),
            (5, "https://github.com/cocos2d/cocos2d-x.git"),
            (6, "https://github.com/pixonic/cocos2d-x.git"),
            (7, "https://github.com/nubic/ncs_navigator_core.git"),
            (8, "https://github.com/sgonyea/rake-compiler.git"),
            (9, "https://github.com/chapuni/llvm.git"),
            (10, "https://github.com/heroku/heroku-buildpack-scala.git"),
            (11, "https://github.com/rafacm/heroku-buildpack-scala.git"),
            (12, "https://github.com/fluttershy/locria.git"),
            (13, "https://github.com/edvorg/cpp-drash.git"), // "drash" in file
            (14, "https://github.com/abarocio80/clide.git"),
            (15, "https://github.com/thorlax402/thor-cms.git"),
            (16, "https://github.com/offsite/taskcodes.git"),
            (17, "https://github.com/markpasc/gameshake.git"),
            (18, "https://github.com/samuelclay/newsblur.git"),
            (19, "https://github.com/chrisjaure/git-lava.git"),
            (20, "https://github.com/es-doc/esdoc-questionnaire.git"), // "djanco-cim-forms" in file
            (21, "https://github.com/adammark/markup.js.git"),
            (22, "https://github.com/leoamigood/1stdibs_v2.1.git"),
            (23, "https://github.com/pyrovski/large-scale-forward-regression-using-a-partitioned-linear-model.git"),
            (24, "https://github.com/podarsmarty/cobertura-plugin.git"),
            (25, "https://github.com/fbettag/scala-vs-erlang.git"),
            (26, "https://github.com/rake-compiler/rake-compiler.git"),
            (27, "https://github.com/opencv/opencv.git"),
            (28, "https://github.com/jkammerl/opencv.git"),
            (29, "https://github.com/gpjt/webgl-lessons.git"),
            (30, "https://github.com/kerolasa/lelux-utiliteetit.git"),
            (31, "https://github.com/snowblindfatal/glomes.git"),
            (32, "https://github.com/pockethub/pockethub.git"),
            (33, "https://github.com/mirocow/yii-easyapns.git"),
            (34, "https://github.com/angular/angular.js.git"),
            (35, "https://github.com/wallysalami/yii-easyapns.git"),
            (36, "https://github.com/macmade/opencv-ios.git"),
            (37, "https://github.com/powmedia/buildify.git"),
            (38, "https://github.com/liberty-concepts/redmine_git_hosting.git"),
            (39, "https://github.com/kubitron/redmine_git_hosting.git"),
            (40, "https://github.com/hpc/iptablesbuild.git"),
            (41, "https://github.com/chenniaoc/opencv-ios.git"),
            (42, "https://github.com/tijsverkoyen/dotfiles.git"),
            (43, "https://github.com/6a68/browserid.git"),
            (44, "https://github.com/samtubbax/dotfiles.git"),
            (45, "https://github.com/jman01/customizations.git"),
            (46, "https://github.com/alexgorbatchev/syntaxhighlighter.git"),
            (47, "https://github.com/fredwu/jquery-endless-scroll.git"),
            (48, "https://github.com/kanishkaganguly/zero-requiem.git"),
            (49, "https://github.com/bronsa/brochure.git"),
            (50, "https://github.com/yui/yui3.git"),
            (51, "https://github.com/jesperes/protobuf-cmake.git"),
            (52, "https://github.com/pculture/unisubs.git"),
            (53, "https://github.com/imtapps/django-request-signer.git"),
            (54, "https://github.com/nadafigment/protobuf-cmake.git"),
            (55, "https://github.com/libram/django-request-signer.git"),
            (56, "https://github.com/fangpenlin/loso.git"),
            (57, "https://github.com/lucaswei/loso.git"),
            (58, "https://github.com/apipkin/yui3.git"),
            (59, "https://github.com/doctag/doctag_java.git"),
            (60, "https://github.com/llvm-mirror/llvm.git"),
            (61, "https://github.com/gini/doctag_java.git"),
            (62, "https://github.com/joyent/libuv.git"),
            (63, "https://github.com/schatten/schatten.github.com.git"),
            (64, "https://github.com/gosquared/nvm-cookbook.git"),
            (65, "https://github.com/davewid/legacy-php-talk.git"),
            (66, "https://github.com/mshk/data-journalism-handbook-ja.git"),
            (67, "https://github.com/russellspitzer/sample_app.git"),
            (68, "https://github.com/willdurand/willdurand.github.io.git"),
            (69, "https://github.com/stof/willdurand.github.com.git"),
            (70, "https://github.com/rxgx/dotfiles.git"),
            (71, "https://github.com/ablu/manaserv.git"),
            (72, "https://github.com/garyrussell/spring-integration.git"),
            (73, "https://github.com/yomoyomo/data-journalism-handbook-ja.git"),
            (74, "https://github.com/mana/manaserv.git"),
            (75, "https://github.com/bjorn/manaserv.git"),
            (76, "https://github.com/fnando/i18n-js.git"),
            (77, "https://github.com/olegz/spring-integration.git"),
            (78, "https://github.com/chapuni/llvm-project.git"),
            (79, "https://github.com/neverabc/libuv.git"),
            (80, "https://github.com/blinkbox/cucumber-js.git"),
            (81, "https://github.com/elaird/supy.git"),
            (82, "https://github.com/janrain/jump.ios.git"),
            (83, "https://github.com/timblinkbox/cucumber-js.git"),
            (84, "https://github.com/angular/angular-seed.git"),
            (85, "https://github.com/mashiro/i18n-js.git"),
            (86, "https://github.com/jakewharton/viewpagerindicator.git"),
            (87, "https://github.com/evh27/angular-seed.git"),
            (88, "https://github.com/leon/play-salat.git"),
            (89, "https://github.com/bnoordhuis/libuv.git"),
            (90, "https://github.com/oftc/libuv.git"),
            (91, "https://github.com/shepheb/jotto.git"),
            (92, "https://github.com/virgo-agent-toolkit/rackspace-monitoring-agent.git"),
            (93, "https://github.com/incuna/django-extensible-profiles.git"),
            (94, "https://github.com/redaemn/angular-seed.git"),
            (95, "https://github.com/zorgleh/try_git.git"),
            (96, "https://github.com/madrobby/zepto.git"),
            (97, "https://github.com/ochameau/addon-sdk.git"),
            (98, "https://github.com/brandonwamboldt/utilphp.git"),
        ].into_iter().map(|(id, url): (u64, &str)| (ProjectId::from(id), url.to_owned())).collect();

        database.projects().for_each(|project| {
            let expected_url = expected.get(&project.id())
                .expect(&format!("Not expected to see a project with id {}", project.id()))
                .to_owned();
            assert_eq!(expected_url, project.url())
        });

        let projects: HashMap<ProjectId, ItemWithData<Project>> = database.projects()
            .map(|project| (project.id(), project)).collect();

        expected.iter().for_each(|(id, url)| {
            let project = projects.get(id)
                .expect(&format!("Expected to see a project with id {}", id))
                .to_owned();
            assert_eq!(url.clone(), project.url())
        })
    }
}