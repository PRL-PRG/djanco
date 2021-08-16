pub mod metadata;
pub mod cache;
pub mod persistent;
pub mod source;
pub mod data;
pub mod extractors;
pub mod lazy;

use std::cell::RefCell;

use anyhow::*;
use delegate::delegate;

use crate::objects::*;
use crate::iterators::*;
use crate::log::*;
use crate::time::Duration;
use crate::{CacheDir, Store, Percentage, Timestamp};

use source::Source;

use data::Data;

// Internally Mutable Data
pub struct Database {
    data: RefCell<Data>,
    source: Source,
    log: Log,
}

// Constructors
impl Database {
    pub fn new(source: Source, cache_dir: CacheDir, log: Log) -> Self {
        let data = RefCell::new(Data::new(cache_dir, log.clone()));
        Database { data, source, log }
    }
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

/* 
 * Entry points into the database. Each method returns an iterator over one of
 * the entity objects represented in tha database: all projects, all commits, 
 * all users, etc. 
 * 
 * Projects, commits, users, and paths are cached by the database. Snapshots 
 * are not.
 * 
 * > What's a quincunx
 * 
 * A quincunx is a shape consisting of four corners and a point in the middle.
 * These iterators are called Quincunx iterators because there are 4 + 1 of 
 * them. Naming them something like `EntityIter` or `ObjectIter` is boring.
 */
impl Database {
    pub fn projects(&self)  -> QuincunxIter<Project>  { QuincunxIter::<Project>::new(&self)  }
    pub fn commits(&self)   -> QuincunxIter<Commit>   { QuincunxIter::<Commit>::new(&self)   }
    pub fn users(&self)     -> QuincunxIter<User>     { QuincunxIter::<User>::new(&self)     }
    pub fn paths(&self)     -> QuincunxIter<Path>     { QuincunxIter::<Path>::new(&self)     }


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
    /*
     * This is a list of automatically generated delegated functions.
     * 
     * Database most often acts as a wrapper for its inner Data object. It 
     * delegates a large number of methods to the inner object, meaning it
     * creates a mutable reference to the inner objects and calls the method
     * on it. This is a lot of boilerplate code, so to make it less verbose 
     * and error prone, the delegated functions are generated from their 
     * signatures. Each delegated method calls a method in self.data with the
     * same name with the same signature as the delegate but with `&Source` 
     * appended.
     * 
     * E.g. 
     * ```
     * #[append_args(&self.source)] 
     * pub fn project(&self, id: &ProjectId) -> Option<Project>;
     * ```
     * 
     * Generates:
     * ```
     * #[inline(always)] 
     * pub fn project(&self, id: &ProjectId) -> Option<Project> {
     *      self.data.borrow_mut().project(id, &self.source)
     * }
     * ```
     * 
     * > Why do we need these delegates?
     * 
     * This is done to implement internal mutability. Mutability is introduced 
     * via lazy laoding and caching.
     * 
     * > Why do we need this internal mutability stuff?
     * 
     * Exposing a bunch of functions with `&mut self` makes implementing the 
     * DSL very difficult. Database objects are thin and populated lazily, so 
     * they all need a reference to the database to really be useful. Only one
     * mutable reference can be held at once.
     * 
     * > Is this thread safe? 
     * 
     * No, but it can be (should be) made thread safe in the future.
     * 
     * > Why do we generate these instead fo writing them by hand?
     * 
     * Even though the delegation is easy to write, it's still three lines per
     * method. Since there are about 100 different delegated methods, each 
     * extra line of code per method adds a lot noise to the source code.
     * So this solution saves around 200 lines of boilerplate noise.
     * 
     * The delegation also leads a lot of room to make mistakes. Eg. calling 
     * `Data::project_authors` instead of `Data::project_committers` in 
     * `Database::project_committers`. Removing this opportunity to make 
     * silly mistakes is a positive thing for a bit of expended macro magic.
     */
    delegate! {
        to self.data.borrow_mut() {
            // Project attributes
            #[append_args(&self.source)] pub fn project(&self, id: &ProjectId) -> Option<Project>;
            #[append_args(&self.source)] pub fn project_issues(&self, id: &ProjectId) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_buggy_issues(&self, id: &ProjectId) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_is_fork(&self, id: &ProjectId) -> Option<bool>;
            #[append_args(&self.source)] pub fn project_is_archived(&self, id: &ProjectId) -> Option<bool>;
            #[append_args(&self.source)] pub fn project_is_disabled(&self, id: &ProjectId) -> Option<bool>;
            #[append_args(&self.source)] pub fn project_star_gazer_count(&self, id: &ProjectId) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_watcher_count(&self, id: &ProjectId) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_size(&self, id: &ProjectId) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_open_issue_count(&self, id: &ProjectId) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_fork_count(&self, id: &ProjectId) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_subscriber_count(&self, id: &ProjectId) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_license(&self, id: &ProjectId) -> Option<String>;
            #[append_args(&self.source)] pub fn project_language(&self, id: &ProjectId) -> Option<Language>;
            #[append_args(&self.source)] pub fn project_substore(&self, id: &ProjectId) -> Option<Store>;
            #[append_args(&self.source)] pub fn project_description(&self, id: &ProjectId) -> Option<String>;
            #[append_args(&self.source)] pub fn project_homepage(&self, id: &ProjectId) -> Option<String>;
            #[append_args(&self.source)] pub fn project_has_issues(&self, id: &ProjectId) -> Option<bool>;
            #[append_args(&self.source)] pub fn project_has_downloads(&self, id: &ProjectId) -> Option<bool>;
            #[append_args(&self.source)] pub fn project_has_wiki(&self, id: &ProjectId) -> Option<bool>;
            #[append_args(&self.source)] pub fn project_has_pages(&self, id: &ProjectId) -> Option<bool>;
            #[append_args(&self.source)] pub fn project_created(&self, id: &ProjectId) -> Option<Timestamp>;
            #[append_args(&self.source)] pub fn project_updated(&self, id: &ProjectId) -> Option<Timestamp>;
            #[append_args(&self.source)] pub fn project_pushed(&self, id: &ProjectId) -> Option<Timestamp>;
            #[append_args(&self.source)] pub fn project_default_branch(&self, id: &ProjectId) -> Option<String>;
            #[append_args(&self.source)] pub fn project_change_contributions(&self, id: &ProjectId) -> Option<Vec<(User, usize)>>;
            #[append_args(&self.source)] pub fn project_change_contribution_ids(&self, id: &ProjectId) -> Option<Vec<(UserId, usize)>>;
            #[append_args(&self.source)] pub fn project_cumulative_change_contributions(&self, id: &ProjectId) -> Option<Vec<Percentage>>;
            #[append_args(&self.source)] pub fn project_commit_contributions(&self, id: &ProjectId) -> Option<Vec<(User, usize)>>;
            #[append_args(&self.source)] pub fn project_commit_contribution_ids(&self, id: &ProjectId) -> Option<Vec<(UserId, usize)>>;
            #[append_args(&self.source)] pub fn project_cumulative_commit_contributions(&self, id: &ProjectId) -> Option<Vec<Percentage>>;
            #[append_args(&self.source)] pub fn project_authors_contributing_commits(&self, id: &ProjectId, percentage: Percentage) -> Option<Vec<User>>;
            #[append_args(&self.source)] pub fn project_authors_contributing_changes(&self, id: &ProjectId, percentage: Percentage) -> Option<Vec<User>>;
            #[append_args(&self.source)] pub fn project_author_ids_contributing_commits(&self, id: &ProjectId, percentage: Percentage) -> Option<Vec<UserId>>;
            #[append_args(&self.source)] pub fn project_author_ids_contributing_changes(&self, id: &ProjectId, percentage: Percentage) -> Option<Vec<UserId>>;
            #[append_args(&self.source)] pub fn project_authors_contributing_commits_count(&self, id: &ProjectId, percentage: Percentage) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_authors_contributing_changes_count(&self, id: &ProjectId, percentage: Percentage) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_url(&self, id: &ProjectId) -> Option<String>;
            #[append_args(&self.source)] pub fn project_heads(&self, id: &ProjectId) -> Option<Vec<Head>>;
            #[append_args(&self.source)] pub fn project_commit_ids(&self, id: &ProjectId) -> Option<Vec<CommitId>>;
            #[append_args(&self.source)] pub fn project_main_branch_commit_ids(&self, id: &ProjectId) -> Option<Vec<CommitId>>;
            #[append_args(&self.source)] pub fn project_main_branch_commits(&self, id: &ProjectId) -> Option<Vec<Commit>>;
            #[append_args(&self.source)] pub fn project_commits(&self, id: &ProjectId) -> Option<Vec<Commit>>;
            #[append_args(&self.source)] pub fn project_commit_count(&self, id: &ProjectId) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_main_branch_commit_count(&self, id: &ProjectId) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_author_ids(&self, id: &ProjectId) -> Option<Vec<UserId>>;
            #[append_args(&self.source)] pub fn project_authors(&self, id: &ProjectId) -> Option<Vec<User>>;
            #[append_args(&self.source)] pub fn project_author_count(&self, id: &ProjectId) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_path_ids(&self, id: &ProjectId) -> Option<Vec<PathId>>;
            #[append_args(&self.source)] pub fn project_paths(&self, id: &ProjectId) -> Option<Vec<Path>>;
            #[append_args(&self.source)] pub fn project_path_count(&self, id: &ProjectId) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_snapshot_ids(&self, id: &ProjectId) -> Option<Vec<SnapshotId>>;
            #[append_args(&self.source)] pub fn project_snapshot_count(&self, id: &ProjectId) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_committer_ids(&self, id: &ProjectId) -> Option<Vec<UserId>>;
            #[append_args(&self.source)] pub fn project_committers(&self, id: &ProjectId) -> Option<Vec<User>>;
            #[append_args(&self.source)] pub fn project_committer_count(&self, id: &ProjectId) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_user_ids(&self, id: &ProjectId) -> Option<Vec<UserId>>;
            #[append_args(&self.source)] pub fn project_users(&self, id: &ProjectId) -> Option<Vec<User>>;
            #[append_args(&self.source)] pub fn project_user_count(&self, id: &ProjectId) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_lifetime(&self, id: &ProjectId) -> Option<Duration>;
            #[append_args(&self.source)] pub fn project_unique_files(&self, id: &ProjectId) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_original_files(&self, id: &ProjectId) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_impact(&self, id: &ProjectId) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_files(&self, id: &ProjectId) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_languages(&self, id: & ProjectId) -> Option<Vec<Language>>;
            #[append_args(&self.source)] pub fn project_language_composition(&self, id: & ProjectId) -> Option<Vec<(Language,usize)>>;
            #[append_args(&self.source)] pub fn project_languages_count(&self, id: & ProjectId) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_major_language(&self, id: &ProjectId) -> Option<Language>;
            #[append_args(&self.source)] pub fn project_major_language_ratio(&self, id: &ProjectId) -> Option<f64>;
            #[append_args(&self.source)] pub fn project_major_language_changes(&self, id: &ProjectId) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_all_forks(&self, id: &ProjectId) -> Option<Vec<ProjectId>>;
            #[append_args(&self.source)] pub fn project_all_forks_count(&self, id: &ProjectId) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_head_trees(&self, id: &ProjectId) -> Option<Vec<(String, Vec<(PathId, SnapshotId)>)>>;
            #[append_args(&self.source)] pub fn project_head_trees_count(&self, id : &ProjectId) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_max_commit_delta(&self, id: &ProjectId) -> Option<i64>;
            #[append_args(&self.source)] pub fn project_experience(&self, id: &ProjectId) -> Option<f64>;
            #[append_args(&self.source)] pub fn project_max_experience(&self, id: &ProjectId) -> Option<i32>;
            #[append_args(&self.source)] pub fn project_max_h_index1(&self, id: &ProjectId) -> Option<u64>;
            #[append_args(&self.source)] pub fn project_max_h_index2(&self, id: &ProjectId) -> Option<u64>;
            #[append_args(&self.source)] pub fn project_max_user_lifetime(&self, id: &ProjectId) -> Option<i64>;
            #[append_args(&self.source)] pub fn project_avg_commit_delta(&self, id: &ProjectId) -> Option<i64>;
            #[append_args(&self.source)] pub fn project_time_since_last_commit(&self, id: &ProjectId) -> Option<i64>;
            #[append_args(&self.source)] pub fn project_time_since_first_commit(&self, id: &ProjectId) -> Option<i64>;
            #[append_args(&self.source)] pub fn project_oldest_commit(&self, id: &ProjectId) -> Option<Commit>;
            #[append_args(&self.source)] pub fn project_newest_commit(&self, id: &ProjectId) -> Option<Commit>;
            #[append_args(&self.source)] pub fn project_is_abandoned(&self, id: &ProjectId) -> Option<bool>;
            #[append_args(&self.source)] pub fn project_locs(&self, id: &ProjectId) -> Option<usize>;
            #[append_args(&self.source)] pub fn project_duplicated_code(&self, id: &ProjectId) -> Option<f64>;
            #[append_args(&self.source)] pub fn project_latest_update_time(&self, id : &ProjectId) -> Option<i64>;
            #[append_args(&self.source)] pub fn project_is_valid(&self, id : &ProjectId) -> Option<bool>;

            // User/developer/author/committer attributes
            #[append_args(&self.source)] pub fn user(&self, id: &UserId) -> Option<User>;
            #[append_args(&self.source)] pub fn user_committed_commit_ids(&self, id: &UserId) -> Option<Vec<CommitId>>;
            #[append_args(&self.source)] pub fn user_authored_commits(&self, id: &UserId) -> Option<Vec<Commit>>;
            #[append_args(&self.source)] pub fn user_authored_commit_ids(&self, id: &UserId) -> Option<Vec<CommitId>>;
            #[append_args(&self.source)] pub fn user_committed_experience(&self, id: &UserId) -> Option<Duration>;
            #[append_args(&self.source)] pub fn user_author_experience(&self, id: &UserId) -> Option<Duration>;
            #[append_args(&self.source)] pub fn user_experience(&self, id: &UserId) -> Option<Duration>;
            #[append_args(&self.source)] pub fn user_committed_commit_count(&self, id: &UserId) -> Option<usize>;
            #[append_args(&self.source)] pub fn user_authored_commit_count(&self, id: &UserId) -> Option<usize>;
            #[append_args(&self.source)] pub fn user_committed_commits(&self, id: &UserId) -> Option<Vec<Commit>>;
            #[append_args(&self.source)] pub fn developer_experience(&self, id: &UserId) -> Option<i32>;
            #[append_args(&self.source)] pub fn user_lifetime(&self, id: &UserId) -> Option<(i64, i64)>;
            #[append_args(&self.source)] pub fn user_h_index1(&self, id: &UserId) -> Option<u64>;
            #[append_args(&self.source)] pub fn user_h_index2(&self, id: &UserId) -> Option<u64>;
            #[append_args(&self.source)] pub fn user_project_ids(&self, id: &UserId) -> Option<Vec<ProjectId>>;
            #[append_args(&self.source)] pub fn user_project_ids_count(&self, id: &UserId) -> Option<usize>;

            // File path attributes
            #[append_args(&self.source)] pub fn path(&self, id: &PathId) -> Option<Path>;

            // Commit attributes
            #[append_args(&self.source)] pub fn commit(&self, id: &CommitId) -> Option<Commit>;
            #[append_args(&self.source)] pub fn commit_hash(&self, id: &CommitId) -> Option<String>;
            #[append_args(&self.source)] pub fn commit_message(&self, id: &CommitId) -> Option<String>;
            #[append_args(&self.source)] pub fn commit_author_timestamp(&self, id: &CommitId) -> Option<Timestamp>;
            #[append_args(&self.source)] pub fn commit_committer_timestamp(&self, id: &CommitId) -> Option<Timestamp>;
            #[append_args(&self.source)] pub fn commit_changes(&self, id: &CommitId) -> Option<Vec<Change>>;
            #[append_args(&self.source)] pub fn commit_changed_paths(&self, id: &CommitId) -> Option<Vec<Path>>;
            #[append_args(&self.source)] pub fn commit_change_count(&self, id: &CommitId) -> Option<usize>;
            #[append_args(&self.source)] pub fn commit_changed_path_count(&self, id: &CommitId) -> Option<usize>;
            #[append_args(&self.source)] pub fn commit_projects(&self, id : &CommitId) -> Option<Vec<Project>>;
            #[append_args(&self.source)] pub fn commit_projects_count(&self, id: &CommitId) -> Option<usize>;
            #[append_args(&self.source)] pub fn commit_languages(&self, id : &CommitId) -> Option<Vec<Language>>;
            #[append_args(&self.source)] pub fn commit_languages_count(&self, id: &CommitId) -> Option<usize>;
            #[append_args(&self.source)] pub fn commit_changes_with_contents(&self, id: &CommitId) -> Option<Vec<Change>>;
            #[append_args(&self.source)] pub fn commit_change_with_contents_count(&self, id: &CommitId) -> Option<usize>;
            #[append_args(&self.source)] pub fn commit_trees(&self, id: &CommitId) -> Tree;
            #[append_args(&self.source)] pub fn commit_preceding_commit_ids(&self, id: &CommitId) -> Vec<CommitId>;
            #[append_args(&self.source)] pub fn commit_preceding_commits(&self, id: &CommitId) -> Vec<Commit>;

            // Snapshot attributes
            #[append_args(&self.source)] pub fn snapshot_locs(&self, id: &SnapshotId) -> Option<usize>;
            #[append_args(&self.source)] pub fn snapshot_unique_projects(&self, id: &SnapshotId) -> usize;
            #[append_args(&self.source)] pub fn snapshot_original_project(&self, id: &SnapshotId) -> ProjectId;
            #[append_args(&self.source)] pub fn snapshot_has_contents(&self, id: &SnapshotId) -> bool;

            // Entity IDs
            #[append_args(&self.source)] pub fn all_project_ids(&self) -> Vec<ProjectId>;
            #[append_args(&self.source)] pub fn all_user_ids(&self)    -> Vec<UserId>;
            #[append_args(&self.source)] pub fn all_path_ids(&self)    -> Vec<PathId>;
            #[append_args(&self.source)] pub fn all_commit_ids(&self)  -> Vec<CommitId>;

            // Misc.
            #[append_args(&self.source)] pub fn export_to_csv<S>(&self, dir: S) -> Result<(), std::io::Error> where S: Into<String>;
        }
    }

    pub fn snapshot(&self, id: &SnapshotId) -> Option<Snapshot> {
        self.source.get_snapshot(id.clone()).map(|bytes| Snapshot::new(id.clone(), bytes))
    }

    pub fn project_snapshots(&self, id: &ProjectId) -> Option<Vec<Snapshot>> {
        self.project_snapshot_ids(id).map(|vector| {
            vector.into_iter()
                .flat_map(|id| self.snapshot(&id))
                .collect::<Vec<Snapshot>>()
        })
    }
}
