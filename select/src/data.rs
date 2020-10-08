use std::collections::BTreeMap;
use std::path::PathBuf;
use std::rc::Weak;
use std::rc::Rc;
use std::cell::RefCell;
use std::fs::{File, create_dir_all};
use std::io::Error;

use itertools::{Itertools, MinMaxResult};

use dcd::{DCD, Database};

use crate::djanco;
use crate::log::*;
use crate::objects::*;
use crate::attrib::*;
use crate::persistence::*;
use crate::djanco::Spec;
use crate::time::{Month, Seconds};

macro_rules! give_me { ($option:expr)  => { $option.as_ref().unwrap() } }

/**===== Data ===================================================================================**/
pub struct Data {
    /** This is a self reference that can be shared out to others as DataPtr. **/
    me: Option<Weak<RefCell<Data>>>, // Thanks for the help, Colette.

    /** Warehouse whence the data originates before it is loaded into memory. **/
    warehouse: DCD,

    /** When loading data from the warehouse, these filters are used to remove some of it.
        If there are no filters, the data is loaded very differently than if there are filters. **/
    filters: Vec<Box<dyn LoadFilter>>,

    /** The basic settings of the entire system: warehouse path, seed, timestamp, log level. **/
    spec: djanco::Spec,

    /** Basic data quincunx. Projects at the center, and everything else in their orbit. **/
    projects: Option<BTreeMap<ProjectId, Project>>,
    commits:  Option<BTreeMap<CommitId,  Commit>>,
    users:    Option<BTreeMap<UserId,    User>>,
    paths:    Option<BTreeMap<PathId,    Path>>,
    //snapshots:    BTreeMap<SnapshotId,    Snapshot>,

    /** Derived relationships. **/
    commits_from_project:      Option<BTreeMap<ProjectId, Vec<CommitId>>>,
    users_from_project:        Option<BTreeMap<ProjectId, Vec<UserId>>>,
    authors_from_project:      Option<BTreeMap<ProjectId, Vec<UserId>>>,
    committers_from_project:   Option<BTreeMap<ProjectId, Vec<UserId>>>,
    paths_from_project:        Option<BTreeMap<ProjectId, Vec<PathId>>>,
    commits_committed_by_user: Option<BTreeMap<UserId,    Vec<CommitId>>>,
    commits_authored_by_user:  Option<BTreeMap<UserId,    Vec<CommitId>>>,

    /** Fields split off from the main objects because they are expected to be used less often, and
        therefore can be prevented from being loaded into memory most of the time. **/
    paths_from_commit:   Option<BTreeMap<CommitId, Vec<PathId>>>,
    //pub(crate) snapshots_from_commit:   Option<BTreeMap<CommitId, HashMap<PathId, SnapshotId>>>,
    message_from_commit: Option<BTreeMap<CommitId, Message>>,
    //pub(crate) metadata_for_project:   Option<BTreeMap<ProjectId, HashMap<String, String>>>,

    /** Derived properties: pre-calculated convenience properties that are expected to be used
        often and therefore are worth doing once and storing. **/
    age_from_project:     Option<BTreeMap<ProjectId, (u64, u64)>>,
    experience_from_user: Option<BTreeMap<UserId,    (u64, u64)>>,
}

/**===== Data: basic methods ====================================================================**/

impl Data {
    /** The constructor does not actually create a Data object. Data objects are only ever
        accessible through DataPtr and must be untangled to be used. **/
    pub fn new(warehouse: &PathBuf, cache: &Option<PathBuf>,
               timestamp: Month, seed: u128, log_level: LogLevel) -> DataPtr {
        Self::new_with_filters(warehouse, cache, timestamp, seed, log_level, None)
    }

    pub fn from_warehouse(dcd: DCD, warehouse: &PathBuf, cache: &Option<PathBuf>,
        timestamp: Month, seed: u128, log_level: LogLevel) -> DataPtr {
        Self::from_warehouse_with_filters(dcd, warehouse, cache, timestamp, seed, log_level, None)
    }

    pub fn new_with_filters(warehouse: &PathBuf, cache: &Option<PathBuf>,
               timestamp: Month, seed: u128, log_level: LogLevel,
               filters: Option<Vec<Box<dyn LoadFilter>>>) -> DataPtr {
        //let spec = djanco::Spec::from_paths(warehouse.clone(), cache.clone(), seed, timestamp, log_level);
        let dcd = DCD::new(warehouse.to_str().unwrap().to_string());
        Self::from_warehouse_with_filters(dcd, warehouse, cache, timestamp, seed, log_level, filters)
    }

    pub fn from_warehouse_with_filters(dcd: DCD, warehouse: &PathBuf, cache: &Option<PathBuf>,
                                       timestamp: Month, seed: u128, log_level: LogLevel,
                                       filters: Option<Vec<Box<dyn LoadFilter>>>) -> DataPtr {

        let spec = Spec::from_paths(warehouse.clone(), cache.clone(), seed, timestamp, log_level);

        let data = Data {
            spec,
            warehouse: dcd,

            filters: filters.unwrap_or(vec![]),

            projects: None, commits: None, users: None, paths: None,

            commits_from_project: None, users_from_project: None,
            authors_from_project: None, committers_from_project: None,
            paths_from_project: None,
            age_from_project: None,

            paths_from_commit: None, message_from_commit: None,

            commits_committed_by_user: None, commits_authored_by_user: None,
            experience_from_user: None,

            me: None,
        };

        // Things we do to avoid unsafe.
        let pointer: DataPtr = Rc::new(RefCell::new(data));
        pointer.borrow_mut().me = Some(Rc::downgrade(&pointer));
        pointer
    }

    pub fn me(&self) -> DataPtr {
        give_me!(self.me).upgrade().unwrap()
    }

    pub fn spec(&self) -> &Spec { &self.spec }
}

/**===== Data: lazy loading methods =============================================================**/
macro_rules! lazy_projects { ($self:expr) => {{ $self.load_projects().unwrap(); give_me!($self.projects) }} }
macro_rules! lazy_commits  { ($self:expr) => {{ $self.load_commits().unwrap();  give_me!($self.commits) }}  }
macro_rules! lazy_paths { ($self:expr) => {{ $self.load_paths().unwrap(); give_me!($self.paths) }} }
macro_rules! lazy_users { ($self:expr) => {{ $self.load_users().unwrap(); give_me!($self.users) }} }
//macro_rules! lazy_snapshots { ($self:expr) => {{ $self.load_snapshots().unwrap(); give_me!($self.snapshots) }} }
macro_rules! lazy_commits_from_project { ($self:expr) => {{ $self.load_commits_from_project().unwrap(); give_me!($self.commits_from_project) }} }
macro_rules! lazy_users_from_project { ($self:expr) => {{ $self.load_users_from_project().unwrap(); give_me!($self.users_from_project) }} }
macro_rules! lazy_authors_from_project { ($self:expr) => {{ $self.load_authors_from_project().unwrap(); give_me!($self.authors_from_project) }} }
macro_rules! lazy_committers_from_project { ($self:expr) => {{ $self.load_committers_from_project().unwrap(); give_me!($self.committers_from_project) }} }
macro_rules! lazy_paths_from_project { ($self:expr) => {{ $self.load_paths_from_project().unwrap(); give_me!($self.paths_from_project) }} }
macro_rules! lazy_paths_from_commit { ($self:expr) => {{ $self.load_paths_from_commit().unwrap(); give_me!($self.paths_from_commit) }} }
//macro_rules! lazy_snapshots_from_commit { ($self:expr) => {{ $self.load_snapshots_from_commit().unwrap(); give_me!($self.snapshots_from_commit) }} }
macro_rules! lazy_message_from_commit { ($self:expr) => {{ $self.load_message_from_commit().unwrap(); give_me!($self.message_from_commit) }} }
//macro_rules! lazy_metadata_from_project { ($self:expr) => {{ $self.load_metadata_for_project().unwrap(); give_me!($self.metadata_for_project) }} }
macro_rules! lazy_age_from_project { ($self:expr) => {{ $self.load_age_from_project().unwrap(); give_me!($self.age_from_project) }} }
macro_rules! lazy_experience_from_user { ($self:expr) => {{ $self.load_experience_from_user().unwrap(); give_me!($self.experience_from_user) }} }
macro_rules! lazy_commits_committed_by_user { ($self:expr) => {{ $self.load_commits_committed_by_user().unwrap(); give_me!($self.commits_committed_by_user) }} }
macro_rules! lazy_commits_authored_by_user { ($self:expr) => {{ $self.load_commits_authored_by_user().unwrap(); give_me!($self.commits_authored_by_user) }} }

/**===== Data: convenience ======================================================================**/
macro_rules! count_relationships {
    ($data:expr) => { $data.iter().map(|(_, v)| v.len()).sum::<usize>() }
}

/**===== Data: data access methods ==============================================================**/
impl /* DataAccess for */ Data {
    pub fn seed(&self) -> u128 { self.spec.seed }

    pub fn project_count(&mut self)     -> usize { lazy_projects!(self).len() }
    pub fn commit_count(&mut self)      -> usize { lazy_commits!(self).len()  }
    pub fn user_count(&mut self)        -> usize { lazy_users!(self).len()    }
    pub fn path_count(&mut self)        -> usize { lazy_paths!(self).len()    }

    pub fn project(&mut self, id: &ProjectId) -> Option<&Project> { lazy_projects!(self).get(id) }
    pub fn commit(&mut self, id: &CommitId)   -> Option<&Commit>  { lazy_commits!(self).get(id)  }
    pub fn user(&mut self, id: &UserId)       -> Option<&User>    { lazy_users!(self).get(id)    }
    pub fn path(&mut self, id: &PathId)       -> Option<&Path>    { lazy_paths!(self).get(id)    }

    pub fn project_ids(&mut self) -> impl Iterator<Item=&ProjectId> { lazy_projects!(self).keys() }
    pub fn commit_ids(&mut self)  -> impl Iterator<Item=&CommitId>  { lazy_commits!(self).keys()  }
    pub fn user_ids(&mut self)    -> impl Iterator<Item=&UserId>    { lazy_users!(self).keys()    }
    pub fn path_ids(&mut self)    -> impl Iterator<Item=&PathId>    { lazy_paths!(self).keys()    }

    /** Project iterators **/
    pub fn project_iter(&mut self) -> impl Iterator<Item=&Project> {
        lazy_projects!(self).iter().map(|(_, project)| project)
    }

    pub fn projects(&mut self) -> Vec<Project> {
        lazy_projects!(self).iter().map(|(_, project)| project.clone()).collect()
    }

    pub fn projects_with_filter<Filter>(&mut self, filter: Filter) -> Vec<Project> where Filter: Fn(&&Project) -> bool {
        lazy_projects!(self).iter()
            .filter(|(_, project)| filter(project))
            .map(|(_, project)| project.clone())
            .collect()
    }

    pub fn projects_with_map<Map,T>(&mut self, map: Map) -> Vec<T> where Map: Fn(&Project) -> T {
        lazy_projects!(self).iter().map(|(_, project)| map(project)).collect()
    }

    pub fn projects_with_flat_map<Map,T,I>(&mut self, map: Map) -> Vec<T> where Map: Fn(&Project) -> I, I: IntoIterator<Item=T> {
        lazy_projects!(self).iter().flat_map(|(_, project)| map(project)).collect()
    }

    /** Commit iterators **/
    pub fn commit_iter(&mut self) -> impl Iterator<Item=&Commit> {
        lazy_commits!(self).iter().map(|(_, commit)| commit)
    }

    pub fn commits(&mut self) -> Vec<Commit> {
        lazy_commits!(self).iter().map(|(_, commit)| commit.clone()).collect()
    }

    pub fn commits_with_filter<Filter>(&mut self, filter: Filter) -> Vec<Commit> where Filter: Fn(&&Commit) -> bool {
        lazy_commits!(self).iter()
            .filter(|(_, commit)| filter(commit))
            .map(|(_, commit)| commit.clone())
            .collect()
    }

    pub fn commits_with_map<Map,T>(&mut self, map: Map) -> Vec<T> where Map: Fn(&Commit) -> T {
        lazy_commits!(self).iter().map(|(_, commit)| map(commit)).collect()
    }

    pub fn commits_with_flat_map<Map,T,I>(&mut self, map: Map) -> Vec<T> where Map: Fn(&Commit) -> I, I: IntoIterator<Item=T> {
        lazy_commits!(self).iter().flat_map(|(_, commit)| map(commit)).collect()
    }

    pub fn commit_refs_from(&mut self, project: &ProjectId) -> Vec<&Commit> {
        self.load_commits().unwrap();
        self.load_commits_from_project().unwrap();
        let commits = give_me!(self.commits);
        let commits_from_project = give_me!(self.commits_from_project);
        commits_from_project.get(project).map_or(Default::default(), |commit_ids| {
            commit_ids.iter()
                .flat_map(|commit_id| commits.get(commit_id))
                .collect()
        })
    }

    pub fn commits_from(&mut self, project: &ProjectId) -> Vec<Commit> {
        self.load_commits().unwrap();
        self.load_commits_from_project().unwrap();
        let commits = give_me!(self.commits);
        let commits_from_project = give_me!(self.commits_from_project);
        commits_from_project.get(project).map_or(Default::default(), |commit_ids| {
            commit_ids.iter()
                .flat_map(|commit_id| commits.get(commit_id))
                .map(|commit| commit.clone())
                .collect()
        })
    }

    // pub fn commits_with_filter_from<Filter>(&mut self, filter: Filter, project: &ProjectId) -> Vec<Commit> where Filter: Fn(&&Commit) -> bool {
    //     self.load_commits().unwrap();
    //     self.load_commits_from_project().unwrap();
    //     let commits = give_me!(self.commits);
    //     let commits_from_project = give_me!(self.commits_from_project);
    //     commits_from_project.get(project).map_or(Default::default(), |commit_ids| {
    //         commit_ids.iter()
    //             .flat_map(|commit_id| commits.get(commit_id))
    //             .filter(filter)
    //             .map(|commit| commit.clone())
    //             .collect()
    //     })
    // }

    pub fn commit_count_from(&mut self, project: &ProjectId) -> usize {
        lazy_commits_from_project!(self).get(project).map_or(0, |v| v.len())
    }

    pub fn commit_conditional_count_from<Filter>(&mut self, project: &ProjectId, filter: Filter) -> usize where Filter: Fn(&&CommitId) -> bool {
        lazy_commits_from_project!(self).get(project)
            .map_or(0, |v| v.iter().filter(filter).count())
    }

    /** User iterators **/
    pub fn user_iter(&mut self) -> impl Iterator<Item=&User> {
        lazy_users!(self).iter().map(|(_, user)| user)
    }

    pub fn users(&mut self) -> Vec<User> {
        lazy_users!(self).iter().map(|(_, user)| user.clone()).collect()
    }

    pub fn users_with_filter<Filter>(&mut self, filter: Filter) -> Vec<User> where Filter: Fn(&&User) -> bool {
        lazy_users!(self).iter()
            .filter(|(_, user)| filter(user))
            .map(|(_, user)| user.clone())
            .collect()
    }
    pub fn users_with_map<Map,T>(&mut self, map: Map) -> Vec<T> where Map: Fn(&User) -> T {
        lazy_users!(self).iter().map(|(_, user)| map(user)).collect()
    }

    pub fn users_with_flat_map<Map,T,I>(&mut self, map: Map) -> Vec<T> where Map: Fn(&User) -> I, I: IntoIterator<Item=T> {
        lazy_users!(self).iter().flat_map(|(_, user)| map(user)).collect()
    }

    pub fn user_refs_from(&mut self, project: &ProjectId) -> Vec<&User> {
        self.load_users().unwrap();
        self.load_users_from_project().unwrap();
        let users = give_me!(self.users);
        let users_from_project = give_me!(self.users_from_project);
        users_from_project.get(project).map_or(Default::default(), |user_ids| {
            user_ids.iter()
                .flat_map(|user_id| users.get(user_id))
                .collect()
        })
    }

    pub fn users_from(&mut self, project: &ProjectId) -> Vec<User> {
        self.load_users().unwrap();
        self.load_users_from_project().unwrap();
        let users = give_me!(self.users);
        let users_from_project = give_me!(self.users_from_project);
        users_from_project.get(project).map_or(Default::default(), |user_ids| {
            user_ids.iter()
                .flat_map(|user_id| users.get(user_id))
                .map(|user| user.clone())
                .collect()
        })
    }

    pub fn user_count_from(&mut self, project: &ProjectId) -> usize {
        lazy_users_from_project!(self).get(project).map_or(0, |v| v.len())
    }

    pub fn user_conditional_count_from<Filter>(&mut self, project: &ProjectId, filter: Filter) -> usize where Filter: Fn(&&UserId) -> bool {
        lazy_users_from_project!(self).get(project)
            .map_or(0, |v| v.iter().filter(filter).count())
    }

    /** Author (user) iterators **/
    pub fn author_refs_from(&mut self, project: &ProjectId) -> Vec<&User> {
        self.load_users().unwrap();
        self.load_authors_from_project().unwrap();
        let users = give_me!(self.users);
        let authors_from_project = give_me!(self.authors_from_project);
        authors_from_project.get(project).map_or(Default::default(), |user_ids| {
            user_ids.iter()
                .flat_map(|user_id| users.get(user_id))
                .collect()
        })
    }

    pub fn authors_from(&mut self, project: &ProjectId) -> Vec<User> {
        self.load_users().unwrap();
        self.load_authors_from_project().unwrap();
        let users = give_me!(self.users);
        let authors_from_project = give_me!(self.authors_from_project);
        authors_from_project.get(project).map_or(Default::default(), |user_ids| {
            user_ids.iter()
                .flat_map(|user_id| users.get(user_id))
                .map(|user| user.clone())
                .collect()
        })
    }

    pub fn author_count_from(&mut self, project: &ProjectId) -> usize {
        lazy_authors_from_project!(self).get(project).map_or(0, |v| v.len())
    }

    pub fn author_conditional_count_from<Filter>(&mut self, project: &ProjectId, filter: Filter) -> usize where Filter: Fn(&&UserId) -> bool {
        lazy_authors_from_project!(self).get(project)
            .map_or(0, |v| v.iter().filter(filter).count())
    }

    /** Committer (user) iterators **/
    pub fn committer_refs_from(&mut self, project: &ProjectId) -> Vec<&User> {
        self.load_users().unwrap();
        self.load_authors_from_project().unwrap();
        let users = give_me!(self.users);
        let committers_from_project = give_me!(self.committers_from_project);
        committers_from_project.get(project).map_or(Default::default(), |user_ids| {
            user_ids.iter()
                .flat_map(|user_id| users.get(user_id))
                .collect()
        })
    }

    pub fn committer_from(&mut self, project: &ProjectId) -> Vec<User> {
        self.load_users().unwrap();
        self.load_committers_from_project().unwrap();
        let users = give_me!(self.users);
        let committers_from_project = give_me!(self.committers_from_project);
        committers_from_project.get(project).map_or(Default::default(), |user_ids| {
            user_ids.iter()
                .flat_map(|user_id| users.get(user_id))
                .map(|user| user.clone())
                .collect()
        })
    }

    pub fn committer_count_from(&mut self, project: &ProjectId) -> usize {
        lazy_committers_from_project!(self).get(project).map_or(0, |v| v.len())
    }

    pub fn committer_conditional_count_from<Filter>(&mut self, project: &ProjectId, filter: Filter) -> usize where Filter: Fn(&&UserId) -> bool {
        lazy_committers_from_project!(self).get(project)
            .map_or(0, |v| v.iter().filter(filter).count())
    }

    /** Path iterators **/
    pub fn path_iter(&mut self) -> impl Iterator<Item=&Path> {
        lazy_paths!(self).iter().map(|(_, path)| path)
    }

    pub fn paths(&mut self) -> Vec<Path> {
        lazy_paths!(self).iter().map(|(_, path)| path.clone()).collect()
    }

    pub fn paths_with_filter<Filter>(&mut self, filter: Filter) -> Vec<Path> where Filter: Fn(&&Path) -> bool {
        lazy_paths!(self).iter()
            .filter(|(_, path)| filter(path)).map(|(_, path)| path.clone()).collect()
    }

    pub fn paths_with_map<Map,T>(&mut self, map: Map) -> Vec<T> where Map: Fn(&Path) -> T {
        lazy_paths!(self).iter().map(|(_, path)| map(path)).collect()
    }

    pub fn paths_with_flat_map<Map,T,I>(&mut self, map: Map) -> Vec<T> where Map: Fn(&Path) -> I, I: IntoIterator<Item=T> {
        lazy_paths!(self).iter().flat_map(|(_, path)| map(path)).collect()
    }

    pub fn path_refs_from(&mut self, project: &ProjectId) -> Vec<&Path> {
        self.load_paths().unwrap();
        self.load_paths_from_project().unwrap();
        let paths = give_me!(self.paths);
        let paths_from_project = give_me!(self.paths_from_project);
        paths_from_project.get(project).map_or(Default::default(), |path_ids| {
            path_ids.iter().flat_map(|path_id| paths.get(path_id)).collect()
        })
    }

    pub fn paths_from(&mut self, project: &ProjectId) -> Vec<Path> {
        self.load_paths().unwrap();
        self.load_paths_from_project().unwrap();
        let paths = give_me!(self.paths);
        let paths_from_project = give_me!(self.paths_from_project);
        paths_from_project.get(project).map_or(Default::default(), |path_ids| {
            path_ids.iter()
                .flat_map(|path_id| paths.get(path_id)).map(|path| path.clone()).collect()
        })
    }

    pub fn path_count_from(&mut self, project: &ProjectId) -> usize {
        lazy_paths_from_project!(self).get(project).map_or(0, |v| v.len())
    }

    pub fn path_conditional_count_from<Filter>(&mut self, project: &ProjectId, filter: Filter) -> usize where Filter: Fn(&&PathId) -> bool {
        lazy_paths_from_project!(self).get(project).map_or(0, |v| v.iter().filter(filter).count())
    }

    pub fn age_of(&mut self, project: &ProjectId) -> Option<Seconds> {
        lazy_age_from_project!(self).get(project).map(|(max, min)| Seconds::from(max - min))
    }

    pub fn message_of(&mut self, commit: &CommitId) -> Option<Message> {
        lazy_message_from_commit!(self).get(commit).map(|msg| msg.clone())
    }

    pub fn experience_of(&mut self, user: &UserId) -> Option<Seconds> {
        lazy_experience_from_user!(self).get(user).map(|(max, min)| Seconds::from(max - min))
    }

    pub fn authored_commits_of(&mut self, user: &UserId) -> Vec<Commit> {
        self.load_commits().unwrap();
        self.load_commits_committed_by_user().unwrap();
        let commits = give_me!(self.commits);
        let commits_authored_by_user = give_me!(self.commits_authored_by_user);
        commits_authored_by_user.get(user)
            .map_or(vec![], |v| {
                v.iter().flat_map(|id| commits.get(id)).map(|c| c.clone()).collect()
            })
    }

    pub fn committed_commits_of(&mut self, user: &UserId) -> Vec<Commit> {
        self.load_commits().unwrap();
        self.load_commits_committed_by_user().unwrap();
        let commits = give_me!(self.commits);
        let commits_committed_by_user = give_me!(self.commits_committed_by_user);
        commits_committed_by_user.get(user)
            .map_or(vec![], |v| {
                v.iter().flat_map(|id| commits.get(id)).map(|c| c.clone()).collect()
            })
    }

    pub fn authored_commit_count_of(&mut self, user: &UserId) -> usize {
        lazy_commits_authored_by_user!(self).get(user).map_or(0, |e| e.len())
    }

    pub fn committed_commit_count_of(&mut self, user: &UserId) -> usize {
        lazy_commits_committed_by_user!(self).get(user).map_or(0, |e| e.len())
    }

    pub fn paths_of(&mut self, commit: &CommitId) -> Vec<Path> {
        self.load_paths().unwrap();
        self.load_paths_from_commit().unwrap();
        let paths = give_me!(self.paths);
        let paths_from_commit = give_me!(self.paths_from_commit);
        paths_from_commit.get(commit).map_or(vec![], |v| {
            v.iter().flat_map(|id| paths.get(id)).map(|p| p.clone()).collect()
        })
    }

    pub fn path_count_of(&mut self, commit: &CommitId) -> usize {
        lazy_paths_from_commit!(self).get(commit).map_or(0, |e| e.len())
    }

    // TODO There's quite a few convenience methods that can be added here.
}

/**===== Data: data loading methods, directs towards caching or filtering =======================**/
impl Data { // TODO Maybe there's better ways of doing this, like composition or just macros
    fn load_projects(&mut self) -> Result<(), Error> {
        if self.projects.is_some() {
            return Ok(())
        }
        if self.file_in_cache("projects") {
            return self.load_projects_from_cache()
        }
        if self.filters.is_empty() {
            self.load_projects_without_filters()?
        } else {
            self.load_projects_with_filters()?
        }
        if self.spec.database.is_some() {
            self.dump_projects_to_cache()
        } else {
            Ok(())
        }
    }

    fn load_commits(&mut self) -> Result<(), Error> {
        if self.commits.is_some() {
            return Ok(())
        }
        if self.file_in_cache("commits") {
            return self.load_commits_from_cache() // FIXME remember filters!!!
        }
        if self.filters.is_empty() {
            self.load_commits_without_filters()?
        } else {
            self.load_commits_with_filters()?
        }
        if self.spec.database.is_some() {
            self.dump_commits_to_cache()
        } else {
            Ok(())
        }
    }

    fn load_users(&mut self) -> Result<(), Error> {
        if self.users.is_some() {
            return Ok(())
        }
        if self.file_in_cache("users") {
            return self.load_users_from_cache()
        }
        if self.filters.is_empty() {
            self.load_users_without_filters()?
        } else {
            self.load_users_with_filters()?
        }
        if self.spec.database.is_some() {
            self.dump_users_to_cache()
        } else {
            Ok(())
        }
    }

    fn load_paths(&mut self) -> Result<(), Error> {
        if self.paths.is_some() {
            return Ok(())
        }
        if self.file_in_cache("paths") {
            return self.load_paths_from_cache()
        }
        if self.filters.is_empty() {
            self.load_paths_without_filters()?
        } else {
            self.load_paths_with_filters()?
        }
        if self.spec.database.is_some() {
            self.dump_paths_to_cache()
        } else {
            Ok(())
        }
    }

    fn load_commits_from_project(&mut self) -> Result<(), Error> {
        if self.commits_from_project.is_some() {
            return Ok(())
        }
        if self.file_in_cache("commits_from_project") {
            return self.load_commits_from_project_from_cache()
        }
        if self.filters.is_empty() {
            self.load_commits_from_project_without_filters()?
        } else {
            self.load_commits_from_project_with_filters()?
        }
        if self.spec.database.is_some() {
            self.dump_commits_from_project_to_cache()
        } else {
            Ok(())
        }
    }

    fn load_users_from_project(&mut self) -> Result<(), Error> {
        if self.users_from_project.is_some() {
            return Ok(())
        }
        if self.file_in_cache("users_from_project") {
            return self.load_users_from_project_from_cache()
        }
        if self.filters.is_empty() {
            self.load_users_from_project_without_filters()?
        } else {
            self.load_users_from_project_with_filters()?
        }
        if self.spec.database.is_some() {
            self.dump_users_from_project_to_cache()
        } else {
            Ok(())
        }
    }

    fn load_authors_from_project(&mut self) -> Result<(), Error> {
        if self.authors_from_project.is_some() {
            return Ok(())
        }
        if self.file_in_cache("authors_from_project") {
            return self.load_authors_from_project_from_cache()
        }
        if self.filters.is_empty() {
            self.load_authors_from_project_without_filters()?
        } else {
            self.load_authors_from_project_with_filters()?
        }
        if self.spec.database.is_some() {
            self.dump_authors_from_project_to_cache()
        } else {
            Ok(())
        }
    }

    fn load_committers_from_project(&mut self) -> Result<(), Error> {
        if self.committers_from_project.is_some() {
            return Ok(())
        }
        if self.file_in_cache("committers_from_project") {
            return self.load_committers_from_project_from_cache()
        }
        if self.filters.is_empty() {
            self.load_committers_from_project_without_filters()?
        } else {
            self.load_committers_from_project_with_filters()?
        }
        if self.spec.database.is_some() {
            self.dump_committers_from_project_to_cache()
        } else {
            Ok(())
        }
    }

    fn load_paths_from_project(&mut self) -> Result<(), Error> {
        if self.paths_from_project.is_some() {
            return Ok(())
        }
        if self.file_in_cache("paths_from_project") {
            return self.load_paths_from_project_from_cache()
        }
        if self.filters.is_empty() {
            self.load_paths_from_project_without_filters()?
        } else {
            self.load_paths_from_project_with_filters()?
        }
        if self.spec.database.is_some() {
            self.dump_paths_from_project_to_cache()
        } else {
            Ok(())
        }
    }

    fn load_age_from_project(&mut self) -> Result<(), Error> {
        if self.age_from_project.is_some() {
            return Ok(())
        }
        if self.file_in_cache("age_from_project") {
            return self.load_age_from_project_from_cache()
        }
        if self.filters.is_empty() {
            self.load_age_from_project_without_filters()?
        } else {
            self.load_age_from_project_with_filters()?
        }
        if self.spec.database.is_some() {
            self.dump_age_from_project_to_cache()
        } else {
            Ok(())
        }
    }

    fn load_paths_from_commit(&mut self) -> Result<(), Error> {
        if self.paths_from_commit.is_some() {
            return Ok(())
        }
        if self.file_in_cache("paths_from_commit") {
            return self.load_paths_from_commit_from_cache()
        }
        if self.filters.is_empty() {
            self.load_paths_from_commit_without_filters()?
        } else {
            self.load_paths_from_commit_with_filters()?
        }
        if self.spec.database.is_some() {
            self.dump_paths_from_commit_to_cache()
        } else {
            Ok(())
        }
    }

    fn load_message_from_commit(&mut self) -> Result<(), Error> {
        if self.message_from_commit.is_some() {
            return Ok(())
        }
        if self.file_in_cache("message_from_commit") {
            return self.load_message_from_commit_from_cache()
        }
        if self.filters.is_empty() {
            self.load_message_from_commit_without_filters()?
        } else {
            self.load_message_from_commit_with_filters()?
        }
        if self.spec.database.is_some() {
            self.dump_message_from_commit_to_cache()
        } else {
            Ok(())
        }
    }

    fn load_experience_from_user(&mut self) -> Result<(), Error> {
        if self.experience_from_user.is_some() {
            return Ok(())
        }
        if self.file_in_cache("experience_from_user") {
            return self.load_experience_from_user_from_cache()
        }
        if self.filters.is_empty() {
            self.load_experience_from_user_without_filters()?
        } else {
            self.load_experience_from_user_with_filters()?
        }
        if self.spec.database.is_some() {
            self.dump_experience_from_user_to_cache()
        } else {
            Ok(())
        }
    }

    fn load_commits_authored_by_user(&mut self) -> Result<(), Error> {
        if self.commits_authored_by_user.is_some() {
            return Ok(())
        }
        if self.file_in_cache("commits_authored_by_user") {
            return self.load_commits_authored_by_user_from_cache()
        }
        if self.filters.is_empty() {
            self.load_commits_authored_by_user_without_filters()?
        } else {
            self.load_commits_authored_by_user_with_filters()?
        }
        if self.spec.database.is_some() {
            self.dump_commits_authored_by_user_to_cache()
        } else {
            Ok(())
        }
    }

    fn load_commits_committed_by_user(&mut self) -> Result<(), Error> {
        if self.commits_committed_by_user.is_some() {
            return Ok(())
        }
        if self.file_in_cache("commits_committed_by_user") {
            return self.load_commits_committed_by_user_from_cache()
        }
        if self.filters.is_empty() {
            self.load_commits_committed_by_user_without_filters()?
        } else {
            self.load_commits_committed_by_user_with_filters()?
        }
        if self.spec.database.is_some() {
            self.dump_commits_committed_by_user_to_cache()
        } else {
            Ok(())
        }
    }
}

/**===== Data: data loading methods (unfiltered) ================================================**/
impl Data {
    fn load_projects_without_filters(&mut self) -> Result<(), Error> {

        log_item!(self.spec.log_level, "loading project data");
        let projects: BTreeMap<ProjectId, Project> =
            self.warehouse.projects().into_iter()
                .map(|project| (ProjectId::from(project.id), Project::from(project)))
                .collect();
        log_addendum!(self.spec.log_level,
                      format!("loaded project data for {} projects", projects.len()));

        Ok(self.projects = Some(projects))
    }

    fn load_commits_without_filters(&mut self) -> Result<(), Error> {

        log_item!(self.spec.log_level, "loading commit data");
        let commits: BTreeMap<CommitId, Commit> =
            self.warehouse.bare_commits().into_iter()
                .map(|commit| (CommitId::from(commit.id), Commit::from(commit)))
                .collect();
        log_addendum!(self.spec.log_level,
                      format!("loaded commit data for {} commits", commits.len()));

        Ok(self.commits = Some(commits))
    }

    fn load_users_without_filters(&mut self) -> Result<(), Error> {

        log_item!(self.spec.log_level, "loading user data");
        let users: BTreeMap<UserId, User> =
            self.warehouse.users().into_iter()
                .map(|user| (UserId::from(user.id), User::from(user)))
                .collect();
        log_addendum!(self.spec.log_level, format!("loaded user data for {} users", users.len()));

        Ok(self.users = Some(users))
    }

    fn load_paths_without_filters(&mut self) -> Result<(), Error> {

        log_item!(self.spec.log_level, "loading path data");
        let paths: BTreeMap<PathId, Path> =
            (0..self.warehouse.num_file_paths())
                .flat_map(|path_id| self.warehouse.get_file_path(path_id))
                .map(|path| (PathId::from(path.id), Path::from(path)))
                .collect();
        log_addendum!(self.spec.log_level, format!("loaded path data for {} paths", paths.len()));

        Ok(self.paths = Some(paths))
    }

    fn load_commits_from_project_without_filters(&mut self) -> Result<(), Error> {

        log_item!(self.spec.log_level, "loading project-commit mapping");
        let commits_from_project: BTreeMap<ProjectId, Vec<CommitId>> =
            self.warehouse.project_ids_and_their_commit_ids().into_iter()
                .map(|(id, commit_ids)| {
                    (ProjectId::from(id),
                     commit_ids.into_iter()
                         .map(|commit_id| CommitId::from(commit_id))
                         .collect())
                })
                .collect();
        log_addendum!(self.spec.log_level, format!("loaded {} relationships",
                                     count_relationships!(commits_from_project)));

        Ok(self.commits_from_project = Some(commits_from_project))
    }

    fn load_users_from_project_without_filters(&mut self) -> Result<(), Error> {
        self.load_commits().unwrap();
        self.load_commits_from_project().unwrap();
        let commits = give_me!(self.commits);
        let commits_from_project = give_me!(self.commits_from_project);

        log_item!(self.spec.log_level, "loading project-user mapping");
        let users_from_project: BTreeMap<ProjectId, Vec<UserId>>  =
            commits_from_project.iter()
                .map(|(id, commit_ids)|
                    (*id,
                     commit_ids.into_iter()
                         .flat_map(|commit_id| commits.get(commit_id))
                         .flat_map(|commit| commit.user_ids())
                         .unique()
                         .collect()))
                .collect();
        log_addendum!(self.spec.log_level, format!("loaded {} relationships",
                                     count_relationships!(users_from_project)));

        Ok(self.users_from_project = Some(users_from_project))
    }

    fn load_paths_from_commit_without_filters(&mut self) -> Result<(), Error> {

        log_item!(self.spec.log_level, "loading commit-path mapping");
        let paths_from_commit: BTreeMap<CommitId, Vec<PathId>> =
            self.warehouse
                .commits()
                .map(|commit|
                    (CommitId::from(commit.id),
                     commit.changes.as_ref().map_or(vec![], |changes| {
                         changes.into_iter()
                             .map(|(path_id, _)| PathId::from(*path_id))
                             .collect()
                     })))
                .collect();
        log_addendum!(self.spec.log_level, format!("loaded {} relationships",
                                     count_relationships!(paths_from_commit)));

        Ok(self.paths_from_commit = Some(paths_from_commit))
    }

    fn load_message_from_commit_without_filters(&mut self) -> Result<(), Error> {

        log_item!(self.spec.log_level, "loading commit messages");
        let message_from_commit: BTreeMap<CommitId, Message> =
            self.warehouse.commits()
                .flat_map(|commit| {
                    commit.message.as_ref().map(|message| {
                        (CommitId::from(commit.id), Message::from(message))
                    })
                })
                .collect();
        log_addendum!(self.spec.log_level, format!("loaded {} messages", message_from_commit.len()));

        Ok(self.message_from_commit = Some(message_from_commit))
    }

    fn load_authors_from_project_without_filters(&mut self) -> Result<(), Error> {
        self.load_commits().unwrap();
        self.load_commits_from_project().unwrap();
        let commits = give_me!(self.commits);
        let commits_from_project = give_me!(self.commits_from_project);

        log_item!(self.spec.log_level, "loading project-author mapping");
        let authors_from_project: BTreeMap<ProjectId, Vec<UserId>>  =
            commits_from_project.iter()
                .map(|(id, commit_ids)|
                    (*id,
                     commit_ids.into_iter()
                         .flat_map(|commit_id| commits.get(commit_id))
                         .map(|commit| commit.author)
                         .unique()
                         .collect()))
                .collect();
        log_addendum!(self.spec.log_level, format!("loaded {} relationships",
                                     count_relationships!(authors_from_project)));

        Ok(self.authors_from_project = Some(authors_from_project))
    }

    fn load_committers_from_project_without_filters(&mut self) -> Result<(), Error> {
        self.load_commits().unwrap();
        self.load_commits_from_project().unwrap();
        let commits = give_me!(self.commits);
        let commits_from_project = give_me!(self.commits_from_project);

        log_item!(self.spec.log_level, "loading project-committer mapping");
        let committers_from_project: BTreeMap<ProjectId, Vec<UserId>>  =
            commits_from_project.iter()
                .map(|(id, commit_ids)|
                    (*id,
                     commit_ids.into_iter()
                         .flat_map(|commit_id| commits.get(commit_id))
                         .map(|commit| commit.committer)
                         .unique()
                         .collect()))
                .collect();
        log_addendum!(self.spec.log_level, format!("loaded {} relationships",
                  count_relationships!(committers_from_project)));

        Ok(self.committers_from_project = Some(committers_from_project))
    }

    fn load_paths_from_project_without_filters(&mut self) -> Result<(), Error> {
        self.load_paths_from_commit().unwrap();
        self.load_commits_from_project().unwrap();
        let paths_from_commit = give_me!(self.paths_from_commit);
        let commits_from_project = give_me!(self.commits_from_project);

        log_item!(self.spec.log_level, "loading project-path mapping");
        let paths_from_project: BTreeMap<ProjectId, Vec<PathId>>  =
            commits_from_project.iter()
                .map(|(id, commit_ids)|
                    (*id,
                     commit_ids.into_iter()
                         .flat_map(|commit_id| paths_from_commit.get(commit_id))
                         .flat_map(|v| v)
                         .map(|path_id| *path_id)
                         .unique()
                         .collect()))
                .collect();
        log_addendum!(self.spec.log_level, format!("loaded {} relationships",
                  count_relationships!(paths_from_project)));

        Ok(self.paths_from_project = Some(paths_from_project))
    }

    fn load_age_from_project_without_filters(&mut self) -> Result<(), Error> {
        self.load_commits().unwrap();
        self.load_commits_from_project().unwrap();
        let commits = give_me!(self.commits);
        let commits_from_project = give_me!(self.commits_from_project);

        log_item!(self.spec.log_level, "loading project ages");
        let age_from_project: BTreeMap<ProjectId, (u64, u64)>  =
            commits_from_project.iter()
                .map(|(id, commit_ids)| {
                    let min_max = commit_ids.into_iter()
                        .flat_map(|commit_id| commits.get(commit_id))
                        .map(|commit| commit.author_time)
                        .minmax();
                    match min_max {
                        MinMaxResult::NoElements => None,
                        MinMaxResult::OneElement(n) => Some((*id, (n as u64 , n as u64))),
                        MinMaxResult::MinMax(min, max) => {
                            assert!(max >= min);
                            Some((*id, (max as u64, min as u64)))
                        }
                    }
                })
                .flat_map(|e| e)
                .collect();
        log_addendum!(self.spec.log_level, format!("loaded ages for {} projects", age_from_project.len()));

        Ok(self.age_from_project = Some(age_from_project))
    }

    fn load_experience_from_user_without_filters(&mut self) -> Result<(), Error> {
        self.load_commits().unwrap();
        self.load_commits_authored_by_user().unwrap();
        let commits = give_me!(self.commits);
        let commits_authored_by_user = give_me!(self.commits_authored_by_user);

        log_item!(self.spec.log_level, "loading user experience");
        let experience_from_user: BTreeMap<UserId, (u64, u64)>  =
            commits_authored_by_user.iter()
                .map(|(id, commit_ids)| {
                    let min_max = commit_ids.into_iter()
                        .flat_map(|commit_id| commits.get(commit_id))
                        .map(|commit| commit.author_time)
                        .minmax();
                    match min_max {
                        MinMaxResult::NoElements => None,
                        MinMaxResult::OneElement(n) => Some((*id, (n as u64 , n as u64))),
                        MinMaxResult::MinMax(min, max) => {
                            assert!(max >= min);
                            Some((*id, (max as u64, min as u64)))
                        }
                    }
                })
                .flat_map(|e| e)
                .collect();
        log_addendum!(self.spec.log_level,
                  format!("loaded experience for {} authors", experience_from_user.len()));

        Ok(self.experience_from_user = Some(experience_from_user))
    }

    fn load_commits_authored_by_user_without_filters(&mut self) -> Result<(), Error> {
        self.load_commits().unwrap();
        let commits = give_me!(self.commits);

        log_item!(self.spec.log_level, "loading user/author-commit mapping");
        let commits_authored_by_user: BTreeMap<UserId, Vec<CommitId>> =
            commits.iter()
                .map(|(commit_id, commit)| (commit.author, commit_id.clone()))
                .into_group_map()
                .into_iter().collect();
        log_addendum!(self.spec.log_level, format!("loaded {} relationships",
                  count_relationships!(commits_authored_by_user)));

        Ok(self.commits_authored_by_user = Some(commits_authored_by_user))
    }

    fn load_commits_committed_by_user_without_filters(&mut self) -> Result<(), Error> {
        self.load_commits().unwrap();
        let commits = give_me!(self.commits);

        log_item!(self.spec.log_level, "loading user/committer-commit mapping");
        let commits_committed_by_user: BTreeMap<UserId, Vec<CommitId>> =
            commits.iter()
                .map(|(commit_id, commit)| (commit.committer, commit_id.clone()))
                .into_group_map()
                .into_iter().collect();
        log_addendum!(self.spec.log_level, format!("loaded {} relationships",
                  count_relationships!(commits_committed_by_user)));

        Ok(self.commits_committed_by_user = Some(commits_committed_by_user))
    }
}

/**===== Data: data loading methods (filtered) ==================================================**/
impl Data {
    fn load_projects_with_filters(&mut self) -> Result<(), Error> {
        self.load_commits_from_project().unwrap();
        let commits_from_project =
            give_me!(self.commits_from_project);

        log_item!(self.spec.log_level, "loading project data");
        let projects: BTreeMap<ProjectId, Project> =
            commits_from_project.iter()
                .flat_map(|(project_id, _)| self.warehouse.get_project(project_id.into()))
                .map(|project| (ProjectId::from(project.id), Project::from(project)) )
                .collect();
        log_addendum!(self.spec.log_level, format!("loaded {} projects", projects.len()));

        Ok(self.projects = Some(projects))
    }

    fn load_commits_with_filters(&mut self) -> Result<(), Error> { // definitely different
        self.load_commits_from_project().unwrap();
        let commits_from_project =
            give_me!(self.commits_from_project);

        log_item!(self.spec.log_level, "loading commit data");
           let commits: BTreeMap<CommitId, Commit> =
            commits_from_project.iter()
                .flat_map(|(_, commit_ids)| commit_ids)
                .unique()
                .flat_map(|commit_id| self.warehouse.get_commit_bare(commit_id.into()))
                .map(|commit| (CommitId::from(commit.id), Commit::from(commit)))
                .collect();
        log_addendum!(self.spec.log_level, format!("loaded {} commits", commits.len()));

        Ok(self.commits = Some(commits))
    }

    fn load_users_with_filters(&mut self) -> Result<(), Error> { // definitely different
        self.load_commits().unwrap();
        let commits = give_me!(self.commits);

        log_item!(self.spec.log_level, "loading user data");
        let users: BTreeMap<UserId, User> =
            commits.iter()
                .flat_map(|(_, commit)| commit.user_ids())
                .unique()
                .flat_map(|user_id| self.warehouse.get_user(user_id.into()))
                .map(|user| (UserId::from(user.id), User::from(user)))
                .collect();
        log_addendum!(self.spec.log_level, format!("loaded {} users", users.len()));

        Ok(self.users = Some(users))
    }

    fn load_paths_with_filters(&mut self) -> Result<(), Error> { // definitely different
        self.load_paths_from_commit().unwrap();
        let paths_from_commit = give_me!(self.paths_from_commit);

        log_item!(self.spec.log_level, "loading path data");
        let paths: BTreeMap<PathId, Path> =
            paths_from_commit.iter()
                .flat_map(|(_, path_ids)| path_ids)
                .unique()
                .flat_map(|path_id| self.warehouse.get_file_path(path_id.into()))
                .map(|path| (PathId::from(path.id), Path::from(path)) )
                .collect();
        log_addendum!(self.spec.log_level, format!("loaded {} paths", paths.len()));

        Ok(self.paths = Some(paths))
    }

    fn load_commits_from_project_with_filters(&mut self) -> Result<(), Error> {
        log_item!(self.spec.log_level, format!("loading project-commit mapping with {} filter{}",
                                         self.filters.len(),
                                         if self.filters.len() > 1 {"s"} else {""} ));

        let commits_from_project: BTreeMap<ProjectId, Vec<CommitId>> =
            self.warehouse.project_ids_and_their_commit_ids()
                .filter(|(project_id, commit_ids)| {
                    self.filters.iter().all(|filter| {
                        filter.filter(&self.warehouse, project_id, commit_ids)
                    })
                })
                .map(|(project_id, commit_ids)| {
                    (ProjectId::from(project_id),
                     commit_ids.iter().map(|commit_id|
                         CommitId::from(*commit_id)).collect())
                })
                .collect();

        log_addendum!(self.spec.log_level, format!("loaded {} relationships",
                                               commits_from_project.iter()
                                               .map(|(_, v)| v.len()).sum::<usize>()));

        Ok(self.commits_from_project = Some(commits_from_project))
    }

    fn load_users_from_project_with_filters(&mut self) -> Result<(), Error> {
        self.load_commits_from_project().unwrap();
        self.load_commits().unwrap();
        let commits_from_project = give_me!(self.commits_from_project);
        let commits = give_me!(self.commits);

        log_item!(self.spec.log_level, "loading project-user mapping");
        let users_from_project: BTreeMap<ProjectId, Vec<UserId>> =
            commits_from_project.iter()
                .map(|(project_id, commit_ids)| {
                    (*project_id,
                     commit_ids.iter().flat_map(|commit_id| {
                         commits.get(commit_id).user_ids()
                     })
                         .unique()
                         .map(|user_id| UserId::from(user_id))
                         .collect::<Vec<UserId>>())
                })
                .collect();
        log_addendum!(self.spec.log_level, format!("loaded {} relationships",
                                       count_relationships!(users_from_project)));

        Ok(self.users_from_project = Some(users_from_project))
    }

    fn load_paths_from_commit_with_filters(&mut self) -> Result<(), Error> {
        self.load_commits_from_project().unwrap();
        let commits_from_project = give_me!(self.commits_from_project);

        log_item!(self.spec.log_level, "loading commit-path mapping");
        let paths_from_commit: BTreeMap<CommitId, Vec<PathId>> =
            commits_from_project.iter()
                .flat_map(|(_, commit_ids)| commit_ids)
                .unique()
                .flat_map(|commit_id| self.warehouse.get_commit(commit_id.into()))
                .map(|commit| {
                    (CommitId::from(commit.id),
                     commit.changes.as_ref()
                         .map_or(Default::default(), |changes| {
                             changes.iter()
                                 .map(|(path_id, _snapshot_id)| *path_id)
                                 .unique()
                                 .map(|path_id| PathId::from(path_id))
                                 .collect::<Vec<PathId>>()
                         }))
                })
                .collect();
        log_addendum!(self.spec.log_level, format!("loaded {} relationships",
                                     count_relationships!(paths_from_commit)));

        Ok(self.paths_from_commit = Some(paths_from_commit))
    }

    fn load_message_from_commit_with_filters(&mut self) -> Result<(), Error> {
        self.load_commits_from_project().unwrap();
        let commits_from_project = give_me!(self.commits_from_project);

        log_item!(self.spec.log_level, "loading commit messages");
        let message_from_commit: BTreeMap<CommitId, Message> =
            commits_from_project.iter()
                .flat_map(|(_, commit_ids)| commit_ids)
                .unique()
                .flat_map(|commit_id| self.warehouse.get_commit(commit_id.into()))
                .flat_map(|commit| {
                    commit.message.as_ref().map(|message| {
                        (CommitId::from(commit.id), Message::from(message))
                    })
                })
                .collect();
        log_addendum!(self.spec.log_level, format!("loaded {} messages", message_from_commit.len()));

        Ok(self.message_from_commit = Some(message_from_commit))
    }

    fn load_authors_from_project_with_filters(&mut self) -> Result<(), Error> {
        self.load_authors_from_project_without_filters()
    }

    fn load_committers_from_project_with_filters(&mut self) -> Result<(), Error> {
        self.load_committers_from_project_without_filters()
    }

    fn load_paths_from_project_with_filters(&mut self) -> Result<(), Error> {
        self.load_paths_from_project_without_filters()
    }

    fn load_age_from_project_with_filters(&mut self) -> Result<(), Error> {
        self.load_age_from_project_without_filters()
    }

    fn load_experience_from_user_with_filters(&mut self) -> Result<(), Error> {
        self.load_age_from_project_without_filters()
    }

    fn load_commits_authored_by_user_with_filters(&mut self) -> Result<(), Error> {
        self.load_commits_authored_by_user_without_filters()
    }

    fn load_commits_committed_by_user_with_filters(&mut self) -> Result<(), Error> {
        self.load_commits_committed_by_user_without_filters()
    }
}

/**===== Data: data loading methods (check cache and other convenience functions) ===============**/
impl Data {
    fn path_in_cache(&self, file: &str) -> PathBuf {
        let cache = give_me!(self.spec.database);
        let mut path = cache.clone();
        path.push(file);
        path.set_extension("bin");
        path
    }
    fn file_in_cache(&self, file: &str) -> bool {
        self.spec.database.as_ref().map_or(false, |_| self.path_in_cache(file).exists())
    }
    fn open_file_in_cache(&self, file: &str) -> Result<File, Error> {
        let path = self.path_in_cache(file);
        //create_dir_all(&path)?;
        File::open(path)
    }
    fn create_file_in_cache(&self, file: &str) -> Result<File, Error> {
        let path = self.path_in_cache(file);
        create_dir_all(give_me!(self.spec.database))?;
        File::create(path)
    }
}

/**===== Data: data loading methods (load from cache) ===========================================**/
impl Data {
    fn load_projects_from_cache(&mut self) -> Result<(), Error> {
        log_item!(self.spec.log_level, "loading project data from cache");
        let mut file = self.open_file_in_cache("projects")?;
        self.projects = Some(BTreeMap::load(&mut file)?);
        log_addendum!(self.spec.log_level, format!("loaded {} projects",
                                                   give_me!(self.projects).len()));
        Ok(())
    }
    fn load_commits_from_cache(&mut self) -> Result<(), Error> {
        log_item!(self.spec.log_level, "loading commit data from cache");
        let mut file = self.open_file_in_cache("commits")?;
        self.commits = Some(BTreeMap::load(&mut file)?);
        log_addendum!(self.spec.log_level, format!("loaded {} commits",
                                                   give_me!(self.commits).len()));
        Ok(())
    }
    fn load_users_from_cache(&mut self) -> Result<(), Error> {
        log_item!(self.spec.log_level, "loading user data from cache");
        let mut file = self.open_file_in_cache("users")?;
        self.users = Some(BTreeMap::load(&mut file)?);
        log_addendum!(self.spec.log_level, format!("loaded {} users",
                                                   give_me!(self.users).len()));
        Ok(())
    }
    fn load_paths_from_cache(&mut self) -> Result<(), Error> {
        log_item!(self.spec.log_level, "loading path data from cache");
        let mut file = self.open_file_in_cache("paths")?;
        self.paths = Some(BTreeMap::load(&mut file)?);
        log_addendum!(self.spec.log_level, format!("loaded {} paths",
                                                   give_me!(self.paths).len()));
        Ok(())
    }
    fn load_commits_from_project_from_cache(&mut self) -> Result<(), Error> {
        log_item!(self.spec.log_level, "loading project-commit mapping from cache");
        let mut file = self.open_file_in_cache("commits_from_project")?;
        self.commits_from_project = Some(BTreeMap::load(&mut file)?);
        log_addendum!(self.spec.log_level, format!("loaded {} relationships",
                                                   count_relationships!(give_me!(self.commits_from_project))));
        Ok(())
    }
    fn load_users_from_project_from_cache(&mut self) -> Result<(), Error> {
        log_item!(self.spec.log_level, "loading project-user mapping from cache");
        let mut file = self.open_file_in_cache("users_from_project")?;
        self.users_from_project = Some(BTreeMap::load(&mut file)?);
        log_addendum!(self.spec.log_level, format!("loaded {} relationships",
                                                   count_relationships!(give_me!(self.users_from_project))));
        Ok(())
    }
    fn load_paths_from_commit_from_cache(&mut self) -> Result<(), Error> {
        log_item!(self.spec.log_level, "loading commit-path mapping from cache");
        let mut file = self.open_file_in_cache("paths_from_commit")?;
        self.paths_from_commit = Some(BTreeMap::load(&mut file)?);
        log_addendum!(self.spec.log_level, format!("loaded {} relationships",
                                                   count_relationships!(give_me!(self.paths_from_commit))));
        Ok(())
    }
    fn load_message_from_commit_from_cache(&mut self) -> Result<(), Error> {
        log_item!(self.spec.log_level, "loading message data from cache");
        let mut file = self.open_file_in_cache("message_from_commit")?;
        self.message_from_commit = Some(BTreeMap::load(&mut file)?);
        log_addendum!(self.spec.log_level, format!("loaded {} messages",
                                                   give_me!(self.message_from_commit).len()));
        Ok(())
    }
    fn load_authors_from_project_from_cache(&mut self) -> Result<(), Error> {
        log_item!(self.spec.log_level, "loading project-author mapping from cache");
        let mut file = self.open_file_in_cache("authors_from_project")?;
        self.authors_from_project = Some(BTreeMap::load(&mut file)?);
        log_addendum!(self.spec.log_level, format!("loaded {} relationships",
                                                   count_relationships!(give_me!(self.authors_from_project))));
        Ok(())
    }
    fn load_committers_from_project_from_cache(&mut self) -> Result<(), Error> {
        log_item!(self.spec.log_level, "loading project-committer mapping from cache");
        let mut file = self.open_file_in_cache("committers_from_project")?;
        self.committers_from_project = Some(BTreeMap::load(&mut file)?);
        log_addendum!(self.spec.log_level, format!("loaded {} relationships",
                                                   count_relationships!(give_me!(self.commits_from_project))));
        Ok(())
    }
    fn load_paths_from_project_from_cache(&mut self) -> Result<(), Error> {
        log_item!(self.spec.log_level, "loading project-path mapping from cache");
        let mut file = self.open_file_in_cache("paths_from_project")?;
        self.paths_from_project = Some(BTreeMap::load(&mut file)?);
        log_addendum!(self.spec.log_level, format!("loaded {} relationships",
                                                   count_relationships!(give_me!(self.paths_from_project))));
        Ok(())
    }
    fn load_age_from_project_from_cache(&mut self) -> Result<(), Error> {
        log_item!(self.spec.log_level, "loading project age data from cache");
        let mut file = self.open_file_in_cache("age_from_project")?;
        self.age_from_project = Some(BTreeMap::load(&mut file)?);
        log_addendum!(self.spec.log_level, format!("loaded age for {} projects",
                                                    give_me!(self.age_from_project).len()));
        Ok(())
    }
    fn load_experience_from_user_from_cache(&mut self) -> Result<(), Error> {
        log_item!(self.spec.log_level, "loading user experience data from cache");
        let mut file = self.open_file_in_cache("experience_from_user")?;
        self.experience_from_user = Some(BTreeMap::load(&mut file)?);
        log_addendum!(self.spec.log_level, format!("loaded experience for {} users",
                                                   give_me!(self.experience_from_user).len()));
        Ok(())
    }
    fn load_commits_authored_by_user_from_cache(&mut self) -> Result<(), Error> {
        log_item!(self.spec.log_level, "loading user/author-commit mapping from cache");
        let mut file = self.open_file_in_cache("commits_authored_by_user")?;
        self.commits_authored_by_user = Some(BTreeMap::load(&mut file)?);
        log_addendum!(self.spec.log_level, format!("loaded {} relationships",
                                                   count_relationships!(give_me!(self.commits_authored_by_user))));
        Ok(())
    }
    fn load_commits_committed_by_user_from_cache(&mut self) -> Result<(), Error> {
        log_item!(self.spec.log_level, "loading user/committer-commit mapping from cache");
        let mut file = self.open_file_in_cache("commits_committed_by_user")?;
        self.commits_committed_by_user = Some(BTreeMap::load(&mut file)?);
        log_addendum!(self.spec.log_level, format!("loaded {} relationships",
                                                   count_relationships!(give_me!(self.commits_committed_by_user))));
        Ok(())
    }
}

/**===== Data: data loading methods (dump to cache) =============================================**/
impl Data {
    fn dump_projects_to_cache(&self) -> Result<(), Error> {
        let mut file = self.create_file_in_cache("projects")?;
        give_me!(self.projects).dump(&mut file)
    }
    fn dump_commits_to_cache(&mut self) -> Result<(), Error> {
        let mut file = self.create_file_in_cache("commits")?;
        give_me!(self.commits).dump(&mut file)
    }
    fn dump_users_to_cache(&mut self) -> Result<(), Error> {
        let mut file = self.create_file_in_cache("users")?;
        give_me!(self.users).dump(&mut file)
    }
    fn dump_paths_to_cache(&mut self) -> Result<(), Error> {
        let mut file = self.create_file_in_cache("paths")?;
        give_me!(self.paths).dump(&mut file)
    }
    fn dump_commits_from_project_to_cache(&mut self) -> Result<(), Error> {
        let mut file = self.create_file_in_cache("commits_from_project")?;
        give_me!(self.commits_from_project).dump(&mut file)
    }
    fn dump_users_from_project_to_cache(&mut self) -> Result<(), Error> {
        let mut file = self.create_file_in_cache("users_from_project")?;
        give_me!(self.users_from_project).dump(&mut file)
    }
    fn dump_paths_from_commit_to_cache(&mut self) -> Result<(), Error> {
        let mut file = self.create_file_in_cache("paths_from_commit")?;
        give_me!(self.paths_from_commit).dump(&mut file)
    }
    fn dump_message_from_commit_to_cache(&mut self) -> Result<(), Error> {
        let mut file = self.create_file_in_cache("message_from_commit")?;
        give_me!(self.message_from_commit).dump(&mut file)
    }
    fn dump_authors_from_project_to_cache(&mut self) -> Result<(), Error> {
        let mut file = self.create_file_in_cache("authors_from_project")?;
        give_me!(self.authors_from_project).dump(&mut file)
    }
    fn dump_committers_from_project_to_cache(&mut self) -> Result<(), Error> {
        let mut file = self.create_file_in_cache("committers_from_project")?;
        give_me!(self.committers_from_project).dump(&mut file)
    }
    fn dump_paths_from_project_to_cache(&mut self) -> Result<(), Error> {
        let mut file = self.create_file_in_cache("paths_from_project")?;
        give_me!(self.paths_from_project).dump(&mut file)
    }
    fn dump_age_from_project_to_cache(&mut self) -> Result<(), Error> {
        let mut file = self.create_file_in_cache("age_from_project")?;
        give_me!(self.age_from_project).dump(&mut file)
    }
    fn dump_experience_from_user_to_cache(&mut self) -> Result<(), Error> {
        let mut file = self.create_file_in_cache("experience_from_user")?;
        give_me!(self.experience_from_user).dump(&mut file)
    }
    fn dump_commits_authored_by_user_to_cache(&mut self) -> Result<(), Error> {
        let mut file = self.create_file_in_cache("commits_authored_by_user")?;
        give_me!(self.commits_authored_by_user).dump(&mut file)
    }
    fn dump_commits_committed_by_user_to_cache(&mut self) -> Result<(), Error> {
        let mut file = self.create_file_in_cache("commits_committed_by_user")?;
        give_me!(self.commits_committed_by_user).dump(&mut file)
    }
}

/**===== DataPtr ================================================================================**/
pub type DataPtr = Rc<RefCell<Data>>;

/**===== DataPtr: convenience ===================================================================**/
#[macro_export] macro_rules! untangle     { ($dataptr:expr) => { $dataptr.as_ref().borrow()     } }
#[macro_export] macro_rules! untangle_mut { ($dataptr:expr) => { $dataptr.as_ref().borrow_mut() } }

/**===== DataPtr: quincunx data extraction methods ==============================================**/
pub trait Quincunx: NamedEntity {
    fn stream_from(data: &DataPtr) -> Vec<Self> where Self: Sized;
}
impl Quincunx for Project {
    fn stream_from(data: &DataPtr) -> Vec<Self> { untangle_mut!(data).projects() }
}
impl Quincunx for Commit {
    fn stream_from(data: &DataPtr) -> Vec<Self> { untangle_mut!(data).commits() }
}
impl Quincunx for Path {
    fn stream_from(data: &DataPtr) -> Vec<Self> { untangle_mut!(data).paths() }
}
impl Quincunx for User {
    fn stream_from(data: &DataPtr) -> Vec<Self> { untangle_mut!(data).users() }
}
// impl Quincunx for Snapshot {
//     fn stream_from(data: DataPtr) -> Vec<Self> { untangle_mut!(data).snapshots() }
// }

/**===== DataPtr: conversions ===================================================================**/
impl From<djanco::Spec> for DataPtr {
    fn from(spec: djanco::Spec) -> Self {
        Data::new(&spec.warehouse, &spec.database, spec.timestamp, spec.seed, spec.log_level)
    }
}
impl From<&djanco::Spec> for DataPtr {
    fn from(spec: &djanco::Spec) -> Self {
        Data::new(&spec.warehouse, &spec.database, spec.timestamp, spec.seed, spec.log_level)
    }
}
impl From<djanco::Lazy> for DataPtr {
    fn from(lazy: djanco::Lazy) -> Self {
        let data_ptr = DataPtr::from(&lazy.spec);
        untangle_mut!(data_ptr).filters = lazy.filters;
        data_ptr
    }
}
impl From<&djanco::Lazy> for DataPtr {
    fn from(lazy: &djanco::Lazy) -> Self {
        let data_ptr = DataPtr::from(&lazy.spec);
        let iter =
            lazy.filters.iter().map(|f| f.clone_box());
        untangle_mut!(data_ptr).filters.extend(iter);
        data_ptr
    }
}


/**===== Data: associated traits ================================================================**/

pub trait WithData { fn get_database_ptr(&self) -> DataPtr; }
impl WithData for Data { fn get_database_ptr(&self) -> DataPtr { self.me() } }

impl Clone for Box<dyn LoadFilter> {
    fn clone(&self) -> Box<dyn LoadFilter> { self.clone_box() }
}

// pub enum NotFound {
//     Project(ProjectId),
//     Commit(CommitId),
//     Path(PathId),
//     User(UserId),
// }
//
// impl fmt::Display for NotFound {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         match self {
//             NotFound::Project(id) => write!(f, "Project {} not found", id),
//             NotFound::Commit(id) => write!(f, "Commit {} not found", id),
//             NotFound::Path(id) => write!(f, "Path {} not found", id),
//             NotFound::User(id) => write!(f, "User {} not found", id),
//         }
//     }
// }