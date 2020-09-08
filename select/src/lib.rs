pub mod dump;
mod io;
pub mod query;
pub mod cachedb;
pub mod meta;
pub mod mockdb;
pub mod selectors;

use chrono::{Date, DateTime, Utc, TimeZone};
use std::path::PathBuf;
use dcd::{DCD, Database, Project, User};
use std::marker::PhantomData;
use itertools::{Itertools, MinMaxResult};
//use crate::meta::*;
use std::hash::Hash;
use std::rc::{Rc, Weak};
use std::cell::RefCell;
use std::ops::Range;
use std::borrow::Borrow;
use std::iter::Map;
use std::collections::{HashSet, VecDeque};
use std::io::{Error, Write};
use std::fs::{create_dir_all, File};
use std::time::Duration;
use std::cmp::Ordering;
use std::fmt::Display;
use crate::meta::ProjectMeta;
//use std::slice::Iter;

pub enum Month {
    January(u16),
    February(u16),
    March(u16),
    April(u16),
    May(u16),
    June(u16),
    July(u16),
    August(u16),
    September(u16),
    October(u16),
    November(u16),
    December(u16),
}

impl Month {
    pub fn month(&self) -> u8 {
        match &self {
            Month::January(_)   => 1,
            Month::February(_)  => 2,
            Month::March(_)     => 3,
            Month::April(_)     => 4,
            Month::May(_)       => 5,
            Month::June(_)      => 6,
            Month::July(_)      => 7,
            Month::August(_)    => 8,
            Month::September(_) => 9,
            Month::October(_)   => 10,
            Month::November(_)  => 11,
            Month::December(_)  => 12,
        }
    }

    pub fn year(&self) -> u16 {
        match &self {
            Month::January(year)   => *year,
            Month::February(year)  => *year,
            Month::March(year)     => *year,
            Month::April(year)     => *year,
            Month::May(year)       => *year,
            Month::June(year)      => *year,
            Month::July(year)      => *year,
            Month::August(year)    => *year,
            Month::September(year) => *year,
            Month::October(year)   => *year,
            Month::November(year)  => *year,
            Month::December(year)  => *year,
        }
    }

    pub fn into_date(&self) -> Date<Utc> {
        Utc.ymd(self.year() as i32, self.month() as u32, 1 as u32)
    }

    pub fn into_datetime(&self) -> DateTime<Utc> {
        Utc.ymd(self.year() as i32, self.month() as u32, 1 as u32)
           .and_hms(0, 0, 0)
    }
}

impl Into<Date<Utc>>     for Month { fn into(self) -> Date<Utc>     { self.into_date()       } }
impl Into<DateTime<Utc>> for Month { fn into(self) -> DateTime<Utc> { self.into_datetime()   } }
impl Into<i64>           for Month { fn into(self) -> i64 { self.into_datetime().timestamp() } }

#[derive(Clone, Copy, Hash, Eq, PartialEq)] pub struct ProjectId(u64);
#[derive(Clone, Copy, Hash, Eq, PartialEq)] pub struct CommitId(u64);
#[derive(Clone, Copy, Hash, Eq, PartialEq)] pub struct UserId(u64);
#[derive(Clone, Copy, Hash, Eq, PartialEq)] pub struct PathId(u64);

impl Into<usize> for ProjectId { fn into(self) -> usize { self.0 as usize } }
impl Into<usize> for CommitId  { fn into(self) -> usize { self.0 as usize } }
impl Into<usize> for UserId    { fn into(self) -> usize { self.0 as usize } }
impl Into<usize> for PathId    { fn into(self) -> usize { self.0 as usize } }

impl Into<u64>   for ProjectId { fn into(self) -> u64 { self.0 } }
impl Into<u64>   for CommitId  { fn into(self) -> u64 { self.0 } }
impl Into<u64>   for UserId    { fn into(self) -> u64 { self.0 } }
impl Into<u64>   for PathId    { fn into(self) -> u64 { self.0 } }

impl From<usize> for ProjectId { fn from(n: usize) -> Self { ProjectId(n as u64) } }
impl From<usize> for CommitId  { fn from(n: usize) -> Self { CommitId(n as u64)  } }
impl From<usize> for UserId    { fn from(n: usize) -> Self { UserId(n as u64)    } }
impl From<usize> for PathId    { fn from(n: usize) -> Self { PathId(n as u64)    } }

impl From<u64>   for ProjectId { fn from(n: u64) -> Self { ProjectId(n) } }
impl From<u64>   for CommitId  { fn from(n: u64) -> Self { CommitId(n)  } }
impl From<u64>   for UserId    { fn from(n: u64) -> Self { UserId(n)    } }
impl From<u64>   for PathId    { fn from(n: u64) -> Self { PathId(n)    } }

impl CSVField    for ProjectId { fn to_csv_field(&self) -> String { self.0.to_csv_field() } }
impl CSVField    for CommitId  { fn to_csv_field(&self) -> String { self.0.to_csv_field() } }
impl CSVField    for UserId    { fn to_csv_field(&self) -> String { self.0.to_csv_field() } }
impl CSVField    for PathId    { fn to_csv_field(&self) -> String { self.0.to_csv_field() } }

trait DataSource {
    fn project_count(&self) -> usize;
    fn commit_count(&self)  -> usize;
    fn user_count(&self)    -> usize;
    fn path_count(&self)    -> usize;

    fn project(&self, id: ProjectId)    -> Option<dcd::Project>;
    fn commit(&self, id: CommitId)      -> Option<dcd::Commit>;
    fn bare_commit(&self, id: CommitId) -> Option<dcd::Commit>;
    fn user(&self, id: UserId)          -> Option<dcd::User>;
    fn path(&self, id: PathId)          -> Option<dcd::FilePath>;

    fn project_ids(&self) -> Map<Range<usize>, fn(usize) -> ProjectId>;
    fn commit_ids(&self)  -> Map<Range<usize>, fn(usize) -> CommitId>;
    fn user_ids(&self)    -> Map<Range<usize>, fn(usize) -> UserId>;
    fn path_ids(&self)    -> Map<Range<usize>, fn(usize) -> PathId>;

    fn projects(&self)     -> EntityIter<ProjectId, dcd::Project>;
    fn commits(&self)      -> EntityIter<CommitId,  dcd::Commit>;
    fn bare_commits(&self) -> EntityIter<CommitId,  dcd::Commit>;
    fn users(&self)        -> EntityIter<UserId,    dcd::User>;
    fn paths(&self)        -> EntityIter<PathId,    dcd::FilePath>;

    fn commits_from(&self, project: &dcd::Project)      -> ProjectEntityIter<dcd::Commit>;
    fn bare_commits_from(&self, project: &dcd::Project) -> ProjectEntityIter<dcd::Commit>;
    fn paths_from(&self, project: &dcd::Project)        -> ProjectEntityIter<dcd::FilePath>;
    fn users_from(&self, project: &dcd::Project)        -> ProjectEntityIter<dcd::User>;
    fn authors_from(&self, project: &dcd::Project)      -> ProjectEntityIter<dcd::User>;
    fn committers_from(&self, project: &dcd::Project)   -> ProjectEntityIter<dcd::User>;

    fn commit_count_from(&self, project: &dcd::Project)    -> usize;
    fn path_count_from(&self, project: &dcd::Project)      -> usize;
    fn user_count_from(&self, project: &dcd::Project)      -> usize;
    fn author_count_from(&self, project: &dcd::Project)    -> usize;
    fn committer_count_from(&self, project: &dcd::Project) -> usize;

    fn age_of(&self, project: &dcd::Project) -> Option<Duration>;

    fn seed(&self) -> u128;
}

type DatabasePtr = Rc<RefCell<Djanco>>;

pub struct Djanco {
    warehouse: DCD,
    me: Option<Weak<RefCell<Djanco>>>, // Thanks for the help, Colette.

    seed: u128,
    _timestamp: i64,
    _path: PathBuf,
}

impl Djanco {
    pub fn from<S: Into<String>, T: Into<i64>>(path: S, seed: u128, timestamp: T) -> DatabasePtr {
        assert_eq!(std::u64::MAX as usize, std::usize::MAX);

        let string_path = path.into();
        let warehouse = DCD::new(string_path.clone());
        let database = Djanco {
            warehouse,
            me: None,
            _path: PathBuf::from(string_path),
            _timestamp: timestamp.into(),
            seed,
        };
        let pointer: DatabasePtr = Rc::new(RefCell::new(database));

        // Things we do to avoid unsafe.
        pointer.borrow_mut().me = Some(Rc::downgrade(&pointer));
        pointer
    }

    pub fn me(&self) -> DatabasePtr {
        self.me.as_ref().unwrap().upgrade().unwrap()
    }
}

impl DataSource for Djanco {
    fn project_count(&self) -> usize { self.warehouse.num_projects()   as usize }
    fn commit_count(&self)  -> usize { self.warehouse.num_commits()    as usize }
    fn path_count(&self)    -> usize { self.warehouse.num_file_paths() as usize }
    fn user_count(&self)    -> usize { self.warehouse.num_users()      as usize }

    fn project(&self, id: ProjectId)    -> Option<dcd::Project>  { self.warehouse.get_project(id.into())     }
    fn commit(&self, id: CommitId)      -> Option<dcd::Commit>   { self.warehouse.get_commit(id.into())      }
    fn bare_commit(&self, id: CommitId) -> Option<dcd::Commit>   { self.warehouse.get_commit_bare(id.into()) }
    fn user(&self, id: UserId)          -> Option<dcd::User>     { self.warehouse.get_user(id.into()).map(|u| u.clone()) }
    fn path(&self, id: PathId)          -> Option<dcd::FilePath> { self.warehouse.get_file_path(id.into())   }

    fn project_ids(&self) -> Map<Range<usize>, fn(usize) -> ProjectId> {
        (0..self.project_count()).map(|e| ProjectId::from(e))
    }
    fn commit_ids(&self) -> Map<Range<usize>, fn(usize) -> CommitId> {
        (0..self.commit_count()).map(|e| CommitId::from(e))
    }
    fn user_ids(&self) -> Map<Range<usize>, fn(usize) -> UserId> {
        (0..self.user_count()).map(|e| UserId::from(e))
    }
    fn path_ids(&self) -> Map<Range<usize>, fn(usize) -> PathId> {
        (0..self.path_count()).map(|e| PathId::from(e))
    }

    fn projects(&self) -> EntityIter<ProjectId, dcd::Project> {
        EntityIter::from(self.me(), self.project_ids())
    }
    fn commits(&self) -> EntityIter<CommitId, dcd::Commit> {
        EntityIter::from(self.me(), self.commit_ids())
    }
    fn bare_commits(&self) -> EntityIter<CommitId, dcd::Commit> {
        EntityIter::from(self.me(), self.commit_ids()).and_make_it_snappy()
    }
    fn users(&self) -> EntityIter<UserId, dcd::User> {
        EntityIter::from(self.me(), self.user_ids())
    }
    fn paths(&self) -> EntityIter<PathId, dcd::FilePath> {
        EntityIter::from(self.me(), self.path_ids())
    }

    fn commits_from(&self, project: &dcd::Project) -> ProjectEntityIter<dcd::Commit> {
        ProjectEntityIter::<dcd::Commit>::from(self.me(), &project)
    }
    fn bare_commits_from(&self, project: &dcd::Project) -> ProjectEntityIter<dcd::Commit> {
        ProjectEntityIter::<dcd::Commit>::from(self.me(), &project).and_make_it_snappy()
    }
    fn users_from(&self, project: &dcd::Project) -> ProjectEntityIter<dcd::User> {
        ProjectEntityIter::<dcd::User>::from(self.me(), &project)
    }
    fn paths_from(&self, project: &dcd::Project) -> ProjectEntityIter<dcd::FilePath> {
        ProjectEntityIter::<dcd::FilePath>::from(self.me(), &project)
    }
    fn authors_from(&self, project: &Project) -> ProjectEntityIter<User> {
        ProjectEntityIter::<dcd::User>::from(self.me(), &project).and_skip_committers()
    }
    fn committers_from(&self, project: &Project) -> ProjectEntityIter<User> {
        ProjectEntityIter::<dcd::User>::from(self.me(), &project).and_skip_authors()
    }


    fn commit_count_from(&self, project: &dcd::Project) -> usize {
        self.bare_commits_from(project).count()
    }
    fn user_count_from(&self, project: &dcd::Project) -> usize {
        self.users_from(project).count()
    }
    fn path_count_from(&self, project: &dcd::Project) -> usize {
        self.paths_from(project).count()
    }
    fn author_count_from(&self, project: &Project) -> usize {
        self.authors_from(project).count()
    }
    fn committer_count_from(&self, project: &Project) -> usize {
        self.committers_from(project).count()
    }

    fn age_of(&self, project: &Project) -> Option<Duration> {
        let minmax = self.commits_from(project)
            .minmax_by(|c1, c2| {
                if c1.author_time < c2.author_time { return Ordering::Less }
                if c1.author_time > c2.author_time { return Ordering::Greater }
                return Ordering::Equal
            });
        match minmax {
            MinMaxResult::NoElements => None,
            MinMaxResult::OneElement(_commit) => None,
            MinMaxResult::MinMax(first_commit, last_commit) => {
                assert!(last_commit.author_time > first_commit.author_time);
                let elapsed_seconds: u64 =
                    (last_commit.author_time - first_commit.author_time) as u64;
                Some(Duration::from_secs(elapsed_seconds))
            }
        }
    }

    fn seed(&self) -> u128 {
        self.seed
    }
}

pub trait WithDatabase { fn get_database_ptr(&self) -> DatabasePtr; }
impl WithDatabase for Djanco { fn get_database_ptr(&self) -> DatabasePtr { self.me() } }

pub struct ProjectEntityIter<T> {
    database: DatabasePtr,
    visited_commits: HashSet<u64>,
    to_visit_commits: VecDeque<u64>,

    snappy: bool,  // TODO encode this in types?
    authors: bool,
    committers: bool,

    seen_entities: HashSet<u64>,
    entity_cache: VecDeque<u64>,

    _entity: PhantomData<T>,
    desired_cache_size: usize,
}

impl<T> ProjectEntityIter<T> {
    pub fn from(database: DatabasePtr, project: &dcd::Project) -> ProjectEntityIter<T> {
        let visited_commits: HashSet<u64> = HashSet::new();
        let to_visit_commits: VecDeque<u64> =
            project.heads.iter().map(|(_, id)| *id).collect();

        ProjectEntityIter {
            visited_commits, to_visit_commits, database,
            snappy: false, committers: true, authors: true,
            _entity: PhantomData, desired_cache_size: 100,
            entity_cache: VecDeque::new(), seen_entities: HashSet::new(),
        }
    }

    /**
     * In snappy mode, the iterator will load only bare bones versions of objects (currently this
     * applies only to commits). This dramatically increases performance.
     */
    pub fn and_make_it_snappy(self) -> Self {
        ProjectEntityIter {
            visited_commits: self.visited_commits,
            to_visit_commits: self.to_visit_commits,
            database: self.database,
            _entity: PhantomData,
            snappy: true,
            committers: self.committers,
            authors: self.authors,
            desired_cache_size: self.desired_cache_size,
            entity_cache: self.entity_cache,
            seen_entities: self.seen_entities,
        }
    }

    pub fn and_skip_committers(self) -> Self {
        ProjectEntityIter {
            visited_commits: self.visited_commits,
            to_visit_commits: self.to_visit_commits,
            database: self.database,
            _entity: PhantomData,
            snappy: self.snappy,
            committers: false,
            authors: self.authors,
            desired_cache_size: self.desired_cache_size,
            entity_cache: self.entity_cache,
            seen_entities: self.seen_entities,
        }
    }

    pub fn and_skip_authors(self) -> Self {
        ProjectEntityIter {
            visited_commits: self.visited_commits,
            to_visit_commits: self.to_visit_commits,
            database: self.database,
            _entity: PhantomData,
            snappy: self.snappy,
            committers: self.committers,
            authors: false,
            desired_cache_size: self.desired_cache_size,
            entity_cache: self.entity_cache,
            seen_entities: self.seen_entities,
        }
    }

    pub fn next_commit(&mut self) -> Option<dcd::Commit> {
        loop {
            if self.to_visit_commits.is_empty() {
                return None;
            }
            let commit_id = self.to_visit_commits.pop_back().unwrap();
            if ! self.visited_commits.insert(commit_id) {
                continue;
            }
            let commit = if self.snappy {
                self.database.bare_commit(CommitId::from(commit_id)).unwrap()
            } else {
                self.database.commit(CommitId::from(commit_id)).unwrap()
            };
            self.to_visit_commits.extend(commit.parents.iter());
            return Some(commit);
        }
    }

    fn next_id_from_cache(&mut self) -> Option<u64> {
        self.entity_cache.pop_front()
    }
}

impl ProjectEntityIter<dcd::User> {
    fn populate_cache(&mut self) -> bool {
        assert!(self.authors || self.committers);
        loop {
            return match self.next_commit() {
                Some(commit) => {
                    if self.authors {
                        if self.seen_entities.insert(commit.author_id) {
                            self.entity_cache.push_back(commit.author_id); // User not yet seen.
                        }
                    }

                    if self.committers {
                        if self.seen_entities.insert(commit.committer_id) {
                            self.entity_cache.push_back(commit.committer_id); // User not yet seen.
                        }
                    }

                    if self.entity_cache.len() < self.desired_cache_size {
                        continue;
                    }

                    true
                },
                None => self.entity_cache.len() != 0
            }
        }
    }
}

impl ProjectEntityIter<dcd::FilePath> {
    fn populate_cache(&mut self) -> bool {
        loop {
            return match self.next_commit() {
                Some(commit) => {
                    let changes: Vec<u64> =
                        commit.changes.map_or(vec![],
                            |map| {
                                map.iter()
                                    .map(|(path_id, _)| *path_id)
                                    .filter(|path_id| {
                                        !self.seen_entities.contains(path_id)
                                    })
                                    .collect()
                            });

                    self.seen_entities.extend(changes);

                    if self.entity_cache.len() < self.desired_cache_size {
                        continue;
                    }

                    true
                },
                None => self.entity_cache.len() != 0
            }
        }
    }
}

impl Iterator for ProjectEntityIter<dcd::Commit> {
    type Item = dcd::Commit;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_commit()
    }
}

impl Iterator for ProjectEntityIter<dcd::User> {
    type Item = dcd::User;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let id_opt =
                self.next_id_from_cache();

            if let Some(id) = id_opt {
                return self.database.user(UserId::from(id))
            }

            if !self.populate_cache() {
                return None
            }
        }
    }
}

impl Iterator for ProjectEntityIter<dcd::FilePath> {
    type Item = dcd::FilePath;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let id_opt =
                self.next_id_from_cache();

            if let Some(id) = id_opt {
                return self.database.path(PathId::from(id))
            }

            if !self.populate_cache() {
                return None
            }
        }
    }
}

pub struct EntityIter<TI: From<usize> + Into<u64>, T> {
    database: DatabasePtr,
    ids: Box<dyn Iterator<Item=TI>>,
    snappy: bool,
    _entity: PhantomData<T>,
}

impl<TI, T> EntityIter<TI, T> where TI: From<usize> + Into<u64> {
    pub fn from(database: DatabasePtr, ids: impl Iterator<Item=TI> + 'static) -> EntityIter<TI, T> {
        EntityIter { ids: Box::new(ids), database, _entity: PhantomData, snappy: false }
    }
    /**
     * In snappy mode, the iterator will load only bare bones versions of objects (currently this
     * applies only to commits). This dramatically increases performance.
     */
    pub fn and_make_it_snappy(self) -> Self {
        EntityIter { ids: self.ids, database: self.database, _entity: PhantomData, snappy: true }
    }
}

impl Iterator for EntityIter<ProjectId, dcd::Project> {
    type Item = dcd::Project;
    fn next(&mut self) -> Option<Self::Item> {
        self.ids.next().map(|id| self.database.project(id.into())).flatten()
    }
}

impl Iterator for EntityIter<CommitId, dcd::Commit> { // FIXME also bare commit
    type Item = dcd::Commit;
    fn next(&mut self) -> Option<Self::Item> {
        if self.snappy {
            self.ids.next().map(|id| self.database.bare_commit(id.into())).flatten()
        } else {
            self.ids.next().map(|id| self.database.commit(id.into())).flatten()
        }
    }
}

macro_rules! untangle { ($self:expr) => {{ let db: &RefCell<Djanco> = $self.borrow(); db.borrow() }} }

impl DataSource for DatabasePtr {
    fn project_count(&self) -> usize { untangle!(self).path_count()   }
    fn commit_count(&self)  -> usize { untangle!(self).commit_count() }
    fn user_count(&self)    -> usize { untangle!(self).user_count()   }
    fn path_count(&self)    -> usize { untangle!(self).path_count()   }

    fn project(&self, id: ProjectId)    -> Option<dcd::Project>  { untangle!(self).project(id)     }
    fn commit(&self, id: CommitId)      -> Option<dcd::Commit>   { untangle!(self).commit(id)      }
    fn bare_commit(&self, id: CommitId) -> Option<dcd::Commit>   { untangle!(self).bare_commit(id) }
    fn user(&self, id: UserId)          -> Option<dcd::User>     { untangle!(self).user(id)        }
    fn path(&self, id: PathId)          -> Option<dcd::FilePath> { untangle!(self).path(id)        }

    fn project_ids(&self) -> Map<Range<usize>, fn(usize)->ProjectId> { untangle!(self).project_ids() }
    fn commit_ids(&self)  -> Map<Range<usize>, fn(usize)->CommitId>  { untangle!(self).commit_ids()  }
    fn user_ids(&self)    -> Map<Range<usize>, fn(usize)->UserId>    { untangle!(self).user_ids()    }
    fn path_ids(&self)    -> Map<Range<usize>, fn(usize)->PathId>    { untangle!(self).path_ids()    }

    fn projects(&self)     -> EntityIter<ProjectId, dcd::Project> { untangle!(self).projects()     }
    fn commits(&self)      -> EntityIter<CommitId, dcd::Commit>   { untangle!(self).commits()      }
    fn bare_commits(&self) -> EntityIter<CommitId, dcd::Commit>   { untangle!(self).bare_commits() }
    fn users(&self)        -> EntityIter<UserId, dcd::User>       { untangle!(self).users()        }
    fn paths(&self)        -> EntityIter<PathId, dcd::FilePath>   { untangle!(self).paths()        }

    fn commits_from(&self, project: &dcd::Project) -> ProjectEntityIter<dcd::Commit> {
        untangle!(self).commits_from(project)
    }
    fn bare_commits_from(&self, project: &dcd::Project) -> ProjectEntityIter<dcd::Commit> {
        untangle!(self).bare_commits_from(project)
    }
    fn users_from(&self, project: &dcd::Project) -> ProjectEntityIter<dcd::User> {
        untangle!(self).users_from(project)
    }
    fn paths_from(&self, project: &dcd::Project) -> ProjectEntityIter<dcd::FilePath> {
        untangle!(self).paths_from(project)
    }
    fn authors_from(&self, project: &dcd::Project) -> ProjectEntityIter<dcd::User> {
        untangle!(self).authors_from(project)
    }
    fn committers_from(&self, project: &dcd::Project) -> ProjectEntityIter<dcd::User> {
        untangle!(self).committers_from(project)
    }

    fn commit_count_from(&self, project: &dcd::Project)    -> usize { untangle!(self).commit_count_from(project)    }
    fn user_count_from(&self, project: &dcd::Project)      -> usize { untangle!(self).user_count_from(project)      }
    fn path_count_from(&self, project: &dcd::Project)      -> usize { untangle!(self).path_count_from(project)      }
    fn author_count_from(&self, project: &dcd::Project)    -> usize { untangle!(self).author_count_from(project)    }
    fn committer_count_from(&self, project: &dcd::Project) -> usize { untangle!(self).committer_count_from(project) }

    fn age_of(&self, project: &dcd::Project) -> Option<Duration> { untangle!(self).age_of(project) }

    fn seed(&self) -> u128 { untangle!(self).seed() }
}

impl Iterator for EntityIter<UserId, dcd::User> {
    type Item = dcd::User;
    fn next(&mut self) -> Option<Self::Item> {
        self.ids.next().map(move |id| self.database.clone().user(id.into())).flatten()
    }
}

impl Iterator for EntityIter<PathId, dcd::FilePath> {
    type Item = dcd::FilePath;
    fn next(&mut self) -> Option<Self::Item> {
        // XXX helpfulstuff
        // let db: Rc<RefCell<Dejaco>> = self.database.clone();
        // self.ids.next().map(move |id| {
        //     let x = (*(self.database.clone())).borrow();
        //     x.path(id.into())
        // }).flatten()
        self.ids.next().map(move |id| self.database.clone().path(id.into())).flatten()
    }
}

impl<TI, T> WithDatabase for EntityIter<TI, T> where TI: From<usize> + Into<u64> {
    fn get_database_ptr(&self) -> DatabasePtr { self.database.clone() }
}

// Project Attributes
// pub enum Attrib {
//     Language,
//     Stars,
//     Commits,
//     Users,
// }
//
// pub trait RequireOperand {}
// impl RequireOperand for Attrib {}
// impl RequireOperand for Stats  {}
//
// pub trait StatsOperand {}
// impl StatsOperand for Attrib {}
//
// pub enum Stats {
//     Count(),
//     Mean(),
//     Median(),
// }

pub trait Attribute {}

pub trait NamedAttribute {
    fn name() -> String { "id".to_owned() }
}

pub trait Group {
    type Key;
    fn select(&self, project: &dcd::Project) -> Self::Key;
}

pub trait SortEach {
    /*type Key;*/ // TODO
    fn sort(&self, database: DatabasePtr, /*key: &Self::Key,*/ projects: &mut Vec<dcd::Project>);
}

pub trait FilterEach {
    /*type Key;*/ // TODO
    fn filter(&self, database: DatabasePtr, /*key: &Self::Key,*/ project: &dcd::Project) -> bool;
}

pub trait SampleEach {
    /*type Key;*/ // TODO
    fn sample(&self, database: DatabasePtr, /*key: &Self::Key,*/ projects: Vec<dcd::Project>) -> Vec<dcd::Project>;
}

pub trait SelectEach {
    type Entity;
    fn select(&self, database: DatabasePtr, /*key: &Self::Key,*/ project: dcd::Project) -> Self::Entity;
}

pub trait NumericalAttribute {
    type Entity;
    fn calculate(&self, database: DatabasePtr, entity: &Self::Entity) -> usize;
}

pub trait CollectionAttribute {
    type Entity;
    //fn calculate(&self, database: DatabasePtr, entity: &Self::Entity) -> usize;
}

pub trait StringAttribute {
    type Entity;
    fn extract(&self, database: DatabasePtr, entity: &Self::Entity) -> String;
}

pub mod sample {
    use crate::{DatabasePtr, SampleEach, DataSource};
    use rand::seq::IteratorRandom;
    use rand_pcg::Pcg64Mcg;
    use rand::SeedableRng;

    #[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Top(pub usize);
    #[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Unique<D>(pub usize, D);
    #[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Random(pub usize);

    impl SampleEach for Top {
        fn sample(&self, _database: DatabasePtr, /*key: &Self::Key,*/ projects: Vec<dcd::Project>) -> Vec<dcd::Project> {
            projects.into_iter().take(self.0).collect()
        }
    }

    impl SampleEach for Random {
        fn sample(&self, database: DatabasePtr, /*key: &Self::Key,*/ projects: Vec<dcd::Project>) -> Vec<dcd::Project> {
            let mut rng = Pcg64Mcg::from_seed(database.seed().to_be_bytes());
            projects.into_iter().choose_multiple(&mut rng, self.0)
        }
    }
}

pub mod require {
    use crate::{DatabasePtr, FilterEach, NumericalAttribute, StringAttribute};
    use regex::Regex;

    #[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct AtLeast<N>(pub N, pub usize);
    #[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Exactly<N>(pub N, pub usize);
    #[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct AtMost<N> (pub N, pub usize);

    impl<N> FilterEach for AtLeast<N> where N: NumericalAttribute<Entity=dcd::Project> {
        fn filter(&self, database: DatabasePtr, project: &dcd::Project) -> bool {
            self.0.calculate(database, project) >= self.1
        }
    }

    impl<N> FilterEach for Exactly<N> where N: NumericalAttribute<Entity=dcd::Project> {
        fn filter(&self, database: DatabasePtr, project: &dcd::Project) -> bool {
            self.0.calculate(database, project) == self.1
        }
    }

    impl<N> FilterEach for AtMost<N> where N: NumericalAttribute<Entity=dcd::Project> {
        fn filter(&self, database: DatabasePtr, project: &dcd::Project) -> bool {
            self.0.calculate(database, project) <= self.1
        }
    }

    #[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Same<'a, S>(pub S, pub &'a str);
    #[derive(Clone,                          )] pub struct Matches<S>(pub S, pub Regex);

    #[macro_export] macro_rules! regex { ($str:expr) => { regex::Regex::new($str).unwrap() }}

    impl<'a,S> FilterEach for Same<'a, S> where S: StringAttribute<Entity=dcd::Project> {
        fn filter(&self, database: DatabasePtr, project: &dcd::Project) -> bool {
            self.0.extract(database, project) == self.1.to_string()
        }
    }

    impl<'a,S> FilterEach for Matches<S> where S: StringAttribute<Entity=dcd::Project> {
        fn filter(&self, database: DatabasePtr, project: &dcd::Project) -> bool {
            self.1.is_match(&self.0.extract(database, project))
        }
    }
}

pub mod project {
    use crate::{Attribute, Group, NumericalAttribute, StringAttribute, SortEach, SelectEach, NamedAttribute, AttributeValue};
    use crate::{ProjectId, DatabasePtr, DataSource};
    use crate::meta::ProjectMeta;
    use dcd::Project;

    #[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Id;
    #[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct URL;

    #[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Language;
    #[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Stars;
    #[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Issues;
    #[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct BuggyIssues;

    #[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Heads;
    #[derive(Eq, PartialEq,       Clone, Hash)] pub struct Metadata(String);

    #[derive(Eq, PartialEq,       Clone, Hash)] pub struct Commits;
    #[derive(Eq, PartialEq,       Clone, Hash)] pub struct Users;
    #[derive(Eq, PartialEq,       Clone, Hash)] pub struct Paths;

    impl Attribute for Id          {}
    impl Attribute for URL         {}

    impl Attribute for Language    {}
    impl Attribute for Stars       {}
    impl Attribute for Issues      {}
    impl Attribute for BuggyIssues {}

    impl Attribute for Heads       {}
    impl Attribute for Metadata    {}

    impl Attribute for Commits     {}
    impl Attribute for Users       {}
    impl Attribute for Paths       {}

    impl NamedAttribute for Id          { fn name() -> String { "id".to_owned()  } }
    impl NamedAttribute for URL         { fn name() -> String { "url".to_owned() } }

    impl NamedAttribute for Language    { fn name() -> String { "language".to_owned()     } }
    impl NamedAttribute for Stars       { fn name() -> String { "stars".to_owned()        } }
    impl NamedAttribute for Issues      { fn name() -> String { "issues".to_owned()       } }
    impl NamedAttribute for BuggyIssues { fn name() -> String { "buggy_issues".to_owned() } }

    impl NamedAttribute for Heads       { fn name() -> String { "heads".to_owned()   } }

    impl NamedAttribute for Commits     { fn name() -> String { "commits".to_owned() } }
    impl NamedAttribute for Users       { fn name() -> String { "users".to_owned()   } }
    impl NamedAttribute for Paths       { fn name() -> String { "paths".to_owned()   } }

    impl StringAttribute for Id {
        type Entity = dcd::Project;
        fn extract(&self, _database: DatabasePtr, entity: &Self::Entity) -> String {
            entity.id.to_string()
        }
    }

    impl StringAttribute for URL {
        type Entity = dcd::Project;
        fn extract(&self, _database: DatabasePtr, entity: &Self::Entity) -> String {
            entity.url.clone()
        }
    }

    impl StringAttribute for Language {
        type Entity = dcd::Project;
        fn extract(&self, _database: DatabasePtr, entity: &Self::Entity) -> String {
            entity.get_language_or_empty()
        }
    }

    impl StringAttribute for Stars {
        type Entity = dcd::Project;
        fn extract(&self, _database: DatabasePtr, entity: &Self::Entity) -> String {
            entity.get_stars().map_or(String::new(), |e| e.to_string())
        }
    }

    impl StringAttribute for Issues {
        type Entity = dcd::Project;
        fn extract(&self, _database: DatabasePtr, entity: &Self::Entity) -> String {
            entity.get_issue_count().map_or(String::new(), |e| e.to_string())
        }
    }

    impl StringAttribute for BuggyIssues {
        type Entity = dcd::Project;
        fn extract(&self, _database: DatabasePtr, entity: &Self::Entity) -> String {
            entity.get_buggy_issue_count().map_or(String::new(), |e| e.to_string())
        }
    }

    impl NumericalAttribute for Id {
        type Entity = dcd::Project;
        fn calculate(&self, _database: DatabasePtr, entity: &Self::Entity) -> usize {
            entity.id as usize
        }
    }

    impl NumericalAttribute for Stars {
        type Entity = dcd::Project;
        fn calculate(&self, _database: DatabasePtr, entity: &Self::Entity) -> usize {
            entity.get_stars_or_zero() as usize
        }
    }

    impl NumericalAttribute for Issues {
        type Entity = dcd::Project;
        fn calculate(&self, _database: DatabasePtr, entity: &Self::Entity) -> usize {
            entity.get_issue_count_or_zero() as usize
        }
    }

    impl NumericalAttribute for BuggyIssues {
        type Entity = dcd::Project;
        fn calculate(&self, _database: DatabasePtr, entity: &Self::Entity) -> usize {
            entity.get_buggy_issue_count_or_zero() as usize
        }
    }

    impl NumericalAttribute for Heads {
        type Entity = dcd::Project;
        fn calculate(&self, _database: DatabasePtr, entity: &Self::Entity) -> usize {
            entity.get_head_count() as usize
        }
    }

    impl NumericalAttribute for Metadata {
        type Entity = dcd::Project;
        fn calculate(&self, _database: DatabasePtr, entity: &Self::Entity) -> usize {
            entity.metadata.len()
        }
    }

    impl NumericalAttribute for Commits {
        type Entity = dcd::Project;
        fn calculate(&self, database: DatabasePtr, entity: &Self::Entity) -> usize {
            database.commit_count_from(entity)
        }
    }

    impl NumericalAttribute for Users {
        type Entity = dcd::Project;
        fn calculate(&self, database: DatabasePtr, entity: &Self::Entity) -> usize {
            database.user_count_from(entity)
        }
    }

    impl NumericalAttribute for Paths {
        type Entity = dcd::Project;
        fn calculate(&self, database: DatabasePtr, entity: &Self::Entity) -> usize {
            database.path_count_from(entity)
        }
    }

    impl Group for Id {
        type Key = ProjectId;
        fn select(&self, project: &dcd::Project) -> Self::Key {
            ProjectId(project.id)
        }
    }

    impl Group for Language {
        type Key = String;
        fn select(&self, project: &dcd::Project) -> Self::Key {
            project.get_language_or_empty()
        }
    }

    impl Group for Stars {
        type Key = u64;
        fn select(&self, project: &dcd::Project) -> Self::Key {
            project.get_stars_or_zero()
        }
    }

    impl Group for Issues {
        type Key = u64;
        fn select(&self, project: &dcd::Project) -> Self::Key {
            project.get_issue_count_or_zero()
        }
    }

    impl Group for BuggyIssues {
        type Key = u64;
        fn select(&self, project: &dcd::Project) -> Self::Key {
            project.get_buggy_issue_count_or_zero()
        }
    }

    impl SortEach for Id {
        fn sort(&self, _database: DatabasePtr, projects: &mut Vec<Project>) {
            projects.sort_by_key(|p| p.id)
        }
    }

    impl SortEach for URL {
        fn sort(&self, _database: DatabasePtr, projects: &mut Vec<Project>) {
            projects.sort_by(|p1, p2| p1.url.cmp(&p2.url))
        }
    }

    impl SortEach for Language {
        fn sort(&self, _database: DatabasePtr, projects: &mut Vec<Project>) {
            projects.sort_by_key(|p| p.get_language())
        }
    }

    impl SortEach for Stars {
        fn sort(&self, _database: DatabasePtr, projects: &mut Vec<Project>) {
            projects.sort_by_key(|p| p.get_stars())
        }
    }

    impl SortEach for Issues {
        fn sort(&self, _database: DatabasePtr, projects: &mut Vec<Project>) {
            projects.sort_by_key(|f| f.get_issue_count())
        }
    }

    impl SortEach for BuggyIssues {
        fn sort(&self, _database: DatabasePtr, projects: &mut Vec<Project>) {
            projects.sort_by_key(|p| p.get_buggy_issue_count())
        }
    }

    impl SortEach for Heads {
        fn sort(&self, _database: DatabasePtr, projects: &mut Vec<Project>) {
            projects.sort_by_key(|p| p.get_head_count())
        }
    }

    impl SortEach for Metadata {
        fn sort(&self, _database: DatabasePtr, projects: &mut Vec<Project>) {
            projects.sort_by(|p1, p2| {
                p1.metadata.get(&self.0).cmp(&p2.metadata.get(&self.0))
            });
        }
    }

    impl SortEach for Commits {
        fn sort(&self, database: DatabasePtr, projects: &mut Vec<Project>) {
            projects.sort_by_key(|p| database.commit_count_from(p))
        }
    }

    impl SortEach for Users {
        fn sort(&self, database: DatabasePtr, projects: &mut Vec<Project>) {
            projects.sort_by_key(|p| database.user_count_from(p))
        }
    }

    impl SortEach for Paths {
        fn sort(&self, database: DatabasePtr, projects: &mut Vec<Project>) {
            projects.sort_by_key(|p| database.path_count_from(p))
        }
    }

    impl SelectEach for Id {
        type Entity = AttributeValue<Id, ProjectId>;
        fn select(&self, _database: DatabasePtr, project: Project) -> Self::Entity {
            AttributeValue::new(self, ProjectId::from(project.id))
        }
    }

    impl SelectEach for URL {
        type Entity = AttributeValue<URL, String>;
        fn select(&self, _database: DatabasePtr, project: Project) -> Self::Entity {
            AttributeValue::new(self, project.url)
        }
    }

    impl SelectEach for Language {
        type Entity = AttributeValue<Language, Option<String>>;
        fn select(&self, _database: DatabasePtr, project: Project) -> Self::Entity {
            AttributeValue::new(self, project.get_language())
        }
    }

    impl SelectEach for Stars {
        type Entity = AttributeValue<Stars, Option<u64>>;
        fn select(&self, _database: DatabasePtr, project: Project) -> Self::Entity {
            AttributeValue::new(self, project.get_stars())
        }
    }

    impl SelectEach for Issues {
        type Entity = AttributeValue<Issues, Option<u64>>;
        fn select(&self, _database: DatabasePtr, project: Project) -> Self::Entity {
            AttributeValue::new(self, project.get_issue_count())
        }
    }

    impl SelectEach for BuggyIssues {
        type Entity = AttributeValue<BuggyIssues, Option<u64>>;
        fn select(&self, _database: DatabasePtr, project: Project) -> Self::Entity {
            AttributeValue::new(self, project.get_buggy_issue_count())
        }
    }

    impl SelectEach for Heads {
        type Entity = AttributeValue<Heads, usize>;
        fn select(&self, _database: DatabasePtr, project: Project) -> Self::Entity {
            AttributeValue::new(self, project.get_head_count())
        }
    }

    impl SelectEach for Metadata {
        //type Entity = AttributeValue<Metadata, Option<String>>;
        type Entity = Option<String>;
        fn select(&self, _database: DatabasePtr, project: Project) -> Self::Entity {
            //AttributeValue::new(self, project.metadata.get(&self.0).map(|s| s.clone()))
            project.metadata.get(&self.0).map(|s| s.clone())
        }
    }

    impl SelectEach for Commits {
        type Entity = AttributeValue<Commits, usize>;
        fn select(&self, database: DatabasePtr, project: Project) -> Self::Entity {
            AttributeValue::new(self, database.commit_count_from(&project))
        }
    }

    impl SelectEach for Users {
        type Entity = AttributeValue<Users, usize>;
        fn select(&self, database: DatabasePtr, project: Project) -> Self::Entity {
            AttributeValue::new(self, database.user_count_from(&project))
        }
    }

    impl SelectEach for Paths {
        type Entity = AttributeValue<Paths, usize>;
        fn select(&self, database: DatabasePtr, project: Project) -> Self::Entity {
            AttributeValue::new(self, database.path_count_from(&project))
        }
    }
}

trait ProjectGroup<'a> {
    fn group_by_attrib<TK>(self, attrib: impl Group<Key=TK>) -> GroupIter<dcd::Project, TK> // FIXME can I make this &self?
        where TK: PartialEq + Eq + Hash;
}

impl<'a> ProjectGroup<'a> for EntityIter<ProjectId, dcd::Project> {
    fn group_by_attrib<TK>(self, attrib: impl Group<Key=TK>) -> GroupIter<dcd::Project, TK>
        where TK: PartialEq + Eq + Hash {

        GroupIter::from(self.get_database_ptr(),
                        self.map(|p: dcd::Project| { (attrib.select(&p), p) })
                            .into_group_map().into_iter().collect::<Vec<(TK, Vec<dcd::Project>)>>())
    }
}

/**
 * There's two thing that can happen in NormilIter. One is to sort the list of things and then
 * return as you go. The other is to pre-group into a map and then yield from that. The second thing
 * happens because there's only so much time I can spend wrangling lifetimes.
 */
pub struct NormilIter<T> {
    database: DatabasePtr,
    data: Vec<T>, // TODO There's gotta be a better way to do this.
    entity_type: PhantomData<T>,
}

impl<T> NormilIter<T> {
    pub fn from(database: DatabasePtr, data: impl Into<Vec<T>>) -> NormilIter<T> {
        NormilIter {
            database,
            data: data.into(),
            entity_type: PhantomData,
        }
    }
}

impl<T> WithDatabase for NormilIter<T> {
    fn get_database_ptr(&self) -> DatabasePtr { self.database.clone() }
}

impl<T> Iterator for NormilIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> { self.data.pop() }
}

/**
 * There's two thing that can happen in GroupIter. One is to sort the list of things and then
 * return as you go. The other is to pre-group into a map and then yield from that. The second thing
 * happens because there's only so much time I can spend wrangling lifetimes (for now).
 */
pub struct GroupIter<T, TK: PartialEq + Eq + Hash> {
    database: DatabasePtr,
    map: Vec<(TK, Vec<T>)>,
    entity_type: PhantomData<T>,
    key_type: PhantomData<TK>,
}

impl<T, TK> GroupIter<T, TK> where TK: PartialEq + Eq + Hash {
    pub fn from(database: DatabasePtr, data: impl Into<Vec<(TK, Vec<T>)>>) -> GroupIter<T, TK> {
        GroupIter {
            database,
            map: data.into(),
            entity_type: PhantomData,
            key_type: PhantomData,
        }
    }
}

impl<T, TK> WithDatabase for GroupIter<T, TK> where TK: PartialEq + Eq + Hash {
    fn get_database_ptr(&self) -> DatabasePtr { self.database.clone() }
}

impl<TK, T> Iterator for GroupIter<T, TK> where TK: PartialEq + Eq + Hash {
    type Item = (TK, Vec<T>);
    fn next(&mut self) -> Option<Self::Item> { self.map.pop() }
}

trait Ops {
    fn filter_by_attrib(self, attrib: impl FilterEach + Clone) -> NormilIter<dcd::Project>; // tombstones?
    fn sort_by_attrib(self, attrib: impl SortEach + Clone) -> NormilIter<dcd::Project>;
    fn sample_by(self, attrib: impl SampleEach + Clone) -> NormilIter<dcd::Project>;
    fn select<IntoT>(self, attrib: impl SelectEach<Entity=IntoT> + Clone) -> NormilIter<IntoT>;
}

impl Ops for NormilIter<dcd::Project> {
    fn filter_by_attrib(self, attrib: impl FilterEach + Clone) -> NormilIter<dcd::Project> {
        let database = self.get_database_ptr();
        let inherited_database = self.get_database_ptr();
        let iterator= self.into_iter().filter(|p| {
            let database = database.clone();
            // FIXME giving up on laziness for now
            attrib.filter(database, /*&key,*/ p)
        });
        NormilIter::from(inherited_database, iterator.collect::<Vec<dcd::Project>>())
    }

    fn sort_by_attrib(mut self, attrib: impl SortEach + Clone) -> NormilIter<dcd::Project> {
        let database = self.get_database_ptr();
        let inherited_database = self.get_database_ptr();
        attrib.sort(database, &mut self.data);
        NormilIter::from(inherited_database, self.data)
    }

    fn sample_by(self, attrib: impl SampleEach + Clone) -> NormilIter<dcd::Project> {
        let database = self.get_database_ptr();
        let inherited_database = self.get_database_ptr();
        let sample = attrib.sample(database, self.data);
        NormilIter::from(inherited_database,sample)
    }

    fn select<IntoT>(self, attrib: impl SelectEach<Entity=IntoT> + Clone) -> NormilIter<IntoT> {
        let database = self.get_database_ptr();
        let inherited_database = self.get_database_ptr();
        let iterator =
            self.data.into_iter().map(|p| attrib.select(database.clone(), p));
        NormilIter::from(inherited_database,
                         iterator.collect::<Vec<IntoT>>())
    }
}

trait GroupOps<TK> where TK: PartialEq + Eq + Hash {
    fn filter_each_by_attrib(self, attrib: impl FilterEach + Clone) -> GroupIter<dcd::Project, TK>;
    fn sort_each_by_attrib(self, attrib: impl SortEach + Clone) -> GroupIter<dcd::Project, TK>;
    fn sample_each(self, attrib: impl SampleEach + Clone) -> GroupIter<dcd::Project, TK>;
    fn select_each<IntoT>(self, attrib: impl SelectEach<Entity=IntoT> + Clone) -> GroupIter<IntoT, TK>;
    fn drop_key(self) -> Map<GroupIter<dcd::Project, TK>, fn((TK, Vec<dcd::Project>)) -> Vec<dcd::Project>>;
    fn squash(self) -> NormilIter<dcd::Project>;
}

impl<TK> GroupOps<TK> for GroupIter<dcd::Project, TK> where TK: PartialEq + Eq + Hash + Clone {
    fn filter_each_by_attrib(self, attrib: impl FilterEach + Clone) -> GroupIter<dcd::Project, TK> {
        let database = self.get_database_ptr();
        let inherited_database = self.get_database_ptr();
        let iterator= self.into_iter()
            .map(|(key, projects)| {
                let database = database.clone();
                (key.clone(), projects.into_iter().filter(|p| {
                    let database = database.clone();
                    // FIXME giving up on laziness for now
                    attrib.filter(database, /*&key,*/ p)
                }).collect::<Vec<dcd::Project>>())
            });
        GroupIter::from(inherited_database,iterator.collect::<Vec<(TK, Vec<dcd::Project>)>>())
    }

    fn sort_each_by_attrib(self, attrib: impl SortEach + Clone) -> GroupIter<Project, TK> {
        let database = self.get_database_ptr();
        let inherited_database = self.get_database_ptr();
        let iterator = self.into_iter()
            .map(|(key, mut projects)| {
                let database = database.clone();
                attrib.sort(database, &mut projects);
                (key, projects)
            });
        GroupIter::from(inherited_database,iterator.collect::<Vec<(TK, Vec<dcd::Project>)>>())
    }

    fn sample_each(self, attrib: impl SampleEach + Clone) -> GroupIter<dcd::Project, TK> {
        let database = self.get_database_ptr();
        let inherited_database = self.get_database_ptr();
        let iterator = self.into_iter()
            .map(|(key, projects)| {
                let database = database.clone();
                (key, attrib.sample(database, projects))
            });
        GroupIter::from(inherited_database,iterator.collect::<Vec<(TK, Vec<dcd::Project>)>>())
    }

    fn select_each<IntoT>(self, attrib: impl SelectEach<Entity=IntoT> + Clone) -> GroupIter<IntoT, TK> {
        let database = self.get_database_ptr();
        let inherited_database = self.get_database_ptr();
        let iterator = self.into_iter()
            .map(|(key, projects)| {
                let database = database.clone();
                (key, projects.into_iter().map(|p| attrib.select(database.clone(), p)).collect())
            });
        GroupIter::from(inherited_database,iterator.collect::<Vec<(TK, Vec<IntoT>)>>())
    }

    fn drop_key(self) -> Map<GroupIter<dcd::Project, TK>, fn((TK, Vec<dcd::Project>)) -> Vec<dcd::Project>> {
        self.into_iter().map(|tupple| tupple.1)
    }

    fn squash(self) -> NormilIter<dcd::Project> {
        let inherited_database = self.get_database_ptr();
        let iterator = self.into_iter().map(|tupple| tupple.1).flatten();
        NormilIter::from(inherited_database, iterator.collect::<Vec<dcd::Project>>())
    }
}

#[allow(non_snake_case)]
pub trait CSVItem {
    fn header_line() -> String;
    fn to_csv_line(&self, db: DatabasePtr) -> String;
}

impl CSVItem for ProjectId {
    fn header_line() -> String { "project_id".to_owned() }
    fn to_csv_line(&self, _db: DatabasePtr) -> String { self.0.to_string() }
}

impl CSVItem for CommitId {
    fn header_line() -> String { "commit_id".to_owned() }
    fn to_csv_line(&self, _db: DatabasePtr) -> String { self.0.to_string() }
}

impl CSVItem for PathId {
    fn header_line() -> String { "path_id".to_owned() }
    fn to_csv_line(&self, _db: DatabasePtr) -> String { self.0.to_string() }
}

impl CSVItem for UserId {
    fn header_line() -> String { "user_id".to_owned() }
    fn to_csv_line(&self, _db: DatabasePtr) -> String { self.0.to_string() }
}

impl CSVItem for Project {
    fn header_line() -> String {
        "id,url,last_update,language,\
         stars,issues,buggy_issues,\
         head_count,commit_count,user_count,path_count,author_count,committer_count,\
         age".to_owned()
    }

    fn to_csv_line(&self, db: DatabasePtr) -> String {
         format!(r#"{},"{}",{},{},{},{},{},{},{},{},{},{},{},{}"#,
             self.id, self.url, self.last_update,
             self.get_language().unwrap_or(String::new()),
             self.get_stars().map_or(String::new(), |e| e.to_string()),
             self.get_issue_count().map_or(String::new(), |e| e.to_string()),
             self.get_buggy_issue_count().map_or(String::new(), |e| e.to_string()),
             self.get_head_count(),
             db.commit_count_from(&self),
             db.user_count_from(&self),
             db.path_count_from(&self),
             db.author_count_from(&self),
             db.committer_count_from(&self),
             db.age_of(&self).map_or(String::new(), |e| e.as_secs().to_string()),
        )
    }
}

pub trait CSVField {
    fn to_csv_field(&self) -> String;
}

impl CSVField for String {
    fn to_csv_field(&self) -> String {
        self.to_string()
    }
}

impl CSVField for u64 {
    fn to_csv_field(&self) -> String {
        self.to_string()
    }
}

impl CSVField for usize {
    fn to_csv_field(&self) -> String {
        self.to_string()
    }
}

impl<T> CSVField for Option<T> where T: Display {
    fn to_csv_field(&self) -> String {
        match self {
            Some(value) => value.to_string(),
            None => "".to_owned(),
        }
    }
}

pub struct AttributeValue<A: NamedAttribute, T: CSVField> {
    value: T,
    attribute_type: PhantomData<A>,
}

impl<T, A> AttributeValue<A, T> where T: CSVField, A: NamedAttribute {
    pub fn new(_attribute: &A, value: T) -> AttributeValue<A, T> { AttributeValue { value, attribute_type: PhantomData } }
}

impl<T, A> CSVItem for AttributeValue<A, T> where T: CSVField, A: NamedAttribute {
    fn header_line() -> String { A::name() }
    fn to_csv_line(&self, _db: DatabasePtr) -> String { self.value.to_csv_field() }
}

#[allow(non_snake_case)]
pub trait CSV {
    fn to_csv(self, location: impl Into<String>) -> Result<(), std::io::Error>;
}

impl<I,T> CSV for I where I: Iterator<Item=T> + WithDatabase, T: CSVItem {
    fn to_csv(self, location: impl Into<String>) -> Result<(), Error> {
        let path = PathBuf::from(location.into());
        let dir_path = { let mut dir_path = path.clone(); dir_path.pop(); dir_path };
        create_dir_all(&dir_path).unwrap();

        let mut file = File::create(path)?;
        let database = self.get_database_ptr();
        writeln!(file, "{}", T::header_line())?;
        for element in self {
            writeln!(file, "{}", element.to_csv_line(database.clone()))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{Djanco, Month, DataSource, ProjectGroup, Ops, GroupOps, regex, project, require, sample, CSV};

    #[test]
    fn example() {
        let database = Djanco::from("/dejavuii/dejacode/dataset-tiny", 0,
                                               Month::August(2020));
        database
            .projects()
            .group_by_attrib(project::Language)
            .filter_each_by_attrib(require::AtLeast(project::Stars, 1))
            //.filter_each_by_attrib(require::AtLeast(project::Commits, 25))
            //.filter_each_by_attrib(require::AtLeast(project::Users, 2))
            //.filter_each_by_attrib(require::Same(project::Language, "Rust"))
            //.filter_each_by_attrib(require::Matches(project::URL, regex!("^https://github.com/PRL-PRG/.*$")))
            .sort_each_by_attrib(project::Stars)
            .sample_each(sample::Top(2))
            .squash()
            //.select(project::Id)
            .to_csv("projects.csv").unwrap()
    }
}