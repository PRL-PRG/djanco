pub mod dump;
mod io;
pub mod query;
pub mod cachedb;
pub mod meta;
pub mod mockdb;
pub mod selectors;

use chrono::{Date, DateTime, Utc, TimeZone};
use std::path::PathBuf;
use dcd::{DCD, Database, User, FilePath, Project, Commit};
use std::marker::PhantomData;
use crate::attrib::Group;
use itertools::Itertools;
//use crate::meta::*;
use std::hash::Hash;
use std::rc::{Rc, Weak};
use std::cell::RefCell;
use std::ops::Range;
use std::borrow::Borrow;
use std::iter::Map;

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

pub struct ProjectId(u64);
pub struct CommitId(u64);
pub struct UserId(u64);
pub struct PathId(u64);

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
}

type DatabasePtr = Rc<RefCell<Djanco>>;

pub struct Djanco {
    warehouse: DCD,
    me: Option<Weak<RefCell<Djanco>>>, // Thanks for the help, Colette.

    _timestamp: i64,
    _seed: u64,
    _path: PathBuf,

    //database: DatabasePtr
}

impl Djanco {
    // pub fn from<S: Into<String>, T: Into<i64>>(path: S, seed: u64, timestamp: T) -> Self {
    //     assert!(std::u64::MAX as usize == std::usize::MAX);
    //     Dejaco {
    //         database: Database::from(path.clone()),
    //         _path: PathBuf::from(path),
    //         _timestamp: timestamp.into(),
    //         _seed: seed,
    //     }
    // }

    pub fn from<S: Into<String>, T: Into<i64>>(path: S, seed: u64, timestamp: T) -> DatabasePtr {
        assert!(std::u64::MAX as usize == std::usize::MAX);

        let string_path = path.into();
        let warehouse = DCD::new(string_path.clone());
        let database = Djanco {
            warehouse,
            me: None,

            _path: PathBuf::from(string_path),
            _timestamp: timestamp.into(),
            _seed: seed,
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
    fn user_count(&self)    -> usize { self.warehouse.num_users()      as usize }
    fn path_count(&self)    -> usize { self.warehouse.num_file_paths() as usize }

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
}

pub trait WithDatabase {
    fn get_database_ptr(&self) -> DatabasePtr;
}

//impl WithDatabase for Dejaco { fn get_database(&self) -> Rc<Dejaco> { Rc::new(self) } }

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

    fn projects(&self)     -> EntityIter<ProjectId, Project> { untangle!(self).projects()     }
    fn commits(&self)      -> EntityIter<CommitId, Commit>   { untangle!(self).commits()      }
    fn bare_commits(&self) -> EntityIter<CommitId, Commit>   { untangle!(self).bare_commits() }
    fn users(&self)        -> EntityIter<UserId, User>       { untangle!(self).users()        }
    fn paths(&self)        -> EntityIter<PathId, FilePath>   { untangle!(self).paths()        }
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
    fn get_database_ptr(&self) -> DatabasePtr {
        self.database.clone()
    }
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
// pub enum Require<Operand> where Operand: RequireOperand {
//     AtLeast(Operand, usize),
//     Exactly(Operand, usize),
//     AtMost(Operand,  usize),
// }
//
// pub trait StatsOperand {}
// impl StatsOperand for Attrib {}
//
// pub enum Stats {
//     Count(),
//     Mean(),
//     Median(),
// }

pub mod attrib {
    use crate::meta::ProjectMeta;

    pub trait Attribute {}

    pub trait Group {
        type Key;
        fn select(&self, project: &dcd::Project) -> Self::Key;
    }

    pub struct Language(String);
    impl Attribute for Language {}
    impl Group for Language {
        type Key = String;

        fn select(&self, project: &dcd::Project) -> Self::Key { project.get_language_or_empty() }
    }
}

trait ProjectGroup<'a> {
    fn group_by_attrib<Iter, TK>(self, attrib: impl attrib::Group<Key=TK>) -> GroupIter<dcd::Project, TK> where TK: PartialEq + Eq + Hash;
}

impl<'a> ProjectGroup<'a> for EntityIter<ProjectId, dcd::Project> {
    fn group_by_attrib<Iter, TK>(self, attrib: impl Group<Key=TK>) -> GroupIter<dcd::Project, TK> where TK: PartialEq + Eq + Hash {
        GroupIter::from(self.get_database_ptr(),
                        self.map(|p: dcd::Project| { (attrib.select(&p), p) })
                            .into_group_map().into_iter().collect::<Vec<(TK, Vec<dcd::Project>)>>())
    }
}

/**
 * There's two thing that can happen in GroupIter. One is to sort the list of things and then
 * return as you go. The other is to pre-group into a map and then yield from that.
 */
pub struct GroupIter<T, TK: PartialEq + Eq + Hash> {
    database: DatabasePtr,
    map: Vec<(TK, Vec<T>)>,

    entity_type: PhantomData<T>,
    key_type: PhantomData<TK>,
}

impl<T, TK> WithDatabase for GroupIter<T, TK> where TK: PartialEq + Eq + Hash {
    fn get_database_ptr(&self) -> DatabasePtr { self.database.clone() }
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

impl<TK, T> Iterator for GroupIter<T, TK> where TK: PartialEq + Eq + Hash {
    type Item = (TK, Vec<T>);

    fn next(&mut self) -> Option<Self::Item> {
        self.map.pop()
    }
}

// impl<'a> Iterator for EntityIter<'a, ProjectId, dcd::Project> {
//     type Item = dcd::Project;
//     fn next(&mut self) -> Option<Self::Item> {
//         self.ids.next().map(|id| self.database.project(id.into())).flatten()
//     }
// }

#[cfg(test)]
mod tests {
    use crate::{Djanco, Month, DataSource};

    #[test]
    fn example() {
        let db = Djanco::from("/dejacode/dataset-tiny", 0, Month::August(2020));

        for url in db.borrow().projects().map(|p| p.url) {
            println!("{}", url);
        }
    }
}