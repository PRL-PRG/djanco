pub mod dump;
mod io;
pub mod query;
pub mod cachedb;
pub mod meta;
pub mod mockdb;
pub mod selectors;

use chrono::{Date, DateTime, Utc, TimeZone};
use std::path::PathBuf;
use dcd::{DCD, Database, FilePath, Project};
use std::ops::{Range, Deref};
use std::thread::current;
use std::borrow::Borrow;
use crate::cachedb::CachedDatabase;
use std::marker::PhantomData;

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

impl Into<Date<Utc>> for Month {
    fn into(self) -> Date<Utc> {
        self.into_date()
    }
}

impl Into<DateTime<Utc>> for Month {
    fn into(self) -> DateTime<Utc> {
        self.into_datetime()
    }
}

impl Into<i64> for Month {
    fn into(self) -> i64 {
        self.into_datetime().timestamp()
    }
}

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

pub struct Dejaco {
    timestamp: i64,
    seed: u64,
    path: PathBuf,
    data_store: Box<dyn Database>,
}

impl Dejaco {
    pub fn from<S: Into<String>, T: Into<i64>>(path: S, seed: u64, timestamp: T) -> Self {
        assert!(std::u64::MAX as usize == std::usize::MAX);
        let path = path.into();
        let data_store = DCD::new(path.clone());
        Dejaco {
            data_store: Box::new(data_store),
            path: PathBuf::from(path),
            timestamp: timestamp.into(),
            seed
        }
    }

    pub fn project_count(&self) -> usize { self.data_store.num_projects()   as usize }
    pub fn commit_count(&self)  -> usize { self.data_store.num_commits()    as usize }
    pub fn user_count(&self)    -> usize { self.data_store.num_users()      as usize }
    pub fn path_count(&self)    -> usize { self.data_store.num_file_paths() as usize }

    pub fn project(&self, id: ProjectId) -> Option<dcd::Project>  { self.data_store.get_project(id.into())   }
    pub fn commit(&self, id: CommitId)   -> Option<dcd::Commit>   { self.data_store.get_commit(id.into())    }
    pub fn user(&self, id: UserId)       -> Option<&dcd::User>    { self.data_store.get_user(id.into())      }
    pub fn path(&self, id: PathId)       -> Option<dcd::FilePath> { self.data_store.get_file_path(id.into()) }

    pub fn project_ids(&self) -> impl Iterator<Item=ProjectId> {
        (0..self.project_count()).map(|e| ProjectId::from(e))
    }
    pub fn commit_ids(&self) -> impl Iterator<Item=CommitId> {
        (0..self.commit_count()).map(|e| CommitId::from(e))
    }
    pub fn user_ids(&self) -> impl Iterator<Item=UserId> {
        (0..self.user_count()).map(|e| UserId::from(e))
    }
    pub fn path_ids(&self) -> impl Iterator<Item=PathId> {
        (0..self.path_count()).map(|e| PathId::from(e))
    }

    pub fn projects(&self) -> impl Iterator<Item=dcd::Project> + '_ {
        EntityIter::from(self.data_store.borrow(), self.project_ids())
    }

    pub fn commits(&self) -> impl Iterator<Item=dcd::Commit> + '_ {
        EntityIter::from(self.data_store.borrow(), self.commit_ids())
    }

    pub fn users(&self) -> impl Iterator<Item=&dcd::User> + '_ {
        EntityIter::from(self.data_store.borrow(), self.user_ids())
    }

    pub fn paths(&self) -> impl Iterator<Item=dcd::FilePath> + '_ {
        EntityIter::from(self.data_store.borrow(), self.path_ids())
    }
}

pub trait WithDatabase {
    fn get_database(&self) -> &Dejaco;
}

pub struct EntityIter<'a, TI: From<usize> + Into<u64>, T> {
    database: &'a dyn Database,
    ids: Box<dyn Iterator<Item=TI>>,
    entity_type: PhantomData<T>
}

impl<'a, TI, T> EntityIter<'a, TI, T> where TI: From<usize> + Into<u64> {
    pub fn from(database: &'a dyn Database, ids: impl Iterator<Item=TI> + 'static) -> EntityIter<TI, T> {
        EntityIter { ids: Box::new(ids), database, entity_type: PhantomData }
    }
}

impl<'a> Iterator for EntityIter<'a, ProjectId, dcd::Project> {
    type Item = dcd::Project;
    fn next(&mut self) -> Option<Self::Item> {
        self.ids.next().map(|id| self.database.get_project(id.into())).flatten()
    }
}

impl<'a> Iterator for EntityIter<'a, CommitId, dcd::Commit> { // FIXME also bare commit
    type Item = dcd::Commit;
    fn next(&mut self) -> Option<Self::Item> {
        self.ids.next().map(|id| self.database.get_commit(id.into())).flatten()
    }
}

impl<'a> Iterator for EntityIter<'a, UserId, &'a dcd::User> {
    type Item = &'a dcd::User;
    fn next(&mut self) -> Option<Self::Item> {
        self.ids.next().map(|id| self.database.get_user(id.into())).flatten()
    }
}

impl<'a> Iterator for EntityIter<'a, PathId, dcd::FilePath> {
    type Item = dcd::FilePath;
    fn next(&mut self) -> Option<Self::Item> {
        self.ids.next().map(|id| self.database.get_file_path(id.into())).flatten()
    }
}

#[cfg(test)]
mod tests {
    use crate::{Dejaco, Month};

    #[test]
    fn example() {
        let db = Dejaco::from("/dejacode/dataset-tiny", 0,Month::August(2020));




    }
}