use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::cmp::Ordering;

use serde::{Serialize, Deserialize};

// use crate::meta::ProjectMeta;
// use crate::data::DataPtr;
// use crate::time::Seconds;
use crate::data::Data;
// use crate::names::WithNames;

/**== Object IDs ================================================================================**/
#[derive(Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Debug)] pub struct ProjectId(pub u64);
#[derive(Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Debug)] pub struct CommitId(pub u64);
#[derive(Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Debug)] pub struct UserId(pub u64);
#[derive(Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Debug)] pub struct PathId(pub u64);
#[derive(Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Debug)] pub struct SnapshotId(pub u64);

/**== Object IDs convenience ====================================================================**/
impl ProjectId  { pub fn to_string(&self) -> String { self.0.to_string() } }
impl CommitId   { pub fn to_string(&self) -> String { self.0.to_string() } }
impl UserId     { pub fn to_string(&self) -> String { self.0.to_string() } }
impl PathId     { pub fn to_string(&self) -> String { self.0.to_string() } }
impl SnapshotId { pub fn to_string(&self) -> String { self.0.to_string() } }

impl Into<String> for ProjectId  { fn into(self) -> String { self.0.to_string() } }
impl Into<String> for CommitId   { fn into(self) -> String { self.0.to_string() } }
impl Into<String> for UserId     { fn into(self) -> String { self.0.to_string() } }
impl Into<String> for PathId     { fn into(self) -> String { self.0.to_string() } }
impl Into<String> for SnapshotId { fn into(self) -> String { self.0.to_string() } }

impl Into<usize> for ProjectId  { fn into(self) -> usize { self.0 as usize } }
impl Into<usize> for CommitId   { fn into(self) -> usize { self.0 as usize } }
impl Into<usize> for UserId     { fn into(self) -> usize { self.0 as usize } }
impl Into<usize> for PathId     { fn into(self) -> usize { self.0 as usize } }
impl Into<usize> for SnapshotId { fn into(self) -> usize { self.0 as usize } }

impl Into<usize> for &ProjectId  { fn into(self) -> usize { self.0 as usize } }
impl Into<usize> for &CommitId   { fn into(self) -> usize { self.0 as usize } }
impl Into<usize> for &UserId     { fn into(self) -> usize { self.0 as usize } }
impl Into<usize> for &PathId     { fn into(self) -> usize { self.0 as usize } }
impl Into<usize> for &SnapshotId { fn into(self) -> usize { self.0 as usize } }

impl Into<u64>   for ProjectId  { fn into(self) -> u64 { self.0 } }
impl Into<u64>   for CommitId   { fn into(self) -> u64 { self.0 } }
impl Into<u64>   for UserId     { fn into(self) -> u64 { self.0 } }
impl Into<u64>   for PathId     { fn into(self) -> u64 { self.0 } }
impl Into<u64>   for SnapshotId { fn into(self) -> u64 { self.0 } }

impl Into<u64>   for &ProjectId  { fn into(self) -> u64 { self.0 } }
impl Into<u64>   for &CommitId   { fn into(self) -> u64 { self.0 } }
impl Into<u64>   for &UserId     { fn into(self) -> u64 { self.0 } }
impl Into<u64>   for &PathId     { fn into(self) -> u64 { self.0 } }
impl Into<u64>   for &SnapshotId { fn into(self) -> u64 { self.0 } }

impl From<usize> for ProjectId  { fn from(n: usize) -> Self { ProjectId(n as u64)  } }
impl From<usize> for CommitId   { fn from(n: usize) -> Self { CommitId(n as u64)   } }
impl From<usize> for UserId     { fn from(n: usize) -> Self { UserId(n as u64)     } }
impl From<usize> for PathId     { fn from(n: usize) -> Self { PathId(n as u64)     } }
impl From<usize> for SnapshotId { fn from(n: usize) -> Self { SnapshotId(n as u64) } }

impl From<&usize> for ProjectId  { fn from(n: &usize) -> Self { ProjectId(*n as u64)  } }
impl From<&usize> for CommitId   { fn from(n: &usize) -> Self { CommitId(*n as u64)   } }
impl From<&usize> for UserId     { fn from(n: &usize) -> Self { UserId(*n as u64)     } }
impl From<&usize> for PathId     { fn from(n: &usize) -> Self { PathId(*n as u64)     } }
impl From<&usize> for SnapshotId { fn from(n: &usize) -> Self { SnapshotId(*n as u64) } }

impl From<u64>   for ProjectId  { fn from(n: u64) -> Self { ProjectId(n)  } }
impl From<u64>   for CommitId   { fn from(n: u64) -> Self { CommitId(n)   } }
impl From<u64>   for UserId     { fn from(n: u64) -> Self { UserId(n)     } }
impl From<u64>   for PathId     { fn from(n: u64) -> Self { PathId(n)     } }
impl From<u64>   for SnapshotId { fn from(n: u64) -> Self { SnapshotId(n) } }

impl From<&u64>   for ProjectId { fn from(n: &u64) -> Self { ProjectId(*n) } }
impl From<&u64>   for CommitId  { fn from(n: &u64) -> Self { CommitId(*n)  } }
impl From<&u64>   for UserId    { fn from(n: &u64) -> Self { UserId(*n)    } }
impl From<&u64>   for PathId    { fn from(n: &u64) -> Self { PathId(*n)    } }

impl Display for ProjectId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0) }
}
impl Display for CommitId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0) }
}
impl Display for UserId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0) }
}
impl Display for PathId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0) }
}
impl Display for SnapshotId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0) }
}

pub trait Identity: Copy + Clone + Hash + Eq + PartialEq + Ord + PartialOrd + Display /*+ WithNames*/ {}
impl Identity for ProjectId  {}
impl Identity for UserId     {}
impl Identity for CommitId   {}
impl Identity for PathId     {}
impl Identity for SnapshotId {}

//impl Deref for ProjectId  { type Target = ProjectId; fn deref(&self) -> &Self::Target { &self.clone() } }
// impl Deref for CommitId   { type Target = Self; fn deref(&self) -> &Self::Target { &self.clone() } }
// impl Deref for UserId     { type Target = Self; fn deref(&self) -> &Self::Target { &self.clone() } }
// impl Deref for PathId     { type Target = Self; fn deref(&self) -> &Self::Target { &self.clone() } }
// impl Deref for SnapshotId { type Target = Self; fn deref(&self) -> &Self::Target { &self.clone() } }

/** ==== Object-ID relationship indication ===================================================== **/
pub trait Identifiable<T> where T: Identity { fn id(&self) -> T; }
pub trait Reifiable<T> { fn reify(&self, db: &mut Data) -> T; }
impl<I, T> Reifiable<Vec<T>> for Vec<I> where I: Reifiable<T> {
    fn reify(&self, db: &mut Data) -> Vec<T> {
        self.iter().map(|e| e.reify(db)).collect()
    }
}

/**== Objects ===================================================================================**/
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Project {
    pub id: ProjectId,
    pub url: String,
    // last_update: Option<i64>,
    // language: Option<Option<String>>,
    // stars: Option<Option<usize>>,
    // issues: Option<Option<usize>>,
    // buggy_issues: Option<Option<usize>>,
    // heads: Option<Vec<(String, CommitId)>>,
}

impl PartialEq for Project {
    fn eq(&self, other: &Self) -> bool { self.id.eq(&other.id) }
}
impl PartialOrd for Project {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering>{ self.id.partial_cmp(&other.id) }
}
impl Eq for Project {  }
impl Ord for Project {
    fn cmp(&self, other: &Self) -> Ordering { self.id.cmp(&other.id) }
}
impl Hash for Project {
    fn hash<H: Hasher>(&self, state: &mut H) { self.id.hash(state) }
}

impl Identifiable<ProjectId> for Project { fn id(&self) -> ProjectId { self.id } }

impl Project {
    pub fn url            (&self)                -> &str                    { self.url.as_str()                    }
    //pub fn timestamp      (&self, db: &mut Data) -> i64                     { db.project_timestamp      (&self.id) }
    pub fn language       (&self, db: &mut Data) -> Option<String>          { db.project_language       (&self.id) }
    pub fn stars          (&self, db: &mut Data) -> Option<usize>           { db.project_stars          (&self.id) }
    pub fn issues         (&self, db: &mut Data) -> Option<usize>           { db.project_issues         (&self.id) }
    pub fn buggy_issues   (&self, db: &mut Data) -> Option<usize>           { db.project_buggy_issues   (&self.id) }
    pub fn heads          (&self, db: &mut Data) -> Vec<(String, CommitId)> { db.project_heads          (&self.id) }
    pub fn users          (&self, db: &mut Data) -> Vec<User>               { db.project_users          (&self.id) }
    pub fn authors        (&self, db: &mut Data) -> Vec<User>               { db.project_authors        (&self.id) }
    pub fn committers     (&self, db: &mut Data) -> Vec<User>               { db.project_committers     (&self.id) }
    pub fn user_count     (&self, db: &mut Data) -> usize                   { db.project_user_count     (&self.id) }
    pub fn author_count   (&self, db: &mut Data) -> usize                   { db.project_author_count   (&self.id) }
    pub fn committer_count(&self, db: &mut Data) -> usize                   { db.project_committer_count(&self.id) }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct User { pub(crate) id: UserId, /*pub(crate) name: String,*/ pub(crate) email: String }
impl User {
    pub fn new(id: UserId, email: String) -> Self { User { id, email } }
    pub fn email(&self) -> &str { self.email.as_str() }
}
impl Identifiable<UserId> for User { fn id(&self) -> UserId { self.id } }
impl Reifiable<User> for UserId { fn reify(&self, db: &mut Data) -> User { db.user(&self).unwrap() } }
impl PartialEq for User {
    fn eq(&self, other: &Self) -> bool { self.id.eq(&other.id) }
}
impl PartialOrd for User {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering>{ self.id.partial_cmp(&other.id) }
}
impl Eq for User {  }
impl Ord for User {
    fn cmp(&self, other: &Self) -> Ordering { self.id.cmp(&other.id) }
}
impl Hash for User {
    fn hash<H: Hasher>(&self, state: &mut H) { self.id.hash(state) }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Commit {
    pub(crate) id: CommitId,
    /*pub(crate) hash: String,*/
    pub(crate) committer: UserId,
    pub(crate) author: UserId,
    pub(crate) parents: Vec<CommitId>,
    // pub(crate) author_timestamp : i64,
    // pub(crate) committer_timestamp : i64,
    // pub changes : HashMap<u64,u64>,
    // pub message : String,
}
impl Commit {
    // pub(crate) fn new(id: CommitId, /*hash: String,*/ committer: UserId, author: UserId, parents: Vec<CommitId>) -> Commit {
    //
    // }
    //pub fn hash(&self) -> &str                          { self.hash.as_str()       }
    pub fn committer(&self, db: &mut Data) -> User        { self.committer.reify(db) }
    pub fn author   (&self, db: &mut Data) -> User        { self.author.reify(db)    }
    pub fn parents  (&self, db: &mut Data) -> Vec<Commit> { self.parents.reify(db)   }
}

impl Identifiable<CommitId> for Commit { fn id(&self) -> CommitId { self.id } }
impl Reifiable<Commit> for CommitId { fn reify(&self, db: &mut Data) -> Commit { db.commit(&self).unwrap() } }
impl PartialEq for Commit {
    fn eq(&self, other: &Self) -> bool { self.id.eq(&other.id) }
}
impl PartialOrd for Commit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering>{ self.id.partial_cmp(&other.id) }
}
impl Eq for Commit {  }
impl Ord for Commit {
    fn cmp(&self, other: &Self) -> Ordering { self.id.cmp(&other.id) }
}
impl Hash for Commit {
    fn hash<H: Hasher>(&self, state: &mut H) { self.id.hash(state) }
}
impl Commit {
    // pub committer : u64,
    // pub committer_time : i64,
    // pub author : u64,
    // pub author_time : i64,
    // pub parents : Vec<u64>,
    // pub changes : HashMap<u64,u64>,
    // pub message : String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Path { id: PathId, location: String }
impl Path {
    pub fn new(id: PathId, location: String) -> Self { Path { id, location } }
}
impl Identifiable<PathId> for Path { fn id(&self) -> PathId { self.id } }
impl Reifiable<Path> for PathId { fn reify(&self, db: &mut Data) -> Path { db.path(&self).unwrap() } }
impl PartialEq for Path {
    fn eq(&self, other: &Self) -> bool { self.id.eq(&other.id) }
}
impl PartialOrd for Path {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering>{ self.id.partial_cmp(&other.id) }
}
impl Eq for Path {  }
impl Ord for Path {
    fn cmp(&self, other: &Self) -> Ordering { self.id.cmp(&other.id) }
}
impl Hash for Path {
    fn hash<H: Hasher>(&self, state: &mut H) { self.id.hash(state) }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Contents {}
//impl Identifiable<ContentsId> for Contents { fn id(&self) -> ContentsId { self.id } }

// #[derive(Clone, Debug, Serialize, Deserialize)]
// pub struct Message {}
//impl Identifiable<CommitId> for Message { fn id(&self) -> CommitId { self.id } }