use chrono::{Date, Utc, DateTime, TimeZone};
use std::fmt::{Display, Formatter};
use std::collections::HashMap;
use crate::meta::ProjectMeta;
use std::hash::{Hash, Hasher};
use std::cmp::Ordering;

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

#[derive(Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord)] pub struct ProjectId(pub u64);
#[derive(Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord)] pub struct CommitId(pub u64);
#[derive(Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord)] pub struct UserId(pub u64);
#[derive(Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord)] pub struct PathId(pub u64);

impl ProjectId { fn to_string(&self) -> String { self.0.to_string() } }
impl CommitId  { fn to_string(&self) -> String { self.0.to_string() } }
impl UserId    { fn to_string(&self) -> String { self.0.to_string() } }
impl PathId    { fn to_string(&self) -> String { self.0.to_string() } }

impl Into<String> for ProjectId { fn into(self) -> String { self.0.to_string() } }
impl Into<String> for CommitId  { fn into(self) -> String { self.0.to_string() } }
impl Into<String> for UserId    { fn into(self) -> String { self.0.to_string() } }
impl Into<String> for PathId    { fn into(self) -> String { self.0.to_string() } }

impl Into<usize> for ProjectId { fn into(self) -> usize { self.0 as usize } }
impl Into<usize> for CommitId  { fn into(self) -> usize { self.0 as usize } }
impl Into<usize> for UserId    { fn into(self) -> usize { self.0 as usize } }
impl Into<usize> for PathId    { fn into(self) -> usize { self.0 as usize } }

impl Into<usize> for &ProjectId { fn into(self) -> usize { self.0 as usize } }
impl Into<usize> for &CommitId  { fn into(self) -> usize { self.0 as usize } }
impl Into<usize> for &UserId    { fn into(self) -> usize { self.0 as usize } }
impl Into<usize> for &PathId    { fn into(self) -> usize { self.0 as usize } }

impl Into<u64>   for ProjectId { fn into(self) -> u64 { self.0 } }
impl Into<u64>   for CommitId  { fn into(self) -> u64 { self.0 } }
impl Into<u64>   for UserId    { fn into(self) -> u64 { self.0 } }
impl Into<u64>   for PathId    { fn into(self) -> u64 { self.0 } }

impl Into<u64>   for &ProjectId { fn into(self) -> u64 { self.0 } }
impl Into<u64>   for &CommitId  { fn into(self) -> u64 { self.0 } }
impl Into<u64>   for &UserId    { fn into(self) -> u64 { self.0 } }
impl Into<u64>   for &PathId    { fn into(self) -> u64 { self.0 } }

impl From<usize> for ProjectId { fn from(n: usize) -> Self { ProjectId(n as u64) } }
impl From<usize> for CommitId  { fn from(n: usize) -> Self { CommitId(n as u64)  } }
impl From<usize> for UserId    { fn from(n: usize) -> Self { UserId(n as u64)    } }
impl From<usize> for PathId    { fn from(n: usize) -> Self { PathId(n as u64)    } }

impl From<&usize> for ProjectId { fn from(n: &usize) -> Self { ProjectId(*n as u64) } }
impl From<&usize> for CommitId  { fn from(n: &usize) -> Self { CommitId(*n as u64)  } }
impl From<&usize> for UserId    { fn from(n: &usize) -> Self { UserId(*n as u64)    } }
impl From<&usize> for PathId    { fn from(n: &usize) -> Self { PathId(*n as u64)    } }

impl From<u64>   for ProjectId { fn from(n: u64) -> Self { ProjectId(n) } }
impl From<u64>   for CommitId  { fn from(n: u64) -> Self { CommitId(n)  } }
impl From<u64>   for UserId    { fn from(n: u64) -> Self { UserId(n)    } }
impl From<u64>   for PathId    { fn from(n: u64) -> Self { PathId(n)    } }

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

#[derive(Clone)] // TODO implement by hand
pub struct Project {
    pub id: ProjectId,
    pub url: String,
    pub last_update: i64,
    pub language: Option<String>,
    pub stars: Option<usize>,
    pub issues: Option<usize>,
    pub buggy_issues: Option<usize>,
    pub heads: Vec<(String, CommitId)>,
    pub metadata: HashMap<String, String>,
}

#[derive(Clone)]
pub struct User {
    pub id: UserId,
    pub email: String,
    pub name: String,
}

#[derive(Clone)] // TODO implement by hand
pub struct Commit {
    pub id: CommitId,
    pub hash: git2::Oid,
    pub author: UserId,
    pub committer: UserId,
    pub author_time: i64,
    pub committer_time: i64,
    pub additions: Option<u64>,
    pub deletions: Option<u64>,
}

#[derive(Clone)] // TODO implement by hand
pub struct Path {
    pub id: PathId,
    pub path: String,
}

#[derive(Clone, Eq, Hash, PartialEq, PartialOrd, Ord)] // TODO implement by hand
pub struct Message {
    contents: Vec<u8>,
}

impl Project {
    pub fn language_or_empty(&self) -> String {
        self.language.as_ref().map_or("", |s| s.as_str()).to_string()
    }

    pub fn stars_or_zero(&self) -> usize { self.stars.map_or(0usize, |n| n as usize) }
    pub fn issues_or_zero(&self) -> usize { self.issues.map_or(0usize, |n| n as usize) }
    pub fn buggy_issues_or_zero(&self) -> usize { self.buggy_issues.map_or(0usize, |n| n as usize) }

    fn filter_metadata(project: &dcd::Project) -> impl Iterator<Item=(String, String)> + '_ {
        project.metadata.iter()
            .filter(|(key, value)| {
                key.as_str() != "ght_language" &&
                    key.as_str() != "star" &&
                    key.as_str() != "issues" &&
                    key.as_str() != "buggy_issues"
            })
            .map(|(key, value)| (key.clone(), value.clone()))
    }
}

impl Hash for Project { fn hash<H: Hasher>(&self, h: &mut H) { self.id.hash(h); } }
impl Hash for User    { fn hash<H: Hasher>(&self, h: &mut H) { self.id.hash(h); } }
impl Hash for Commit  { fn hash<H: Hasher>(&self, h: &mut H) { self.id.hash(h); } }
impl Hash for Path    { fn hash<H: Hasher>(&self, h: &mut H) { self.id.hash(h); } }

impl PartialEq for Project {
    fn eq(&self, other: &Self) -> bool { self.id.eq(&other.id) }
    fn ne(&self, other: &Self) -> bool { self.id.ne(&other.id) }
}
impl PartialEq for Commit {
    fn eq(&self, other: &Self) -> bool { self.id.eq(&other.id) }
    fn ne(&self, other: &Self) -> bool { self.id.ne(&other.id) }
}
impl PartialEq for User {
    fn eq(&self, other: &Self) -> bool { self.id.eq(&other.id) }
    fn ne(&self, other: &Self) -> bool { self.id.ne(&other.id) }
}
impl PartialEq for Path {
    fn eq(&self, other: &Self) -> bool { self.id.eq(&other.id) }
    fn ne(&self, other: &Self) -> bool { self.id.ne(&other.id) }
}

impl Eq for Project {}
impl Eq for User    {}
impl Eq for Commit  {}
impl Eq for Path    {}

impl PartialOrd for Project {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { self.id.partial_cmp(&other.id) }
    fn lt(&self, other: &Self) -> bool { self.id.lt(&other.id) }
    fn le(&self, other: &Self) -> bool { self.id.le(&other.id) }
    fn gt(&self, other: &Self) -> bool { self.id.gt(&other.id) }
    fn ge(&self, other: &Self) -> bool { self.id.ge(&other.id) }
}

impl PartialOrd for Commit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { self.id.partial_cmp(&other.id) }
    fn lt(&self, other: &Self) -> bool { self.id.lt(&other.id) }
    fn le(&self, other: &Self) -> bool { self.id.le(&other.id) }
    fn gt(&self, other: &Self) -> bool { self.id.gt(&other.id) }
    fn ge(&self, other: &Self) -> bool { self.id.ge(&other.id) }
}

impl PartialOrd for User {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { self.id.partial_cmp(&other.id) }
    fn lt(&self, other: &Self) -> bool { self.id.lt(&other.id) }
    fn le(&self, other: &Self) -> bool { self.id.le(&other.id) }
    fn gt(&self, other: &Self) -> bool { self.id.gt(&other.id) }
    fn ge(&self, other: &Self) -> bool { self.id.ge(&other.id) }
}

impl PartialOrd for Path {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { self.id.partial_cmp(&other.id) }
    fn lt(&self, other: &Self) -> bool { self.id.lt(&other.id) }
    fn le(&self, other: &Self) -> bool { self.id.le(&other.id) }
    fn gt(&self, other: &Self) -> bool { self.id.gt(&other.id) }
    fn ge(&self, other: &Self) -> bool { self.id.ge(&other.id) }
}

impl Ord for Project {
    fn cmp(&self, other: &Self) -> Ordering { self.id.cmp(&other.id) }
    fn max(self, other: Self) -> Self where Self: Sized { if self.id < other.id {other} else {self} }
    fn min(self, other: Self) -> Self where Self: Sized { if self.id < other.id {self} else {other}  }
}

impl Ord for Commit {
    fn cmp(&self, other: &Self) -> Ordering { self.id.cmp(&other.id) }
    fn max(self, other: Self) -> Self where Self: Sized { if self.id < other.id {other} else {self} }
    fn min(self, other: Self) -> Self where Self: Sized { if self.id < other.id {self} else {other}  }
}

impl Ord for User {
    fn cmp(&self, other: &Self) -> Ordering { self.id.cmp(&other.id) }
    fn max(self, other: Self) -> Self where Self: Sized { if self.id < other.id {other} else {self} }
    fn min(self, other: Self) -> Self where Self: Sized { if self.id < other.id {self} else {other}  }
}

impl Ord for Path {
    fn cmp(&self, other: &Self) -> Ordering { self.id.cmp(&other.id) }
    fn max(self, other: Self) -> Self where Self: Sized { if self.id < other.id {other} else {self} }
    fn min(self, other: Self) -> Self where Self: Sized { if self.id < other.id {self} else {other}  }
}

impl From<dcd::Project> for Project {
    fn from(project: dcd::Project) -> Self {
        Project {
            id: ProjectId::from(project.id),
            last_update: project.last_update,
            language: project.get_language(),
            stars: project.get_stars().map(|n| n as usize),
            issues: project.get_issue_count().map(|n| n as usize),
            buggy_issues: project.get_buggy_issue_count().map(|n| n as usize),
            metadata: Self::filter_metadata(&project).collect(),
            heads: project.heads.into_iter()
                .map(|(name, commit_id)| (name, CommitId::from(commit_id)))
                .collect(),
            url: project.url,
        }
    }
}

impl From<&dcd::Project> for Project {
    fn from(project: &dcd::Project) -> Self {
        Project {
            id: ProjectId::from(project.id),
            last_update: project.last_update,
            language: project.get_language(),
            stars: project.get_stars().map(|n| n as usize),
            issues: project.get_issue_count().map(|n| n as usize),
            buggy_issues: project.get_buggy_issue_count().map(|n| n as usize),
            heads: project.heads.iter()
                .map(|(name, commit_id)| {
                    (name.clone(), CommitId::from(commit_id))
                })
                .collect(),
            metadata: project.metadata.iter()
                .filter(|(key, value)|
                    key.as_str() != "ght_language" && key.as_str() != "star" &&
                        key.as_str() != "issues" && key.as_str() != "buggy_issues")
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            url: project.url.clone(),
        }
    }
}

impl From<dcd::Commit> for Commit {
    fn from(commit: /*bare*/ dcd::Commit) -> Self {
        Commit {
            id: CommitId::from(commit.id),
            author: UserId::from(commit.author_id),
            committer: UserId::from(commit.committer_id),
            author_time: commit.author_time,
            committer_time: commit.committer_time,
            additions: commit.additions,
            deletions: commit.deletions,
            hash: commit.hash,
        }
    }
}

impl From<&dcd::Commit> for Commit {
    fn from(commit: &/*bare*/ dcd::Commit) -> Self {
        Commit {
            id: CommitId::from(commit.id),
            author: UserId::from(commit.author_id),
            committer: UserId::from(commit.committer_id),
            author_time: commit.author_time,
            committer_time: commit.committer_time,
            additions: commit.additions,
            deletions: commit.deletions,
            hash: commit.hash.clone(),
        }
    }
}

impl From<dcd::User> for User {
    fn from(user: dcd::User) -> Self {
        User {
            id: UserId::from(user.id),
            email: user.email.clone(),
            name: user.name,
        }
    }
}

impl From<&dcd::User> for User {
    fn from(user: &dcd::User) -> Self {
        User {
            id: UserId::from(user.id),
            email: user.email.clone(),
            name: user.name.clone(),
        }
    }
}

impl From<dcd::FilePath> for Path {
    fn from(path: dcd::FilePath) -> Self {
        Path {
            id: PathId::from(path.id),
            path: path.path,
        }
    }
}

impl From<&dcd::FilePath> for Path {
    fn from(path: &dcd::FilePath) -> Self {
        Path {
            id: PathId::from(path.id),
            path: path.path.clone(),
        }
    }
}

impl From<Vec<u8>> for Message {
    fn from(bytes: Vec<u8>) -> Self { Message { contents: bytes } }
}
impl From<&Vec<u8>> for Message {
    fn from(bytes: &Vec<u8>) -> Self { Message { contents: bytes.clone() } }
}

pub trait Roster { fn users(&self) -> Vec<UserId>; }

impl Roster for Commit {
    fn users(&self) -> Vec<UserId> {
        if self.author == self.committer { vec![self.author]                 }
        else                             { vec![self.author, self.committer] }
    }
}

impl Roster for Option<Commit> {
    fn users(&self) -> Vec<UserId> {
        match self.as_ref() {
            Some(commit) => commit.users(),
            None => Default::default(),
        }
    }
}

impl Roster for Option<&Commit> {
    fn users(&self) -> Vec<UserId> {
        match self {
            &Some(commit) => commit.users(),
            &None => Default::default(),
        }
    }
}

impl Roster for dcd::Commit {
    fn users(&self) -> Vec<UserId> {
        if self.author_id == self.committer_id { vec![UserId::from(self.author_id)]    }
        else                                   { vec![UserId::from(self.author_id),
                                                      UserId::from(self.committer_id)] }
    }
}

impl Roster for Option<dcd::Commit> {
    fn users(&self) -> Vec<UserId> {
        match self.as_ref() {
            Some(commit) => commit.users(),
            None => Default::default(),
        }
    }
}

impl Roster for Option<&dcd::Commit> {
    fn users(&self) -> Vec<UserId> {
        match self {
            &Some(commit) => commit.users(),
            &None => Default::default(),
        }
    }
}