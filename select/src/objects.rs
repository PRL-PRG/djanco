use std::fmt::{Display, Formatter};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::cmp::Ordering;

use serde::{Serialize, Deserialize};

use crate::meta::ProjectMeta;
use crate::data::DataPtr;
use crate::time::Seconds;

/**== Object IDs ================================================================================**/
#[derive(Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize)] pub struct ProjectId(pub u64);
#[derive(Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize)] pub struct CommitId(pub u64);
#[derive(Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize)] pub struct UserId(pub u64);
#[derive(Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize)] pub struct PathId(pub u64);
#[derive(Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize)] pub struct SnapshotId(pub u64);

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

pub trait Identity: Copy + Clone + Hash + Eq + PartialEq + Ord + PartialOrd {}
impl Identity for ProjectId  {}
impl Identity for UserId     {}
impl Identity for CommitId   {}
impl Identity for PathId     {}
impl Identity for SnapshotId {}

/**== Objects ===================================================================================**/
#[derive(Clone, Serialize, Deserialize)] // TODO implement by hand
pub struct Project {
    pub id: ProjectId,
    pub url: String,
    pub last_update: i64,
    pub language: Option<String>,
    pub stars: Option<usize>,
    pub issues: Option<usize>,
    pub buggy_issues: Option<usize>,
    pub heads: Vec<(String, CommitId)>,
    pub metadata: HashMap<String, String>, // FIXME remove
}

impl Project {
    pub fn age(&self, data: DataPtr) -> Option<Seconds> { untangle_mut!(data).age_of(&self.id) }

    pub fn commits(&self, data: DataPtr) -> Vec<Commit> { untangle_mut!(data).commits_from(&self.id) }
    pub fn paths(&self, data: DataPtr) -> Vec<Path> { untangle_mut!(data).paths_from(&self.id) }
    pub fn users(&self, data: DataPtr) -> Vec<User> { untangle_mut!(data).users_from(&self.id) }

    pub fn commit_count(&self, data: DataPtr) -> usize { untangle_mut!(data).commit_count_from(&self.id) }
    pub fn path_count(&self, data: DataPtr) -> usize { untangle_mut!(data).path_count_from(&self.id) }
    pub fn user_count(&self, data: DataPtr) -> usize { untangle_mut!(data).user_count_from(&self.id) }

    pub fn all_issues(&self)  -> Option<usize> {
        match (self.issues, self.buggy_issues) {
            (None, None) => None,
            (Some(x), None) => Some(x),
            (None, Some(y)) => Some(y),
            (Some(x), Some(y)) => Some(x + y),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct User {
    pub id: UserId,
    pub email: String,
    pub name: String,
}

impl User {
    pub fn experience(&self, data: DataPtr) -> Option<Seconds> { untangle_mut!(data).experience_of(&self.id) }
    pub fn authored_commits(&self, data: DataPtr) -> Vec<Commit> { untangle_mut!(data).authored_commits_of(&self.id) }
    pub fn committed_commits(&self, data: DataPtr) -> Vec<Commit> { untangle_mut!(data).committed_commits_of(&self.id) }
}

#[derive(Clone, Serialize, Deserialize)] // TODO implement by hand
pub struct Commit {
    pub id: CommitId,
    pub hash: String,
    pub author: UserId,
    pub committer: UserId,
    pub author_time: i64,
    pub committer_time: i64,
    pub additions: Option<u64>,
    pub deletions: Option<u64>,
    pub parents: Vec<CommitId>,
}

impl Commit {
    pub fn message(&self, data: DataPtr) -> Option<Message> {
        untangle_mut!(data).message_of(&self.id)
    }
    pub fn paths(&self, data: DataPtr) -> Vec<Path> {
        untangle_mut!(data).paths_of(&self.id)
    }
    pub fn path_count(&self, data: DataPtr) -> usize {
        untangle_mut!(data).path_count_of(&self.id)
    }
    pub fn author(&self, data: DataPtr) -> Option<User> {
        untangle_mut!(data).user(&self.author).map(|u| u.clone())
    }
    pub fn committer(&self, data: DataPtr) -> Option<User> {
        untangle_mut!(data).user(&self.committer).map(|u| u.clone())
    }
    pub fn parents(&self, data: DataPtr) -> Vec<Commit> {
        self.parents.iter().flat_map(|id| {
            untangle_mut!(data.clone()).commit(id).map(|c| c.clone())
        }).collect()
    }
}

#[derive(Clone, Serialize, Deserialize)] // TODO implement by hand
pub struct Path {
    pub id: PathId,
    pub path: String,
}

impl Path {
    pub fn language(&self) -> Option<String> {
        std::path::Path::new(&self.path).extension().map(|extension| {
            match extension.to_str().unwrap() {
                "c"                                                     => Some("C"),
                "C" | ".cc" | "cpp" | "cxx" | "c++"                     => Some("C++"),
                "m" | "mm" | "M"                                        => Some("Objective-C"),
                "go"                                                    => Some("Go"),
                "java"                                                  => Some("Java"),
                "coffee" | "litcoffee"                                  => Some("Coffeescript"),
                "js" | "mjs"                                            => Some("Javascript"),
                "ts" | "tsx"                                            => Some("Typescript"),
                "rb"                                                    => Some("Ruby"),
                "php" | "phtml" | "php3" | "php4" | "php5" | "php7" | "phps" | "php-s" | "pht" | "phar"
                                                                        => Some("Php"),
                "py" | "pyi" | "pyc" | "pyd" | "pyo" | "pyw" | "pyz"    => Some("Python"),
                "plx" | "pl" | "pm" | "xs" | "t" | "pod"                => Some("Perl"),
                "clj" | "cljs" | "cljc" | "edn"                         => Some("Clojure"),
                "erl" | "hrl"                                           => Some("Erlang"),
                "hs" | "lhs"                                            => Some("Haskell"),
                "scala" | "sc"                                          => Some("Scala"),
                _                                                       => None,
            }.map(|s: &str| s.to_owned())
        }).flatten()
    }
}

#[derive(Clone, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize)] // TODO implement by hand
pub struct Message {
    pub contents: Vec<u8>,
}

/**== Objects convenience =======================================================================**/
impl Project {
    pub fn language_or_empty(&self) -> String {
        self.language.as_ref().map_or("", |s| s.as_str()).to_string()
    }

    pub fn stars_or_zero(&self) -> usize { self.stars.map_or(0usize, |n| n as usize) }
    pub fn all_issues_or_zero(&self) -> usize { self.issues_or_zero() + self.buggy_issues_or_zero() }
    pub fn issues_or_zero(&self) -> usize { self.issues.map_or(0usize, |n| n as usize) }
    pub fn buggy_issues_or_zero(&self) -> usize { self.buggy_issues.map_or(0usize, |n| n as usize) }

    fn filter_metadata(project: &dcd::Project) -> impl Iterator<Item=(String, String)> + '_ {
        project.metadata.iter()
            .filter(|(key, _)| {
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
                .filter(|(key, _)|
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
            hash: commit.hash.to_string(),
            parents: commit.parents.iter().map(|n| CommitId::from(n)).collect(),
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
            hash: commit.hash.to_string(),
            parents: commit.parents.iter().map(|n| CommitId::from(n)).collect(),
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

// Message as its length.
//impl Into<String> for Message  { fn into(self) -> String { self.contents.to_string() } }
// impl Into<usize>  for Message  { fn into(self) -> usize { self.contents.len() } }
// impl Into<usize>  for &Message { fn into(self) -> usize { self.contents.len() } }
// impl Into<u64>    for Message  { fn into(self) -> u64   { self.contents.len() as u64 } }
// impl Into<u64>    for &Message { fn into(self) -> u64   { self.contents.len() as u64 } }
// impl Into<f64>    for Message  { fn into(self) -> f64   { self.contents.len() as f64 } }
// impl Into<f64>    for &Message { fn into(self) -> f64   { self.contents.len() as f64 } }

/** ==== Convenience functions for dealing with two different user types in commits ============ **/
pub trait Roster {
    fn user_ids(&self) -> Vec<UserId>;
    fn users(&self, data: DataPtr) -> Vec<User> {
        self.user_ids().iter().flat_map(|e|
            untangle_mut!(data).user(e).map(|u| u.clone())
        ).collect()
    }
}

impl Roster for Commit {
    fn user_ids(&self) -> Vec<UserId> {
        if self.author == self.committer { vec![self.author]                 }
        else                             { vec![self.author, self.committer] }
    }
}

impl Roster for Option<Commit> {
    fn user_ids(&self) -> Vec<UserId> {
        match self.as_ref() {
            Some(commit) => commit.user_ids(),
            None => Default::default(),
        }
    }
}

impl Roster for Option<&Commit> {
    fn user_ids(&self) -> Vec<UserId> {
        match self {
            &Some(commit) => commit.user_ids(),
            &None => Default::default(),
        }
    }
}

impl Roster for dcd::Commit {
    fn user_ids(&self) -> Vec<UserId> {
        if self.author_id == self.committer_id { vec![UserId::from(self.author_id)]    }
        else                                   { vec![UserId::from(self.author_id),
                                                      UserId::from(self.committer_id)] }
    }
}

impl Roster for Option<dcd::Commit> {
    fn user_ids(&self) -> Vec<UserId> {
        match self.as_ref() {
            Some(commit) => commit.user_ids(),
            None => Default::default(),
        }
    }
}

impl Roster for Option<&dcd::Commit> {
    fn user_ids(&self) -> Vec<UserId> {
        match self {
            &Some(commit) => commit.user_ids(),
            &None => Default::default(),
        }
    }
}

/** ==== Object-ID relationship indication ===================================================== **/
pub trait Identifiable<T> where T: Identity { fn id(&self) -> T; }
impl Identifiable<ProjectId> for Project { fn id(&self) -> ProjectId { self.id } }
impl Identifiable<CommitId>  for Commit  { fn id(&self) -> CommitId  { self.id } }
impl Identifiable<UserId>    for User    { fn id(&self) -> UserId    { self.id } }
impl Identifiable<PathId>    for Path    { fn id(&self) -> PathId    { self.id } }

/**== Object names ==============================================================================**/
pub trait NamedEntity {
    fn singular() -> &'static str;
    fn plural()   -> &'static str;
}
impl NamedEntity for Project {
    fn singular() -> &'static str { "project"  }
    fn plural()   -> &'static str { "projects" }
}
impl NamedEntity for Commit {
    fn singular() -> &'static str { "commit"  }
    fn plural()   -> &'static str { "commits" }
}
impl NamedEntity for User {
    fn singular() -> &'static str { "user"  }
    fn plural()   -> &'static str { "users" }
}
impl NamedEntity for Path {
    fn singular() -> &'static str { "path"  }
    fn plural()   -> &'static str { "paths" }
}
impl NamedEntity for Message {
    fn singular() -> &'static str { "message"  }
    fn plural()   -> &'static str { "messages" }
}
// impl NamedEntity for Snapshot {
//     fn singular() -> &'static str { "snapshot"  }
//     fn plural()   -> &'static str { "snapshots" }
// }