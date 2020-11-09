use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::cmp::Ordering;

use std::time::Duration;
use serde::{Serialize, Deserialize};

use crate::tuples::Pick;
use crate::data::Database;
//use crate::piracy::*;

#[derive(Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Debug)]
pub enum Language {
    C, Cpp, ObjectiveC, Go, Java, CoffeeScript, JavaScript, TypeScript, Ruby, Rust,
    PHP, Python, Perl, Clojure, Erlang, Haskell, Scala,
}

impl Language {
    pub fn from_str(string: &str) -> Option<Self> {
        match string.to_lowercase().as_str() {
            "c"   => Some(Language::C),
            "c++" => Some(Language::Cpp),
            "objective-c" | "objective c" | "objectivec" => Some(Language::ObjectiveC),
            "go" => Some(Language::Go),
            "java" => Some(Language::Java),
            "coffeescript" => Some(Language::CoffeeScript),
            "javascript" => Some(Language::JavaScript),
            "typescript" => Some(Language::TypeScript),
            "ruby" => Some(Language::Ruby),
            "rust" => Some(Language::Rust),
            "php" => Some(Language::PHP),
            "python" => Some(Language::Python),
            "perl" => Some(Language::Perl),
            "clojure" => Some(Language::Clojure),
            "erlang" => Some(Language::Erlang),
            "haskell" => Some(Language::Haskell),
            "scala" => Some(Language::Scala),
            _ => None,
        }
    }

    fn from_path(path: &str) -> Option<Self> {
        std::path::Path::new(path).extension().map(|extension| {
            extension.to_str().map(|extension| Language::from_extension(extension))
        }).flatten().flatten()
    }

    fn from_extension(extension: &str) -> Option<Self> {
        match extension {
            "c"                                                     => Some(Language::C),
            "C" | ".cc" | "cpp" | "cxx" | "c++"                     => Some(Language::Cpp),
            "m" | "mm" | "M"                                        => Some(Language::ObjectiveC),
            "go"                                                    => Some(Language::Go),
            "java"                                                  => Some(Language::Java),
            "coffee" | "litcoffee"                                  => Some(Language::CoffeeScript),
            "js" | "mjs"                                            => Some(Language::JavaScript),
            "ts" | "tsx"                                            => Some(Language::TypeScript),
            "rb"                                                    => Some(Language::Ruby),
            "rs"                                                    => Some(Language::Rust),
            "py" | "pyi" | "pyc" | "pyd" | "pyo" | "pyw" | "pyz"    => Some(Language::Python),
            "plx" | "pl" | "pm" | "xs" | "t" | "pod"                => Some(Language::Perl),
            "clj" | "cljs" | "cljc" | "edn"                         => Some(Language::Clojure),
            "erl" | "hrl"                                           => Some(Language::Erlang),
            "hs" | "lhs"                                            => Some(Language::Haskell),
            "scala" | "sc"                                          => Some(Language::Scala),
            "php" | "phtml" | "php3" | "php4" | "php5" |
            "php7" | "phps" | "php-s" | "pht" | "phar"              => Some(Language::PHP),
            _                                                       => None,
        }
    }
}

impl Display for Language {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let string = match self {
            Language::C => "C",
            Language::Cpp => "C++",
            Language::ObjectiveC => "Objective-C",
            Language::Go => "Go",
            Language::Java => "Java",
            Language::CoffeeScript => "CoffeeScript",
            Language::JavaScript => "JavaScript",
            Language::TypeScript => "TypeScript",
            Language::Ruby => "Ruby",
            Language::Rust => "Rust",
            Language::PHP => "PHP",
            Language::Python => "Python",
            Language::Perl => "Perl",
            Language::Clojure => "Clojure",
            Language::Erlang => "Erlang",
            Language::Haskell => "Haskell",
            Language::Scala => "Scala",
            //Language::Other(language) => language,
        };
        f.write_str(string)
    }
}

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

impl From<&u64>   for ProjectId  { fn from(n: &u64) -> Self { ProjectId(*n)  } }
impl From<&u64>   for CommitId   { fn from(n: &u64) -> Self { CommitId(*n)   } }
impl From<&u64>   for UserId     { fn from(n: &u64) -> Self { UserId(*n)     } }
impl From<&u64>   for PathId     { fn from(n: &u64) -> Self { PathId(*n)     } }
impl From<&u64>   for SnapshotId { fn from(n: &u64) -> Self { SnapshotId(*n) } }

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

pub trait Identity: Copy + Clone + Hash + Eq + PartialEq + Ord + PartialOrd + Display + Serialize /*+ WithNames*/ {}
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
pub trait Identifiable { type Identity: Identity; fn id(&self) -> Self::Identity; }
pub trait Reifiable<T> { fn reify(&self, store: &Database) -> T; }
impl<I, T> Reifiable<Vec<T>> for Vec<I> where I: Reifiable<T> {
    fn reify(&self, store: &Database) -> Vec<T> {
        self.iter().map(|e| e.reify(store)).collect()
    }
}
impl<Ia, Ib, Ta, Tb> Reifiable<(Ta, Tb)> for (Ia, Ib) where Ia: Reifiable<Ta>, Ib: Reifiable<Tb> {
    fn reify(&self, store: &Database) -> (Ta, Tb) {
        (self.0.reify(store), self.1.reify(store))
    }
}
impl<I, T> Reifiable<Option<T>> for Option<I> where I: Reifiable<T> {
    fn reify(&self, store: &Database) -> Option<T> {
        self.as_ref().map(|e| e.reify(store))
    }
}
// impl<I, T> Reified for T where I: Reifiable<T>, T: Identifiable<I> {
//     type From = I;
//     fn reified_from(&self) -> Self::From { self.id() }
// }

/**== Objects ===================================================================================**/
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Project {
    pub id: ProjectId,
    pub url: String,
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

impl Identifiable for Project { type Identity = ProjectId; fn id(&self) -> ProjectId { self.id } }

impl Project {
    pub fn new              (id: ProjectId, url: String) -> Self                            { Project { id, url }                            }
    pub fn url              (&self)                      -> &str                            { self.url.as_str()                              }

    pub fn timestamp        (&self,     _: &Database)    -> i64                             { unimplemented!()                               }
    pub fn issue_count      (&self, store: &Database)    -> Option<usize>                   { store.project_issues(&self.id)                 }
    pub fn buggy_issue_count(&self, store: &Database)    -> Option<usize>                   { store.project_buggy_issues(&self.id)           }

    pub fn is_fork          (&self, store: &Database)    -> Option<bool>                    { store.project_is_fork(&self.id)                }
    pub fn is_archived      (&self, store: &Database)    -> Option<bool>                    { store.project_is_archived(&self.id)            }
    pub fn is_disabled      (&self, store: &Database)    -> Option<bool>                    { store.project_is_disabled(&self.id)            }
    pub fn star_count       (&self, store: &Database)    -> Option<usize>                   { store.project_star_gazer_count(&self.id)       }
    pub fn watcher_count    (&self, store: &Database)    -> Option<usize>                   { store.project_watcher_count(&self.id)          }
    pub fn size             (&self, store: &Database)    -> Option<usize>                   { store.project_size(&self.id)                   }
    pub fn open_issue_count (&self, store: &Database)    -> Option<usize>                   { store.project_open_issue_count(&self.id)       }
    pub fn network_count    (&self, store: &Database)    -> Option<usize>                   { store.project_fork_count(&self.id)             }
    pub fn subscriber_count (&self, store: &Database)    -> Option<usize>                   { store.project_subscriber_count(&self.id)       }
    pub fn license          (&self, store: &Database)    -> Option<String>                  { store.project_license(&self.id)                }
    pub fn language         (&self, store: &Database)    -> Option<Language>                { store.project_language(&self.id)               }
    pub fn description      (&self, store: &Database)    -> Option<String>                  { store.project_description(&self.id)            }
    pub fn homepage         (&self, store: &Database)    -> Option<String>                  { store.project_homepage(&self.id)               }
    pub fn head_ids         (&self, store: &Database)    -> Option<Vec<(String, CommitId)>> { store.project_head_ids(&self.id)               }
    pub fn heads            (&self, store: &Database)    -> Option<Vec<(String, Commit)>>   { store.project_heads(&self.id)                  }
    pub fn commit_ids       (&self, store: &Database)    -> Option<Vec<CommitId>>           { store.project_commit_ids(&self.id)             }
    pub fn commits          (&self, store: &Database)    -> Option<Vec<Commit>>             { store.project_commits(&self.id)                }
    pub fn commit_count     (&self, store: &Database)    -> Option<usize>                   { store.project_commit_count(&self.id)           }
    pub fn author_ids       (&self, store: &Database)    -> Option<Vec<UserId>>             { store.project_author_ids(&self.id)             }
    pub fn authors          (&self, store: &Database)    -> Option<Vec<User>>               { store.project_authors(&self.id)                }
    pub fn author_count     (&self, store: &Database)    -> Option<usize>                   { store.project_author_count(&self.id)           }
    pub fn committer_ids    (&self, store: &Database)    -> Option<Vec<UserId>>             { store.project_committer_ids(&self.id)          }
    pub fn committers       (&self, store: &Database)    -> Option<Vec<User>>               { store.project_committers(&self.id)             }
    pub fn committer_count  (&self, store: &Database)    -> Option<usize>                   { store.project_committer_count(&self.id)        }
    pub fn user_ids         (&self, store: &Database)    -> Option<Vec<UserId>>             { store.project_user_ids(&self.id)               }
    pub fn users            (&self, store: &Database)    -> Option<Vec<User>>               { store.project_users(&self.id)                  }
    pub fn user_count       (&self, store: &Database)    -> Option<usize>                   { store.project_user_count(&self.id)             }
    pub fn lifetime         (&self, store: &Database)    -> Option<Duration>                { store.project_lifetime(&self.id)               }
    pub fn has_issues       (&self, store: &Database)    -> Option<bool>                    { store.project_has_issues(&self.id)             }
    pub fn has_downloads    (&self, store: &Database)    -> Option<bool>                    { store.project_has_downloads(&self.id)          }
    pub fn has_wiki         (&self, store: &Database)    -> Option<bool>                    { store.project_has_wiki(&self.id)               }
    pub fn has_pages        (&self, store: &Database)    -> Option<bool>                    { store.project_has_pages(&self.id)              }
    pub fn created          (&self, store: &Database)    -> Option<i64>                     { store.project_created(&self.id)                }
    pub fn updated          (&self, store: &Database)    -> Option<i64>                     { store.project_updated(&self.id)                }
    pub fn pushed           (&self, store: &Database)    -> Option<i64>                     { store.project_pushed(&self.id)                 }
    pub fn master_branch    (&self, store: &Database)    -> Option<String>                  { store.project_master(&self.id)                 }

    // TODO project commit frequency
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct User { pub(crate) id: UserId, /*pub(crate) name: String,*/ pub(crate) email: String }
impl User {
    pub fn new                   (id: UserId, email: String) -> Self                  { User { id, email }                                 }
    pub fn email                 (&self)                     -> &str                  { self.email.as_str()                                }
    pub fn authored_commit_ids   (&self, store: &Database)   -> Option<Vec<CommitId>> { store.user_authored_commit_ids(&self.id)           }
    pub fn authored_commits      (&self, store: &Database)   -> Option<Vec<Commit>>   { store.user_authored_commits(&self.id)              }
    pub fn authored_commit_count (&self, store: &Database)   -> Option<usize>         { store.user_authored_commit_count(&self.id)         }
    pub fn committed_commit_ids  (&self, store: &Database)   -> Option<Vec<CommitId>> { store.user_committed_commit_ids(&self.id)          }
    pub fn committed_commits     (&self, store: &Database)   -> Option<Vec<Commit>>   { store.user_committed_commits(&self.id)             }
    pub fn committed_commit_count(&self, store: &Database)   -> Option<usize>         { store.user_committed_commit_count(&self.id)        }
    pub fn committed_experience  (&self, store: &Database)   -> Option<Duration>      { store.user_committed_experience(&self.id)          }
    pub fn author_experience     (&self, store: &Database)   -> Option<Duration>      { store.user_author_experience(&self.id)             }
    pub fn experience            (&self, store: &Database)   -> Option<Duration>      { store.user_experience(&self.id)                    }
}
impl Identifiable for User { type Identity = UserId; fn id(&self) -> Self::Identity { self.id } }
impl Reifiable<User> for UserId { fn reify(&self, store: &Database) -> User { store.user(&self).unwrap().clone() } }
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
}

impl Commit {
    pub fn committer_id       (&self)                   -> UserId                             {  self.committer               }
    pub fn author_id          (&self)                   -> UserId                             {  self.author                  }
    pub fn parent_ids         (&self)                   -> &Vec<CommitId>                     { &self.parents                 }

    pub fn committer          (&self, store: &Database) -> User                               {  self.committer.reify(store)  }
    pub fn author             (&self, store: &Database) -> User                               {  self.author.reify(store)     }
    pub fn parents            (&self, store: &Database) -> Vec<Commit>                        {  self.parents.reify(store)    }

    pub fn hash               (&self, store: &Database) -> Option<String>                     {  store.commit_hash(&self.id)                        }
    pub fn message            (&self, store: &Database) -> Option<String>                     {  store.commit_message(&self.id)                     }

    pub fn author_timestamp   (&self, store: &Database) -> Option<i64>                        {  store.commit_author_timestamp(&self.id)            }
    pub fn committer_timestamp(&self, store: &Database) -> Option<i64>                        {  store.commit_committer_timestamp(&self.id)         }

    pub fn change_ids          (&self, store: &Database) -> Option<Vec<(PathId, SnapshotId)>> {  store.commit_change_ids(&self.id)                  }
    pub fn changed_path_ids    (&self, store: &Database) -> Option<Vec<PathId>>               {  store.commit_change_ids(&self.id).left()           }
    pub fn changed_snapshot_ids(&self, store: &Database) -> Option<Vec<SnapshotId>>           {  store.commit_change_ids(&self.id).right()          }

    pub fn changes             (&self, store: &Database) -> Option<Vec<(Path, Snapshot)>>     {  store.commit_changes(&self.id)                     }
    pub fn changed_paths       (&self, store: &Database) -> Option<Vec<Path>>                 {  self.changed_path_ids(store).reify(store)          }
    pub fn changed_snapshots   (&self, store: &Database) -> Option<Vec<Snapshot>>             {  self.changed_snapshot_ids(store).reify(store)      }
}

impl Identifiable for Commit { type Identity = CommitId; fn id(&self) -> Self::Identity { self.id } }
impl Reifiable<Commit> for CommitId { fn reify(&self, store: &Database) -> Commit { store.commit(&self).unwrap().clone() } }
impl PartialEq for Commit {
    fn eq(&self, other: &Self) -> bool { self.id.eq(&other.id) }
}
impl PartialOrd for Commit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering>{ self.id.partial_cmp(&other.id) }
}
impl Eq for Commit {}
impl Ord for Commit {
    fn cmp(&self, other: &Self) -> Ordering { self.id.cmp(&other.id) }
}
impl Hash for Commit {
    fn hash<H: Hasher>(&self, state: &mut H) { self.id.hash(state) }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Path { id: PathId, location: String }
impl Path {
    pub fn new(id: PathId, location: String) -> Self { Path { id, location } }
    pub fn language(&self) -> Option<Language> { Language::from_path(self.location.as_str()) }
}
impl Identifiable for Path { type Identity = PathId; fn id(&self) -> Self::Identity { self.id } }
impl Reifiable<Path> for PathId { fn reify(&self, store: &Database) -> Path { store.path(&self).unwrap().clone() } }
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
pub struct Snapshot { id: SnapshotId, contents: Vec<u8> }
impl Snapshot {
    pub fn new(id: SnapshotId, contents: Vec<u8>) -> Self { Snapshot { id, contents } }
}
impl Identifiable for Snapshot { type Identity = SnapshotId; fn id(&self) -> Self::Identity { self.id } }
impl Reifiable<Snapshot> for SnapshotId { fn reify(&self, store: &Database) -> Snapshot {
    store.snapshot(&self).unwrap().clone() }
}
