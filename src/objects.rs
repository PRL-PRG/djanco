use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::cmp::Ordering;
use std::borrow::Cow;
use std::io::Write;
use std::fs::{File, create_dir_all};
use std::path::PathBuf;

use bstr::ByteSlice;
use itertools::Itertools;
use serde::{Serialize, Deserialize};

use crate::data::Database;
use crate::time::Duration;
use crate::iterators::*;
use crate::weights_and_measures::Weighed;
use crate::Timestamp;

#[derive(Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Debug)]
pub enum Language {
    ASM, ASP, ActionScript, C, Cpp, CSharp, CoffeeScript, Lisp, Cobol, CSS, Clojure, D, Eiffel,
    Elixir, Elm, Erlang, FSharp, Fortran, Go, Groovy, HTML, Haskell, Java, JavaScript, Julia,
    Kotlin, Lua, ObjectiveC, OCaml, PHP, Pascal, Python, Perl, /*Prolog,*/ R, Racket, Ruby, Rust,
    Scala, SQL, Scheme, Swift, TypeScript, VisualBasic
}

// HTML, CSS, Jupyter Notebook, Shell, Rich Text Format, Dart, R, Makefile, Vue, TeX, Vim script, Meson, Roff, CMake, Smarty, MATLAB, Elixir, Julia, F#,
impl Language {
    pub fn from_str(string: &str) -> Option<Self> {
        match string.to_lowercase().as_str() {
            "asm" | "assembly" => Some(Language::ASM),
            "asp" | "classic asp" => Some(Language::ASP),
            "actionscript" => Some(Language::ActionScript),
            "c"   => Some(Language::C),
            "c++" => Some(Language::Cpp),
            "c#" => Some(Language::CSharp),
            "coffeescript" => Some(Language::CoffeeScript),
            "common lisp" | "lisp" => Some(Language::Lisp),
            "cobol" => Some(Language::Cobol),
            "css" => Some(Language::CSS),
            "clojure" => Some(Language::Clojure),
            "d" => Some(Language::D),
            "eiffel" => Some(Language::Eiffel),
            "elixir" => Some(Language::Elixir),
            "elm" => Some(Language::Elm),
            "erlang" => Some(Language::Erlang),
            "f#" => Some(Language::FSharp),
            "fortran" => Some(Language::Fortran),
            "go" => Some(Language::Go),
            "groovy" => Some(Language::Groovy),
            "html" => Some(Language::HTML),
            "haskell" => Some(Language::Haskell),
            "java" => Some(Language::Java),
            "javascript" => Some(Language::JavaScript),
            "julia" => Some(Language::Julia),
            "kotlin" => Some(Language::Kotlin),
            "lua" => Some(Language::Lua),
            "objective-c" | "objective c" | "objectivec" => Some(Language::ObjectiveC),
            "ocaml" => Some(Language::OCaml),
            "php" => Some(Language::PHP),
            "pascal" => Some(Language::Pascal),
            "python" => Some(Language::Python),
            "perl" | "perl 6" | "perl6" => Some(Language::Perl),
            //"prolog" => Some(Language::Prolog),
            "r" => Some(Language::R),
            "racket" => Some(Language::Racket),
            "ruby" => Some(Language::Ruby),
            "rust" => Some(Language::Rust),
            "scala" => Some(Language::Scala),
            "sql" | "sqlpl" => Some(Language::SQL),
            "scheme" => Some(Language::Scheme),
            "swift" => Some(Language::Swift),
            "typescript" => Some(Language::TypeScript),
            "visual basic" | "visual basic .net" => Some(Language::VisualBasic),
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
            "c" | "h"                                               => Some(Language::C),
            "C" | "cc" | "cpp" | "cxx" | "c++" | "hpp"              => Some(Language::Cpp),
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
            "r" | "rscript"                                         => Some(Language::R),
            "php" | "phtml" | "php3" | "php4" | "php5" |
            "php7" | "phps" | "php-s" | "pht" | "phar"              => Some(Language::PHP),
            "vb"                                                    => Some(Language::VisualBasic),
            // also: bas, cls, ctl, ctx, dca, ddf,dep, dob, dox, dsr, dsx, dws, frm, frx, log, oca,
            //       pag, pgx, res, tbl, vbg, vbl, vbp, vbr, vbw, vbz, wct, apparently
            "swift"                                                 => Some(Language::Swift),
            "scm" | "ss" | "sls" | "sps" | "sld"                    => Some(Language::Scheme),
            "rkt"                                                   => Some(Language::Racket),
            "sql" | "pls" | "pks"                                   => Some(Language::SQL),
            //"pl" | "pro"                                            => Some(Language::Prolog), // conflict
            "pp" | "pas" | "inc"                                    => Some(Language::Pascal),
            "ml" | "mli"                                            => Some(Language::OCaml),
            "lua"                                                   => Some(Language::Lua),
            "kt" | "kts"                                            => Some(Language::Kotlin),
            "jl"                                                    => Some(Language::Julia),
            "html" | "htm"                                          => Some(Language::HTML),
            "groovy" | "gvy" | "gy" | "gsh"                         => Some(Language::Groovy),
            "f90" | "for" | "f"                                     => Some(Language::Fortran),
            "fs" | "fsi"                                            => Some(Language::FSharp),
            "elm"                                                   => Some(Language::Elm),
            "ex" | "exs"                                            => Some(Language::Elixir),
            "e"                                                     => Some(Language::Eiffel),
            "d"                                                     => Some(Language::D),
            "css"                                                   => Some(Language::CSS),
            "cbl" | "cob" | "cpy"                                   => Some(Language::Cobol),
            "lisp" | "lsp" | "l" | "cl" | "fasl"                    => Some(Language::Lisp),
            "cs"                                                    => Some(Language::CSharp),
            "as" | "swf"                                            => Some(Language::ActionScript),
            "asp"                                                   => Some(Language::ASP),
            "asm" | "s"                                             => Some(Language::ASM),
            _                                                       => None,
        }
    }
}

impl Display for Language {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let string = match self {
            Language::ASM => "Assembly",
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
            Language::ASP => "ASP",
            Language::ActionScript => "ActionScript",
            Language::CSharp => "C#",
            Language::Lisp => "LISP",
            Language::Cobol => "Cobol",
            Language::CSS => "CSS",
            Language::D => "D",
            Language::Eiffel => "Eiffel",
            Language::Elixir => "Elixir",
            Language::Elm => "Elm",
            Language::FSharp => "F#",
            Language::Fortran => "Fortran",
            Language::Groovy => "Groovy",
            Language::HTML => "HTML",
            Language::Julia => "Julia",
            Language::Kotlin => "Kotlin",
            Language::Lua => "Lua",
            Language::OCaml => "OCaml",
            Language::Pascal => "Pascal",
            //Language::Prolog => "Prolog",
            Language::R => "R",
            Language::Racket => "Racket",
            Language::SQL => "SQL",
            Language::Scheme => "Scheme",
            Language::Swift => "Swift",
            Language::VisualBasic => "Visual Basic"
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

//impl Into<usize> for ProjectId  { fn into(self) -> usize { self.0 as usize } }
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

impl From<parasite::ProjectId> for ProjectId { fn from(id: parasite::ProjectId) -> Self { ProjectId(id.into()) } }
impl From<parasite::CommitId> for CommitId { fn from(id: parasite::CommitId) -> Self { CommitId(id.into()) } }
impl From<parasite::UserId> for UserId { fn from(id: parasite::UserId) -> Self { UserId(id.into()) } }
impl From<parasite::PathId> for PathId { fn from(id: parasite::PathId) -> Self { PathId(id.into()) } }
//impl From<parasite::Id> for SnapshotId { fn from(id: parasite::Id) -> Self { SnapshotId(id.into()) } }

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

pub trait Identity: Copy + Clone + Hash + Eq + PartialEq + Ord + PartialOrd + Display + Serialize + Weighed /*+ WithNames*/ {}
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

impl Identifiable for Project {
    type Identity = ProjectId;
    fn id(&self) -> ProjectId { self.id }
}



impl Project {
    pub fn new              (id: ProjectId, url: String) -> Self                            { Project { id, url }                            }
    pub fn url              (&self)                      -> String                          { self.url.to_string()                           }

    pub fn timestamp        (&self,     _: &Database)    -> Timestamp                             { unimplemented!()  /* waiting for parasite */   }
    pub fn issue_count      (&self, store: &Database)    -> Option<usize>                   { store.project_issues(&self.id)                 }
    pub fn buggy_issue_count(&self, store: &Database)    -> Option<usize>                   { store.project_buggy_issues(&self.id)           }

    pub fn is_fork          (&self, store: &Database)    -> Option<bool>                    { store.project_is_fork(&self.id)                }
    pub fn is_archived      (&self, store: &Database)    -> Option<bool>                    { store.project_is_archived(&self.id)            }
    pub fn is_disabled      (&self, store: &Database)    -> Option<bool>                    { store.project_is_disabled(&self.id)            }
    pub fn star_count       (&self, store: &Database)    -> Option<usize>                   { store.project_star_gazer_count(&self.id)       }
    pub fn watcher_count    (&self, store: &Database)    -> Option<usize>                   { store.project_watcher_count(&self.id)          }
    pub fn size             (&self, store: &Database)    -> Option<usize>                   { store.project_size(&self.id)                   }
    pub fn open_issue_count (&self, store: &Database)    -> Option<usize>                   { store.project_open_issue_count(&self.id)       }
    pub fn fork_count       (&self, store: &Database)    -> Option<usize>                   { store.project_fork_count(&self.id)             }
    pub fn subscriber_count (&self, store: &Database)    -> Option<usize>                   { store.project_subscriber_count(&self.id)       }
    pub fn license          (&self, store: &Database)    -> Option<String>                  { store.project_license(&self.id)                }
    pub fn language         (&self, store: &Database)    -> Option<Language>                { store.project_language(&self.id)               }
    pub fn description      (&self, store: &Database)    -> Option<String>                  { store.project_description(&self.id)            }
    pub fn homepage         (&self, store: &Database)    -> Option<String>                  { store.project_homepage(&self.id)               }
    //pub fn head_ids         (&self, store: &Database)    -> Option<Vec<(String, CommitId)>> { store.project_head_ids(&self.id)               }
    pub fn heads            (&self, store: &Database)    -> Option<Vec<Head>>               { store.project_heads(&self.id)                  }
    pub fn head_count       (&self, store: &Database)    -> Option<usize>                   { self.heads(store).map(|v| v.len())             }
    pub fn commit_ids       (&self, store: &Database)    -> Option<Vec<CommitId>>           { store.project_commit_ids(&self.id)             }
    pub fn commits          (&self, store: &Database)    -> Option<Vec<Commit>>             { store.project_commits(&self.id)                }
    pub fn commit_count     (&self, store: &Database)    -> Option<usize>                   { store.project_commit_count(&self.id)           }
    pub fn author_ids       (&self, store: &Database)    -> Option<Vec<UserId>>             { store.project_author_ids(&self.id)             }
    pub fn authors          (&self, store: &Database)    -> Option<Vec<User>>               { store.project_authors(&self.id)                }
    pub fn author_count     (&self, store: &Database)    -> Option<usize>                   { store.project_author_count(&self.id)           }
    pub fn path_ids         (&self, store: &Database)    -> Option<Vec<PathId>>             { store.project_path_ids(&self.id)               }
    pub fn paths            (&self, store: &Database)    -> Option<Vec<Path>>               { store.project_paths(&self.id)                  }
    pub fn path_count       (&self, store: &Database)    -> Option<usize>                   { store.project_path_count(&self.id)             }
    pub fn snapshot_ids     (&self, store: &Database)    -> Option<Vec<SnapshotId>>         { store.project_snapshot_ids(&self.id)           }
    pub fn snapshots        (&self, store: &Database)    -> Option<Vec<Snapshot>>           { store.project_snapshots(&self.id)              }
    pub fn snapshot_count   (&self, store: &Database)    -> Option<usize>                   { store.project_snapshot_count(&self.id)         }
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
    pub fn created          (&self, store: &Database)    -> Option<Timestamp>                     { store.project_created(&self.id)                }
    pub fn updated          (&self, store: &Database)    -> Option<Timestamp>                     { store.project_updated(&self.id)                }
    pub fn pushed           (&self, store: &Database)    -> Option<Timestamp>                     { store.project_pushed(&self.id)                 }
    pub fn default_branch   (&self, store: &Database)    -> Option<String>                  { store.project_master(&self.id)                 }
    // TODO project commit frequency
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Head {
    pub(crate) name: String,
    pub(crate) commit: CommitId,
}

impl Head {
    pub fn new(name: String, commit: CommitId) -> Self { Head { name, commit } }
    pub fn name(&self) -> String { self.name.to_string() }
    pub fn commit_id(&self) -> CommitId { self.commit.clone() }
    pub fn commit(&self, store: &Database) -> Option<Commit> { store.commit(&self.commit) }
}

impl From<(CommitId, String)> for Head {
    fn from((commit_id, name): (CommitId, String)) -> Self {
        Head::new(name, commit_id)
    }
}

impl From<(String, CommitId)> for Head {
    fn from((name, commit_id): (String, CommitId)) -> Self {
        Head::new(name, commit_id)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Change {
    pub(crate) path: PathId,
    //pub(crate) hash: u64, // TODO could change into HeadId
    pub(crate) snapshot: Option<SnapshotId>,
}

impl Change {
    pub fn new(path: PathId, /*hash: u64,*/ snapshot: Option<SnapshotId>) -> Self {
        Change { path, snapshot }
    }
    pub fn snapshot_id(&self) -> Option<SnapshotId> {
        self.snapshot.clone()
    }
    pub fn path_id(&self) -> PathId {
        self.path.clone()
    }
    pub fn snapshot(&self, store: &Database) -> Option<Snapshot> { self.snapshot.map(|id| store.snapshot(&id)).flatten() }
    pub fn path(&self, store: &Database) -> Option<Path> { store.path(&self.path) }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct User { pub(crate) id: UserId, /*pub(crate) name: String,*/ pub(crate) email: String }
impl User {
    pub fn new                   (id: UserId, email: String) -> Self                  { User { id, email }                                 }
    pub fn email                 (&self)                     -> String                  { self.email.as_str().to_owned()                   }
    pub fn authored_commit_ids   (&self, store: &Database)   -> Option<Vec<CommitId>> { store.user_authored_commit_ids(&self.id)           }
    pub fn authored_commits      (&self, store: &Database)   -> Option<Vec<Commit>>   { store.user_authored_commits(&self.id)              }
    pub fn authored_commit_count (&self, store: &Database)   -> Option<usize>         { store.user_authored_commit_count(&self.id)         }
    pub fn committed_commit_ids  (&self, store: &Database)   -> Option<Vec<CommitId>> { store.user_committed_commit_ids(&self.id)          }
    pub fn committed_commits     (&self, store: &Database)   -> Option<Vec<Commit>>   { store.user_committed_commits(&self.id)             }
    pub fn committed_commit_count(&self, store: &Database)   -> Option<usize>         { store.user_committed_commit_count(&self.id)        }
    pub fn committer_experience  (&self, store: &Database)   -> Option<Duration>      { store.user_committed_experience(&self.id)          }
    pub fn author_experience     (&self, store: &Database)   -> Option<Duration>      { store.user_author_experience(&self.id)             }
    pub fn experience            (&self, store: &Database)   -> Option<Duration>      { store.user_experience(&self.id)                    }
}
impl Identifiable for User {
    type Identity = UserId;
    fn id(&self) -> Self::Identity { self.id }
}
impl Reifiable<User> for UserId {
    fn reify(&self, store: &Database) -> User { store.user(&self).unwrap().clone() }
}
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
    pub fn new(id: CommitId, committer: UserId, author: UserId, parents: Vec<CommitId>) -> Self {
        Commit { id, committer, author, parents }
    }
}

impl Commit {
    pub fn committer_id       (&self)                   -> UserId                             {  self.committer               }
    pub fn author_id          (&self)                   -> UserId                             {  self.author                  }
    pub fn parent_ids         (&self)                   -> &Vec<CommitId>                     { &self.parents                 }
    pub fn parent_count       (&self)                   -> usize                              {  self.parents.len()           }

    pub fn committer          (&self, store: &Database) -> Option<User>                       {  store.user(&self.committer) }
    pub fn author             (&self, store: &Database) -> Option<User>                       {  store.user(&self.committer) }
    pub fn parents            (&self, store: &Database) -> Vec<Commit>                        {  self.parents.reify(store)    }

    pub fn hash               (&self, store: &Database) -> Option<String>                     {  store.commit_hash(&self.id)                        }
    pub fn message            (&self, store: &Database) -> Option<String>                     {  store.commit_message(&self.id)                     }
    pub fn message_length     (&self, store: &Database) -> Option<usize>                      {  self.message(store).map(|s| s.len()) }

    pub fn author_timestamp   (&self, store: &Database) -> Option<Timestamp>                        {  store.commit_author_timestamp(&self.id)            }
    pub fn committer_timestamp(&self, store: &Database) -> Option<Timestamp>                        {  store.commit_committer_timestamp(&self.id)         }

    pub fn changes             (&self, store: &Database) -> Option<Vec<Change>>               {  store.commit_changes(&self.id)                     }
    pub fn changed_path_ids    (&self, store: &Database) -> Option<Vec<PathId>>               {  store.commit_changes(&self.id).map(|v| v.into_iter().map(|change| change.path_id()).unique().collect())     }
    pub fn changed_snapshot_ids(&self, store: &Database) -> Option<Vec<SnapshotId>>           {  store.commit_changes(&self.id).map(|v| v.into_iter().flat_map(|change| change.snapshot_id()).unique().collect())     }
    pub fn change_count        (&self, store: &Database) -> Option<usize>                     {  store.commit_change_count(&self.id)                }

    pub fn changed_paths       (&self, store: &Database) -> Option<Vec<Path>>                 {  store.commit_changed_paths(&self.id)               }
    pub fn changed_path_count  (&self, store: &Database) -> Option<usize>                     {  store.commit_changed_path_count(&self.id)          }
    pub fn changed_snapshots   (&self, store: &Database) -> Option<Vec<Snapshot>>             {  self.changed_snapshot_ids(store).reify(store)      }
    pub fn changed_snapshot_count (&self, store: &Database) -> Option<usize>                  {  self.changed_snapshot_ids(store).map(|v| v.len() ) }
}

impl Identifiable for Commit {
    type Identity = CommitId;
    fn id(&self) -> Self::Identity { self.id }
}
impl Reifiable<Commit> for CommitId {
    fn reify(&self, store: &Database) -> Commit { store.commit(&self).unwrap().clone() }
}
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
pub struct Path { pub(crate) id: PathId, pub(crate) location: String }
impl Path {
    pub fn new(id: PathId, location: String) -> Self { Path { id, location } }
    pub fn location(&self) -> String { self.location.to_string() }
    pub fn language(&self) -> Option<Language> { Language::from_path(self.location.as_str()) }
}
impl Identifiable for Path {
    type Identity = PathId;
    fn id(&self) -> Self::Identity { self.id }
}
impl Reifiable<Path> for PathId {
    fn reify(&self, store: &Database) -> Path { store.path(&self).unwrap().clone() }
}
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
pub struct Snapshot { pub(crate) id: SnapshotId, pub(crate) contents: Vec<u8> }
impl Snapshot {
    pub fn new(id: SnapshotId, contents: Vec<u8>) -> Self {
        if contents.is_empty() {
            eprintln!("WARNING: constructing snapshot id={} from empty contents", id);
        }
        Snapshot { id, contents }
    }
    pub fn raw_contents(&self) -> &Vec<u8> { &self.contents }
    pub fn raw_contents_owned(&self) -> Vec<u8> { self.contents.clone() }
    //pub fn id(&self) -> SnapshotId { self.id.clone() }
    pub fn contents(&self) -> Cow<str> { self.contents.to_str_lossy() }
    pub fn contents_owned(&self) -> String { self.contents.to_str_lossy().to_string() }
    pub fn contains(&self, needle: &str) -> bool { self.contents().contains(needle) }
    pub fn write_contents_to<'a, S>(&self, path: S) -> Result<(), std::io::Error> where S: Into<PathBuf> {
        let path = path.into();
        let dir = {
            let mut dir = path.clone();
            dir.pop();
            dir
        };
        create_dir_all(dir)?;
        let mut file = File::create(path)?;
        self.write_contents_to_file(&mut file)
    }
    pub fn write_contents_to_file<F>(&self, file: &mut F) -> Result<(), std::io::Error> where F: Write {
        file.write_all(self.contents.as_slice())
    }
    // FIXME add hashes
}
impl Identifiable for Snapshot {
    type Identity = SnapshotId;
    fn id(&self) -> Self::Identity { self.id }
}
impl Reifiable<Snapshot> for SnapshotId { fn reify(&self, store: &Database) -> Snapshot {
    store.snapshot(&self).unwrap().clone() }
}

pub trait ItemWithoutData where Self: Sized {
    fn attach_data<'a>(self, data: &'a Database) -> ItemWithData<'a, Self>;
}
impl<T> ItemWithoutData for T where T: Sized {
    fn attach_data<'a>(self, data: &'a Database) -> ItemWithData<'a, Self> {
        ItemWithData { data, item: self }
    }
}

pub trait OptionWithoutData<T> where T: ItemWithoutData {
    fn attach_data_to_inner<'a>(self, data: &'a Database) -> Option<ItemWithData<'a, T>>;
}
impl<T> OptionWithoutData<T> for Option<T> where T: ItemWithoutData {
    fn attach_data_to_inner<'a>(self, data: &'a Database) -> Option<ItemWithData<'a, T>> {
        self.map(|inner| inner.attach_data(data) )
    }
}

pub struct ItemWithData<'a, T> { pub data: &'a Database, pub item: T }
impl<'a, T> ItemWithData<'a, T> {
    pub fn new(data: &'a Database, item: T) -> Self {
        ItemWithData { data, item }
    }
    //pub fn as_ref(&self) -> ItemWithData<&T> { Self::new(self.data, self.item) }
    pub fn rewrap<Tb>(&self, object: Tb) -> ItemWithData<Tb> {
        ItemWithData::<Tb>::new(self.data, object)
    }
}
impl<'a, T> Clone for ItemWithData<'a, T> where T: Clone {
    fn clone(&self) -> Self {
        ItemWithData::new(self.data, self.item.clone())
    }
}

impl<'a, T> PartialEq for ItemWithData<'a, T> where T: PartialEq {
    fn eq(&self, other: &Self) -> bool {
        self.item.eq(&other.item)
    }
}

impl<'a, T> Eq for ItemWithData<'a, T> where ItemWithData<'a, T>: PartialEq, T: Eq {}

impl<'a, T> PartialOrd for ItemWithData<'a, T> where T: PartialOrd {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.item.partial_cmp(&other.item)
    }
}

impl<'a, T> Ord for ItemWithData<'a, T> where T: Ord, ItemWithData<'a, T>: Eq {
    fn cmp(&self, other: &Self) -> Ordering {
        self.item.cmp(&other.item)
    }
}

impl<'a, T> Hash for ItemWithData<'a, T> where T: Hash {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.item.hash(state)
    }
}

impl<'a> Into<Project> for ItemWithData<'a, Project> { fn into(self) -> Project { self.item } }
impl<'a> Into<Commit> for ItemWithData<'a, Commit> { fn into(self) -> Commit { self.item } }
impl<'a> Into<User> for ItemWithData<'a, User> { fn into(self) -> User { self.item } }
impl<'a> Into<Path> for ItemWithData<'a, Path> { fn into(self) -> Path { self.item } }
impl<'a> Into<Snapshot> for ItemWithData<'a, Snapshot> { fn into(self) -> Snapshot { self.item } }
impl<'a> Into<Head> for ItemWithData<'a, Head> { fn into(self) -> Head { self.item } }

impl<'a> Into<ProjectId> for ItemWithData<'a, ProjectId> { fn into(self) -> ProjectId { self.item } }
impl<'a> Into<CommitId> for ItemWithData<'a, CommitId> { fn into(self) -> CommitId { self.item } }
impl<'a> Into<UserId> for ItemWithData<'a, UserId> { fn into(self) -> UserId { self.item } }
impl<'a> Into<PathId> for ItemWithData<'a, PathId> { fn into(self) -> PathId { self.item } }
impl<'a> Into<SnapshotId> for ItemWithData<'a, SnapshotId> { fn into(self) -> SnapshotId { self.item } }

impl<'a> Into<String> for ItemWithData<'a, String> { fn into(self) -> String { self.item } }
impl<'a> Into<u64> for ItemWithData<'a, u64> { fn into(self) -> u64 { self.item } }
impl<'a> Into<u32> for ItemWithData<'a, u32> { fn into(self) -> u32 { self.item } }
impl<'a> Into<i64> for ItemWithData<'a, i64> { fn into(self) -> i64 { self.item } }
impl<'a> Into<i32> for ItemWithData<'a, i32> { fn into(self) -> i32 { self.item } }
impl<'a> Into<f64> for ItemWithData<'a, f64> { fn into(self) -> f64 { self.item } }
impl<'a> Into<f32> for ItemWithData<'a, f32> { fn into(self) -> f32 { self.item } }
impl<'a> Into<usize> for ItemWithData<'a, usize> { fn into(self) -> usize { self.item } }

impl<'a,A,B> Into<(A,B)> for ItemWithData<'a, (A,B)> { fn into(self) -> (A,B) { self.item } }

impl<'a> ItemWithData<'a, Project> {
    pub fn id               (&self)    -> ProjectId                       { self.item.id()                                     }
    pub fn url              (&self)    -> String                          { self.item.url().to_string()                        }
    pub fn issue_count      (&self)    -> Option<usize>                   { self.item.issue_count(&self.data)            }
    pub fn buggy_issue_count(&self)    -> Option<usize>                   { self.item.buggy_issue_count(&self.data)      }
    pub fn is_fork          (&self)    -> Option<bool>                    { self.item.is_fork(&self.data)                }
    pub fn is_archived      (&self)    -> Option<bool>                    { self.item.is_archived(&self.data)            }
    pub fn is_disabled      (&self)    -> Option<bool>                    { self.item.is_disabled(&self.data)            }
    pub fn star_count       (&self)    -> Option<usize>                   { self.item.star_count(&self.data)             }
    pub fn watcher_count    (&self)    -> Option<usize>                   { self.item.watcher_count(&self.data)          }
    pub fn size             (&self)    -> Option<usize>                   { self.item.size(&self.data)                   }
    pub fn open_issue_count (&self)    -> Option<usize>                   { self.item.open_issue_count(&self.data)       }
    pub fn fork_count       (&self)    -> Option<usize>                   { self.item.fork_count(&self.data)             }
    pub fn subscriber_count (&self)    -> Option<usize>                   { self.item.subscriber_count(&self.data)       }
    pub fn license          (&self)    -> Option<String>                  { self.item.license(&self.data)                }
    pub fn language         (&self)    -> Option<Language>                { self.item.language(&self.data)               }
    pub fn description      (&self)    -> Option<String>                  { self.item.description(&self.data)            }
    pub fn homepage         (&self)    -> Option<String>                  { self.item.homepage(&self.data)               }
    //pub fn head_ids         (&self)    -> Option<Vec<(String, CommitId)>> { self.item.head_ids(&self.data)               }
    pub fn heads            (&self)    -> Option<Vec<Head>>               { self.item.heads(&self.data)                  } // TODO test
    pub fn head_count       (&self)    -> Option<usize>                   { self.item.head_count(&self.data)             }
    pub fn commit_ids       (&self)    -> Option<Vec<CommitId>>           { self.item.commit_ids(&self.data)             } // TODO test
    pub fn commits          (&self)    -> Option<Vec<Commit>>             { self.item.commits(&self.data)                } // TODO test
    pub fn commit_count     (&self)    -> Option<usize>                   { self.item.commit_count(&self.data)           } // TODO test
    pub fn author_ids       (&self)    -> Option<Vec<UserId>>             { self.item.author_ids(&self.data)             } // TODO test
    pub fn authors          (&self)    -> Option<Vec<User>>               { self.item.authors(&self.data)                } // TODO test
    pub fn author_count     (&self)    -> Option<usize>                   { self.item.author_count(&self.data)           } // TODO test
    pub fn path_ids         (&self)    -> Option<Vec<PathId>>             { self.item.path_ids(&self.data)               } // TODO test
    pub fn paths            (&self)    -> Option<Vec<Path>>               { self.item.paths(&self.data)                  } // TODO test
    pub fn path_count       (&self)    -> Option<usize>                   { self.item.path_count(&self.data)             } // TODO test
    pub fn snapshot_ids     (&self)    -> Option<Vec<SnapshotId>>         { self.item.snapshot_ids(&self.data)           } // TODO test
    pub fn snapshots        (&self)    -> Option<Vec<Snapshot>>           { self.item.snapshots(&self.data)              } // TODO test
    pub fn snapshot_count   (&self)    -> Option<usize>                   { self.item.snapshot_count(&self.data)         } // TODO test
    pub fn committer_ids    (&self)    -> Option<Vec<UserId>>             { self.item.committer_ids(&self.data)          } // TODO test
    pub fn committers       (&self)    -> Option<Vec<User>>               { self.item.committers(&self.data)             } // TODO test
    pub fn committer_count  (&self)    -> Option<usize>                   { self.item.committer_count(&self.data)        } // TODO test
    pub fn user_ids         (&self)    -> Option<Vec<UserId>>             { self.item.user_ids(&self.data)               } // TODO test
    pub fn users            (&self)    -> Option<Vec<User>>               { self.item.users(&self.data)                  } // TODO test
    pub fn user_count       (&self)    -> Option<usize>                   { self.item.user_count(&self.data)             } // TODO test
    pub fn lifetime         (&self)    -> Option<Duration>                { self.item.lifetime(&self.data)               } // TODO test
    pub fn has_issues       (&self)    -> Option<bool>                    { self.item.has_issues(&self.data)             }
    pub fn has_downloads    (&self)    -> Option<bool>                    { self.item.has_downloads(&self.data)          }
    pub fn has_wiki         (&self)    -> Option<bool>                    { self.item.has_wiki(&self.data)               }
    pub fn has_pages        (&self)    -> Option<bool>                    { self.item.has_pages(&self.data)              }
    pub fn created          (&self)    -> Option<Timestamp>                     { self.item.created(&self.data)                }
    pub fn updated          (&self)    -> Option<Timestamp>                     { self.item.updated(&self.data)                }
    pub fn pushed           (&self)    -> Option<Timestamp>                     { self.item.pushed(&self.data)                 }
    pub fn default_branch   (&self)    -> Option<String>                  { self.item.default_branch(&self.data)         }

    pub fn commits_with_data<'b>(&'b self) -> Option<Vec<ItemWithData<'a, Commit>>> {
        self.item.commits(&self.data).attach_data_to_each(self.data)
    }
    pub fn authors_with_data<'b>(&'b self) -> Option<Vec<ItemWithData<'a, User>>> {
        self.item.authors(&self.data).attach_data_to_each(self.data)
    }
    pub fn committers_with_data<'b>(&'b self) -> Option<Vec<ItemWithData<'a, User>>> {
        self.item.committers(&self.data).attach_data_to_each(self.data)
    }
    pub fn users_with_data<'b>(&'b self) -> Option<Vec<ItemWithData<'a, User>>> {
        self.item.users(&self.data).attach_data_to_each(self.data)
    }
    pub fn snapshots_with_data<'b>(&'b self) -> Option<Vec<ItemWithData<'a, Snapshot>>> {
        self.item.snapshots(&self.data).attach_data_to_each(self.data)
    }
    pub fn heads_with_data<'b>(&'b self) -> Option<Vec<ItemWithData<'a, Head>>> {
        self.item.heads(&self.data).attach_data_to_each(self.data)
    }
    pub fn paths_with_data<'b>(&'b self) -> Option<Vec<ItemWithData<'a, Path>>> {
        self.item.paths(&self.data).attach_data_to_each(self.data)
    }
}
impl<'a> ItemWithData<'a, Snapshot> {
    pub fn raw_contents(&self) -> &Vec<u8> { self.item.raw_contents() }
    pub fn raw_contents_owned(&self) -> Vec<u8> { self.item.raw_contents_owned() }
    pub fn id(&self) -> SnapshotId { self.item.id() }
    pub fn contents(&self) -> Cow<str> { self.item.contents() }
    pub fn contents_owned(&self) -> String { self.item.contents_owned() }
    pub fn contains(&self, needle: &str) -> bool { self.item.contains(needle) }
}

impl<'a> ItemWithData<'a, User> {
    pub fn id                    (&self)   -> UserId                { self.item.id()    }
    pub fn email                 (&self)   -> String                { self.item.email() }
    pub fn authored_commit_ids   (&self)   -> Option<Vec<CommitId>> { self.item.authored_commit_ids(&self.data)    }
    pub fn authored_commits      (&self)   -> Option<Vec<Commit>>   { self.item.authored_commits(&self.data)       }
    pub fn authored_commit_count (&self)   -> Option<usize>         { self.item.authored_commit_count(&self.data)  }
    pub fn committed_commit_ids  (&self)   -> Option<Vec<CommitId>> { self.item.committed_commit_ids(&self.data)   }
    pub fn committed_commits     (&self)   -> Option<Vec<Commit>>   { self.item.committed_commits(&self.data)      }
    pub fn committed_commit_count(&self)   -> Option<usize>         { self.item.committed_commit_count(&self.data) }
    pub fn committer_experience  (&self)   -> Option<Duration>      { self.item.committer_experience(&self.data)   }
    pub fn author_experience     (&self)   -> Option<Duration>      { self.item.author_experience(&self.data)      }
    pub fn experience            (&self)   -> Option<Duration>      { self.item.experience(&self.data)             }

    pub fn authored_commits_with_data<'b>(&'b self) -> Option<Vec<ItemWithData<'a, Commit>>> {
        self.item.authored_commits(&self.data).attach_data_to_each(self.data)
    }

    pub fn committed_commits_with_data<'b>(&'b self) -> Option<Vec<ItemWithData<'a, Commit>>> {
        self.item.committed_commits(&self.data).attach_data_to_each(self.data)
    }
}

impl<'a> ItemWithData<'a, Commit> {
    pub fn id                 (&self) -> CommitId                           { self.item.id()           }
    pub fn committer_id       (&self) -> UserId                             { self.item.committer_id() }
    pub fn author_id          (&self) -> UserId                             { self.item.author_id()    }
    pub fn parent_ids         (&self) -> Vec<CommitId>                      { self.item.parent_ids().clone() }
    pub fn parent_count       (&self) -> usize                              { self.item.parent_count() }
    pub fn committer          (&self) -> Option<User>                       { self.item.committer(&self.data)            }
    pub fn author             (&self) -> Option<User>                       { self.item.author(self.data)                }
    pub fn parents            (&self) -> Vec<Commit>                        { self.item.parents(self.data)               }
    pub fn hash               (&self) -> Option<String>                     { self.item.hash(&self.data)                 }
    pub fn message            (&self) -> Option<String>                     { self.item.message(&self.data)              }
    pub fn message_length     (&self) -> Option<usize>                      { self.item.message_length(&self.data)       }
    pub fn author_timestamp   (&self) -> Option<Timestamp>                        { self.item.author_timestamp(&self.data)     }
    pub fn committer_timestamp(&self) -> Option<Timestamp>                        { self.item.committer_timestamp(&self.data)  }
    pub fn changes            (&self) -> Option<Vec<Change>>                { self.item.changes(&self.data)              }
    pub fn change_count       (&self) -> Option<usize>                      { self.item.change_count(&self.data)         }
    pub fn changed_path_ids    (&self) -> Option<Vec<PathId>>               { self.item.changed_path_ids(&self.data)     }
    pub fn changed_snapshot_ids(&self) -> Option<Vec<SnapshotId>>           { self.item.changed_snapshot_ids(&self.data) }
    pub fn changed_paths       (&self) -> Option<Vec<Path>>                 { self.item.changed_paths(&self.data)        }
    pub fn changed_path_count  (&self) -> Option<usize>                     { self.item.changed_path_count(&self.data)   }
    pub fn changed_snapshots   (&self) -> Option<Vec<Snapshot>>             { self.item.changed_snapshots(&self.data)    }
    pub fn changed_snapshot_count (&self) -> Option<usize>                  { self.item.changed_snapshot_count(&self.data) }

    pub fn author_with_data<'b>(&'b self) -> Option<ItemWithData<'a, User>> {
        self.item.author(self.data).attach_data_to_inner(self.data)
    }
    pub fn committer_with_data<'b>(&'b self) -> Option<ItemWithData<'a, User>> {
        self.item.committer(self.data).attach_data_to_inner(self.data)
    }
    pub fn parents_with_data<'b>(&'b self) -> Vec<ItemWithData<'a, Commit>> {
        self.item.parents(self.data).attach_data_to_each(self.data)
    }
    pub fn changes_with_data<'b>(&'b self) -> Option<Vec<ItemWithData<'a, Change>>> {
        self.item.changes(self.data).attach_data_to_each(self.data)
    }
    pub fn changed_paths_with_data<'b>(&'b self) -> Option<Vec<ItemWithData<'a, Path>>> {
        self.item.changed_paths(self.data).attach_data_to_each(self.data)
    }
    pub fn changed_snapshots_with_data<'b>(&'b self) -> Option<Vec<ItemWithData<'a, Snapshot>>> {
        self.item.changed_snapshots(self.data).attach_data_to_each(self.data)
    }

}
impl<'a> ItemWithData<'a, Path> {
    pub fn id      (&self) -> PathId           { self.item.id()       }
    pub fn location(&self) -> String           { self.item.location() }
    pub fn language(&self) -> Option<Language> { self.item.language() }
}

impl<'a> ItemWithData<'a, Head> {
    pub fn name(&self) -> String { self.item.name() }
    pub fn commit_id(&self) -> CommitId { self.item.commit_id() }
    pub fn commit(&self) -> Option<Commit> { self.item.commit(&self.data) }

    pub fn commit_with_data<'b> (&'b self) -> Option<ItemWithData<'a, Commit>> {
        self.item.commit(self.data).attach_data_to_inner(self.data)
    }
}


impl<'a> ItemWithData<'a, Change> {
    pub fn path_id(&self) -> PathId { self.item.path_id() }
    pub fn snapshot_id(&self) -> Option<SnapshotId> { self.item.snapshot_id() }
    pub fn path(&self) -> Option<Path> { self.item.path(&self.data) }
    pub fn snapshot(&self) -> Option<Snapshot> { self.item.snapshot(&self.data) }

    pub fn path_with_data<'b> (&'b self) -> Option<ItemWithData<'a, Path>> {
        self.item.path(self.data).attach_data_to_inner(self.data)
    }
    pub fn snapshot_with_data<'b> (&'b self) -> Option<ItemWithData<'a, Snapshot>> {
        self.item.snapshot(self.data).attach_data_to_inner(self.data)
    }
}