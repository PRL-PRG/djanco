# Entities

These are directly retrievable from the database. Any query has to start with one of these:

- **Projects** (each is a repository identified by the user/project pair, most queries start here)
- **Commits**
- **User**
- **Snapshot** (each is the contents of a single file at a specific point in time, the same snapshot can occur in many repo, under many paths)
- **Paths** (each is a specific file location, eg. `"src/main.c"`, the same path can occur in many repos)

Each entity has a number of attributes. Some attributes are optional, which means that these are missing from data. Each entity has a unique ID that can be depended on for comparisons etc.

Rust type primer: 
- `bool` is Boolean
- `u64`, `u32`, `usize` are unsigned integers
- `i64`, `i32`, are signed integers
- `String`, `&str`, `Cow<str>` are strings
- `Vec<...>` is a collection/a vector of some type
- `Vec<u8>` is a byte array
- `Duration`, `Instant` are types related to dates, time and timestamps

Also our objects, for reference:
- `Language` is an enum that factorizes strings representing languages, full details below
- `Path`, `Snapshot`, `Commit`, `User`, `Project` are the aforementioned entities
- `PathId`, `SnapshotId`, `CommitId`, `UserId`, `ProjectId` are the IDs of those (wrapped `u64` for the curious)

## Project attributes

- **id**  (type: `ProjectId`)
- **url** (type: `String`)
- **is_fork** (type: `bool`, optional)
- **is_archived** (type: `bool`, optional)
- **is_disabled** (type: `bool`, optional)
- **star_count** (type: `usize`, optional)
- **watcher_count** (type: `usize`, optional)
- **size** (type: `usize`, optional)
- **open_issue_count** (type: `usize`, optional)
- **fork_count** (type: `usize`, optional)
- **subscriber_count** (type: `usize`, optional)
- **license** (type: `String`, optional)
- **language** (type: `Language`, optional)
- **description** (type: `String`, optional)
- **homepage** (type: `String`, optional)
- **heads** (type: `Vec<(CommitId, String)>`, optional)
- **head_count** (type: `usize`, optional)
- **commit_ids** (type: `Vec<CommitId>`, optional)
- **commits** (type: `Vec<Commit>`, optional)
- **commit_count** (type: `usize`, optional)
- **author_ids** (type: `Vec<UserId>`, optional)
- **authors** (type: `Vec<User>`, optional)
- **author_count** (type: `usize`, optional)
- **path_ids** (type: `Vec<PathId>`, optional)
- **paths** (type: `Vec<Path>`, optional)
- **path_count** (type: `usize`, optional)
- **snapshot_ids** (type: `Vec<SnapshotId>`, optional)
- **snapshots** (type: `Vec<Snapshot>`, optional)
- **snapshot_count** (type: `usize`, optional)
- **committer_ids** (type: `Vec<UserId>`, optional)
- **committers** (type: `Vec<User>`, optional)
- **committer_count** (type: `usize`, optional)
- **user_ids** (type: `Vec<UserId>`, optional)
- **users** (type: `Vec<User>`, optional)
- **user_count** (type: `usize`, optional)
- **lifetime** (type: `Duration`, optional)
- **has_issues** (type: `bool`, optional)
- **has_downloads** (type: `bool`, optional)
- **has_wiki** (type: `bool`, optional)
- **has_pages** (type: `bool`, optional)
- **created** (type: `i64`, optional)
- **updated** (type: `i64`, optional)
- **pushed** (type: `i64`, optional)
- **default_branch** (type: `String`, optional)

## User attributes

- **id**  (type: `UserId`)
- **email** (type: `String`)
- **authored_commit_ids** (type: `Vec<CommitId>`, optional)
- **authored_commits** (type: `Vec<Commit>`, optional)
- **authored_commit_count** (type: `usize`, optional)
- **committed_commit_ids** (type: `Vec<CommitId>`, optional)
- **committed_commits** (type: `Vec<Commit>`, optional)
- **committed_commit_count** (type: `usize`, optional)
- **committer_experience** (type: `Duration`, optional)
- **author_experience** (type: `Duration`, optional)
- **experience** (type: `Duration`, optional)

## Commit attributes

- **id**  (type: `CommitId`)
- **committer_id** (type: `UserId`)
- **author_id** (type: `UserId`)
- **parent_ids** (type: `&Vec<CommitId>`)
- **parent_count** (type: `usize`)

- **committer** (type: `User`, optional)
- **author** (type: `User`, optional)
- **parents** (type: `Vec<Commit>`, optional)

- **hash** (type: `String`, optional)
- **message** (type: `String`, optional)
- **message_length** (type: `usize`, optional)

- **author_timestamp** (type: `i64`, optional)
- **committer_timestamp** (type: `i64`, optional)

- **change_ids** (type: `Vec<(PathId, SnapshotId)>`, optional)
- **changed_path_ids** (type: `Vec<PathId>`, optional)
- **changed_snapshot_ids** (type: `Vec<SnapshotId>`, optional)

- **changed_paths** (type: `Vec<Path>`, optional)
- **changed_path_count** (type: `usize`, optional)
- **changed_snapshots** (type: `Vec<Snapshot>`, optional)
- **changed_snapshot_count** (type: `usize`, optional)

## Snapshot attributes

- **id**  (type: `SnapshotId`)
- **raw_contents** (type: `&Vec<u8>`)
- **contents** (type: `Cow<str>`)

## Path attributes

- **id**  (type: `PathId(u64)`)
- **new(id: PathId, location: String) -> Self { Path { id, location } }
- **location** (type: `String`)
- **language** (type: `Option<Language>`, yields `None` if language is not recognized (see below for details))

## Languages

We recognize the following languages:

- **C**
- **Cpp**
- **ObjectiveC**
- **Go**
- **Java**
- **CoffeeScript**
- **JavaScript**
- **TypeScript**
- **Ruby**
- **Rust**
- **PHP**
- **Python**
- **Perl**
- **Clojure**
- **Erlang**
- **Haskell**
- **Scala**

If a language is not on that list it will be listed as **Other**.

Languages are translated from file extensions as follows. In case of C and C++, it's worth noting how headers are treated.

```
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
"php" | "phtml" | "php3" | "php4" | "php5" |
"php7" | "phps" | "php-s" | "pht" | "phar"              => Some(Language::PHP),
_                                                       => None,
```