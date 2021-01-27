# Djanco

## Installation

```
cargo build
```

## Examples

Usage:

All of the examples share this commandline interface:

Examples:

```
cargo run --bin example1 -- --dataset=/mnt/data/dejacode/dataset --output=/mnt/data/dejacode/output --cache=/mnt/data/dejacode/cache
cargo run --bin example2 -- --dataset=/mnt/data/dejacode/dataset --output=/mnt/data/dejacode/output --cache=/mnt/data/dejacode/cache
cargo run --bin example3 -- --dataset=/mnt/data/dejacode/dataset --output=/mnt/data/dejacode/output --cache=/mnt/data/dejacode/cache
cargo run --bin example_queries -- --dataset=/mnt/data/dejacode/dataset --output=/mnt/data/dejacode/output --cache=/mnt/data/dejacode/cache
cargo run --bin paper_example -- --dataset=/mnt/data/dejacode/dataset --output=/mnt/data/dejacode/output --cache=/mnt/data/dejacode/cache
cargo run --bin dsl -- --dataset=/mnt/data/dejacode/dataset --output=/mnt/data/dejacode/output --cache=/mnt/data/dejacode/cache
```

All examples share the same commandline interface:

```
USAGE:
    cargo run --bin example1 -- [FLAGS] [OPTIONS] --dataset <DCD_PATH> --output <OUTPUT_PATH> --cache <PERSISTENT_CACHE_PATH>

FLAGS:
    -h, --help              Prints help information
    -V, --version           Prints version information

OPTIONS:  
    -d, --dataset <DCD_PATH>               
    -o, --output <OUTPUT_PATH>             
    -c, --cache <PERSISTENT_CACHE_PATH>  
``` 

## DSL (WIP)

### Entry points

- `projects`
- `commits`
- `snapshots`
- `users`
- `paths`

### Verbs

- `filter_by(Filter)`
- `map_into(Attribute)`
- `sort_by(Attribute)`
- `group_by(Attribute)`
- `ungroup`
- `sample(Sampler)`

### Samplers

- `Top(usize)`
- `Random(usize, Seed(u128))`
- `Distinct(Sampler, SimilarityCritertion)` 

There is currently only one similarity critetion:

- `MinRatio(Attribute, f64)` 

Here, `f64` is the expected minimum ratio of unique elements in `Attribute` between two different objects.

### Predicates

Numeric conditions:

- `LessThan(Attribute, Number)`
- `AtMost(Attribute, Number)`
- `Equal(Attribute, Number)`
- `AtLeast(Attribute, Number)`
- `MoreThan(Attribute, Number)`

Boolean conditions:

- `And(Attribute, Attrtibute)`
- `Or(Attribute, Attribute)`
- `Not(Attribute)`

Option conditions:

- `Exists(Attribute)`
- `Missing(Attribute)`

String comparisons:

- `Same(Attribute, str)`
- `Contains(Attribute, str)`
- `Matches(Attribute, Regex)` and there's a macro `regex!(str)` that compiles a string into a regular expression.

Collection conditions:

- `Member(Attribute, Collection)`
- `AllIn(Attribute, Collection)`
- `AnyIn(Attribute, Collection)`

### Statistical functions

- `Min(Attribute)`
- `Max(Attribute)`
- `MinMax(Attribute)`
- `Mean(Attribute)`
- `Median(Attribute)`

### Extracting attributes of attributes

- `From(Attribute, Attribute)`
- `FromEach(Attribute, Attribute)`
- `FromEachIf(Attribute, Attribute, Predicate)`
- `Select1(Attribute)`
- `Select2(Attribute, Attribute)`
- `Select3(Attribute, Attribute, Attribute)`
- `Select4(Attribute, Attribute, Attribute, ...)`
- `Select5(Attribute, Attribute, Attribute, ...)`
- `Select6(Attribute, Attribute, Attribute, ...)`
- `Select7(Attribute, Attribute, Attribute, ...)`
- `Select8(Attribute, Attribute, Attribute, ...)`
- `Select9(Attribute, Attribute, Attribute, ...)`
- `Select10(Attribute, Attribute, Attribute, ...)`

### Attributes

Project attributes:

  - `Id` -> `ProjectId`
  - `URL` -> `String`
  - `Issues` -> `usize` (currently panics)
  - `BuggyIssues` -> `usize` (currently panics)
  - `IsFork` -> `bool`
  - `IsArchived` -> `bool`
  - `IsDisabled` -> `bool`
  - `Stars` -> `usize`
  - `Watchers` -> `usize`
  - `Size` -> `usize`
  - `OpenIssues` -> `usize`
  - `Forks` -> `usize`
  - `Subscribers` -> `usize`
  - `License` -> `String`
  - `Language` -> `Language` enum
  - `Description` -> `String`
  - `Homepage` -> `String`
  - `HasIssues` -> `bool`
  - `HasDownloads` -> `bool`
  - `HasWiki` -> `bool`
  - `HasPages` -> `bool`
  - `Created` -> `i64` timestamp
  - `Updated` -> `i64` timestamp
  - `Pushed` -> `i64` timestamp
  - `DefaultBranch` -> `String`
  - `Age` -> `usize`
  - `Heads` -> `Vec<Head>` 
  - `CommitIds` -> `Vec<CommitId>`
  - `AuthorIds` -> `Vec<UserId>`
  - `CommitterIds` -> `Vec<UserId>`
  - `UserIds` -> `Vec<User>`
  - `PathIds` -> `Vec<PathId>`
  - `SnapshotIds` -> `Vec<SnapshotId>`
  - `Commits` -> `Vec<Commit>`
  - `Authors` -> `Vec<User>`
  - `Committers` -> `Vec<User>`
  - `Users` -> `Vec<User>`
  - `Paths` -> `Vec<Path>`
  - `Snapshots` -> `Vec<Snapshot>`
  - `Itself` -> `Project`
  - `Raw` -> `Project` without a reference to the database

Commit attributes:

  - `Id` -> `CommitId`
  - `CommitterId` -> `UserId`
  - `AuthorId` -> `UserId`
  - `Committer` -> `User`
  - `Author` -> `User`
  - `Hash` -> `String`
  - `Message` -> `String`
  - `MessageLength` -> `usize`
  - `AuthoredTimestamp` -> `i64` timestamp
  - `CommittedTimestamp` -> `i64` timestamp
  - `PathIds` -> `Vec<PathId>`
  - `SnapshotIds` -> `Vec<SnapshotId>`
  - `ParentIds` -> `Vec<ParentId>`
  - `Paths` -> `Vec<Path>`
  - `Snapshots` -> `Vec<Snapshot>`
  - `Parents` -> `Vec<Commit>`
  - `Itself` -> `Commit`
  - `Raw` -> `Commit` without a reference to the database
  
Head attributes:

  - `Name` -> `String`
  - `CommitId` -> `CommitId`
  - `Commit` -> `Commit`
  - `Itself` -> `Head`
  - `Raw` -> `Head` without a reference to the database
  
Change attributes:

  - `PathId` -> `PathId`
  - `SnapshotId` -> `SnapshotId`
  - `Path` -> `Path`
  - `Snapshot` -> `Snapshot`
  - `Itself` -> `Change`
  - `Raw` -> `Change` without a reference to the database
  
User attributes:  

  - `Id` -> `UserId`
  - `Email` -> `String`
  - `AuthorExperience` -> `i64` timestamp
  - `CommitterExperience` -> `i64` timestamp
  - `Experience` -> `i64` timestamp
  - `AuthoredCommitIds` -> `Vec<CommitId>`
  - `CommittedCommitIds` -> `Vec<CommitId>`
  - `AuthoredCommits` -> `Vec<Commit>`
  - `CommittedCommits` -> `Vec<Commit>`
  - `Itself` -> `User`
  - `Raw` -> `User` without a reference to the database
  
Path attributes:

  - `Id` -> `PathId`
  - `Location` -> `String`
  - `Language` -> `Language` enum
  - `Itself` -> `Path`
  - `Raw` -> `Path` without a reference to the database
  
Snapshot attributes:

  - `Id` -> `SnapshotId`
  - `Bytes` -> `Vec<u8>` (faithful)
  - `Contents` -> `String` (lossy UTF-8)
  - `Itself` -> `Snapshot`
  - `Raw` -> `Snapshot` without a reference to the database 