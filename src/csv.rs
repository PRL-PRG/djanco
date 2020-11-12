use std::io::Write;

use serde::export::fmt::Display;
use itertools::Itertools;

use crate::objects::*;
use crate::iterators::ItemWithData;
use std::fs::File;

macro_rules! create_file {
    ($location:expr) => {{
        let path = std::path::PathBuf::from($location.into());
        let dir_path = { let mut dir_path = path.clone(); dir_path.pop(); dir_path };
        std::fs::create_dir_all(&dir_path)?;
        std::fs::File::create(path)
    }}
}

pub trait CSV {
    fn into_csv(self, location: impl Into<String>) -> Result<(), std::io::Error>;
}

impl<I, T> CSV for I where I: Iterator<Item=T>, T: CSVItem {
    fn into_csv(self, location: impl Into<String>) -> Result<(), std::io::Error> {
        let mut file = create_file!(location)?;
        writeln!(file, "{}", T::csv_header())?;
        for element in self { writeln!(file, "{}", element.to_csv_item())?; }
        Ok(())
    }
}

pub trait StringConvenience {
    fn escape_quotes(&self) -> String;
    fn quoted(&self) -> String;
}

impl StringConvenience for String {
    fn escape_quotes(&self) -> String { self.replace("\"", "\"\"") }
    fn quoted(&self) -> String { format!("\"{}\"", self) }
}

impl StringConvenience for &String {
    fn escape_quotes(&self) -> String { self.replace("\"", "\"\"") }
    fn quoted(&self) -> String { format!("\"{}\"", self) }
}

impl StringConvenience for &str {
    fn escape_quotes(&self) -> String { self.replace("\"", "\"\"") }
    fn quoted(&self) -> String { format!("\"{}\"", self) }
}

pub trait Missing {
    fn to_string_or_empty(&self) -> String;
}

impl<T> Missing for Option<T> where T: Display {
    fn to_string_or_empty(&self) -> String {
        self.as_ref().map_or(String::new(), |e| e.to_string())
    }
}

#[allow(non_snake_case)]
pub trait CSVItem {
    fn column_headers() -> Vec<&'static str>;
    fn column_values(&self) -> Vec<String>;
    fn csv_header() -> String {
        Self::column_headers().into_iter().map(|header| header.to_owned()).join(", ")
    }
    fn to_csv_item(&self) -> String {
        self.column_values().join(", ")
    }
}

macro_rules! impl_csv_item {
    ($type:ident, $header:expr, $to_string:expr) => {
        impl CSVItem for $type {
            fn column_headers() -> Vec<&'static str> { vec![$header] }
            fn column_values(&self) -> Vec<String> { $to_string(self) }
        }
    }
}

macro_rules! impl_csv_item_quoted {
    ($type:ident, $header:expr) => {
        impl_csv_item!($type, $header, |selfie: &$type| vec![selfie.quoted()]);
    }
}

macro_rules! impl_csv_item_to_string {
    ($type:ident, $header:expr) => {
        impl_csv_item!($type, $header, |selfie: &$type| vec![selfie.to_string()]);
    }
}

macro_rules! impl_csv_item_inner {
    ($type:ident, $header:expr) => {
        impl_csv_item!($type, $header, |selfie: &$type| selfie.0.column_values());
    }
}

impl_csv_item_to_string!(bool, "b");
impl_csv_item_to_string!(u64, "n");
impl_csv_item_to_string!(i64, "n");
impl_csv_item_to_string!(u32, "n");
impl_csv_item_to_string!(i32, "n");
impl_csv_item_to_string!(f64, "n");
impl_csv_item_to_string!(f32, "n");
impl_csv_item_to_string!(usize, "n");

impl_csv_item_quoted!(String, "string");

impl_csv_item_inner!(ProjectId, "project_id");
impl_csv_item_inner!(CommitId, "commit_id");
impl_csv_item_inner!(UserId, "user_id");
impl_csv_item_inner!(PathId, "path_id");
impl_csv_item_inner!(SnapshotId, "snapshot_id");

impl CSVItem for Project {
    fn column_headers() -> Vec<&'static str> {
        vec![ "project_id", "url" ]
    }
    fn column_values(&self) -> Vec<String>  {
        vec![ self.id().to_string(),
              self.url().to_string() ]
    }
}

impl CSVItem for User {
    fn column_headers() -> Vec<&'static str> {
        vec![ "user_id", "email" ]
    }
    fn column_values(&self) -> Vec<String>  {
        vec![ self.id().to_string(),
              self.email().to_string() ]
    }
}

impl CSVItem for Path {
    fn column_headers() -> Vec<&'static str> {
        vec![ "path_id", "path", "language" ]
    }
    fn column_values(&self) -> Vec<String>  {
        vec![ self.id().to_string(),
              self.location().to_string(),
              self.language().to_string_or_empty() ]
    }
}

impl CSVItem for Commit {
    fn column_headers() -> Vec<&'static str> {
        vec![ "commit_id", "parent_id", "author_id", "committer_id" ]
    }
    fn column_values(&self) -> Vec<String>  {
        vec![ self.id().to_string(),
              self.parent_ids().into_iter().map(|id| id.to_string()).join(" "),
              self.author_id().to_string(),
              self.committer_id().to_string() ]
    }
}


impl CSVItem for Snapshot {
    fn column_headers() -> Vec<&'static str> {
        vec!["snapshot_id", "content"]
    }
    fn column_values(&self) -> Vec<String>  {
        vec![ self.id().to_string(),
              self.contents().to_string().escape_quotes().quoted() ]
    }
}

impl<'a> CSVItem for ItemWithData<'a, Project> {
    fn column_headers() -> Vec<&'static str> {
        vec!["id", "url",
             "is_fork", "is_archived", "is_disabled",
             "stars", "watchers", "size", "open_issues", "forks", "subscribers",
             "language",
             "heads", "commits", "authors", "paths", "snapshots", "committers", "users",
             "lifetime",
             "has_issues", "has_downloads", "has_wiki", "has_pages",
             "created", "updated", "pushed",
             "master_branch",
             "license", "homepage", "description"]
    }

    fn column_values(&self) -> Vec<String> {
        vec![self.id().to_string(),
             self.url(),
             self.is_fork().to_string_or_empty(),
             self.is_archived().to_string_or_empty(),
             self.is_disabled().to_string_or_empty(),
             self.star_count().to_string_or_empty(),
             self.watcher_count().to_string_or_empty(),
             self.size().to_string_or_empty(),
             self.open_issue_count().to_string_or_empty(),
             self.fork_count().to_string_or_empty(),
             self.subscriber_count().to_string_or_empty(),
             self.language().to_string_or_empty(),
             self.head_count().to_string_or_empty(),
             self.commit_count().to_string_or_empty(),
             self.author_count().to_string_or_empty(),
             self.path_count().to_string_or_empty(),
             self.snapshot_count().to_string_or_empty(),
             self.committer_count().to_string_or_empty(),
             self.user_count().to_string_or_empty(),
             self.lifetime().to_string_or_empty(),
             self.has_issues().to_string_or_empty(),
             self.has_downloads().to_string_or_empty(),
             self.has_wiki().to_string_or_empty(),
             self.has_pages().to_string_or_empty(),
             self.created().to_string_or_empty(),
             self.updated().to_string_or_empty(),
             self.pushed().to_string_or_empty(),
             self.master_branch().to_string_or_empty().escape_quotes().quoted(),
             self.license().to_string_or_empty().escape_quotes().quoted(),
             self.homepage().to_string_or_empty().escape_quotes().quoted(),
             self.description().to_string_or_empty().escape_quotes().quoted()]
    }
}

impl<'a> CSVItem for ItemWithData<'a, User> {
    fn column_headers() -> Vec<&'static str> {
        vec!["id", "email",
             "authored_commits", "committed_committs",
             "author_experience", "committer_experience", "experience"]
    }

    fn column_values(&self) -> Vec<String> {
        vec![self.id().to_string(),
             self.email().to_string(),
             self.authored_commit_count().to_string_or_empty(),
             self.committed_commit_count().to_string_or_empty(),
             self.author_experience().to_string_or_empty(),
             self.committer_experience().to_string_or_empty(),
             self.experience().to_string_or_empty()]
    }
}

pub trait FromCSV: Sized {
    fn from_csv<S>(location: S) -> Result<Self, std::io::Error> where S: Into<String>;
}

impl FromCSV for Vec<SnapshotId> {
    fn from_csv<S>(location: S) -> Result<Self, std::io::Error> where S: Into<String> {
        let file = File::open(location.into())?;
        let mut reader = csv::ReaderBuilder::new()
                .has_headers(true)
                .from_reader(file);

        let headers = reader.headers()?;

        let column_indexes: Vec<usize> =
            headers.iter()
                .filter(|field| *field == "snapshot_id")
                .enumerate()
                .map(|(n, _)| n)
                .take(1)
                .collect();

        let column_index= *column_indexes.first().unwrap();

        let snapshot_ids: Vec<SnapshotId> = reader.records()
            .map(|e| e.unwrap())
            .map(|string_record| {
                let field = string_record.get(column_index).unwrap();
                let n: u64 = field.parse().unwrap();
                SnapshotId::from(n)
            }).collect();

        Ok(snapshot_ids)
    }
}