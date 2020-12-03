use std::io::Write;
use std::fs::File;
use std::collections::HashMap;
use std::collections::hash_map::RandomState;

use serde::export::fmt::Display;
use itertools::Itertools;

use crate::objects::*;
use crate::iterators::*;
use crate::fraction::*;

macro_rules! create_file {
    ($location:expr) => {{
        let path = std::path::PathBuf::from($location.clone());
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
        let location = location.into();
        eprintln!("Writing to CSV file at {}", location);
        let mut file = create_file!(location)?;
        writeln!(file, "{}", T::csv_header())?;
        for element in self { writeln!(file, "{}", element.to_csv_item())?; }
        eprintln!("Done writing to CSV file at {}", location);
        Ok(())
    }
}

pub trait JoinConvenience {
    fn to_space_separated_string(&self) -> String;
    fn to_comma_separated_string(&self) -> String;
}

impl<T> JoinConvenience for Vec<T> where T: Display {
    fn to_space_separated_string(&self) -> String {
        self.iter().map(|s| s.to_string()).join(" ")
    }
    fn to_comma_separated_string(&self) -> String {
        self.iter().map(|s| s.to_string()).join(","
        )
    }
}

impl<T> JoinConvenience for Option<T> where T: JoinConvenience {
    fn to_space_separated_string(&self) -> String {
        self.as_ref().map_or(String::new(),|v| v.to_space_separated_string())
    }
    fn to_comma_separated_string(&self) -> String {
        self.as_ref().map_or(String::new(),|v| v.to_comma_separated_string())
    }
}

impl<T> JoinConvenience for &Vec<T> where T: Display {
    fn to_space_separated_string(&self) -> String {
        self.iter().map(|s| s.to_string()).join(" ")
    }
    fn to_comma_separated_string(&self) -> String {
        self.iter().map(|s| s.to_string()).join(",")
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
        Self::column_headers().to_comma_separated_string()
    }
    fn to_csv_item(&self) -> String {
        self.column_values().to_comma_separated_string()
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

// FIXME more typess of N
impl<N> CSVItem for Fraction<N> where N: Clone + Into<usize> {
    fn column_headers() -> Vec<&'static str> { vec!["n"] }
    fn column_values(&self) -> Vec<String> { vec![self.as_fraction_string()] }
}

impl_csv_item_inner!(ProjectId, "project_id");
impl_csv_item_inner!(CommitId, "commit_id");
impl_csv_item_inner!(UserId, "user_id");
impl_csv_item_inner!(PathId, "path_id");
impl_csv_item_inner!(SnapshotId, "snapshot_id");

impl<T> CSVItem for Option<T> where T: CSVItem {
    fn column_headers() -> Vec<&'static str> { T::column_headers() }
    fn column_values(&self) -> Vec<String> {
        self.as_ref().map_or(vec![], |e| e.column_values())
    }
}

impl<Ta, Tb> CSVItem for (Ta, Tb) where Ta: CSVItem, Tb: CSVItem {
    fn column_headers() -> Vec<&'static str> {
        let mut combined = Vec::new();
        combined.append(&mut Ta::column_headers());
        combined.append(&mut Tb::column_headers());
        combined
    }
    fn column_values(&self) -> Vec<String> {
        let mut combined = Vec::new();
        combined.append(&mut self.0.column_values());
        combined.append(&mut self.1.column_values());
        combined
    }
}

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
              self.parent_ids().to_space_separated_string().quoted(),
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
        vec!["project_id", "url",
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
             self.default_branch().to_string_or_empty().escape_quotes().quoted(),
             self.license().to_string_or_empty().escape_quotes().quoted(),
             self.homepage().to_string_or_empty().escape_quotes().quoted(),
             self.description().to_string_or_empty().escape_quotes().quoted()]
    }
}

impl<'a> CSVItem for ItemWithData<'a, Commit> {
    fn column_headers() -> Vec<&'static str> {
        vec!["commit_id", "hash",
             "committer_id", "author_id",
             "parent_ids", "parent_count",
             "author_timestamp", "committer_timestamp",
             "changed_paths", "changed_path_count" ,
             "message", "message_length"]
    }

    fn column_values(&self) -> Vec<String> {
        vec![self.id().to_string(), self.hash().to_string_or_empty(),
            self.committer_id().to_string(), self.author_id().to_string(),
            self.parent_ids().to_space_separated_string().quoted(), self.parent_count().to_string(),
            self.author_timestamp().to_string_or_empty(), self.committer_timestamp().to_string_or_empty(),
            self.changed_path_ids().to_space_separated_string().quoted(), self.changed_snapshot_count().to_string_or_empty(),
            self.message().to_string_or_empty().escape_quotes().quoted(), self.message_length().to_string_or_empty()]
    }
}


impl<'a> CSVItem for ItemWithData<'a, User> {
    fn column_headers() -> Vec<&'static str> {
        vec!["user_id", "email",
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


impl<'a> CSVItem for ItemWithData<'a, Path> {
    fn column_headers() -> Vec<&'static str> {
        Path::column_headers()
    }

    fn column_values(&self) -> Vec<String> {
        self.item.column_values()
    }
}


impl<'a> CSVItem for ItemWithData<'a, Option<Project>> { // FIXME implement a full complement of types, do a macro
    fn column_headers() -> Vec<&'static str> {
        ItemWithData::<Project>::column_headers()
    }
    fn column_values(&self) -> Vec<String> {
        self.item.as_ref()
            .map(|object| ItemWithData::new(self.data, object.clone()).column_values())
            .unwrap_or(vec![])
    }
}

impl<'a> CSVItem for ItemWithData<'a, Option<usize>> {
    fn column_headers() -> Vec<&'static str> {
        ItemWithData::<usize>::column_headers()
    }
    fn column_values(&self) -> Vec<String> {
        self.item.as_ref()
            .map(|object| ItemWithData::new(self.data, object.clone()).column_values())
            .unwrap_or(vec![])
    }
}

impl<'a, N> CSVItem for ItemWithData<'a, Fraction<N>> where N: Clone + Into<usize> {
    fn column_headers() -> Vec<&'static str> {
        Fraction::<N>::column_headers()
    }
    fn column_values(&self) -> Vec<String> {
        self.item.column_values()
    }
}

impl<'a, N> CSVItem for ItemWithData<'a, Option<Fraction<N>>> where N: Clone + Into<usize> {
    fn column_headers() -> Vec<&'static str> {
        ItemWithData::<Fraction<N>>::column_headers()
    }
    fn column_values(&self) -> Vec<String> {
        self.item.as_ref()
            .map(|object| ItemWithData::new(self.data, object.clone()).column_values())
            .unwrap_or(vec![])
    }
}


// impl<'a, T> CSVItem for ItemWithData<'a, Option<T>> where ItemWithData<'a, T>: CSVItem, T: Clone {
//     fn column_headers() -> Vec<&'static str> {
//         ItemWithData::<T>::column_headers()
//     }
//
//     fn column_values(&self) -> Vec<String> {
//         self.item.as_ref()
//             .map(|object| ItemWithData::new(self.data, object.clone()).column_values())
//             .unwrap_or(vec![])
//     }
// }

impl<'a> CSVItem for ItemWithData<'a, usize> { // TODO all the other ones
    fn column_headers() -> Vec<&'static str> {
        usize::column_headers()
    }
    fn column_values(&self) -> Vec<String> {
        self.item.column_values()
    }
}

// FIXME could we make GroupIter also work with this directly? (right now one needs to ungroup)

#[derive(Debug)]
pub enum Error {
    IO(std::io::Error),
    CSV(csv::Error),
    ParseInt(std::num::ParseIntError),
    MissingColumn(String),
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self { Error::IO(error) }
}
impl From<csv::Error> for Error {
    fn from(error: csv::Error) -> Self { Error::CSV(error) }
}
impl From<std::num::ParseIntError> for Error {
    fn from(error: std::num::ParseIntError) -> Self { Error::ParseInt(error) }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::IO(error) => error.fmt(f),
            Error::CSV(error) => error.fmt(f),
            Error::ParseInt(error) => error.fmt(f),
            Error::MissingColumn(column) => write!(f, "column {} does not exist", column)
        }
    }
}

pub trait FromCSV: Sized {
    fn item_from_csv_row(values: HashMap<String, String>) -> Result<Self, Error>;

    fn from_csv<S>(location: S) -> Result<Vec<Self>, Error> where S: Into<String> {
        let file = File::open(location.into())?;
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(file);

        let headers: Vec<String> = reader.headers()
            .map_err(|e| Error::from(e))?
            .iter().map(|s| s.to_string()).collect();

        let mut vector: Vec<Self> = Vec::new();
        for record in reader.records() {
            let record = record.map_err(|e| Error::from(e))?;
            let fields =
                record.iter()
                    .map(|s| s.to_string());
            let values: HashMap<String, String> =
                headers.iter()
                    .map(|s| s.to_string())
                    .zip(fields).collect();
            let item = Self::item_from_csv_row(values)?;
            vector.push(item);
        }
        Ok(vector)
    }
}

macro_rules! from_single_column {
    ($item:ident, $values:expr, $t:ident) => {{
        let column: &str = SnapshotId::column_headers().pop().unwrap();
        let str: Option<&String> = $values.get(column);
        if let Some(str) = str {
           let n: $t = str.parse().map_err(|e| Error::from(e))?;
           Ok($item::from(n))
        } else {
            Err(Error::MissingColumn(column.to_owned()))
        }
    }}
}

impl FromCSV for PathId {
    fn item_from_csv_row(values: HashMap<String, String, RandomState>) -> Result<Self, Error> {
        from_single_column!(Self, values, u64)
    }
}

impl FromCSV for UserId {
    fn item_from_csv_row(values: HashMap<String, String, RandomState>) -> Result<Self, Error> {
        from_single_column!(Self, values, u64)
    }
}

impl FromCSV for ProjectId {
    fn item_from_csv_row(values: HashMap<String, String, RandomState>) -> Result<Self, Error> {
        from_single_column!(Self, values, u64)
    }
}

impl FromCSV for CommitId {
    fn item_from_csv_row(values: HashMap<String, String, RandomState>) -> Result<Self, Error> {
        from_single_column!(Self, values, u64)
    }
}

impl FromCSV for SnapshotId {
    fn item_from_csv_row(values: HashMap<String, String, RandomState>) -> Result<Self, Error> {
        from_single_column!(Self, values, u64)
    }
}