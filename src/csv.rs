use std::io::Error;

use crate::objects::*;
use serde::export::fmt::Display;
use itertools::Itertools;

macro_rules! create_file {
    ($location:expr) => {{
        let path = std::path::PathBuf::from($location.into());
        let dir_path = { let mut dir_path = path.clone(); dir_path.pop(); dir_path };
        std::fs::create_dir_all(&dir_path)?;
        std::fs::File::create(path)
    }}
}

pub trait CSV {
    fn to_csv(&self, location: impl Into<String>) -> Result<(), std::io::Error>;
}

impl<I, T> CSV for I where I: Iterator<Item=T>, T: CSVItem {
    fn to_csv(&self, location: impl Into<String>) -> Result<(), std::io::Error> {
        unimplemented!()
    }
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
    fn header() -> Vec<&'static str>;
    fn to_csv(&self) -> Vec<String>;
}

macro_rules! impl_csv_item {
    ($type:ident, $header:expr, $to_string:expr) => {
        impl CSVItem for $type {
            fn header() -> Vec<&'static str> { vec![$header] }
            fn to_csv(&self) -> Vec<String> { $to_string(self) }
        }
    }
}

macro_rules! impl_csv_item_quoted {
    ($type:ident, $header:expr) => {
        impl_csv_item!($type, $header, |selfie: &$type| vec![format!(r#"{}"#, selfie)]);
    }
}

macro_rules! impl_csv_item_to_string {
    ($type:ident, $header:expr) => {
        impl_csv_item!($type, $header, |selfie: &$type| vec![selfie.to_string()]);
    }
}

macro_rules! impl_csv_item_inner {
    ($type:ident, $header:expr) => {
        impl_csv_item!($type, $header, |selfie: &$type| selfie.0.to_csv());
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
    fn header() -> Vec<&'static str> {
        vec![ "project_id", "url" ]
    }
    fn to_csv(&self) -> Vec<String>  {
        vec![ self.id().to_string(),
              self.url().to_string() ]
    }
}

impl CSVItem for User {
    fn header() -> Vec<&'static str> {
        vec![ "user_id", "email" ]
    }
    fn to_csv(&self) -> Vec<String>  {
        vec![ self.id().to_string(),
              self.email().to_string() ]
    }
}

impl CSVItem for Path {
    fn header() -> Vec<&'static str> {
        vec![ "path_id", "path", "language" ]
    }
    fn to_csv(&self) -> Vec<String>  {
        vec![ self.id().to_string(),
              self.location().to_string(),
              self.language().to_string_or_empty() ]
    }
}

impl CSVItem for Commit {
    fn header() -> Vec<&'static str> {
        vec![ "commit_id", "parent_id", "author_id", "committer_id" ]
    }
    fn to_csv(&self) -> Vec<String>  {
        vec![ self.id().to_string(),
              self.parent_ids().into_iter().map(|id| id.to_string()).join(" "),
              self.author_id().to_string(),
              self.committer_id().to_string() ]
    }
}

impl CSVItem for Snapshot {
    fn header() -> Vec<&'static str> {
        unimplemented!()
    }
    fn to_csv(&self) -> Vec<String>  {
        unimplemented!()
    }
}