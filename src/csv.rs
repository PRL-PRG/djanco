use std::io::Write;
use std::fs::{File, create_dir_all};
use std::path::PathBuf;
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::RandomState;
use std::fmt::Display;

use itertools::Itertools;

use parasite;

use crate::objects::*;
use crate::fraction::*;
use crate::product::*;
use crate::time::Duration;
use crate::metadata::ProjectMetadata;
use crate::Store;

macro_rules! create_file {
    ($location:expr) => {{
        let path = std::path::PathBuf::from($location.clone());
        let dir_path = { let mut dir_path = path.clone(); dir_path.pop(); dir_path };
        std::fs::create_dir_all(&dir_path)?;
        std::fs::File::create(path)
    }}
}

pub trait CSV<T>: Sized where T: CSVItem {
    fn into_csv_with_headers(self, headers: Vec<&str>, location: impl Into<String>) -> Result<(), std::io::Error>;
    fn into_csv_with_headers_in_dir(self, headers: Vec<&str>, dir: &std::path::Path, file: impl Into<String>) -> Result<(), std::io::Error> {
        let location = dir.join(PathBuf::from(file.into()));
        self.into_csv_with_headers(headers, location.into_os_string().to_str().unwrap())
    }
    fn into_csv(self, location: impl Into<String>) -> Result<(), std::io::Error> {
        self.into_csv_with_headers(T::column_headers(), location)
    }
    fn into_csv_in_dir(self, dir: &std::path::Path, file: impl Into<String>) -> Result<(), std::io::Error> {
        let location = dir.join(PathBuf::from(file.into()));
        self.into_csv(location.into_os_string().to_str().unwrap())
    }
}

impl<I, T> CSV<T> for I where I: Iterator<Item=T>, T: CSVItem {
    fn into_csv_with_headers(self, headers: Vec<&str>, location: impl Into<String>) -> Result<(), std::io::Error> {
        let location = location.into();
        eprintln!("Writing to CSV file at {}", location);
        let mut file = create_file!(location)?;
        //T::write_column_headers_to(&mut file)?;
        writeln!(file, "{}", headers.to_comma_separated_string())?;
        for element in self {
            element.write_csv_items_to(&mut file)?;
        }
        eprintln!("Done writing to CSV file at {}", location);
        Ok(())
    }
}

#[allow(non_snake_case)]
pub trait CSVItem {
    fn column_headers() -> Vec<&'static str>;
    fn row(&self) -> Vec<String>;
    fn rows(&self) -> Vec<Vec<String>>; //{ vec![self.row()] }
    fn csv_header() -> String {
        Self::column_headers().to_comma_separated_string()
    }
    fn to_csv_items(&self) -> Vec<String> {
        self.rows().into_iter().map(|row: Vec<String>| {
            row.to_comma_separated_string()
        }).collect()
    }
    fn write_column_headers_to<F>(file: &mut F) -> Result<(), std::io::Error> where F: Write {
        writeln!(file, "{}", Self::csv_header())
    }
    fn write_csv_items_to<F>(&self, file: &mut F) -> Result<(), std::io::Error> where F: Write {
        for item in self.to_csv_items() {
            writeln!(file, "{}", item)?;
        }
        Ok(())
    }
}

//--- CG macros ------------------------------------------------------------------------------------

macro_rules! impl_csv_item {
    ($type:path, $header:expr, $to_string:expr) => {
        impl CSVItem for $type {
            fn column_headers() -> Vec<&'static str> { vec![$header] }
            fn row(&self) -> Vec<String> { $to_string(self) }
            fn rows(&self) -> Vec<Vec<String>> { vec![$to_string(self)] }
        }
    };
    ($type:tt<$($generic:tt),+>, $header:expr, $to_string:expr) => {
        impl<$($generic,)+> CSVItem for $type<$($generic,)+> {
            fn column_headers() -> Vec<&'static str> { vec![$header] }
            fn row(&self) -> Vec<String> { $to_string(self) }
            fn rows(&self) -> Vec<Vec<String>> { vec![$to_string(self)] }
        }
    };
    ($type:tt<$($generic:tt),+> where $($type_req:tt: $($type_req_def:tt),+);+-> $header:expr, $to_string:expr) => {
        impl<$($generic,)+> CSVItem for $type<$($generic,)+> where $($type_req: $($type_req_def+)+)+ {
            fn column_headers() -> Vec<&'static str> { vec![$header] }
            fn row(&self) -> Vec<String> { $to_string(self) }
            fn rows(&self) -> Vec<Vec<String>> { vec![$to_string(self)] }
        }
    }
}

macro_rules! impl_csv_item_quoted {
    ($type:ident, $header:expr) => {
        impl_csv_item!($type, $header, |selfie: &$type| vec![selfie.quoted()]);
    }
}

macro_rules! impl_csv_item_to_string {
    ($type:tt, $header:expr) => {
        impl_csv_item!($type, $header, |selfie: &$type| vec![selfie.to_string()]);
    };
    ($type:path, $header:expr) => {
        impl_csv_item!($type, $header, |selfie: &$type| vec![selfie.to_string()]);
    }
}

macro_rules! impl_csv_item_inner {
    ($type:tt, $header:expr) => {
        impl_csv_item!($type, $header, |selfie: &$type| selfie.0.row());
    }
}

macro_rules! impl_csv_item_with_data_inner {
    ($type:tt) => {
        impl<'a> CSVItem for ItemWithData<'a, $type> {
            fn column_headers() -> Vec<&'static str> {
                $type::column_headers()
            }
            fn row(&self) -> Vec<String> {
                self.item.row()
            }
            fn rows(&self) -> Vec<Vec<String>> {
                self.item.rows()
            }
        }
        impl<'a> CSVItem for ItemWithData<'a, Option<$type>> {
            fn column_headers() -> Vec<&'static str> {
                ItemWithData::<$type>::column_headers()
            }
            fn row(&self) -> Vec<String> {
                self.item.as_ref()
                    .map(|object| ItemWithData::new(self.data, object.clone()).row())
                    .unwrap_or(vec![])
            }
            fn rows(&self) -> Vec<Vec<String>> {
                self.item.as_ref()
                    .map(|object| ItemWithData::new(self.data, object.clone()).rows())
                    .unwrap_or(vec![])
            }
        }
    };
    ($type:tt<$($generic:tt),+>) => {
        impl<'a, $($generic,)+> CSVItem for ItemWithData<'a, $type<$($generic,)+>> {
            fn column_headers() -> Vec<&'static str> {
                $type::<$($generic,)+>::column_headers()
            }
            fn row(&self) -> Vec<String> {
                self.item.row()
            }
        }
        impl<'a, $($generic,)+> CSVItem for ItemWithData<'a, Option<$type<$($generic,)+>>> {
            fn column_headers() -> Vec<&'static str> {
                ItemWithData::<$type<$($generic,)+>>::column_headers()
            }
            fn row(&self) -> Vec<String> {
                self.item.as_ref()
                    .map(|object| ItemWithData::new(self.data, object.clone()).row())
                    .unwrap_or(vec![])
            }
        }
    };
    ($type:tt<$($generic:tt),+> where $($type_req:tt: $($type_req_def:tt),+);+) => {
        impl<'a, $($generic,)+> CSVItem for ItemWithData<'a, $type<$($generic,)+>> where $($type_req: $($type_req_def+)+)+ {
            fn column_headers() -> Vec<&'static str> {
                $type::<$($generic,)+>::column_headers()
            }
            fn row(&self) -> Vec<String> {
                self.item.row()
            }
            fn rows(&self) -> Vec<Vec<String>> {
                self.item.rows()
            }
        }
        impl<'a, $($generic,)+> CSVItem for ItemWithData<'a, Option<$type<$($generic,)+>>> where $($type_req: $($type_req_def+)+)+ {
            fn column_headers() -> Vec<&'static str> {
                ItemWithData::<$type<$($generic,)+>>::column_headers()
            }
            fn row(&self) -> Vec<String> {
                self.item.as_ref()
                    .map(|object| ItemWithData::new(self.data, object.clone()).row())
                    .unwrap_or(vec![])
            }
            fn rows(&self) -> Vec<Vec<String>> {
                self.item.as_ref()
                    .map(|object| ItemWithData::new(self.data, object.clone()).rows())
                    .unwrap_or(vec![])
            }
        }
    }
}

macro_rules! impl_csv_item_tuple {
    ($($types:tt -> $indices:tt),+) => {
        impl<$($types,)+> CSVItem for ($($types,)+) where $($types: CSVItem,)+ {
            fn column_headers() -> Vec<&'static str> {
                let mut combined = Vec::new();
                $(combined.append(&mut $types::column_headers());)+
                combined
            }
            fn row(&self) -> Vec<String> {
                let mut combined = Vec::new();
                $(combined.append(&mut self.$indices.row());)+
                combined
            }
            fn rows(&self) -> Vec<Vec<String>> {
                vec![$(self.$indices.rows(),)+].into_iter().into_megaproduct().collect()
            }
        }

        // On one hand this is incorrect since the inner iter should be provided with data so
        //     self.item.$indices.row())
        // should be
        //     ItemWithData::new(self.data, self.item.$indices).row()
        //
        // On the other hand, I've failed to implement it without causing infinite recursion during
        // type checking, so for now I guess it has to stay like this.
        //
        // If the inner types are individually wrapped, it should still work correctly though.
        impl<'a, $($types,)+> CSVItem for ItemWithData<'a, ($($types,)+)> where $(/*ItemWithData<'a,*/ $types/*>*/: CSVItem,)+ $($types: Clone,)+ {
            fn column_headers() -> Vec<&'static str> {
                let mut combined = Vec::new();
                //$(combined.append(&mut ItemWithData::<$types>::column_headers());)+
                $(combined.append(&mut $types::column_headers());)+
                combined
            }
            fn row(&self) -> Vec<String> {
                let mut combined = Vec::new();
                //$(combined.append(&mut ItemWithData::new(self.data, self.item.$indices.clone()).row() );)+
                $(combined.append(&mut self.item.$indices.row());)+
                combined
            }
            fn rows(&self) -> Vec<Vec<String>> {
               vec![$(self.item.$indices.rows(),)+].into_iter().into_megaproduct().collect()
            }
        }
    }
}


//--- generic CSV items ----------------------------------------------------------------------------

impl<T> CSVItem for Option<T> where T: CSVItem {
    fn column_headers() -> Vec<&'static str> { T::column_headers() }
    fn row(&self) -> Vec<String> {
        self.as_ref().map_or(vec![], |e| e.row())
    }
    fn rows(&self) -> Vec<Vec<String>> {
        self.as_ref().map_or(vec![], |e| e.rows())
    }
}

//impl_csv_item_tuple!(Ta -> 0);
impl_csv_item_tuple!(Ta -> 0, Tb -> 1);
impl_csv_item_tuple!(Ta -> 0, Tb -> 1, Tc -> 2);
impl_csv_item_tuple!(Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3);
impl_csv_item_tuple!(Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4);
impl_csv_item_tuple!(Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4, Tf -> 5);
impl_csv_item_tuple!(Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4, Tf -> 5, Tg -> 6);
impl_csv_item_tuple!(Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4, Tf -> 5, Tg -> 6, Th -> 7);
impl_csv_item_tuple!(Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4, Tf -> 5, Tg -> 6, Th -> 7, Ti -> 8);
impl_csv_item_tuple!(Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4, Tf -> 5, Tg -> 6, Th -> 7, Ti -> 8, Tj -> 9);
impl_csv_item_tuple!(Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4, Tf -> 5, Tg -> 6, Th -> 7, Ti -> 8, Tj -> 9, Tk -> 10);
impl_csv_item_tuple!(Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4, Tf -> 5, Tg -> 6, Th -> 7, Ti -> 8, Tj -> 9, Tk -> 10, Tl -> 11);

impl<T> CSVItem for Vec<T> where T: CSVItem {
    fn column_headers() -> Vec<&'static str> {
       T::column_headers()
    }
    fn row(&self) -> Vec<String> {
        panic!("There is no implementation of `row` for a vector of CSVItems, \
                since it is implied it is always multiple rows")
    }
    fn rows(&self) -> Vec<Vec<String>> {
        self.iter().flat_map(|e| e.rows()).collect()
    }
}

impl<T> CSVItem for &T where T: CSVItem {
    fn column_headers() -> Vec<&'static str> { T::column_headers() }
    fn row(&self) -> Vec<String> { T::row(self) }
    fn rows(&self) -> Vec<Vec<String>> { T::rows(self) }
}

//--- easy CSV Items -------------------------------------------------------------------------------

impl_csv_item_to_string!(bool, "b");

impl_csv_item_to_string!(usize, "n");
impl_csv_item_to_string!(u128,  "n");
impl_csv_item_to_string!(u64,   "n");
impl_csv_item_to_string!(u32,   "n");
impl_csv_item_to_string!(u16,   "n");
impl_csv_item_to_string!(u8,    "n");

impl_csv_item_to_string!(i128, "n");
impl_csv_item_to_string!(i64,  "n");
impl_csv_item_to_string!(i32,  "n");
impl_csv_item_to_string!(i16,  "n");
impl_csv_item_to_string!(i8,   "n");

impl_csv_item_to_string!(f64, "n");
impl_csv_item_to_string!(f32, "n");

impl_csv_item_quoted!(String, "string");

impl_csv_item_to_string!(Language, "language");
impl_csv_item_to_string!(Store, "store");
impl_csv_item_to_string!(Duration, "duration");
impl_csv_item!(Fraction<N> where N: Fractionable -> "n", |selfie: &Fraction<N>| vec![selfie.as_fraction_string()]);

//--- parasite CSV items ---------------------------------------------------------------------------

impl_csv_item_to_string!(parasite::ProjectId, "project_id");

//--- item with data where it doesn't matter -------------------------------------------------------

impl_csv_item_with_data_inner!(bool);

impl_csv_item_with_data_inner!(usize);
impl_csv_item_with_data_inner!(u128);
impl_csv_item_with_data_inner!(u64);
impl_csv_item_with_data_inner!(u32);
impl_csv_item_with_data_inner!(u16);
impl_csv_item_with_data_inner!(u8);

impl_csv_item_with_data_inner!(i128);
impl_csv_item_with_data_inner!(i64);
impl_csv_item_with_data_inner!(i32);
impl_csv_item_with_data_inner!(i16);
impl_csv_item_with_data_inner!(i8);

impl_csv_item_with_data_inner!(f64);
impl_csv_item_with_data_inner!(f32);

impl_csv_item_with_data_inner!(String);

impl_csv_item_with_data_inner!(Language);
impl_csv_item_with_data_inner!(Duration);
impl_csv_item_with_data_inner!(Fraction<N> where N: Fractionable, Clone);

//--- IDs as CSV items -----------------------------------------------------------------------------

impl_csv_item_inner!(ProjectId, "project_id");
impl_csv_item_inner!(CommitId, "commit_id");
impl_csv_item_inner!(UserId, "user_id");
impl_csv_item_inner!(PathId, "path_id");
impl_csv_item_inner!(SnapshotId, "snapshot_id");

impl_csv_item_with_data_inner!(ProjectId);
impl_csv_item_with_data_inner!(CommitId);
impl_csv_item_with_data_inner!(UserId);
impl_csv_item_with_data_inner!(PathId);
impl_csv_item_with_data_inner!(SnapshotId);

//--- entities as CSV items ------------------------------------------------------------------------

impl CSVItem for Project {
    fn column_headers() -> Vec<&'static str> {
        vec![ "project_id", "url" ]
    }
    fn row(&self) -> Vec<String>  {
        vec![
            self.id().to_string(),
            self.url().to_string()
        ]
    }
    fn rows(&self) -> Vec<Vec<String>> {
        vec![vec![
            self.id().to_string(),
            self.url().to_string()
        ]]
    }
}

impl<'a> CSVItem for ItemWithData<'a, Project> {
    fn column_headers() -> Vec<&'static str> {
        vec!["project_id", "substore", "url",
             "is_fork", "is_archived", "is_disabled",
             "stars", "watchers", "size", 
             "open_issues", 
             "forks", "subscribers",
             "language",
             "heads", "commits", "authors", "paths", "snapshots", "committers", "users",
             "lifetime",
             "has_issues", "has_downloads", "has_wiki", "has_pages",
             "created", "updated", "pushed",
             "default_branch",
             "license", "homepage", "description",
             "all_issues", "issues", "buggy_issues", 
             "unique_files", "original_files", "impact",
             "files",
             "major_language", "major_language_ratio", "major_language_changes",
             "all_forks_count",
             "project_locs"]
    }

    fn row(&self) -> Vec<String> {        
        vec![self.id().to_string(),
             self.substore().to_string_or_empty(),
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
             self.description().to_string_or_empty().escape_quotes().quoted(),
             self.combined_issue_count().to_string_or_empty(),
             self.issue_count().to_string_or_empty(),
             self.buggy_issue_count().to_string_or_empty(),
             self.unique_files().to_string_or_empty(),
             self.original_files().to_string_or_empty(),
             self.impact().to_string_or_empty(),
             self.files().to_string_or_empty(),
             self.major_language().to_string_or_empty(),
             self.major_language_ratio().to_string_or_empty(),
             self.major_language_changes().to_string_or_empty(),
             self.all_forks_count().to_string_or_empty(),
             self.project_locs().to_string_or_empty()]
    }

    fn rows(&self) -> Vec<Vec<String>> {
        vec![vec![
            self.id().to_string(),
            self.substore().to_string_or_empty(),
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
            self.description().to_string_or_empty().escape_quotes().quoted(),
            self.combined_issue_count().to_string_or_empty(),
            self.issue_count().to_string_or_empty(),
            self.buggy_issue_count().to_string_or_empty(),
            self.unique_files().to_string_or_empty(),
            self.original_files().to_string_or_empty(),
            self.impact().to_string_or_empty(),
            self.files().to_string_or_empty(),
            self.major_language().to_string_or_empty(),
            self.major_language_ratio().to_string_or_empty(),
            self.major_language_changes().to_string_or_empty(),
            self.all_forks_count().to_string_or_empty(),
            self.project_locs().to_string_or_empty()
        ]]
    }
}

impl CSVItem for User {
    fn column_headers() -> Vec<&'static str> {
        vec![ "user_id", "email" ]
    }
    fn row(&self) -> Vec<String>  {
        vec![
            self.id().to_string(),
            self.email().to_string()
        ]
    }
    fn rows(&self) -> Vec<Vec<String>> {
        vec![vec![
            self.id().to_string(),
            self.email().to_string()
        ]]
    }
}

impl<'a> CSVItem for ItemWithData<'a, User> {
    fn column_headers() -> Vec<&'static str> {
        vec!["user_id", "email",
             "authored_commits", "committed_commits",
             "author_experience", "committer_experience", "experience"]
    }

    fn row(&self) -> Vec<String> {
        vec![
            self.id().to_string(),
            self.email().to_string(),
            self.authored_commit_count().to_string_or_empty(),
            self.committed_commit_count().to_string_or_empty(),
            self.author_experience().to_string_or_empty(),
            self.committer_experience().to_string_or_empty(),
            self.experience().to_string_or_empty(),
        ]
    }

    fn rows(&self) -> Vec<Vec<String>> {
        vec![vec![
            self.id().to_string(),
            self.email().to_string(),
            self.authored_commit_count().to_string_or_empty(),
            self.committed_commit_count().to_string_or_empty(),
            self.author_experience().to_string_or_empty(),
            self.committer_experience().to_string_or_empty(),
            self.experience().to_string_or_empty(),
        ]]
    }
}

impl CSVItem for Path {
    fn column_headers() -> Vec<&'static str> {
        vec![ "path_id", "path", "language" ]
    }
    fn row(&self) -> Vec<String>  {
        vec![
            self.id().to_string(),
            self.location().to_string(),
            self.language().to_string_or_empty()
        ]
    }
    fn rows(&self) -> Vec<Vec<String>> {
        vec![vec![
            self.id().to_string(),
            self.location().to_string(),
            self.language().to_string_or_empty()
        ]]
    }
}
impl_csv_item_with_data_inner!(Path);

impl CSVItem for Change {
    fn column_headers() -> Vec<&'static str> { vec!["path_id", "snapshot_id"] }
    fn row(&self) -> Vec<String> {
        vec![
            self.path.to_string(),
            self.snapshot.to_string_or_empty(),
        ]
    }
    fn rows(&self) -> Vec<Vec<String>> {
        vec![vec![
            self.path.to_string(),
            self.snapshot.to_string_or_empty(),
        ]]
    }
}
impl_csv_item_with_data_inner!(Change);

impl CSVItem for Commit {
    fn column_headers() -> Vec<&'static str> {
        vec![ "commit_id", "parent_id", "author_id", "committer_id" ]
    }
    fn row(&self) -> Vec<String>  {
        vec![
            self.id().to_string(),
            self.parent_ids().to_space_separated_string().quoted(),
            self.author_id().to_string(),
            self.committer_id().to_string()
        ]
    }
    fn rows(&self) -> Vec<Vec<String>> {
        vec![vec![
            self.id().to_string(),
            self.parent_ids().to_space_separated_string().quoted(),
            self.author_id().to_string(),
            self.committer_id().to_string(),
        ]]
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

    fn row(&self) -> Vec<String> {
        vec![self.id().to_string(), self.hash().to_string_or_empty(),
             self.committer_id().to_string(), self.author_id().to_string(),
             self.parent_ids().to_space_separated_string().quoted(), self.parent_count().to_string(),
             self.author_timestamp().to_string_or_empty(), self.committer_timestamp().to_string_or_empty(),
             self.changed_path_ids().to_space_separated_string().quoted(), self.changed_snapshot_count().to_string_or_empty(),
             self.message().to_string_or_empty().escape_quotes().quoted(), self.message_length().to_string_or_empty()]
    }

    fn rows(&self) -> Vec<Vec<String>> {
        vec![vec![
            self.id().to_string(), self.hash().to_string_or_empty(),
            self.committer_id().to_string(), self.author_id().to_string(),
            self.parent_ids().to_space_separated_string().quoted(), self.parent_count().to_string(),
            self.author_timestamp().to_string_or_empty(), self.committer_timestamp().to_string_or_empty(),
            self.changed_path_ids().to_space_separated_string().quoted(), self.changed_snapshot_count().to_string_or_empty(),
            self.message().to_string_or_empty().escape_quotes().quoted(), self.message_length().to_string_or_empty()
        ]]
    }
}

impl CSVItem for Snapshot {
    fn column_headers() -> Vec<&'static str> {
        vec!["snapshot_id", "content"]
    }
    fn row(&self) -> Vec<String>  {
        vec![ self.id().to_string(),
              self.contents().to_string().escape_quotes().quoted() ]
    }
    fn rows(&self) -> Vec<Vec<String>> {
        vec![vec![
            self.id().to_string(),
            self.contents().to_string().escape_quotes().quoted(),
        ]]
    }
}
impl_csv_item_with_data_inner!(Snapshot);

impl CSVItem for ProjectMetadata {
    fn column_headers() -> Vec<&'static str> { vec![
        "project_id", "is_fork", "is_archived", "is_disabled", "star_gazers", "watchers", "size",
        "open_issues", "forks", "subscribers", "license", "description", "homepage", "language",
        "has_issues", "has_downloads", "has_wiki", "has_pages", "created", "updated", "pushed",
        "master",
    ] }
    fn row(&self) -> Vec<String>  {
        vec![
            self.id.to_string(),
            self.is_fork.to_string_or_empty(),
            self.is_archived.to_string_or_empty(),
            self.is_disabled.to_string_or_empty(),
            self.star_gazers.to_string_or_empty(),
            self.watchers.to_string_or_empty(),
            self.size.to_string_or_empty(),
            self.open_issues.to_string_or_empty(),
            self.forks.to_string_or_empty(),
            self.subscribers.to_string_or_empty(),
            self.license.to_string_or_empty(),
            self.description.to_string_or_empty(),
            self.homepage.to_string_or_empty(),
            self.language.to_string_or_empty(),
            self.has_issues.to_string_or_empty(),
            self.has_downloads.to_string_or_empty(),
            self.has_wiki.to_string_or_empty(),
            self.has_pages.to_string_or_empty(),
            self.created.to_string_or_empty(),
            self.updated.to_string_or_empty(),
            self.pushed.to_string_or_empty(),
            self.default_branch.to_string_or_empty(),
        ]
    }
    fn rows(&self) -> Vec<Vec<String>> {
        vec![vec![
            self.id.to_string(),
            self.is_fork.to_string_or_empty(),
            self.is_archived.to_string_or_empty(),
            self.is_disabled.to_string_or_empty(),
            self.star_gazers.to_string_or_empty(),
            self.watchers.to_string_or_empty(),
            self.size.to_string_or_empty(),
            self.open_issues.to_string_or_empty(),
            self.forks.to_string_or_empty(),
            self.subscribers.to_string_or_empty(),
            self.license.to_string_or_empty(),
            self.description.to_string_or_empty(),
            self.homepage.to_string_or_empty(),
            self.language.to_string_or_empty(),
            self.has_issues.to_string_or_empty(),
            self.has_downloads.to_string_or_empty(),
            self.has_wiki.to_string_or_empty(),
            self.has_pages.to_string_or_empty(),
            self.created.to_string_or_empty(),
            self.updated.to_string_or_empty(),
            self.pushed.to_string_or_empty(),
            self.default_branch.to_string_or_empty(),
        ]]
    }
}
impl_csv_item_with_data_inner!(ProjectMetadata);

impl CSVItem for Head {
    fn column_headers() -> Vec<&'static str> {
        vec!["name", "commit_id"]
    }
    fn row(&self) -> Vec<String> {
        vec![self.name(), self.commit_id().to_string()]
    }
    fn rows(&self) -> Vec<Vec<String>> {
        vec![vec![self.name(), self.commit_id().to_string()]]
    }
}
impl_csv_item_with_data_inner!(Head);

// --- loading from CSV ----------------------------------------------------------------------------

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

// --- convenience functions -----------------------------------------------------------------------

pub trait JoinConvenience {
    fn to_space_separated_string(&self) -> String;
    fn to_comma_separated_string(&self) -> String;
    fn to_newline_separated_string(&self) -> String;
}

impl<T> JoinConvenience for Vec<T> where T: Display {
    fn to_space_separated_string(&self) -> String {
        self.iter().map(|s| s.to_string()).join(" ")
    }
    fn to_comma_separated_string(&self) -> String {
        self.iter().map(|s| s.to_string()).join(","
        )
    }
    fn to_newline_separated_string(&self) -> String {
        self.iter().map(|s| s.to_string()).join("\n"
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
    fn to_newline_separated_string(&self) -> String {
        self.as_ref().map_or(String::new(),|v| v.to_newline_separated_string())
    }
}

impl<T> JoinConvenience for &Vec<T> where T: Display {
    fn to_space_separated_string(&self) -> String {
        self.iter().map(|s| s.to_string()).join(" ")
    }
    fn to_comma_separated_string(&self) -> String {
        self.iter().map(|s| s.to_string()).join(",")
    }
    fn to_newline_separated_string(&self) -> String {
        self.iter().map(|s| s.to_string()).join("\n")
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

// ---- dump ---------------------------------------------------------------------------------------

pub trait Dump {
    fn dump_all_info_to<S>(self, location: S) -> Result<(), std::io::Error> where S: Into<String>;
}

impl<'a, I> Dump for I where I: Iterator<Item=ItemWithData<'a, Project>> {
    fn dump_all_info_to<S>(self, location: S) -> Result<(), std::io::Error> where S: Into<String> {

        let dir_path = PathBuf::from(location.into());
        create_dir_all(&dir_path)?;

        macro_rules! create_file {
             ($location:expr) => {{
                 let path = {
                     let mut path = dir_path.clone();
                     path.push($location.to_owned());
                     path
                 };
                 std::fs::File::create(path)
             }}
        }

        macro_rules! create_dir {
             ($location:expr) => {{
                 let dir_path = {
                     let mut dir_path = dir_path.clone();
                     dir_path.push($location.to_owned());
                     dir_path
                 };
                 std::fs::create_dir_all(&dir_path)?;
                 dir_path
             }}
        }

        let mut project_sink            = create_file!("projects.csv")?;
        let mut commit_sink             = create_file!("commits.csv")?;
        let mut user_sink               = create_file!("users.csv")?;
        let mut path_sink               = create_file!("paths.csv")?;

        let snapshot_dir             = create_dir!("snapshots");

        println!("--");

        let mut project_commit_map_sink = create_file!("project_commit_map.csv")?;
        let mut project_user_map_sink   = create_file!("project_user_map.csv")?;
        let mut commit_parent_map_sink  = create_file!("commit_parent_map.csv")?;
        let mut commit_change_map_sink  = create_file!("commit_change_map.csv")?;

        println!("..");

        let mut visited_commits:   HashSet<CommitId>   = HashSet::new();
        let mut visited_users:     HashSet<UserId>     = HashSet::new();
        let mut visited_paths:     HashSet<PathId>     = HashSet::new();
        let mut visited_snapshots: HashSet<SnapshotId> = HashSet::new();

        println!("<<");

        eprintln!("Dumping to directory at {}", dir_path.as_os_str().to_str().unwrap_or("???"));
        eprintln!("Initializing CSV files at {}", dir_path.as_os_str().to_str().unwrap_or("???"));

        ItemWithData::<'a, Project>::write_column_headers_to(&mut project_sink)?;
        ItemWithData::<'a, Commit>::write_column_headers_to(&mut commit_sink)?;
        ItemWithData::<'a, Path>::write_column_headers_to(&mut path_sink)?;
        ItemWithData::<'a, User>::write_column_headers_to(&mut user_sink)?;
        //ItemWithData::<'a, Snapshot>::write_column_headers_to(&mut snapshot_sink)?;

        <(ProjectId, CommitId)>::write_column_headers_to(&mut project_commit_map_sink)?;
        <(ProjectId, UserId)>::write_column_headers_to(&mut project_user_map_sink)?;
        <(CommitId, ItemWithData::<'a, Change>)>::write_column_headers_to(&mut commit_change_map_sink)?;
        <(CommitId, ProjectId)>::write_column_headers_to(&mut commit_parent_map_sink)?;

        println!(">>");

        for project in self {
            eprintln!("Dumping data for project {}", project.url());
            eprintln!("  - project info");
            project.write_csv_items_to(&mut project_sink)?;

            let commits: Vec<ItemWithData<Commit>> = project.commits_with_data().unwrap_or(vec![]);
            eprintln!("  - project-commit mapping & info");
            for commit in commits {
                (project.id(), commit.id()).write_csv_items_to(&mut project_commit_map_sink)?;
                if !visited_commits.contains(&commit.id()) {
                    commit.write_csv_items_to(&mut commit_sink)?;
                    visited_commits.insert(commit.id());
                }

                let changes = commit.changes_with_data().unwrap_or(vec![]);
                for change in changes {
                    if let Some(path) = &change.path() {
                        if !visited_paths.contains(&path.id()) {
                            path.write_csv_items_to(&mut path_sink)?;
                            visited_paths.insert(path.id());
                        }
                    }

                    if let Some(snapshot) = &change.snapshot() {
                        if !visited_snapshots.contains(&snapshot.id()) {
                            let mut path = snapshot_dir.clone();
                            path.push(snapshot.id().to_string());
                            snapshot.write_contents_to(path)?;
                            visited_snapshots.insert(snapshot.id());
                        }
                    }

                    (project.id(), change).write_csv_items_to(&mut commit_change_map_sink)?;
                }
            }

            let users: Vec<ItemWithData<User>> = project.users_with_data().unwrap_or(vec![]);
            eprintln!("  - project-user mapping & info");
            for user in users {
                (project.id(), user.id()).write_csv_items_to(&mut project_user_map_sink)?;
                if !visited_users.contains(&user.id()) {
                    user.write_csv_items_to(&mut user_sink)?;
                    visited_users.insert(user.id());
                }
            }
        }
        eprintln!("Done dumping to directory at {}", dir_path.as_os_str().to_str().unwrap_or("???"));
        Ok(())
    }
}
