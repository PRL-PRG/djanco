pub mod require;
pub mod sample;
pub mod attrib;
pub mod project;

//mod pythagorean;
#[macro_use] mod log;
pub mod data;
pub mod objects;
pub mod csv;
//pub mod dump;
//mod io;
//pub mod query;
//pub mod cachedb;
pub mod meta;
//pub mod mockdb;
//pub mod selectors;

use std::path::PathBuf;
use dcd::DCD;
use std::marker::PhantomData;
use itertools::Itertools;
//use crate::meta::*;
use std::hash::Hash;
use std::rc::{Rc, Weak};
use std::cell::{RefCell};
use std::ops::Range;
use std::borrow::Borrow;
use std::iter::Map;
use std::collections::{HashSet, VecDeque};
use std::time::Duration;
use crate::csv::WithNames;
use crate::objects::{Project, Commit, User, Path, ProjectId, CommitId, UserId, PathId, Month};
use crate::log::LogLevel;
use crate::data::Data;
use std::fmt;
use crate::attrib::{LoadFilter, Group, FilterEach, SortEach, SampleEach, SelectEach};

/**
 * This is a Djanco API starting point. Query and database construction starts here.
 */
struct Djanco;

impl Djanco {
    pub fn from<S: Into<String>>(path: S, seed: u128, timestamp: Month) -> () {
        //DjancoPrototype::from(path, seed, timestamp)
        unimplemented!()
    }
}

struct Error { message: String }

impl Error {
    pub fn from<S>(message: S) -> Self where S: Into<String> { Error { message: message.into() } }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.message) }
}

#[cfg(test)]
mod tests {
    //use crate::{Djanco, DataSource, ProjectGroup, Ops, GroupOps, regex, project, require, sample, csv::*, objects::*};

    //#[test]
    // fn example() {
    //     let database = Djanco::from("/dejavuii/dejacode/dataset-tiny", 0,
    //                                 Month::August(2020));
    //     database
    //         .projects()
    //         .group_by_attrib(project::Stars)
    //         .filter_each_by_attrib(require::AtLeast(project::Stars, 1))
    //         .filter_each_by_attrib(require::AtLeast(project::Commits, 25))
    //         .filter_each_by_attrib(require::AtLeast(project::Users, 2))
    //         .filter_each_by_attrib(require::Same(project::Language, "Rust"))
    //         .filter_each_by_attrib(require::Matches(project::URL, regex!("^https://github.com/PRL-PRG/.*$")))
    //         .sort_each_by_attrib(project::Stars)
    //         .sample_each(sample::Top(2))
    //         .squash()
    //         .select(project::Id)
    //         .to_csv("projects.csv").unwrap()
    // }
}