#[macro_use] pub mod require;
#[macro_use] pub mod log;
#[macro_use] pub mod data;

pub mod names;
pub mod persistence;
pub mod djanco;
pub mod sample;
pub mod attrib;
pub mod project;
pub mod objects;
pub mod csv;
pub mod meta;

//mod pythagorean;
//pub mod dump;
//mod io;
//pub mod query;
//pub mod cachedb;

//pub mod mockdb;
//pub mod selectors;
//__lib.rs
//csv2.rs

use crate::objects::*;
use crate::log::*;
use crate::djanco::*;

/**
 * This is a Djanco API starting point. Query and database construction starts here.
 */
pub struct Djanco;

impl Djanco {
    pub fn from<S: Into<String>>(warehouse_path: S, seed: u128, timestamp: Month) -> Lazy {
        let spec = Spec::new(warehouse_path, None, seed, timestamp, LogLevel::Verbose);
        Lazy::from(spec)
    }

    pub fn cached<S: Into<String>>(warehouse_path: S, cache_path: S, seed: u128, timestamp: Month) -> Lazy {
        let spec = Spec::new(warehouse_path, Some(cache_path), seed, timestamp, LogLevel::Verbose);
        Lazy::from(spec)
    }

    pub fn from_spec(spec: Spec) -> Lazy {
        Lazy::from(spec)
    }
}

/** Errors **/
// pub struct Error { message: String }
// impl Error {
//     pub fn from<S>(message: S) -> Self where S: Into<String> { Error { message: message.into() } }
// }
// impl fmt::Display for Error {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.message) }
// }
// impl fmt::Debug for Error {
//     fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result { write!(f, "{}", self.message) }
// }

#[cfg(test)]
mod tests {
    use crate::Djanco;
    use crate::project;
    use crate::require;
    use crate::sample;
    use crate::objects::*;
    use crate::csv::*;

    #[test]
    fn example() {
        let database = Djanco::from("/dejavuii/dejacode/dataset-tiny", 0, Month::August(2020))
            .with_cache("/dejavuii/dejacode/cache-tiny");
            //.with_filter(require::AtLeast(project::Commits, 10));

        database.projects()
             //.filter_by_attrib(require::AtLeast(project::Commits, 28))
             //.group_by_attrib(project::Stars)
             //.filter_by_attrib(require::AtLeast(project::Stars, 1))
             //.filter_by_attrib(require::AtLeast(project::Commits, 25))
             //.filter_by_attrib(require::AtLeast(project::Users, 2))
             //.filter_by_attrib(require::Same(project::Language, "Rust"))
             //.filter_by_attrib(require::Matches(project::URL, regex!("^https://github.com/PRL-PRG/.*$")))
             //.sort_by_attrib(project::Stars)
             //.sample(sample::Top(2))
             //.squash()
             //.select_attrib(project::Id)
             .to_csv("projects_.csv").unwrap();
    }
}

// TODO
// * cache preprocessed data for greater good
// * CommitsWhere, PathsWhere, UsersWhere, etc.
// * snapshots
// * keep and produce receipt snippets
// * plug in dump