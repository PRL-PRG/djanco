mod names;
#[macro_use] pub mod require;
#[macro_use] pub mod log;
#[macro_use] pub mod data;

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

use std::fmt;

use crate::objects::*;
use crate::log::*;
use crate::djanco::*;

/**
 * This is a Djanco API starting point. Query and database construction starts here.
 */
pub struct Djanco;

impl Djanco {
    pub fn from<S: Into<String>>(path: S, seed: u128, timestamp: Month) -> Lazy {
        let spec = Spec::new(path, seed, timestamp, LogLevel::Verbose);
        Lazy::from(spec)
    }
}

/** Errors **/
pub struct Error { message: String }
impl Error {
    pub fn from<S>(message: S) -> Self where S: Into<String> { Error { message: message.into() } }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.message) }
}

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
        let database = Djanco::from("/dejavuii/dejacode/dataset-tiny", 0, Month::August(2020));

        database.projects()
             .filter_by_attrib(require::AtLeast(project::Commits, 28))
             .group_by_attrib(project::Stars)
             .filter_by_attrib(require::AtLeast(project::Stars, 1))
             .filter_by_attrib(require::AtLeast(project::Commits, 25))
             .filter_by_attrib(require::AtLeast(project::Users, 2))
             .filter_by_attrib(require::Same(project::Language, "Rust"))
             .filter_by_attrib(require::Matches(project::URL, regex!("^https://github.com/PRL-PRG/.*$")))
             .sort_by_attrib(project::Stars)
             .sample(sample::Top(2))
             .squash()
             .select_attrib(project::Id)
             .to_csv("projects.csv").unwrap();
    }
}