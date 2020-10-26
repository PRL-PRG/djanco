#[macro_use] pub mod require;
#[macro_use] pub mod log;
#[macro_use] pub mod data;
#[macro_use] pub mod receipt;

pub mod djanco;

pub mod time;

pub mod attrib;
pub mod prototype;
pub mod sample;
pub mod stats;
pub mod retrieve;

pub mod project;
pub mod path;
pub mod user;
pub mod commit;

pub mod objects;
pub mod meta;

pub mod names;
pub mod persistence;

pub mod csv;
pub mod dump;

mod helpers;

//pub mod io;
//pub mod query;

use crate::log::*;
use crate::djanco::*;
use crate::time::Month;

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

pub mod message {
    use crate::attrib::Attribute;

    #[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Length;
    impl Attribute for Length   {}
}

//** Errors **//
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