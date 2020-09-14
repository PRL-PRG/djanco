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
pub mod dump;
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