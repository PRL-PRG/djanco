#![type_length_limit="1405002"]
#![recursion_limit="512"] // For Select10 in query
//#![recursion_limit="1024"] // For Select20 in query

             pub mod fraction;
             pub mod ordf64;
             pub mod commandline;
             pub mod weights_and_measures;
#[macro_use] pub mod log;
             pub mod query;
             pub mod csv;
#[macro_use] pub mod attrib;
             pub mod metadata;
             pub mod persistent;
             pub mod iterators;
             pub mod tuples;
             pub mod data;
             pub mod objects;
             pub mod receipt;
             pub mod spec;
             pub mod time;
             pub mod piracy;

#[macro_use] extern crate mashup;
