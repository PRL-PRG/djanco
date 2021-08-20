#![type_length_limit="1405002"]

             pub mod database;
             pub mod fraction;
             pub mod ordf64;
             pub mod weights_and_measures;
#[macro_use] pub mod log;
             pub mod csv;
#[macro_use] pub mod attrib;
             pub mod iterators;
             pub mod tuples;
             pub mod objects;
             pub mod receipt;
             pub mod spec;
#[macro_use] pub mod time;
             pub mod dump;
             mod piracy;
             mod product;
#[cfg(test)] mod testing;
#[macro_use] pub mod utils;
             pub mod commandline;

#[macro_use] extern crate mashup;

// Re-export
pub use commandline::*;

// TODO features
// maybe it's time to capitulate from the whole ItemWithData oidea and just make it a trait.
// CSV export (for entire database)
// commit frequency
// fill in CSV-capable objects
// maybe length for all strings
// maybe non-empty precicate for vectors
// buckets
// Fraction vs f64
// more comprehensive unit tests
// flat_map select
// explore parallelism
// prefiltering
// dump metadata, also make raw metadata accessible from objects::Project
// TODO rename Users to Contributors

use std::fs::{remove_file, remove_dir_all, read_dir, metadata};
use std::iter::{Sum, FromIterator};
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::collections::*;
use std::env;
use std::path::PathBuf;
use std::fmt::Display;

use itertools::Itertools;
use rand_pcg::Pcg64Mcg;
use rand::SeedableRng;
use rand::seq::IteratorRandom;
use chrono::{NaiveDateTime, DateTime, Utc};
use anyhow::*;

use parasite;
use parasite::{StoreKind};

use crate::attrib::*;
use crate::fraction::*;
use crate::database::Database;
use crate::log::{Log, Verbosity};
use crate::database::source::{Source, MERGED_SUBSTORE_DIR_NAME};

pub type Timestamp = i64; // Epoch
pub type Percentage = u8; // Positive integer value 0-100.

pub trait AsTimestamp {
    fn as_naive_date_string(&self) -> String;
    fn as_utc_rfc2822_string(&self) -> String;
    fn as_utc_rfc3339_string(&self) -> String;
}

impl AsTimestamp for Timestamp {
    fn as_naive_date_string(&self) -> String {
        NaiveDateTime::from_timestamp(*self, 0).to_string()
    }
    fn as_utc_rfc2822_string(&self) -> String {
        DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(*self, 0), Utc).to_rfc2822()
    }
    fn as_utc_rfc3339_string(&self) -> String {
        DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(*self, 0), Utc).to_rfc3339()
    }
}

pub mod store {
    use std::fmt::Display;

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Ord, PartialOrd, serde::Serialize, serde::Deserialize)]
    pub enum Language {
        C,
        Cpp,
        CSharp,
        Clojure,
        CoffeeScript,
        Erlang,
        Go,
        Haskell,
        HTML,
        Java,
        JavaScript,
        ObjectiveC,
        Perl,
        PHP,
        Python,
        Ruby,
        Scala,
        Shell,
        TypeScript,
    }

    impl Display for Language {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            match self {
                Language::C => write!(f, "C"),
                Language::Cpp => write!(f, "Cpp"),
                Language::CSharp => write!(f, "CSharp"),
                Language::Clojure => write!(f, "Clojure"),
                Language::CoffeeScript => write!(f, "CoffeeScript"),
                Language::Erlang => write!(f, "Erlang"),
                Language::Go => write!(f, "Go"),
                Language::Haskell => write!(f, "Haskell"),
                Language::HTML => write!(f, "HTML"),
                Language::Java => write!(f, "Java"),
                Language::JavaScript => write!(f, "JavaScript"),
                Language::ObjectiveC => write!(f, "ObjectiveC"),
                Language::Perl => write!(f, "Perl"),
                Language::PHP => write!(f, "PHP"),
                Language::Python => write!(f, "Python"),
                Language::Ruby => write!(f, "Ruby"),
                Language::Scala => write!(f, "Scala"),
                Language::Shell => write!(f, "Shell"),
                Language::TypeScript => write!(f, "TypeScript"),
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Ord, PartialOrd, serde::Serialize, serde::Deserialize)]
pub enum Store {
    Small,
    Large(store::Language),
    Generic,
}

impl Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Store::Small => write!(f, "SmallProjects"),
            Store::Large(language) => write!(f, "{}", language),
            Store::Generic => write!(f, "Generic"),
        }
    }
}

impl Store {
    pub fn all_subsets() -> Vec<Store> {
        StoreKind::all().map(|kind| Store::from(kind)).collect()
    }
    pub fn discretize_selection(selection: Vec<Store>) -> Vec<Store> {
        let mut selection = 
            if selection.len() == 0 { Self::all_subsets() } else { selection };
        selection.sort();
        selection
    }
    pub fn kind(&self) -> StoreKind {
        match self {
            Store::Generic => StoreKind::Generic,
            Store::Small => StoreKind::SmallProjects,
            Store::Large(store::Language::JavaScript) => StoreKind::JavaScript,
            Store::Large(store::Language::C) => StoreKind::C,
            Store::Large(store::Language::Cpp) => StoreKind::Cpp,
            Store::Large(store::Language::CSharp) => StoreKind::CSharp,
            Store::Large(store::Language::Clojure) => StoreKind::Clojure,
            Store::Large(store::Language::CoffeeScript) => StoreKind::CoffeeScript,
            Store::Large(store::Language::Erlang) => StoreKind::Erlang,
            Store::Large(store::Language::Go) => StoreKind::Go,
            Store::Large(store::Language::Haskell) => StoreKind::Haskell,
            Store::Large(store::Language::HTML) => StoreKind::Html,
            Store::Large(store::Language::Java) => StoreKind::Java,
            Store::Large(store::Language::ObjectiveC) => StoreKind::ObjectiveC,
            Store::Large(store::Language::Perl) => StoreKind::Perl,
            Store::Large(store::Language::PHP) => StoreKind::Php,
            Store::Large(store::Language::Python) => StoreKind::Python,
            Store::Large(store::Language::Ruby) => StoreKind::Ruby,
            Store::Large(store::Language::Scala) => StoreKind::Scala,
            Store::Large(store::Language::Shell) => StoreKind::Shell,
            Store::Large(store::Language::TypeScript) => StoreKind::TypeScript,
        }
    }
}
impl std::convert::From<StoreKind> for Store {
    fn from(kind: StoreKind) -> Self {
        match kind {
            StoreKind::Generic => Store::Generic,
            StoreKind::SmallProjects => Store::Small,
            StoreKind::JavaScript => Store::Large(store::Language::JavaScript),
            StoreKind::C => Store::Large(store::Language::C),
            StoreKind::Cpp => Store::Large(store::Language::Cpp),
            StoreKind::CSharp => Store::Large(store::Language::CSharp),
            StoreKind::Clojure => Store::Large(store::Language::Clojure),
            StoreKind::CoffeeScript => Store::Large(store::Language::CoffeeScript),
            StoreKind::Erlang => Store::Large(store::Language::Erlang),
            StoreKind::Go => Store::Large(store::Language::Go),
            StoreKind::Haskell => Store::Large(store::Language::Haskell),
            StoreKind::Html => Store::Large(store::Language::HTML),
            StoreKind::Java => Store::Large(store::Language::Java),
            StoreKind::ObjectiveC => Store::Large(store::Language::ObjectiveC),
            StoreKind::Perl => Store::Large(store::Language::Perl),
            StoreKind::Php => Store::Large(store::Language::PHP),
            StoreKind::Python => Store::Large(store::Language::Python),
            StoreKind::Ruby => Store::Large(store::Language::Ruby),
            StoreKind::Scala => Store::Large(store::Language::Scala),
            StoreKind::Shell => Store::Large(store::Language::Shell),
            StoreKind::TypeScript => Store::Large(store::Language::TypeScript),
            StoreKind::Unspecified =>
                panic!("StoreKind::Unspecified is a sentinel, so it is not expected to occur."),
        }
    }
}
impl std::convert::From<&str> for Store {
    fn from(str: &str) -> Self {
        if &str.to_lowercase() == "generic" {
            Store::Generic
        } else {
            StoreKind::from_string(str)
                .map(|kind| Self::from(kind))
                .expect(&format!("`{}` is not a valid store", str))
        }
    }
}
impl std::convert::From<String> for Store {
    fn from(string: String) -> Self {
        Store::from(string.as_str())
    }
}
// impl std::convert::Into<StoreKind> for Store {
//     fn into(self) -> StoreKind {
//         self.0
//     }
// }
// impl std::fmt::Display for Store { // FIXME delegate to parasite
//     fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
//         self.0.fmt(f)
//     }
// }

#[macro_export] macro_rules! stores {
    (All) => {
        Vec::<crate::Store>::new()
    };
    ($($t:tt)*) => {{
        let list: Vec<String> =
            std::stringify!($($t)*).split(",").map(|s| s.to_owned()).collect();
        let mut stores: Vec<crate::Store> = Vec::new();
        for name in list {
            let mut clean_name = name;
            clean_name.retain(|c| !c.is_whitespace());
            stores.push(Store::from(clean_name.as_str()));
        }
        stores
    }};
}

#[macro_export] macro_rules! store {
    ($($t:tt)+) => { stores!($($t)+) }
}

#[derive(Debug)]
pub struct CacheDir {
    root_dir: PathBuf,
    savepoint: Timestamp,
    substores: Vec<Store>,
}

impl CacheDir {
    pub fn from<S>( cache_path: S, savepoint: Timestamp, mut substores: Vec<Store>) -> Self where S: Into<String> {
        //<PathBuf as std::convert::From<String>>.from(root_dir.into());
        let root_dir: PathBuf = std::convert::From::<String>::from(cache_path.into());
        substores.sort();
        CacheDir { root_dir, savepoint, substores }
    }
    pub fn as_path(&self) -> std::path::PathBuf {
        let mut cache_dir = self.root_dir.clone();
        let stores = self.substores.iter().map(|store| store.to_string()).join("_");
        let date_time = chrono::NaiveDateTime::from_timestamp(self.savepoint, 0);
        cache_dir.push(stores);
        cache_dir.push(date_time.format("%Y-%m-%d-%H-%M-%S").to_string());
        cache_dir
    }
    pub fn as_string(&self) -> String {
        self.as_path().to_str().unwrap().to_owned()
    }
}

pub struct Djanco;
impl Djanco {
    pub fn from_spec<Sd, Sc>(dataset_path: Sd, cache_path: Sc, savepoint: Timestamp, substores: Vec<Store>, log: Log) -> anyhow::Result<Database> where Sd: Into<String>, Sc: Into<String> {
        Self::from_full_spec(dataset_path, cache_path, savepoint, substores, log, false, false)
    }
    // FIXME this still sucks
    pub fn from_full_spec<Sd, Sc>(dataset_path: Sd, cache_path: Sc, savepoint: Timestamp, substores: Vec<Store>, log: Log, preclean: bool, preclean_merged_substores: bool) -> anyhow::Result<Database> where Sd: Into<String>, Sc: Into<String> {
        //DatastoreView::new(&dataset_path.into(), savepoint).with_cache(cache_path)
        let cache_path = cache_path.into();
        let substores = Store::discretize_selection(substores);
        let cache_dir = CacheDir::from(cache_path.clone(), savepoint, substores.clone());
        if preclean {
            match read_dir(cache_dir.as_string()) {
                Err(e) => {
                    println!("Cannot clean directory {}, skipping. Reason: {}", cache_dir.as_string(), e);
                }
                Ok(contents) => 
                    contents.into_iter()
                        .flat_map(|e| e.map(|e| {
                            e.path().as_os_str().to_str().unwrap().to_owned()
                        }))
                        .filter(|path| preclean_merged_substores || path != MERGED_SUBSTORE_DIR_NAME)
                        .for_each(|path| {
                            if metadata(&path).unwrap().is_dir() {
                                println!("Cleaning {:?}", &path);
                                remove_dir_all(&path)                        
                            } else {
                                println!("Cleaning {:?}", &path);
                                remove_file(&path)
                            }.with_context(|| format!("Preclean error: cannot remove {:?}", path)).unwrap();
                        })
            };      
        }   
        let source = Source::new(dataset_path, cache_dir.as_string(), savepoint, substores)?;
        Ok(Database::new(source, cache_dir, log))
    }
    pub fn from_store<Sd>(dataset_path: Sd, savepoint: Timestamp, substores: Vec<Store>, preclean: bool, preclean_merged_substores: bool) -> Result<Database> where Sd: Into<String> {
        let dataset_path = dataset_path.into();
        let cache_path = env::var("DJANCO_CACHE_PATH").unwrap_or_else(|_| {
            let mut path = PathBuf::from(dataset_path.clone());
            let top = path.file_name()
                .expect(&format!("Cannot create a cache path from {}:
                        there's no parent directory to put the cache in", dataset_path))
                .to_str().unwrap().to_owned();
            path.pop();
            path.push("djanco".to_owned());
            path.push("cache".to_owned());
            path.push(top);
            path.into_os_string().to_str().unwrap().to_owned()
        });
        let log = Log::new(Verbosity::Log);
        Djanco::from_full_spec(dataset_path, cache_path, savepoint, substores, log, preclean, preclean_merged_substores)
    }
    pub fn from<Sd>(dataset_path: Sd, preclean: bool, preclean_merged_substores: bool) -> Result<Database>  where Sd: Into<String> {
        Djanco::from_store(dataset_path, chrono::Utc::now().timestamp(), vec![], preclean, preclean_merged_substores)
    }
    pub fn new() -> Result<Database> {
        let dataset_path = env::var("DJANCO_DATASET_PATH")
                .unwrap_or("/data/djcode/dataset".to_owned());
        let preclean = env::var("DJANCO_CACHE_PRECLEAN")
                .unwrap_or("false".to_owned()).parse::<bool>().unwrap();
        let preclean_merged_substores = env::var("DJANCO_CACHE_PRECLEAN")
                .unwrap_or("false".to_owned()).parse::<bool>().unwrap();                
        Djanco::from(dataset_path, preclean, preclean_merged_substores)
    }
    // pub fn from_config(config: &Configuration, savepoint: Timestamp, substores: Vec<Store>, log: Log) -> Result<Database> {
    //     Djanco::from_full_spec(
    //         config.dataset_path(), 
    //         config.cache_path(), 
    //         savepoint, 
    //         substores, 
    //         log, 
    //         config.preclean_cache, 
    //         config.preclean_merged_substores
    //     )
    // }
    pub fn from_config(options: &Configuration, savepoint: Timestamp, substores: Vec<Store>, log: Log) -> Result<Database> {
        Djanco::from_full_spec(
            options.dataset_path_as_str(), 
            options.cache_path_as_str(), 
            savepoint, 
            substores, 
            log, 
            options.preclean_cache, 
            options.preclean_merged_substores
        )
    }
}

macro_rules! impl_attribute_definition {
    [$object:ty, $attribute:ident ()] => {
        #[derive(Eq, PartialEq, Copy, Clone, Hash, Debug)] pub struct $attribute;
        impl Attribute for $attribute { type Object = $object; }
    };
    [$object:ty, $attribute:ident ( $($arg_type:ty),+ ) ] => {
        #[derive(Eq, PartialEq, Copy, Clone, Hash, Debug)] 
        pub struct $attribute($(pub $arg_type,)+);
        impl Attribute for $attribute { type Object = $object; }
    };
}

macro_rules! call_n {
    ($object:expr, $method:ident, $self:expr) => { 
        $object.$method() 
    };
    ($object:expr, $method:ident, $self:expr,) => { 
        $object.$method()
    };
    ($object:expr, $method:ident, $self:expr, $p0:ty) => { 
        $object.$method($self.0) 
    };
    ($object:expr, $method:ident, $self:expr, $p0:ty, $p1:ty) => { 
        $object.$method($self.0, $self.1) 
    };
    ($object:expr, $method:ident, $self:expr, $p0:ty, $p1:ty, $p2:ty) => { 
        $object.$method($self.0, $self.1, $self.2) 
    };
    ($object:expr, $method:ident, $self:expr, $p0:ty, $p1:ty, $p2:ty, $p3:ty) => { 
        $object.$method($self.0, $self.1, $self.2, $self.3) 
    };
    ($object:expr, $method:ident, $self:expr, $p0:ty, $p1:ty, $p2:ty, $p3:ty, $p4:ty) => { 
        $object.$method($self.0, $self.1, $self.2, $self.3, $self.4) 
    };
}

macro_rules! impl_attribute_getter {
    [! $object:ty, $attribute:ident ()] => { 
        impl<'a> Getter<'a> for $attribute {
            type IntoItem = Self::Object;
            fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                object.item.clone()
            }
        }
        impl<'a> OptionGetter<'a> for $attribute { 
            type IntoItem = Self::Object;
            fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                Some(object.item.clone())
            }
        }
    };
    [!+ $object:ty, $attribute:ident ()] => { 
        impl<'a> Getter<'a> for $attribute {
            type IntoItem = objects::ItemWithData<'a, Self::Object>;
            fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                object.clone()
            }
        }
        impl<'a> OptionGetter<'a> for $attribute {
            type IntoItem = objects::ItemWithData<'a, Self::Object>;
            fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                Some(object.clone())
            }
        }
    };
    [! $object:ty, $attribute:ident ( $($parameter:ty),* ), $small_type:ty, $getter:ident ] => { 
        impl<'a> Getter<'a> for $attribute {
            type IntoItem = $small_type;
            fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                call_n!(object, $getter, &self, $($parameter),* )
            }
        }
        impl<'a> OptionGetter<'a> for $attribute {
            type IntoItem = $small_type;
            fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                Some(call_n!(object, $getter, &self, $($parameter),* ))
            }
        }
    };
    [? $object:ty, $attribute:ident ( $($parameter:ty),* ), $small_type:ty, $getter:ident] => { 
        impl<'a> Getter<'a> for $attribute {
            type IntoItem = Option<$small_type>;
            fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                call_n!(object, $getter, &self, $($parameter),*)
            }
        }
        impl<'a> OptionGetter<'a> for $attribute {
            type IntoItem = $small_type;
            fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                call_n!(object, $getter, &self, $($parameter),*)
            }
        }
    };
    [!+ $object:ty, $attribute:ident ( $($parameter:ty),* ), $small_type:ty, $getter:ident] => { 
        impl<'a> Getter<'a> for $attribute {
            type IntoItem = objects::ItemWithData<'a, $small_type>;
            fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                call_n!(object, $getter, &self, $($parameter),*)
            }
        }
        impl<'a> OptionGetter<'a> for $attribute {
            type IntoItem = objects::ItemWithData<'a, $small_type>;
            fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                Some(call_n!(object, $getter, &self, $($parameter),*))
            }
        }
    };
    [?+ $object:ty, $attribute:ident ( $($parameter:ty),* ), $small_type:ty, $getter:ident] => { 
        impl<'a> Getter<'a> for $attribute {
            type IntoItem = Option<objects::ItemWithData<'a, $small_type>>;
            fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                call_n!(object, $getter, &self, $($parameter),*)
            }
        }
        impl<'a> OptionGetter<'a> for $attribute {
            type IntoItem = objects::ItemWithData<'a, $small_type>;
            fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                call_n!(object, $getter, &self, $($parameter),*)
            }
        }
    };
    [!+.. $object:ty, $attribute:ident ( $($parameter:ty),* ), $small_type:ty, $getter:ident] => { 
        impl<'a> Getter<'a> for $attribute {
            type IntoItem = Vec<objects::ItemWithData<'a, $small_type>>;
            fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                call_n!(object, $getter, &self, $($parameter),*)
            }
        }
        impl<'a> OptionGetter<'a> for $attribute {
            type IntoItem = Vec<objects::ItemWithData<'a, $small_type>>;
            fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                Some(call_n!(object, $getter, &self, $($parameter),*))
            }
        }
    };
    [?+.. $object:ty, $attribute:ident ( $($parameter:ty),* ), $small_type:ty, $getter:ident] => { 
        impl<'a> Getter<'a> for $attribute {
            type IntoItem = Option<Vec<objects::ItemWithData<'a, $small_type>>>;
            fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                call_n!(object, $getter, &self, $($parameter),*)
            }
        }
        impl<'a> OptionGetter<'a> for $attribute {
            type IntoItem = Vec<objects::ItemWithData<'a, $small_type>>;
            fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                call_n!(object, $getter, &self, $($parameter),*)
            }
        }
    };
}

macro_rules! impl_attribute_count {
    [! $object:ty, $attribute:ident ( $($parameter:ty),* ), $counter:ident] => {
        impl<'a> Countable<'a> for $attribute {
            fn count(&self, object: &objects::ItemWithData<'a, Self::Object>) -> usize {
                call_n!(object, $counter, &self, $($parameter),*)
            }
        }
        impl<'a> OptionCountable<'a> for $attribute {
            fn count(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<usize> {
                Some(call_n!(object, $counter, &self, $($parameter),*))
            }
        }
    };
    [? $object:ty, $attribute:ident ( $($parameter:ty),* ), $counter:ident] => {
        impl<'a> OptionCountable<'a> for $attribute {
            fn count(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<usize> {
                call_n!(object, $counter, &self, $($parameter),*)
            }
        }
    }
}

macro_rules! impl_attribute_filter {
    [! $object:ty, $attribute:ident] => {
        impl<'a> Filter<'a> for $attribute {
            type Item = $object;
            fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
                self.get(item_with_data)
            }
        }
    };
    [? $object:ty, $attribute:ident] => {
        impl<'a> Filter<'a> for $attribute {
            type Item = $object;
            fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
                self.get(item_with_data).unwrap_or(false)
            }
        }
    }
}

macro_rules! impl_attribute {
    [! $object:ty, $attribute:ident ] => { 
        impl_attribute_definition![$object, $attribute()];
        impl_attribute_getter![! $object, $attribute()];
    };        
    [!+ $object:ty, $attribute:ident] => { 
        impl_attribute_definition![$object, $attribute()];
        impl_attribute_getter![!+ $object, $attribute()];
    };
    [! $object:ty, $attribute:ident, bool, $getter:ident] => { 
        impl_attribute![! $object, $attribute(), bool, $getter];
    };
    [! $object:ty, $attribute:ident ($($parameter:ty),*), bool, $getter:ident ] => { 
        impl_attribute_definition![$object, $attribute($($parameter),*)];
        impl_attribute_getter![! $object, $attribute($($parameter),*), bool, $getter ];
        impl_attribute_filter![! $object, $attribute];
    };
    [! $object:ty, $attribute:ident, $small_type:ty, $getter:ident] => { 
        impl_attribute![! $object, $attribute(), $small_type, $getter];
    };
    [! $object:ty, $attribute:ident ($($parameter:ty),*), $small_type:ty, $getter:ident] => { 
        impl_attribute_definition![$object, $attribute($($parameter),*)];
        impl_attribute_getter![! $object, $attribute($($parameter),*), $small_type, $getter];
    };
    [!+ $object:ty, $attribute:ident, $small_type:ty, $getter:ident] => { 
        impl_attribute![!+ $object, $attribute(), $small_type, $getter];
    };
    [!+ $object:ty, $attribute:ident ($($parameter:ty),*), $small_type:ty, $getter:ident] => { 
        impl_attribute_definition![$object, $attribute($($parameter),*)]; 
        impl_attribute_getter![!+ $object, $attribute($($parameter),*), $small_type, $getter];
    };
    [? $object:ty, $attribute:ident, bool, $getter:ident] => {
        impl_attribute![? $object, $attribute(), bool, $getter];
    };
    [? $object:ty, $attribute:ident ($($parameter:ty),*), bool, $getter:ident] => {
        impl_attribute_definition![$object, $attribute($($parameter),*)];
        impl_attribute_getter![? $object, $attribute($($parameter),*), bool, $getter];
        impl_attribute_filter![? $object, $attribute];
    };
    [? $object:ty, $attribute:ident, $small_type:ty, $getter:ident] => {
        impl_attribute![? $object, $attribute(), $small_type, $getter];
    };
    [? $object:ty, $attribute:ident ($($parameter:ty),*), $small_type:ty, $getter:ident] => {
        impl_attribute_definition![$object, $attribute($($parameter),*)];
        impl_attribute_getter![? $object, $attribute($($parameter),*), $small_type, $getter];
    };
    [?+ $object:ty, $attribute:ident, $small_type:ty, $getter:ident] => {
        impl_attribute![?+ $object, $attribute(), $small_type, $getter];
    };
    [?+ $object:ty, $attribute:ident ($($parameter:ty),*), $small_type:ty, $getter:ident] => {
        impl_attribute_definition![$object, $attribute($($parameter),*)];
        impl_attribute_getter![?+ $object, $attribute($($parameter),*), $small_type, $getter];
    };
    [!.. $object:ty, $attribute:ident, $small_type:ty, $getter:ident, $counter:ident] => {
        impl_attribute![!.. $object, $attribute(), $small_type, $getter, $counter];
    };
    [!.. $object:ty, $attribute:ident ($($parameter:ty),*), $small_type:ty, $getter:ident, $counter:ident] => {
        impl_attribute_definition![$object, $attribute($($parameter),*)];
        impl_attribute_getter![! $object, $attribute($($parameter),*), Vec<$small_type>, $getter];
        impl_attribute_count![! $object, $attribute($($parameter),*), $counter];
    };
    [!+.. $object:ty, $attribute:ident, $small_type:ty, $getter:ident, $counter:ident] => {
        impl_attribute![!+.. $object, $attribute(), $small_type, $getter, $counter];
    };
    [!+.. $object:ty, $attribute:ident ($($parameter:ty),*), $small_type:ty, $getter:ident, $counter:ident] => {
        impl_attribute_definition![$object, $attribute($($parameter),*)];
        impl_attribute_getter![!+.. $object, $attribute($($parameter),*), $small_type, $getter];
        impl_attribute_count![! $object, $attribute($($parameter),*), $counter];
    };
    [?.. $object:ty, $attribute:ident, $small_type:ty, $getter:ident, $counter:ident] => {
        impl_attribute![?.. $object, $attribute(), $small_type, $getter, $counter];
    };
    [?.. $object:ty, $attribute:ident ($($parameter:ty),*), $small_type:ty, $getter:ident, $counter:ident] => {
        impl_attribute_definition![$object, $attribute($($parameter),*)];
        impl_attribute_getter![? $object, $attribute($($parameter),*), Vec<$small_type>, $getter];
        impl_attribute_count![? $object, $attribute($($parameter),*), $counter];
    };
    [?+.. $object:ty, $attribute:ident, $small_type:ty, $getter:ident, $counter:ident] => {
        impl_attribute![?+.. $object, $attribute(), $small_type, $getter, $counter];
    };
    [?+.. $object:ty, $attribute:ident ($($parameter:ty),*), $small_type:ty, $getter:ident, $counter:ident] => {
        impl_attribute_definition![$object, $attribute($($parameter),*)];
        impl_attribute_getter![?+.. $object, $attribute($($parameter),*), $small_type, $getter];
        impl_attribute_count![? $object, $attribute($($parameter),*), $counter];
    };
}

pub mod project {
    use crate::objects;
    use crate::time;
    use crate::attrib::*;
    use crate::Timestamp;
    use crate::Store;
    use crate::Percentage;

    /* 
     * Retrieves the entire Project object (wrapped in an ItemWithData object).
     * 
     * Useful for Select! to retrieve specific attribtues from inside the objects as well as the object itself.
     */
    impl_attribute![!+    objects::Project, Itself];

    /*
     * Retrieves the entire Project object (now wrapped in an ItemWithData object).
     * 
     * Useful for Select! to retrieve specific attribtues from inside the objects as well as the object itself.
     */
    impl_attribute![!     objects::Project, Raw];

    /*
     * Retrieves the ID of the project. It identifies the project uniquely in the dataset.
     */
    impl_attribute![!     objects::Project, Id, objects::ProjectId, id];

    /*
     * Retrieves the URL of the project.
     */
    impl_attribute![!     objects::Project, URL, String, url];

    /*
     * Retrieves the number of non-buggy issues in the project. 
     * A buggy issue is an issue that is marked as `buggy` in GH.
     * 
     * This is currrently not collected in the dataset, so it always yields None.
     */
    impl_attribute![?     objects::Project, Issues, usize, issue_count];

    /*
     * Retrieves the number of buggy issues in the project. 
     * A buggy issue is an issue that is marked as `buggy` in GH.
     * 
     * This is currrently not collected in the dataset, so it always yields None.
     */
    impl_attribute![?     objects::Project, BuggyIssues, usize, buggy_issue_count];

    /*
     * Retrieves the number of buggy and non-buggy issues in the project. 
     * Its calculated as the sum of BuggyIssues and Issues.
     * A buggy issue is an issue that is marked as `buggy` in GH.
     * 
     * This is currrently not collected in the dataset, so it always yields None.
     */
    impl_attribute![?     objects::Project, AllIssues, usize, combined_issue_count];

    /*
     * Retrieves the number of open issues in the project. 
     * An open issue is an issue that is marked as such in GH.
     * 
     * This attribute is retrieved from GH REST API.
     */
    impl_attribute![?     objects::Project, OpenIssues, usize, open_issue_count];

    /*
     * Returns true if the project is a fork.
     * A fork is a copy of another user's repository. A project is a fork if it has been forked in GH. 
     * Projects that are implicitly forked (copied manually and given a new origin) are not covered by this attribute.
     * 
     * This attribute is retrieved from GH REST API.
     */
    impl_attribute![?     objects::Project, IsFork, bool, is_fork];

    /*
     * Returns true if the project was archived in GH.
     * An archived project is a project that is no longer actively maintained and read-only.
     * 
     * This attribute is retrieved from GH REST API.
     */
    impl_attribute![?     objects::Project, IsArchived, bool, is_archived];

    /*
     * Returns true if the project was disabled in GH.
     * A disabled project is a project that is no longer used. 
     * It is hidden from the GH website and inaccessible GH API.
     * 
     * This attribute is retrieved from GH REST API.
     */
    impl_attribute![?     objects::Project, IsDisabled, bool, is_disabled];

    /*
     * Returns the number of stars this project received.
     * A star is akin to a `like` on the GH website. 
     * 
     * This attribute is retrieved from GH REST API.
     */
    impl_attribute![?     objects::Project, Stars, usize, star_count];

    /*
     * Returns the number of watchers this project has.
     * Subscribing to a project is done via a button on the GH website. 
     * It is the equivalent of `following` it. 
     * When a user watches a project they receive notifications about all discussions (issues, pull requests, etc.) in that project.
     * 
     * This attribute is retrieved from GH REST API.
     */
    impl_attribute![?     objects::Project, Watchers, usize, watcher_count];

    /*
     * Returns the number of subsribers this project has.
     * Watching a project is done via a button on the GH website. 
     * It is the equivalent of `following` it. 
     * When a user watches a project they receive notifications about all activity (commits, etc.) in that project.
     * 
     * This attribute is retrieved from GH REST API.
     */
    impl_attribute![?     objects::Project, Subscribers, usize, subscriber_count];

    /*
     * The size of the project on disk.
     * This is the size of the repository directory. If a project is forked, the size excludes the size of the original project.
     * 
     * This attribute is retrieved from GH REST API.
     */
    impl_attribute![?     objects::Project, Size, usize, size];
    
    /*
     * Retrieves the number of projects that are forks of this project (in all of GH).
     * A fork is a copy of another user's repository. A project is a fork if it has been forked in GH. 
     * Projects that are implicitly forked (copied manually and given a new origin) are not covered by this attribute.
     * 
     * This attribute is retrieved from GH REST API.
     */
    impl_attribute![?     objects::Project, Forks, usize, fork_count];

    /*
     * This project's license.
     * GH recognizes the license from the project's tree.
     * 
     * This attribute is retrieved from GH REST API.
     */
    impl_attribute![?     objects::Project, License, String, license];

    /*
     * Retrieves this project's language.
     * The language is the major language as recognized by GH.
     * There are a number of rpedefined languages we recognize. 
     * If the language fits neither or these, it is reported as None.
     * 
     * This attribute is retrieved from GH REST API.
     */
    impl_attribute![?     objects::Project, Language, objects::Language, language];

    /*
     * Retrieves this project's substore.
     * When Parasite downloads projects it allocates them to distinct substores. 
     * Generally there is one substore for small proejcts and a separate substore for each language. 
     * If a datastore is created by merging multiple substores into a sinlge substore, that substore is called general.
     */
    impl_attribute![?     objects::Project, Substore, Store, substore];

    /*
     * Retrieves the descriptin of the project.
     * The description is set on the GH website.
     * 
     * This attribute is retrieved from GH REST API.
     */
    impl_attribute![?     objects::Project, Description, String, description];

    /*
     * Retrieves URL of the project's homepage.
     * The homepage is set on the GH website.
     * 
     * This attribute is retrieved from GH REST API.
     */
    impl_attribute![?     objects::Project, Homepage, String, homepage];

    /*
     * Returns true if the project has the issues page active on GH.
     * The issues page is turned on in the project's settings on GH. It is turned on by default.
     * 
     * This attribute is retrieved from GH REST API.
     */
    impl_attribute![?     objects::Project, HasIssues, bool, has_issues];

    /*
     * TODO
     * 
     * This attribute is retrieved from GH REST API.
     */
    impl_attribute![?     objects::Project, HasDownloads, bool, has_downloads];

    /*
     * Returns true if the project has the wiki page active on GH.
     * The wiki page is turned on in the project's settings on GH. It is turned on by default.
     * 
     * This attribute is retrieved from GH REST API.
     */
    impl_attribute![?     objects::Project, HasWiki, bool, has_wiki];

    /*
     * Returns true if the project has an associated github.io page.
     * The page is set up on in the project's settings on GH. It is turned off by default.
     * 
     * This attribute is retrieved from GH REST API.
     */
    impl_attribute![?     objects::Project, HasPages, bool, has_pages];

    /*
     * Returns the timestamp at which the project was created on GH.
     * 
     * This attribute is retrieved from GH REST API.
     */
    impl_attribute![?     objects::Project, Created, Timestamp, created];

    /*
     * Returns the timestamp at which the project was last updated on GH.
     * This involves any changes to the repository in GH (eg. pushing a commit) and 
     * strictly GH-related changes (eg. changing the project description), 
     * including GH-internal updates (eg. last time the language composition was recalculated).
     * 
     * This attribute is retrieved from GH REST API.
     */
    impl_attribute![?     objects::Project, Updated, Timestamp, updated];

    /*
     * Returns the timestamp at which the project was last upshed to on GH.
     * This time reflects the most recent push of a commit to any of the branches in the project.
     * 
     * This attribute is retrieved from GH REST API.
     */
    impl_attribute![?     objects::Project, Pushed, Timestamp, pushed];

    /*
     * Retrieves the name of the default branch as set in GH, eg. "master" or "main".
     * 
     * This attribute is retrieved from GH REST API.
     */
    impl_attribute![?     objects::Project, DefaultBranch, String, default_branch];

    /*
     * Retrieves the lifespan of the project calculated as the duration between the earliest and most recent commit.
     */
    impl_attribute![?     objects::Project, Age, time::Duration, lifetime];

    /*
     * Retrieves the heads of this project.
     * A head represents the last commit of a given named branch.
     */
    impl_attribute![?+..  objects::Project, Heads, objects::Head, heads_with_data, head_count];

    /*
     * Returns the IDs of all the commits in all the branches of this project.
     */
    impl_attribute![?..   objects::Project, CommitIds, objects::CommitId, commit_ids, commit_count];

    /* 
     * Returns the IDs of all the commits in the main branch of this project.  
     */
    impl_attribute![?..   objects::Project, MainBranchCommitIds, objects::CommitId, main_branch_commit_ids, main_branch_commit_count];

    /* 
     * Returns the commits that belong to the main branch of this project. 
     */
    impl_attribute![?+..   objects::Project, MainBranchCommits, objects::Commit, main_branch_commits_with_data, main_branch_commit_count];
    /*
     * Returns the IDs of all the users who authored any commit in any of the branches of this project.
     */
    impl_attribute![?..   objects::Project, AuthorIds, objects::UserId, author_ids, author_count];

    /*
     * Returns the IDs of all the users who committed any commit in any of the branches of this project.
     */
    impl_attribute![?..   objects::Project, CommitterIds, objects::UserId, committer_ids, committer_count];

    /*
     * Returns the IDs of all the users who authored or committed anything in any of the branches of this project.
     */
    impl_attribute![?..   objects::Project, UserIds, objects::UserId, user_ids, user_count];

    /*
     * Returns the IDs in of all the file paths constituting any of the branches of this project.
     */
    impl_attribute![?..   objects::Project, PathIds, objects::PathId, path_ids, path_count];

    /*
     * Returns the IDs in of all the file contents constituting any of the branches of this project.
     */
    impl_attribute![?..   objects::Project, SnapshotIds, objects::SnapshotId, snapshot_ids, snapshot_count];

    /*
     * Returns all the commits in all the branches of this project.
     */
    impl_attribute![?+..  objects::Project, Commits, objects::Commit, commits_with_data, commit_count];

    /*
     * Returns all the users who authored any commit in any of the branches of this project.
     */
    impl_attribute![?+..  objects::Project, Authors, objects::User, authors_with_data, author_count];

    /*
     * Returns all the users who committed any commit in any of the branches of this project.
     */
    impl_attribute![?+..  objects::Project, Committers, objects::User, committers_with_data, committer_count];

    /*
     * Returns all the users who authored or committed anything in any of the branches of this project.
     */
    impl_attribute![?+..  objects::Project, Users, objects::User, users_with_data, user_count];

    /*
     * Returns the IDs in of all the file paths constituting any of the branches of this project.
     */
    impl_attribute![?+..  objects::Project, Paths, objects::Path, paths_with_data, path_count];

    /*
     * Returns the IDs in of all the file contents constituting any of the branches of this project.
      
       Only files for which the contents is in the database are counted. 
     */
    impl_attribute![?+..  objects::Project, Snapshots, objects::Snapshot, snapshots_with_data, snapshot_count];

    /*
     * For each project, it goes through the commits, sorts them according to their timestamp
     * and then find the time passed between 2 commits (deltas) that are adjacents in terms of time.
     * Finally, returns the max delta found.
    */
    impl_attribute![?    objects::Project, MaxCommitDelta, i64, max_commit_delta];


    /*
     * For each project, it goes through the commits, sorts them according to their timestamp
     * and then find the time passed between 2 commits (deltas) that are adjacents in terms of time.
     * Finally, returns the average of these deltas.
    */
    impl_attribute![?    objects::Project, AvgCommitDelta, i64, avg_commit_delta];

    /*
     * Returns the time in seconds. It is the time passed between the last commit and the last time
     * parasite updated a given project. 
     * 
     * TODO should be removed? superseeded by NewestCommit & LatestUpdateTime
    */
    impl_attribute![?    objects::Project, TimeSinceLastCommit, i64, time_since_last_commit];

    /*
     * Returns the time in seconds. It is the time passed between the first commit and the last time
     * parasite updated a given project.
     * 
     * TODO should be removed? superseeded by OldestCommit & LatestUpdateTime
    */
    impl_attribute![?    objects::Project, TimeSinceFirstCommit, i64, time_since_first_commit];

    /* Committer time of the oldest commit that belongs to the project.
     */
    impl_attribute![?+    objects::Project, OldestCommit, objects::Commit, oldest_commit_with_data];

    /* Committer time of the newest commit that belongs to the project.
     */
    impl_attribute![?+    objects::Project, NewestCommit, objects::Commit, newest_commit_with_data];

    /* Newest time at which parasite checked the project, i.e. the time at which the stored information was obtained.
     */
    impl_attribute![?    objects::Project, LatestUpdateTime, i64, latest_update_time];

    /*
     * If MaxCommitDelta is less than TimeSinceLastCommit then it returns true.
    */
    impl_attribute![?    objects::Project, IsAbandoned, bool, is_abandoned];

    /*
     * Reconstructs the main branch to get the latest snapshots and return
     * the number of lines of code associated with a project.
    */
    impl_attribute![?    objects::Project, Locs, usize, project_locs];

    /*
     * Return the max developer experience that we can find among all the Authors
     * of a given project.
    */
    impl_attribute![?    objects::Project, MaxExperience, i32, project_max_experience];

    /* Returns the longest lifetime of a user contributing to the project. 

       User lifetime is calculated as the distance between the oldest and youngest commits ever committed by them. 
     */
    impl_attribute![?    objects::Project, MaxUserLifetime, i64, max_user_lifetime];

    /* Max HIndex1 for all users in the project. See the details in user's hindex 1 description.
     */
    impl_attribute![?    objects::Project, MaxHIndex1, u64, max_h_index1];

    /* Max HIndex2 for all users in the project. See the details in user's hindex 2 description.
     */
    impl_attribute![?    objects::Project, MaxHIndex2, u64, max_h_index2];

    /*
     * Return a single number. Let DE be a number that describes some developer's experience,
     * DC the number of commits of a given developer, and PC the total sum of commits of a given project.
     * Then for a given project, x,  we sum DE_x*DC_x/PC. 
    */
    impl_attribute![?    objects::Project, ProjectExperience, f64, project_experience];
    
    /*
     * Calculates the percentage of duplicated code by first looking at the changes of files
     * related to a commit, and the snapshot related to a given change. 
     * (Recall each change is related to one path). 
     * If this snapshot has differente project_id than the current one then a given 
     * file is considered cloned.
     * Returns a number between 0 and 1. It is a percetange of duplicated code 
    */
    impl_attribute![?    objects::Project, DuplicatedCode, f64, duplicated_code];

    /*
     * Calculates the number of changes each author added to the project. 
     * Returns a list of authors with the number of changes they did. 
     * The list is sorted by the number of changes in desceding order.
     */
    impl_attribute![?..   objects::Project, ChangeContributions, (objects::User, usize), change_contributions, author_count];

    /*
     * Calculates the number of commits each user authored in the project. 
     * Returns a list of authors with the number of committs they are responsible for.
     * The list is sorted by the number of commits in desceding order.
     */
    impl_attribute![?..   objects::Project, CommitContributions, (objects::User, usize), commit_contributions, author_count];

    /*
     * Calculates the percentage of commits successive users authored in the project.
     * The users are added to the aggregate in descending size of contributions.
     * 
     * CommitContributions                        => CummulativeCommitContributions
     * [(User1, 50%), (User3, 40%), (User2, 10%)] => [50%, 90%, 100%]
     * 
     * The list is sorted so that [0] represents the contribution of 1 user, and 
     * [i] represents the cumulative contribution of i-1 users.
     */
    impl_attribute![?..   objects::Project, CummulativeCommitContributions, Percentage, cumulative_commit_contributions, author_count];

    /*
     * Calculates the percentage of commits successive authors added to the project. 
     * The users are added to the aggregate in descending size of contributions.
     *
     * ChangesContributions                       => CummulativeChangesContributions
     * [(User1, 50%), (User3, 40%), (User2, 10%)] => [50%, 90%, 100%]
     * 
     * The list is sorted so that [0] represents the contribution of 1 user, and 
     * [i] represents the cumulative contribution of i-1 users.
     */
    impl_attribute![?..   objects::Project, CummulativeChangeContributions, Percentage, cumulative_change_contributions, author_count];

    /*
     * Calculates the (minimum) number of authors responsible for N% commits. 
     * The authors are return in decreasing order of most contributions
     * 
     * CommitContributions                        => AuthorsContributingCommits(95)
     * [(User1, 50%), (User3, 40%), (User2, 10%)] => [User1, User3, User2]
     * 
     *                                            => AuthorsContributingCommits(80)
     *                                            => [User1, User3]
     * 
     *                                            => AuthorsContributingCommits(50)
     *                                            => [User1]
     * 
     * Since this is parameterized, this attribute is not cached. 
     * CommitContributions is cached.
     */
    impl_attribute![?+..  objects::Project, AuthorsContributingCommits(Percentage), objects::User, authors_contributing_commits_with_data, authors_contributing_commits_count];

    /*
     * Calculates the (minimum) number of authors responsible for N% changes. 
     * The authors are return in decreasing order of most contributions
     * 
     * ChangesContributions                       => AuthorsContributingChanges(95)
     * [(User1, 50%), (User3, 40%), (User2, 10%)] => [User1, User3, User2]
     * 
     *                                            => AuthorsContributingChanges(80)
     *                                            => [User1, User3]
     * 
     *                                            => AuthorsContributingChanges(50)
     *                                            => [User1]
     * 
     * Since this is parameterized, this attribute is not cached. 
     * CommitContributions is cached.
     */
    impl_attribute![?+..  objects::Project, AuthorsContributingChanges(Percentage), objects::User, authors_contributing_changes_with_data, authors_contributing_changes_count];

    /* Number of snapshots in the project that only ever exist in the project.
    
       Takes only the snapshots for which we have data into account. 
     */
    impl_attribute![?     objects::Project, UniqueFiles, usize, unique_files];
    /* Number of snapshots in the project where the project is the original, i.e. the oldest occurence of the particular snapshot in the dataset. 

       Takes only the snapshots for which we have data into account. 
     */
    impl_attribute![?     objects::Project, OriginalFiles, usize, original_files];
    /* The impact of the project. 
     
        Sum of impact of its snapshots, where 0 is added for clones, 1 for unique files and the number of projects using a snapshot for original snapshots.
     */
    impl_attribute![?     objects::Project, Impact, usize, impact];
    /* Number of unique files in the project. 

       TODO the current version does not provide information about deletions and so there is no way we can reconstruct only active files. At this moment this metric simply returns the number of unique paths throughout the project's history.
     */
    impl_attribute![?     objects::Project, Files, usize, files];

    /* Languages of the project. 

       For each language used in the project, ordered in according to the number of snapshots in that language in the project.

       TODO for now, this is only done using file extensions. We might want to do this using real contents analysis. 
     */
    impl_attribute![?..   objects::Project, Languages, objects::Language, languages, languages_count];
    /* Language composition of the project. 

       For each language used in the project returns the number of snapshots throught the project history ordered by descending number. 

       TODO for now, this is only done using file extensions. We might want to do this using real contents analysis. 
     */
    impl_attribute![?..   objects::Project, LanguageComposition, (objects::Language, usize), language_composition, languages_count];
    /* Shorthand for the major language. 
     
        This is the language with most changes in the project history. 
     */
    impl_attribute![?     objects::Project, MajorLanguage, objects::Language, major_language];
    /* The ratio of the changes to the major languages vs. all changes in the project. 
     
       We are only counting changes to languages, i.e. where we keep snapshots. 
     */
    impl_attribute![?     objects::Project, MajorLanguageRatio, f64, major_language_ratio];
    /* Number of changes to the major language. 
     */
    impl_attribute![?     objects::Project, MajorLanguageChanges, usize, major_language_changes];

    /* 
     * A project is `valid` if it was downloaded correctly in the most recent download attempt.
     * Otherwise the project is `corrupted`: the downloaded data may be incoherent.
     */
    impl_attribute![?     objects::Project, IsValid, bool, is_valid];
    /* Returns the list of projects that have been forked from the current project. 

       For simplicity we assume a project is a fork if it is younger *and* if it shares at least one commit by hash. 
     */
    impl_attribute![?..   objects::Project, AllForks, objects::ProjectId, all_forks, all_forks_count];

    /* For each branch returns the head tree.
     
       The tree is a mapping from path ids to snapshot ids. All paths are considered. Deleted files are not displayed.
     */
    impl_attribute![?..   objects::Project, HeadTrees, (String, Vec<(objects::PathId, objects::SnapshotId)>), head_trees, head_trees_count];

}

pub mod commit {
    use crate::objects;
    use crate::attrib::*;
    use crate::Timestamp;

    impl_attribute![!+   objects::Commit, Itself];
    impl_attribute![!    objects::Commit, Raw];
    impl_attribute![!    objects::Commit, Id, objects::CommitId, id];
    impl_attribute![!    objects::Commit, CommitterId, objects::UserId, committer_id];
    impl_attribute![!    objects::Commit, AuthorId, objects::UserId, author_id];
    impl_attribute![?+   objects::Commit, Committer, objects::User, committer_with_data];
    impl_attribute![?+   objects::Commit, Author, objects::User, author_with_data];
    impl_attribute![?    objects::Commit, Hash, String, hash];
    impl_attribute![?    objects::Commit, Message, String, message];
    impl_attribute![?    objects::Commit, MessageLength, usize, message_length];
    impl_attribute![?    objects::Commit, AuthoredTimestamp, Timestamp, author_timestamp];
    impl_attribute![?    objects::Commit, CommittedTimestamp, Timestamp, committer_timestamp];
    /* Returns all changes made by the commit. 
     
        These include deleted files (snapshot id is None) and files for which we do not have snapshots. 
     */
    impl_attribute![?+.. objects::Commit, Changes, objects::Change, changes_with_data, change_count];
    /* Returns changes made by the commit that we have data for. 
     
        These include deleted files (snapshot id is None), but *not* files for which we do not have data. 
     */ 
    impl_attribute![?+.. objects::Commit, ChangesWithContents, objects::Change, changes_with_contents, change_with_contents_count];
    impl_attribute![?..  objects::Commit, PathIds, objects::PathId, changed_path_ids, changed_path_count];
    impl_attribute![?..  objects::Commit, SnapshotIds, objects::SnapshotId, changed_snapshot_ids, changed_snapshot_count];
    impl_attribute![!..  objects::Commit, ParentIds, objects::CommitId, parent_ids, parent_count];
    impl_attribute![?+.. objects::Commit, Paths, objects::Path, changed_paths_with_data, changed_path_count];
    impl_attribute![?+.. objects::Commit, Snapshots, objects::Snapshot, changed_snapshots_with_data, changed_snapshot_count];
    impl_attribute![!+.. objects::Commit, Parents, objects::Commit, parents_with_data, parent_count];
    impl_attribute![!+   objects::Commit, Tree, objects::Tree, tree_with_data];

    /* The list of projects the commit belongs to. 

       This is pretty much the reverse of the project-commits mapping.
     */
    impl_attribute![?..  objects::Commit, Projects, objects::Project, projects, projects_count];

    /*
     * The list of all languages in a commit.
     */
    impl_attribute![?..  objects::Commit, Languages, objects::Language, languages, languages_count];

    /*
     * All commit ids that are the ancestors of this commit in the commit graph. I.e. all the commits in the branch if this commit is the head of that branch.
     */ 
    impl_attribute![!..  objects::Commit, PrecedingCommitIds, objects::CommitId, preceding_commit_ids, preceding_commit_count];

    /*
     * All commit ids that are the ancestors of this commit in the commit graph. I.e. all the commits in the branch if this commit is the head of that branch.
     */
    impl_attribute![!+.. objects::Commit, PrecedingCommits, objects::Commit, preceding_commits_with_data, preceding_commit_count];
}

pub mod head {
    use crate::objects;
    use crate::attrib::*;

    impl_attribute![!+   objects::Head, Itself];
    impl_attribute![!    objects::Head, Raw];
    impl_attribute![!    objects::Head, Name, String, name];
    impl_attribute![!    objects::Head, CommitId, objects::CommitId, commit_id];
    impl_attribute![?+   objects::Head, Commit, objects::Commit, commit_with_data];
    impl_attribute![!..  objects::Head, CommitIds, objects::CommitId, commit_ids, commit_count];
    impl_attribute![!+.. objects::Head, Commits, objects::Commit, commits_with_data, commit_count];
}

pub mod change {
    use crate::objects;
    use crate::attrib::*;

    impl_attribute![!+  objects::Change, Itself];
    impl_attribute![!   objects::Change, Raw];
    impl_attribute![!   objects::Change, PathId, objects::PathId, path_id];
    impl_attribute![?   objects::Change, SnapshotId, objects::SnapshotId, snapshot_id];
    impl_attribute![?+  objects::Change, Path, objects::Path, path_with_data];
    impl_attribute![?+  objects::Change, Snapshot, objects::Snapshot, snapshot_with_data];
}

pub mod tree {
    use crate::objects;
    use crate::attrib::*;

    impl_attribute![!+   objects::Tree, Itself];
    impl_attribute![!    objects::Tree, Raw];
    impl_attribute![!..  objects::Tree, PathIds, objects::PathId, path_ids, path_count];
    impl_attribute![!..  objects::Tree, SnapshotIds, objects::SnapshotId, snapshot_ids, snapshot_count];
    impl_attribute![!+.. objects::Tree, Paths, objects::Path, paths_with_data, path_count];
    impl_attribute![!+.. objects::Tree, Snapshots, objects::Snapshot, snapshots_with_data, snapshot_count];
    impl_attribute![!+.. objects::Tree, Changes, objects::Change, changes_with_data, change_count];
}

pub mod user {
    use crate::objects;
    use crate::time;
    use crate::attrib::*;

    impl_attribute![!+   objects::User, Itself];
    impl_attribute![!    objects::User, Raw];
    impl_attribute![!    objects::User, Id, objects::UserId, id];
    impl_attribute![!    objects::User, Email, String, email];
    impl_attribute![?    objects::User, AuthorExperience, time::Duration, author_experience];
    impl_attribute![?    objects::User, CommitterExperience, time::Duration, committer_experience];
    impl_attribute![?    objects::User, Experience, time::Duration, experience];
    impl_attribute![?..  objects::User, AuthoredCommitIds, objects::CommitId, authored_commit_ids, authored_commit_count];
    impl_attribute![?..  objects::User, CommittedCommitIds, objects::CommitId, committed_commit_ids, committed_commit_count];
    impl_attribute![?+.. objects::User, AuthoredCommits, objects::Commit, authored_commits_with_data, authored_commit_count];
    impl_attribute![?+.. objects::User, CommittedCommits, objects::Commit, committed_commits_with_data, committed_commit_count];
    impl_attribute![?    objects::User, DeveloperExperience, i32, developer_experience];

    /* The min and max of commit times of commits where the user is a committer.
     */
    impl_attribute![?    objects::User, Lifetime, (i64, i64), lifetime];

    /* H-Index of a user is the largest N such that:
     
        - the user committed at least N distinct commits to at least N projects (h_index1) 
        - the user committed at least N distinct commits to at least N projects where each of the projects has at least N developers (h_index2)

        The commits are distinct by their SHA. 
     */
    impl_attribute![?    objects::User, HIndex1, u64, h_index1];
    impl_attribute![?    objects::User, HIndex2, u64, h_index2];
    
    /* Ids of all projects the user contributes to (at least one authored commit)
     */
    impl_attribute![?..  objects::User, ProjectIds, objects::ProjectId, project_ids, project_ids_count];
}

pub mod path {
    use crate::objects;
    use crate::attrib::*;

    impl_attribute![!+  objects::Path, Itself];
    impl_attribute![!   objects::Path, Raw];
    impl_attribute![!   objects::Path, Id, objects::PathId, id];
    impl_attribute![!   objects::Path, Location, String, location];
    impl_attribute![?   objects::Path, Language, objects::Language, language];
}

pub mod snapshot {
    use crate::objects;
    use crate::attrib::*;

    impl_attribute![!+  objects::Snapshot, Itself];
    impl_attribute![!   objects::Snapshot, Raw];
    impl_attribute![!   objects::Snapshot, Id, objects::SnapshotId, id];
    impl_attribute![!   objects::Snapshot, Bytes, Vec<u8>, raw_contents_owned];
    impl_attribute![!   objects::Snapshot, Contents, String, contents_owned];
    impl_attribute![?   objects::Snapshot, Loc, usize, snapshot_locs];
    impl_attribute![!   objects::Snapshot, HasContents, bool, snapshot_has_contents];

    /* Number of projects in the database that contain the snapshot (or did in the past). 
    
       This is count of projects, not occurences, i.e. if the file appears multiple times in a project, this will still only grow by one. 

       Contains indformation about *all* snapshots, not just those for which we have contents stored as well. 
     */
    impl_attribute![!   objects::Snapshot, NumProjects, usize, unique_projects];

    /* The oldest project in which we have seen the snapshot appear. 

       This is either the project where the snapshot appears in the oldest commit, or if there are more such projects, the oldest of these.

       Contains indformation about *all* snapshots, not just those for which we have contents stored as well. 
     */
    impl_attribute![!   objects::Snapshot, OriginalProject, objects::ProjectId, original_project];
}

pub trait AttributeIterator<'a, T>: Sized + Iterator<Item=objects::ItemWithData<'a, T>> {
    fn filter_by<A>(self, attribute: A)
                    -> AttributeFilterIter<Self, A>
        where A: Filter<'a, Item=T> {
        AttributeFilterIter { iterator: self, attribute }
    }

    fn map_into<A, Ta, Tb>(self, attribute: A)
                           -> AttributeMapIter<Self, A, Ta, Tb>
        where A: Select<'a, Ta, Tb> {
        AttributeMapIter { iterator: self, attribute, function: PhantomData }
    }

    fn sort_by<A: 'a, I>(self, attribute: A)
                         -> std::vec::IntoIter<objects::ItemWithData<'a, T>>
        where A: Sort<'a, T, I>, I: Ord {
        self.sort_with_direction(sort::Direction::Descending, attribute)
    }

    fn sort_with_direction<A: 'a, I>(self, direction: sort::Direction, attribute: A)
                                     -> std::vec::IntoIter<objects::ItemWithData<'a, T>>
        where A: Sort<'a, T, I>, I: Ord {
        let mut vector = Vec::from_iter(self);
        attribute.sort(direction, &mut vector);
        vector.into_iter()
    }

    fn sample<S>(self, sampler: S)
                 -> std::vec::IntoIter<objects::ItemWithData<'a, T>>
        where S: Sampler<'a, T> {
        sampler.sample(self).into_iter()
    }

    fn group_by<A, K>(self, attribute: A)
                      -> std::collections::hash_map::IntoIter<K, Vec<objects::ItemWithData<'a, T>>>
        where A: Group<'a, T, K>, K: Hash + Eq {
        self.map(|item_with_data| {
            let key = attribute.select_key(&item_with_data);
            (key, item_with_data)
        }).into_group_map().into_iter()
    }

    // TODO drop options
}

impl<'a, T, I> AttributeIterator<'a, T> for I
    where I: Sized + Iterator<Item=objects::ItemWithData<'a, T>> {}

pub trait AttributeGroupIterator<'a, K, T>: Sized + Iterator<Item=(K, Vec<objects::ItemWithData<'a, T>>)> {
    fn filter_by<A>(self, attribute: A)
                    -> AttributeGroupFilterIter<Self, A>
        where A: Filter<'a, Item=T> {
        AttributeGroupFilterIter { iterator: self, attribute }
    }
    // TODO filter_key

    fn map_into<A, Ta, Tb>(self, attribute: A)
                           -> AttributeGroupMapIter<Self, A, Ta, Tb>
        where A: Select<'a, Ta, Tb> {
        AttributeGroupMapIter { iterator: self, attribute, function: PhantomData }
    }

    fn sort_by<A: 'a, I>(self, attribute: A)
                         -> std::vec::IntoIter<(K, Vec<objects::ItemWithData<'a, T>>)>
        where A: Sort<'a, T, I>, I: Ord {
        self.sort_with_direction(sort::Direction::Descending, attribute)
    }

    fn sort_with_direction<A: 'a, I>(self, direction: sort::Direction, attribute: A)
                                     -> std::vec::IntoIter<(K, Vec<objects::ItemWithData<'a, T>>)>
        where A: Sort<'a, T, I>, I: Ord {
        let vector: Vec<(K, Vec<objects::ItemWithData<'a, T>>)> =
            self.map(|(key, mut vector)| {
                attribute.sort(direction, &mut vector);
                (key, vector)
            }).collect();
        vector.into_iter()
    }
    // TODO sort_key, sort_key_by, sort_key_with, sort_values, sort_values_by, sort_values_with

    fn sample<S>(self, sampler: S)
                 -> std::vec::IntoIter<(K, Vec<objects::ItemWithData<'a, T>>)>
        where S: Sampler<'a, T> {
        let vector: Vec<(K, Vec<objects::ItemWithData<'a, T>>)> =
            self.map(|(key, vector)| {
                (key, sampler.sample_from(vector))
            }).collect();
        vector.into_iter()
    }
    // TODO sample_key

    fn ungroup(self) -> std::vec::IntoIter<objects::ItemWithData<'a, T>> {
        let vector: Vec<objects::ItemWithData<'a, T>> =
            self.flat_map(|(_, vector)| vector).collect();
        vector.into_iter()
    }
}

impl<'a, K, T, I> AttributeGroupIterator<'a, K, T> for I
    where I: Sized + Iterator<Item=(K, Vec<objects::ItemWithData<'a, T>>)> {}


macro_rules! impl_comparison {
        ($name:ident, $trait_limit:ident, $comparator:ident, $default:expr) => {
            pub struct $name<A, N>(pub A, pub N) where A: Attribute; // + OptionGetter<'a, IntoItem=N>;
            impl<'a, A, N, T> Filter<'a> for $name<A, N> where A: OptionGetter<'a, IntoItem=N> + Attribute<Object=T>, N: $trait_limit {
                type Item = T;
                fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
                    self.0.get_opt(item_with_data).map_or($default, |n| n.$comparator(&self.1))
                }
            }
        }
    }

impl_comparison!(LessThan, PartialOrd, lt, false);
impl_comparison!(AtMost,   PartialOrd, le, false);
impl_comparison!(Equal,    Eq,         eq, false);
impl_comparison!(AtLeast,  PartialOrd, ge, true);
impl_comparison!(MoreThan, PartialOrd, gt, true);

macro_rules! impl_binary {
        ($name:ident, $comparator:expr) => {
            pub struct $name<A, B>(pub A, pub B); // where A: Attribute, B: Attribute;
            impl<'a, A, B, T> Filter<'a> for $name<A, B> where A: Filter<'a, Item=T>, B: Filter<'a, Item=T> {
                type Item = T;
                fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
                    $comparator(self.0.accept(item_with_data),
                                self.1.accept(item_with_data))
                }
            }
        }
    }

impl_binary!(And, |a, b| a && b); // TODO Effectively does not short circuit.
impl_binary!(Or,  |a, b| a || b);

macro_rules! impl_unary {
        ($name:ident, $comparator:expr) => {
            pub struct $name<A>(pub A); // where A: Attribute;
            impl<'a, A, T> Filter<'a> for $name<A> where A: Filter<'a, Item=T> {
                type Item = T;
                fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
                    $comparator(self.0.accept(item_with_data))
                }
            }
        }
    }

impl_unary!(Not,  |a: bool| !a);

macro_rules! impl_existential {
        ($name:ident, $method:ident) => {
            pub struct $name<A>(pub A) where A: Attribute; // + OptionGetter<'a>;
            impl<'a, A, T> Filter<'a> for $name<A> where A: OptionGetter<'a>, A: Attribute<Object=T> {
                type Item = T;
                fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
                    self.0.get_opt(item_with_data).$method()
                }
            }
        }
    }

impl_existential!(Exists,  is_some);
impl_existential!(Missing, is_none);

pub struct Same<'a, A>(pub A, pub &'a str) where A: OptionGetter<'a>;
impl<'a, A, T> Filter<'a> for Same<'a, A> where A: OptionGetter<'a, IntoItem=String>, A: Attribute<Object=T> {
    type Item = T;
    fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
        self.0.get_opt(item_with_data).map_or(false, |e| e.as_str() == self.1)
    }
}

pub struct Contains<'a, A>(pub A, pub &'a str) where A: OptionGetter<'a>;
impl<'a, A, T> Filter<'a> for Contains<'a, A> where A: OptionGetter<'a, IntoItem=String>, A: Attribute<Object=T> {
    type Item = T;
    fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
        self.0.get_opt(item_with_data).map_or(false, |e| e.contains(self.1))
    }
}

#[macro_export] macro_rules! regex { ($str:expr) => { regex::Regex::new($str).unwrap() }}
pub struct Matches<A>(pub A, pub regex::Regex) where A: Attribute;
impl<'a, A, T> Filter<'a> for  Matches<A> where A: OptionGetter<'a, IntoItem=String>, A: Attribute<Object=T> {
    type Item = T;
    fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
        self.0.get_opt(item_with_data).map_or(false, |e| self.1.is_match(&e))
    }
}

macro_rules! impl_collection_membership {
        ($collection_type:tt<I> where I: $($requirements:tt),+) => {
            impl<'a, A, T, I> Filter<'a> for Member<A, $collection_type<I>>
                where A: OptionGetter<'a, IntoItem=I>,
                A: Attribute<Object=T>,
                I: $($requirements+)+ {
                type Item = T;
                fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
                    self.0.get_opt(item_with_data).map_or(false, |e| self.1.contains(&e))
                }
            }
            impl<'a, A, T, I> Filter<'a> for AnyIn<A, $collection_type<I>>
                where A: OptionGetter<'a, IntoItem=Vec<I>>,
                      A: Attribute<Object=T>,
                      I: $($requirements+)+ {
                type Item = T;
                fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
                    self.0.get_opt(item_with_data).map_or(false, |vector| {
                        vector.iter().any(|e| self.1.contains(e))
                    })
                }
            }
            impl<'a, A, T, I> Filter<'a> for AllIn<A, $collection_type<I>>
                where A: OptionGetter<'a, IntoItem=Vec<I>>,
                      A: Attribute<Object=T>,
                      I: $($requirements+)+ {
                type Item = T;
                fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
                    self.0.get_opt(item_with_data).map_or(false, |vector| {
                        vector.iter().all(|e| self.1.contains(e))
                    })
                }
            }
            impl<'a, A, T, I> Filter<'a> for AnyWithin<A, $collection_type<I>>
            where A: OptionGetter<'a, IntoItem=Vec<I>>,
                  A: Attribute<Object=T>,
                  I: $($requirements+)+ {
            type Item = T;
            fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
                let vector = self.0.get_opt(item_with_data);
                let vector_ref = vector.as_ref();
                self.1.iter().any(|e| {
                    vector_ref.map_or(false, |vector| vector.contains(e))
                })
            }
        }
            impl<'a, A, T, I> Filter<'a> for AllWithin<A, $collection_type<I>>
                where A: OptionGetter<'a, IntoItem=Vec<I>>,
                      A: Attribute<Object=T>,
                      I: $($requirements+)+ {
                type Item = T;
                fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
                    let vector = self.0.get_opt(item_with_data);
                    let vector_ref = vector.as_ref();
                    self.1.iter().all(|e| {
                        vector_ref.map_or(false, |vector| vector.contains(e))
                    })
                }
            }
        }
    }

pub struct Member<A, C>(pub A, pub C);
pub struct AnyIn<A, C>(pub A, pub C);
pub struct AllIn<A, C>(pub A, pub C);
pub struct Within<A, C>(pub A, pub C);
pub struct AnyWithin<A, C>(pub A, pub C);
pub struct AllWithin<A, C>(pub A, pub C);

impl_collection_membership!(Vec<I> where I: Eq);
impl_collection_membership!(BTreeSet<I> where I: Ord);
impl_collection_membership!(HashSet<I> where I: Hash, Eq);

impl<'a, A, T, I> Filter<'a> for Within<A, I>
where A: OptionGetter<'a, IntoItem=Vec<I>>,
    A: Attribute<Object=T>,
    I: Eq {
    type Item = T;
    fn accept(&self, item_with_data: &objects::ItemWithData<'a, Self::Item>) -> bool {
        self.0.get_opt(item_with_data).map_or(false, |e| {
            e.contains(&self.1)
        })
    }
}

#[macro_export]
macro_rules! Strata {
    ($($name:literal -> $sampler:expr),+) =>  {{
        let strata = vec![$(($name, $sampler),)+];
        // Vec here rather than a set to remove additional include
        let mut uniq: Vec<&str> = Vec::new(); 
        strata.iter().for_each(|(name, _)| {
            if uniq.contains(name) {
                panic!("Attempting to define two or more strata named {}", name);               
            }
            uniq.push(name)
        });
        strata.into_iter().collect()
    }}
}

pub trait StrataClassifier<T> {
    fn classify(&self, item: Option<&T>) -> &'static str;
}

pub struct Custom<F>(pub F);
impl<T, F> StrataClassifier<T> for Custom<F> where F: Fn(Option<&T>) -> &'static str {
    fn classify(&self, item: Option<&T>) -> &'static str {
        self.0(item)
    }
}

pub enum Threshold<T>{
    Inclusive(T, &'static str, &'static str),
    Exclusive(T, &'static str, &'static str),
}

impl<T> StrataClassifier<T> for Threshold<T> where T: Ord {
    fn classify(&self, item: Option<&T>) -> &'static str {
        match (item, self) {
            (None, _) => "NA",
            (Some(item), Threshold::Inclusive(threshold, yes, no)) => { if threshold >= item { yes } else { no } },
            (Some(item), Threshold::Exclusive(threshold, yes, no)) => { if threshold >  item { yes } else { no } },
        }
    }
}

#[macro_export]
macro_rules! Conditions {
    ($($name:literal -> $sampler:expr),+) =>  {{
        let strata = vec![$(($name, $sampler),)+];
        // Vec here rather than a set to remove additional include
        let mut uniq: Vec<&str> = Vec::new(); 
        strata.iter().for_each(|(name, _)| {
            if uniq.contains(name) {
                panic!("Attempting to define multiple conditions for stratum named {}", name);               
            }
            uniq.push(name)
        });
        strata.into_iter().collect()
    }}
}

pub enum Thresholds<T>{
    Inclusive(Vec<(&'static str, T)>, &'static str),
    Exclusive(Vec<(&'static str, T)>, &'static str),
}

impl<T> StrataClassifier<T> for Thresholds<T> where T: Ord {
    fn classify(&self, item: Option<&T>) -> &'static str {
        match (item, self) {
            (None, _) => "NA",
            (Some(item), Thresholds::Inclusive(thresholds, default)) => { 
                for (stratum, threshold) in thresholds {
                    if threshold >= item { 
                        return stratum;
                    }
                }
                return default;
            },
            (Some(item), Thresholds::Exclusive(thresholds, default)) => { 
                for (stratum, threshold) in thresholds {
                    if threshold > item { 
                        return stratum;
                    }
                }
                return default;
            }
        }
    }
}

pub struct Stratified<A, S, C>(pub A, pub HashMap<&'static str, S>, pub C);
impl<'a, A, V, T, C, S> Sampler<'a, T> for Stratified<A, S, C> where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=V>, S: Sampler<'a, T>, C: StrataClassifier<V> {
    fn sample<I>(&self, iter: I) -> Vec<objects::ItemWithData<'a, T>>
        where I: Iterator<Item=objects::ItemWithData<'a, T>> {       
            let strata = iter
                .map(|object| (self.0.get_opt(&object), object))
                .map(|(attribute, object)| (self.2.classify(attribute.as_ref()), object)).into_group_map();

            strata.into_iter()
                .flat_map(|(stratum, objects)| {
                    let sampler = self.1.get(&stratum);
                    if let Some(sampler) = sampler {
                        Some((stratum, sampler, objects))
                    } else {
                        eprintln!("WARNING: No sampling method was specified for stratum {}, 
                                  so the sample will not contain any objects from this stratum.", 
                                  stratum);
                        None
                    }
                })
                .flat_map(|(_stratum, sampler, objects)|{
                    sampler.sample(objects.into_iter())
                })
                .collect()
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)] 
pub struct Top(pub usize);
impl<'a, T> Sampler<'a, T> for Top {
    fn sample<I>(&self, iter: I) -> Vec<objects::ItemWithData<'a, T>>
        where I: Iterator<Item=objects::ItemWithData<'a, T>> {

        iter.take(self.0).collect()
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)] pub struct Seed(pub u128);
impl Seed {
    pub fn to_be_bytes(&self) -> [u8; 16] { self.0.to_be_bytes() }
    pub fn to_le_bytes(&self) -> [u8; 16] { self.0.to_le_bytes() }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)] pub struct Random(pub usize, pub Seed);
impl<'a, T> Sampler<'a, T> for Random {
    fn sample<I>(&self, iter: I) -> Vec<objects::ItemWithData<'a, T>>
        where I: Iterator<Item=objects::ItemWithData<'a, T>> {

        let mut rng = Pcg64Mcg::from_seed(self.1.to_be_bytes());
        iter.choose_multiple(&mut rng, self.0)
    }
}

pub trait SimilarityCriterion<'a> {
    type Item;
    type IntoItem;
    type Similarity: Similarity<Self::IntoItem>;
    fn from(&self, object: &objects::ItemWithData<'a, Self::Item>) -> Self::Similarity;
}
pub trait Similarity<T>: Eq + Hash { }

// TODO hide
pub struct _MinRatio<T> { min_ratio: f64, items: Option<BTreeSet<T>> }
impl<T> Hash for _MinRatio<T> {
    // Everything needs to be compared explicitly.
    fn hash<H: Hasher>(&self, state: &mut H) { state.write_u64(42) }
}
impl<T> Eq for _MinRatio<T> where T: Ord {}
impl<T> PartialEq for _MinRatio<T> where T: Ord {
    fn eq(&self, other: &Self) -> bool {
        match (&self.items, &other.items) {
            (None, None) => true,
            (Some(me), Some(them)) => {
                let mine: f64 = me.len() as f64;
                let same: f64 = me.intersection(&them).count() as f64;
                same / mine > self.min_ratio
            }
            _ => false,
        }
    }
}
impl<T> Similarity<T> for _MinRatio<T> where T: Ord {}

#[derive(Debug, Clone, Copy)] pub struct MinRatio<A: Attribute>(pub A, pub f64);
impl<'a, A, T, I> SimilarityCriterion<'a> for MinRatio<A>
    where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<I>>, I: Ord {
    type Item = T;
    type IntoItem = I;
    type Similarity = _MinRatio<Self::IntoItem>;

    fn from(&self, object: &objects::ItemWithData<'a, Self::Item>) -> Self::Similarity {
        let items = self.0.get_opt(object).map(|e| {
            BTreeSet::from_iter(e.into_iter())
        });
        _MinRatio { min_ratio: self.1, items }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)] pub struct Distinct<S, C>(pub S, pub C);
impl<'a, T, S, C> Sampler<'a, T> for Distinct<S, C> where S: Sampler<'a, T>, C: SimilarityCriterion<'a, Item=T> {
    fn sample<I>(&self, iter: I) -> Vec<objects::ItemWithData<'a, T>>
        where I: Iterator<Item=objects::ItemWithData<'a, T>> {
        let filtered_iter = iter.unique_by(|object| {
            self.1.from(object)
        });
        self.0.sample(filtered_iter)
    }
}

pub struct Length<A: Attribute>(pub A);
impl<A, T> Attribute for Length<A> where A: Attribute<Object=T> {
    type Object = T;
}
impl<'a, A, T> Getter<'a> for Length<A> where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=String> {
    type IntoItem = Option<usize>;
    fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
        self.0.get_opt(object).map(|s| s.len())
    }
}
impl<'a, A, T> OptionGetter<'a> for Length<A> where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=String> {
    type IntoItem = usize;
    fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
        self.0.get_opt(object).map(|s| s.len())
    }
}

pub struct Count<A: Attribute>(pub A);
impl<A, T> Attribute for Count<A> where A: Attribute<Object=T> {
    type Object = T;
}
impl<'a, A, T> Getter<'a> for Count<A> where A: Attribute<Object=T> + OptionCountable<'a> {
    type IntoItem = usize;
    fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
        self.0.count(object).unwrap_or(0)
    }
}
impl<'a, A, T> OptionGetter<'a> for Count<A> where A: Attribute<Object=T> + OptionCountable<'a> {
    type IntoItem = usize;
    fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
        self.0.count(object)
    }
}

trait CalculateStat<N, T>{ fn calculate(vector: Vec<N>) -> T; }
macro_rules! impl_calculator {
        ($name:ident -> $result:ty where N: $($requirements:path),+; $calculate:item) => {
            pub struct $name<A: Attribute>(pub A);
            impl<A, T> Attribute for $name<A> where A: Attribute<Object=T> {
                type Object = T;
            }
            impl<'a, A, N, T> Getter<'a> for $name<A>
                where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<N>>, N: $($requirements +)+ {
                type IntoItem = Option<$result>;
                fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                    self.0.get_opt(object).map(|object| Self::calculate(object)).flatten()
                }
            }
            impl<'a, A, N, T> OptionGetter<'a> for $name<A>
                where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<N>>, N: $($requirements +)+  { //$n: $(as_item!($requirements) +)+ {
                type IntoItem = $result;
                fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                    self.0.get_opt(object).map(|object| Self::calculate(object)).flatten()
                }
            }
            impl<A, N, T> CalculateStat<N, Option<$result>> for $name<A> where A: Attribute<Object=T>, N: $($requirements +)+  {
                $calculate
            }
        }
    }

// trait Unwrap<T,I> { fn unwrap(&self) -> I; }
// impl Unwrap<usize, f64> for std::result::Result<f64, <usize as TryInto<f64>>::Err> {
//     fn unwrap(&self) -> f64 {
//         self.unwrap()
//     }
// }

//TODO min_by/max_by/minmax_by
impl_calculator!(Min -> N where N: Ord, Clone;
        fn calculate(vector: Vec<N>) -> Option<N> { vector.into_iter().min() }
    );
impl_calculator!(Max -> N where N: Ord, Clone;
        fn calculate(vector: Vec<N>) -> Option<N> { vector.into_iter().max() }
    );
impl_calculator!(MinMax -> (N, N) where N: Ord, Clone;
        fn calculate(vector: Vec<N>) -> Option<(N,N)> { vector.into_iter().minmax().into_option() }
    );
impl_calculator!(Mean -> Fraction<N> where N: Sum;
        fn calculate(vector: Vec<N>) -> Option<Fraction<N>> {
            let length = vector.len();
            let sum = vector.into_iter().sum::<N>();
            if length == 0 {
                None
            } else {
                Some(Fraction::new(sum, length))
            }
        }
    );
impl_calculator!(Median -> Fraction<N> where N: Ord, Clone, Sum;
        fn calculate(mut items: Vec<N>) -> Option<Fraction<N>> {
            items.sort();
            let length = items.len();
            if length == 0 {
                None
            } else {
                let value: Fraction<N> =
                    if length == 1 {
                        Fraction::new(items[0].clone(), 1)
                    } else if length % 2 != 0usize {
                        Fraction::new(items[length / 2].clone(), 1)
                    } else {
                        let left: N = items[(length / 2) - 1].clone();
                        let right: N = items[(length / 2)].clone();
                        Fraction::new(vec![left, right].into_iter().sum(), 2)
                    };
                Some(value)
            }
        }
    );
//
// type BinKey = Option<i64>;
// pub trait BinningFunction {
//     type From;
//     fn calculate_key(&self, value: Self::From) -> BinKey;
//     fn calculate_limits(&self, key: BinKey) -> (Self::From, Self::From);
//     fn bin(value: Self::From) -> Bin<Self> {
//         Bin { value, binning_function: PhantomData }
//     }
// }
//
// pub struct Bin<F: BinningFunction> { value: F::From, binning_function: PhantomData<F> }
// impl<F> Bin<F> where F: BinningFunction {
//     // pub fn from(value: F::From) -> Self {
//     //
//     // }
// }
//
// impl<F, T> std::convert::From<T> for Bin<F> where F: BinningFunction<From=T>, T: Div /*TODO*/ {
//     fn from(value: T) -> Self {
//         Bin { value, binning_function: PhantomData }
//     }
// }
//
// #[derive(Clone, Debug)]
// pub struct Interval(pub usize);
// impl BinningFunction for Interval {
//     type From = f64;
//     fn calculate_key(&self, value: Self::From) -> BinKey {
//
//         let bin = (value / self.0 as f64).to_i64();
//
//     }
//     fn calculate_limits(&self, key: BinKey) -> (Self::From, Self::From) {
//
//     }
// }

// impl<T, F> Display for Bin<T, F> where F: BinningFunction<T> {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         write!(f, "{}..{}", self.binning_function.minimum(), self.binning_function.maximum())
//     }
// }

// pub trait BinningFunction<T>: Clone {
//     type Into;
//     fn bin(&self, value: T) -> Bin<Self::Into, Self>;
//
//     fn minimum(&self, bin: Self::Into) -> T;
//     fn maximum(&self, bin: Self::Into) -> T;
//     // fn minimum(bin: Bin<Self::Into, Self>) -> T {
//     //     bin.binning_function.minimum_value(bin.bin)
//     // }
//     // fn maximum(bin: Bin<Self::Into, Self>) -> T {
//     //     bin.binning_function.minimum_value(bin.bin)
//     // }
// }

// macro_rules! impl_interval_binning_function {
//     ($from:tt -> $to:tt) => {
//         impl BinningFunction for Interval<usize> {
//             type From = $from;
//             type Into = $to;
//             fn bin(&self, value: Self::From) -> Bin<Self> {
//                 let binning_function = self.clone();
//                 let bin = value / self.0;
//                 Bin { value , bin, binning_function }
//             }
//         }
//     }
// }

// pub trait DivInto<T> {
//     fn div_by(&self, other: &T) -> T;
//     fn convert_into(&self) -> T;
//     fn convert_from(other: &T) -> Self;
// }
//
// impl<T> BinningFunction<T> for Interval<i64> where T: DivInto<i64> {
//     type Into = i64;
//     fn bin(&self, value: T) -> Bin<T, Self> {
//         let binning_function = self.clone();
//         let bin = value.div_by(&self.0);
//         Bin { value , bin, binning_function }
//     }
//     fn minimum(&self, bin: Self::Into) -> T {
//         T::convert_from(&(bin * self.0))
//     }
//     fn maximum(&self, bin: Self::Into) -> T {
//         T::convert_from(&(((bin + 1) * self.0) - 1))
//     }
// }

//         100
// 0 ->      0  0..99         (n) * N..( (n+1) * N )-1
// 10->      0  0..99
// 100->     1  100..199
// 101->     1  100..199
// 1000     10  1000..1099
// -1 ->     0

pub struct Bucket<A: Attribute, F>(pub A, pub F);
impl<A, F, T> Attribute for Bucket<A, F> where A: Attribute<Object=T> {
    type Object = T;
}
// impl<'a, A, F, T, I> OptionGetter<'a> for Bucket<A, F>
//     where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=I>, F: BinningFunction<I> {
//     type IntoItem = Bin<I, F>;
//     fn get_opt(&self, object: &ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
//         self.0.get_opt(object).map(|item| self.1.bin(item))
//     }
// }
// impl<'a, A, F, T, I> Getter<'a> for Bucket<A, F>
//     where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=I>, F: BinningFunction<I> {
//     type IntoItem = Option<Bin<I, F>>;
//     fn get(&self, object: &ItemWithData<'a, Self::Object>) -> Self::IntoItem {
//         self.0.get_opt(object).map(|item| self.1.bin(item))
//     }
// }

pub struct Ratio<A: Attribute<Object=T>, P: Attribute<Object=T>, T>(pub A, pub P);
impl<A, P, T> Attribute for Ratio<A, P, T>
    where A: Attribute<Object=T>,
          P: Attribute<Object=T> {

    type Object = T;
}
impl<'a, A, P, T> OptionGetter<'a> for Ratio<A, P, T>
    where A: Attribute<Object=T> + OptionCountable<'a>,
          P: Attribute<Object=T> + OptionCountable<'a> {
    type IntoItem = Fraction<usize>;
    fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
        match (self.0.count(object), self.1.count(object)) {
            (Some(n), Some(m)) => Some(Fraction::new(n, m)),
            _ => None,
        }
    }
}

impl<'a, A, P, T> Getter<'a> for Ratio<A, P, T>
    where A: Attribute<Object=T> + OptionCountable<'a>,
          P: Attribute<Object=T> + OptionCountable<'a> {
    type IntoItem = Option<Fraction<usize>>;
    fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
        match (self.0.count(object), self.1.count(object)) {
            (Some(n), Some(m)) => Some(Fraction::new(n, m)),
            _ => None,
        }
    }
}

/// Get an attribute's attribute.
pub struct From<O: Attribute, A: Attribute> (pub O, pub A);

impl<'a, O, A, T, I> Attribute for From<O, A>
    where O: Attribute<Object=T>, A: Attribute<Object=I> {
    type Object = T;
}

impl<'a, O, A, T, I, E> Getter<'a> for From<O, A>
    where O: Attribute<Object=T> + OptionGetter<'a, IntoItem=objects::ItemWithData<'a, I>>,
          A: Attribute<Object=I> + Getter<'a, IntoItem=E> {
    type IntoItem = Option<E>;
    fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
        self.0.get_opt(object).map(|object| self.1.get(&object))
    }
}

impl<'a, O, A, T, I, E> OptionGetter<'a> for From<O, A>
    where O: Attribute<Object=T> + OptionGetter<'a, IntoItem=objects::ItemWithData<'a, I>>,
          A: Attribute<Object=I> + OptionGetter<'a, IntoItem=E> {
    type IntoItem = E;
    fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
        self.0.get_opt(object).map(|object| self.1.get_opt(&object)).flatten()
    }
}

/// Get an attribute from each of a sequence of attributes.
pub struct FromEach<O: Attribute, A: Attribute> (pub O, pub A);

impl<'a, O, A, T> Attribute for FromEach<O, A>
    where O: Attribute<Object=T> /*+ OptionGetter<'a, IntoItem=Vec<I>>)*/, A: Attribute {
    //<Object=I>*/ {
    type Object = T;
}

impl<'a, O, A, T, I, E> Getter<'a> for FromEach<O, A>
    where O: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<objects::ItemWithData<'a, I>>>,
          A: Attribute<Object=I> + Getter<'a, IntoItem=E> {
    type IntoItem = Option<Vec<E>>;
    fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
        self.0.get_opt(object).map(|v| {
            v.iter().map(|object| { self.1.get(object) }).collect()
        })
    }
}

impl<'a, O, A, T, I, E> OptionGetter<'a> for FromEach<O, A>
    where O: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<objects::ItemWithData<'a, I>>>,
          A: Attribute<Object=I> + OptionGetter<'a, IntoItem=E> {
    type IntoItem = Vec<E>;
    fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
        self.0.get_opt(object).map(|v| {
            v.iter().flat_map(|object| { self.1.get_opt(object) }).collect()
        })
    }
}

// Get an attribute from each of a sequence of attributes buy only if a specific condition was met.
pub struct FromEachIf<A: Attribute, P> (pub A, pub P);

impl<'a, A, P, T> Attribute for FromEachIf<A, P>
    where A: Attribute<Object=T> {
    type Object = T;
}

impl<'a, A, P, T, I> OptionGetter<'a> for FromEachIf<A, P>
    where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<objects::ItemWithData<'a, I>>>,
          P: Filter<'a, Item=I> {
    type IntoItem = Vec<objects::ItemWithData<'a, I>>;
    fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
        self.0.get_opt(object).map(|items| {
            items.into_iter()
                .filter(|item| self.1.accept(item))
                .collect()
        })
    }
}

impl<'a, A, P, T, I> Getter<'a> for FromEachIf<A, P>
    where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<objects::ItemWithData<'a, I>>>,
          P: Filter<'a, Item=I> {
    type IntoItem = Option<Vec<objects::ItemWithData<'a, I>>>;
    fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
        self.0.get_opt(object).map(|items| {
            items.into_iter()
                .filter(|item| self.1.accept(item))
                .collect()
        })
    }
}

impl<'a, A, P, T, I> Countable<'a> for FromEachIf<A, P>
    where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<objects::ItemWithData<'a, I>>>,
          P: Filter<'a, Item=I> {
    fn count(&self, object: &objects::ItemWithData<'a, Self::Object>) -> usize {
        self.get_opt(object).map_or(0, |vector| vector.len())
        // Could potentially count straight from iter, but would have to reimplement all of
        // get_opt. It would save allocating the vector.
    }
}

impl<'a, A, P, T, I> OptionCountable<'a> for FromEachIf<A, P>
    where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<objects::ItemWithData<'a, I>>>,
          P: Filter<'a, Item=I> {
    fn count(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<usize> {
        self.get_opt(object).map(|vector| vector.len())
    }
}

macro_rules! impl_select {
        ($n:ident, $($ti:ident -> $i:tt),+) => {
            pub struct $n<$($ti: Attribute,)+> ($(pub $ti,)+);
            impl<T, $($ti,)+> Attribute for $n<$($ti,)+>
                where $($ti: Attribute<Object=T>,)+ {
                type Object = T;
            }
            impl<'a, T, $($ti,)+> OptionGetter<'a> for $n<$($ti,)+>
                where $($ti: Attribute<Object=T> + OptionGetter<'a>,)+ {
                type IntoItem = ($(Option<$ti::IntoItem>,)+);
                fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                    Some(($(self.$i.get_opt(object),)+))
                }
            }
            impl<'a, T, $($ti,)+> Getter<'a> for $n<$($ti,)+>
                where $($ti: Attribute<Object=T> + Getter<'a>,)+ {
                type IntoItem = ($($ti::IntoItem,)+);

                fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                    ($(self.$i.get(object),)+)
                }
            }
        }
    }

impl_select!(Select1,  Ta -> 0);
impl_select!(Select2,  Ta -> 0, Tb -> 1);
impl_select!(Select3,  Ta -> 0, Tb -> 1, Tc -> 2);
impl_select!(Select4,  Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3);
impl_select!(Select5,  Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4);
impl_select!(Select6,  Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4, Tf -> 5);
impl_select!(Select7,  Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4, Tf -> 5, Tg -> 6);
impl_select!(Select8,  Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4, Tf -> 5, Tg -> 6, Th -> 7);
impl_select!(Select9,  Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4, Tf -> 5, Tg -> 6, Th -> 7, Ti -> 8);
impl_select!(Select10, Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4, Tf -> 5, Tg -> 6, Th -> 7, Ti -> 8, Tj -> 9);
impl_select!(Select11, Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4, Tf -> 5, Tg -> 6, Th -> 7, Ti -> 8, Tj -> 9, Tk -> 10);
impl_select!(Select12, Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4, Tf -> 5, Tg -> 6, Th -> 7, Ti -> 8, Tj -> 9, Tk -> 10, Tl -> 11);

#[macro_export]
macro_rules! Select {
    ($ta:expr) => {
        Select1($ta)
    };
    ($ta:expr, $tb:expr) => {
        Select2($ta, $tb)
    };
    ($ta:expr, $tb:expr, $tc:expr) => {
        Select3($ta, $tb, $tc)
    };
    ($ta:expr, $tb:expr, $tc:expr, $td:expr) => {
        Select4($ta, $tb, $tc, $td)
    };
    ($ta:expr, $tb:expr, $tc:expr, $td:expr, $te:expr) => {
        Select5($ta, $tb, $tc, $td, $te)
    };
    ($ta:expr, $tb:expr, $tc:expr, $td:expr, $te:expr, $tf:expr) => {
        Select6($ta, $tb, $tc, $td, $te, $tf)
    };
    ($ta:expr, $tb:expr, $tc:expr, $td:expr, $te:expr, $tf:expr, $tg:expr) => {
        Select7($ta, $tb, $tc, $td, $te, $tf, $tg)
    };
    ($ta:expr, $tb:expr, $tc:expr, $td:expr, $te:expr, $tf:expr, $tg:expr, $th:expr) => {
        Select8($ta, $tb, $tc, $td, $te, $tf, $tg, $th)
    };
    ($ta:expr, $tb:expr, $tc:expr, $td:expr, $te:expr, $tf:expr, $tg:expr, $th:expr, $ti:expr) => {
        Select9($ta, $tb, $tc, $td, $te, $tf, $tg, $th, $ti)
    };
    ($ta:expr, $tb:expr, $tc:expr, $td:expr, $te:expr, $tf:expr, $tg:expr, $th:expr, $ti:expr, $tj:expr) => {
        Select10($ta, $tb, $tc, $td, $te, $tf, $tg, $th, $ti, $tj)
    };
    ($ta:expr, $tb:expr, $tc:expr, $td:expr, $te:expr, $tf:expr, $tg:expr, $th:expr, $ti:expr, $tj:expr, $tk:expr) => {
        Select11($ta, $tb, $tc, $td, $te, $tf, $tg, $th, $ti, $tj, $tk)
    };
    ($ta:expr, $tb:expr, $tc:expr, $td:expr, $te:expr, $tf:expr, $tg:expr, $th:expr, $ti:expr, $tj:expr, $tk:expr, $tl:expr) => {
        Select12($ta, $tb, $tc, $td, $te, $tf, $tg, $th, $ti, $tj, $tk, $tl)
    };
}

// pub trait DatabaseFactory {
//     fn with_cache<S>(self, cache_dir: S) -> Database where S: Into<String>;
//     //fn with<S>(self, cache_dir: S, log: log::Log) -> Database where S: Into<String>; // TODO figure out how to do this better
// }
//
// impl DatabaseFactory for parasite::DatastoreView {
//     fn with_cache<S>(self, cache_dir: S) -> Database where S: Into<String> {
//         Database::from_store(self, cache_dir)
//     }
// }

#[derive(Copy, Clone, Debug, PartialOrd, PartialEq, Ord, Eq)]
pub enum Date {
    NaiveDate,
    Rfc2822,
    Rfc3339,
}

impl Date {
    pub fn format_date<T>(&self, timestamp: T) -> String where T: AsTimestamp {
        match self {
            Date::NaiveDate => timestamp.as_naive_date_string(),
            Date::Rfc2822 => timestamp.as_utc_rfc2822_string(),
            Date::Rfc3339 => timestamp.as_utc_rfc3339_string(),
        }
    }
}

pub struct FormatDate<A: Attribute> (pub Date, pub A);

impl<'a, A, T> Attribute for FormatDate<A> where A: Attribute<Object=T> {
    type Object = T;
}

impl<'a, A, T> OptionGetter<'a> for FormatDate<A>
    where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=Timestamp> {
    type IntoItem = String;
    fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
        self.1.get_opt(object).map(|timestamp| self.0.format_date(timestamp))
    }
}

impl<'a, A, T> Getter<'a> for FormatDate<A>
    where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=Timestamp> {
    type IntoItem = Option<String>;
    fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
        self.1.get_opt(object).map(|timestamp| self.0.format_date(timestamp))
    }
}

pub struct FormatDuration<A: Attribute> (pub A);

impl<A> FormatDuration<A> where A: Attribute {
    const SECONDS_IN_A_YEAR: u64 = 60 * 60 * 24 * 365;
    const SECONDS_IN_A_MONTH: u64 = 60 * 60 * 24 * 30;
    const SECONDS_IN_A_WEEK: u64 = 60 * 60 * 24 * 7;
    const SECONDS_IN_A_DAY: u64 = 60 * 60 * 24;
    const SECONDS_IN_AN_HOUR: u64 = 60 * 60;
    const SECONDS_IN_A_MINUTE: u64 = 60;

    pub fn to_fuzzy_duration(duration: crate::time::Duration) -> String { // TODO this should be somewhere more useful like Duration or Seconds
        let seconds = duration.as_seconds();
        let (years, seconds) = (seconds / Self::SECONDS_IN_A_YEAR, seconds % Self::SECONDS_IN_A_YEAR);
        let (months, seconds) = (seconds / Self::SECONDS_IN_A_MONTH, seconds % Self::SECONDS_IN_A_MONTH);
        if years != 0 {
            if months != 0 {
                return format!("{}yr {}mt", years, months)
            } else {
                return format!("{}yr", years)
            }
        }

        let (weeks, seconds) = (seconds / Self::SECONDS_IN_A_WEEK, seconds % Self::SECONDS_IN_A_WEEK);
        if months != 0 {
            if weeks != 0 {
                return format!("{}mt {}wk", months, weeks)
            } else {
                return format!("{}mt", weeks)
            }
        }

        let (days, seconds) = (seconds / Self::SECONDS_IN_A_DAY, seconds % Self::SECONDS_IN_A_DAY);
        if weeks != 0 {
            if days != 0 {
                return format!("{}wk {}d", weeks, days)
            } else {
                return format!("{}wk", weeks)
            }
        }

        let (hours, seconds) = (seconds / Self::SECONDS_IN_AN_HOUR, seconds % Self::SECONDS_IN_AN_HOUR);
        if days != 0 {
            if hours != 0 {
                return format!("{}d {}hr", days, hours)
            } else {
                return format!("{}d", days)
            }
        }

        let (minutes, seconds) = (seconds / Self::SECONDS_IN_A_MINUTE, seconds % Self::SECONDS_IN_A_MINUTE);
        if hours != 0 {
            if minutes != 0 {
                return format!("{}hr {}min", hours, minutes)
            } else {
                return format!("{}hr", hours)
            }
        }

        return format!("{}min {}sec", minutes, seconds)
    }
}

impl<'a, A, T> Attribute for FormatDuration<A> where A: Attribute<Object=T> {
    type Object = T;
}

impl<'a, A, T> OptionGetter<'a> for FormatDuration<A>
    where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=crate::time::Duration> {
    type IntoItem = String;
    fn get_opt(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
        self.0.get_opt(object).map(|duration| Self::to_fuzzy_duration(duration))
    }
}

impl<'a, A, T> Getter<'a> for FormatDuration<A>
    where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=crate::time::Duration> {
    type IntoItem = Option<String>;
    fn get(&self, object: &objects::ItemWithData<'a, Self::Object>) -> Self::IntoItem {
        self.0.get_opt(object).map(|duration| Self::to_fuzzy_duration(duration))
    }
}