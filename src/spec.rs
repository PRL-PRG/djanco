use std::path::PathBuf;
use crate::time::Month;

#[derive(Clone,Debug)]
pub struct Spec {
    pub warehouse: PathBuf,
    pub database: Option<PathBuf>,
    pub seed: u128,
    pub timestamp: Month,
    //pub log_level: LogLevel,
}

impl Spec {
    pub fn new<S: Into<String>>(warehouse: S, database: Option<S>, seed: u128, timestamp: Month) -> Self {
        Spec { warehouse: PathBuf::from(warehouse.into()),
            database: database.map(|database| PathBuf::from(database.into())),
            seed, timestamp }
    }
    pub fn from_paths(warehouse: PathBuf, database: Option<PathBuf>, seed: u128, timestamp: Month) -> Self {
        Spec { warehouse, database, seed, timestamp }
    }
    pub fn path_as_string(&self) -> String {
        self.warehouse.as_os_str().to_str().unwrap().to_owned()
    }
}