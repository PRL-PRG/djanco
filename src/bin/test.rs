use std::path::PathBuf;
use std::collections::BTreeSet;
use std::iter::FromIterator;

use structopt::StructOpt;
use itertools::Itertools;

use dcd::DatastoreView;

use djanco::data::*;
use djanco::time;
use djanco::objects::*;
use djanco::csv::*;
use djanco::log::*;

// TODO
// * snapshots aka file contents
// * keep and produce receipt snippets
// * fix load filters, maybe base on git commit hash of query
// * logging everywhere

#[derive(StructOpt,Debug)]
pub struct Configuration {
    #[structopt(parse(from_os_str), short = "o", long = "output", name = "OUTPUT_PATH")]
    pub output_path: PathBuf,

    #[structopt(parse(from_os_str), short = "d", long = "dataset", name = "DCD_PATH")]
    pub dataset_path: PathBuf,

    // #[structopt(parse(from_os_str), short = "l", long = "timing-log", name = "TIMING_LOG_PATH", default_value = "timing.log")]
    // pub timing_log: PathBuf,

    // #[structopt(long = "experiment-group", short = "g", name = "EXPERIMENT_NAME", default_value = "")]
    // pub group: String,

    #[structopt(parse(from_os_str), short = "c", long = "cache", name = "PERSISTENT_CACHE_PATH")]
    pub cache_path: PathBuf,

    #[structopt(parse(from_os_str), long = "data-dump", name = "DATA_DUMP_PATH")]
    pub dump_path: Option<PathBuf>,

    #[structopt(long)]
    pub grep_snapshots: bool,
}

impl Configuration {
    fn path_to_string(p: &PathBuf) -> String { p.to_str().unwrap().to_owned() }

    pub fn dataset_path(&self) -> &str           { self.dataset_path.to_str().unwrap() }
    pub fn output_path(&self)  -> &str           { self.output_path.to_str().unwrap()  }
    pub fn cache_path(&self)   -> &str           { self.cache_path.to_str().unwrap()   }
    pub fn dump_path(&self)    -> Option<String> { self.dump_path.as_ref().map(Configuration::path_to_string) }

    pub fn output_csv_path<S>(&self, file: S) -> String where S: Into<String> {
        let mut path: PathBuf = self.output_path.clone();
        path.push(file.into());
        path.set_extension("csv");
        path.to_str().unwrap().to_owned()
    }
}

macro_rules! with_elapsed_secs {
    ($name:expr,$thing:expr) => {{
        eprintln!("Starting task {}...", $name);
        let start = std::time::Instant::now();
        let result = { $thing };
        let secs = start.elapsed().as_secs();
        eprintln!("Finished task {} in {}s.", $name, secs);
        (result, secs)
    }}
}

macro_rules! elapsed_secs {
    ($name:expr,$thing:expr) => {{
        eprintln!("Starting task {}...", $name);
        let start = std::time::Instant::now();
        { $thing };
        let secs = start.elapsed().as_secs();
        eprintln!("Finished task {} in {}s.", $name, secs);
        secs
    }}
}

// works with downloader from commit 5e4e9d5deb0fe8f9c8bb3bae0ca6947633701346 
// `cargo run --bin example --release -- -o ~/output -d /mnt/data/dataset -c /mnt/data/cache --data-dump=~/output/dump`
fn main() {
    let now = time::now();
    let config = Configuration::from_args();
    let log = Log::new(Verbosity::Debug);

    let (store, store_secs) = with_elapsed_secs!("open data store", {
        DatastoreView::new(config.dataset_path(), now)
    });


    let (database, database_secs) = with_elapsed_secs!("open database", {
        Database::from_store(store, config.cache_path(), log)
    });

    elapsed_secs!("projects", {
        database.projects().for_each(|p| eprintln!("{} {}", p.id(), p.url()));
    });

    elapsed_secs!("project_commits", {
        database.projects().map(|p| p.commits().is_some()).collect::<Vec<bool>>();
    });
}
