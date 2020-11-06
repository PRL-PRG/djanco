use structopt::StructOpt;
use std::path::PathBuf;

use djanco::data::Data;
use dcd::DatastoreView;
use djanco::time;

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
    pub dump_path: Option<PathBuf>
}

impl Configuration {
    fn path_to_string(p: &PathBuf) -> String { p.to_str().unwrap().to_owned() }

    pub fn dataset_path(&self) -> &str           { self.dataset_path.to_str().unwrap() }
    pub fn output_path(&self)  -> &str           { self.output_path.to_str().unwrap()  }
    pub fn cache_path(&self)   -> &str           { self.cache_path.to_str().unwrap()   }
    pub fn dump_path(&self)    -> Option<String> { self.dump_path.as_ref().map(Configuration::path_to_string) }
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

// macro_rules! elapsed_secs {
//     ($name:expr,$thing:expr) => {{
//         eprintln!("Starting task {}...", $name);
//         let start = std::time::Instant::now();
//         { $thing };
//         let secs = start.elapsed().as_secs();
//         eprintln!("Finished task {} in {}s.", $name, secs);
//         secs
//     }}
// }

// works with downloader from commit  146e55e34ca1f4cc5b826e0c909deac96afafc17
// `cargo run --bin example --release -- -o ~/output -d /mnt/data/dataset -c /mnt/data/cache --data-dump=~/output/dump`
fn main() {
    let now = time::now();
    let config = Configuration::from_args();

    let (store, store_secs) = with_elapsed_secs!("open data store", {
        DatastoreView::new(config.dataset_path(), now)
    });

    let (mut data, data_secs) = with_elapsed_secs!("open database", {
        Data::from_store(store, config.cache_path())
    });

    let (count_projects, count_projects_secs) = with_elapsed_secs!("count projects", {
        data.projects()//.count()
    });

    eprintln!("Summary");
    eprintln!("   open data store:       {}s", store_secs);
    eprintln!("   open database:         {}s", data_secs);
    eprintln!("   count projects:        {}s", count_projects_secs);
}
