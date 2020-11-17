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
    pub dump_path: Option<PathBuf>
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

// works with downloader from commit  146e55e34ca1f4cc5b826e0c909deac96afafc17
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

    // let (snapshot_ids, find_snapshots_secs) = with_elapsed_secs!("find snapshots", {
    //     let snapshot_ids = database.snapshots().flat_map(|snapshot| {
    //         if snapshot.contains("#include <memory_resource>") {
    //             let id = snapshot.id();
    //             eprint!("\\u001b[32m+{} \\u001b[0m", id);
    //             Some(snapshot.id())
    //         } else {
    //             let id = snapshot.id();
    //             eprint!("-{} ", id);
    //             None
    //         }
    //     }).collect::<Vec<SnapshotId>>();
    //     eprintln!("\nfound {} snapshot_ids", snapshot_ids.len());
    //     snapshot_ids
    // });

    // let save_snapshots_secs = elapsed_secs!("save snapshots", {
    //     snapshot_ids.into_iter()
    //         .into_csv(config.output_csv_path("snapshots_with_memory_resource")).unwrap()
    // });

    let (selected_snapshot_ids, load_snapshots_secs) = with_elapsed_secs!("load snapshots", {
        let selected_snapshot_ids: Vec<SnapshotId> =
            SnapshotId::from_csv(config.output_csv_path("snapshots_with_memory_resource")).unwrap();
        BTreeSet::from_iter(selected_snapshot_ids.into_iter())
    });

    let (selected_projects, select_projects_secs) = with_elapsed_secs!("select projects", {
        database.projects().filter(|project| {
            project.snapshot_ids()
                .map_or(false, |snapshot_ids| {
                    snapshot_ids.iter().any(|snapshot_id| {
                        selected_snapshot_ids.contains(snapshot_id)
                    })
                })
        }).sorted_by_key(|project| project.star_count())
    });

    let save_selected_projects_secs = elapsed_secs!("save selected projects", {
        selected_projects
            .into_csv(config.output_csv_path("projects_with_memory_resource")).unwrap()
    });

    eprintln!("Summary");
    eprintln!("   open data store:        {}s", store_secs);
    eprintln!("   open database:          {}s", database_secs);
    //eprintln!("   find snapshots:         {}s", find_snapshots_secs);
    //eprintln!("   save snapshots:         {}s", save_snapshots_secs);
    eprintln!("   load snapshots:         {}s", load_snapshots_secs);
    eprintln!("   select projects:        {}s", select_projects_secs);
    eprintln!("   save selected projects: {}s", save_selected_projects_secs);
}
