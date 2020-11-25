use djanco::objects::{Project, ProjectId};
use std::path::PathBuf;
use std::collections::BTreeSet;
use std::iter::FromIterator;

use structopt::StructOpt;
use itertools::Itertools;

use dcd::DatastoreView;

#[macro_use] use djanco::*;
use djanco::data::*;
use djanco::time;
use djanco::objects::*;
use djanco::csv::*;
use djanco::log::*;
use djanco::commandline::*;
use chrono::format::Fixed::ShortMonthName;

fn main() {
    let now = time::now();
    let config = Configuration::from_args();
    let log = Log::new(Verbosity::Debug);

    let store = DatastoreView::new(config.dataset_path(), now);
    let database = Database::from_store(store, config.cache_path(), log);

    let snapshot1 =
        database.snapshot(&SnapshotId(375603357u64))
            .map(|s| s.contents().to_string());
    eprintln!("snapshot1\n{:?}\n------------------------------------------------------", snapshot1);

    let snapshot2: Vec<String> =
        database.snapshots()
            .filter(|s| s.id() == SnapshotId(375603357u64))
            .map(|s| s.contents().to_string())
            .collect();
    for string in snapshot2 {
        eprintln!("snapshot2\n{:?}\n------------------------------------------------------", string);
    }
}
