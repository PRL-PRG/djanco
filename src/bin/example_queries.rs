use std::collections::BTreeSet;
use std::iter::FromIterator;

use structopt::StructOpt;
use itertools::Itertools;

use dcd::DatastoreView;

use djanco::*;
use djanco::data::*;
use djanco::time;
use djanco::objects::*;
use djanco::csv::*;
use djanco::log::*;
use djanco::commandline::*;
use djanco::attrib::*;
use djanco::query::*;
use djanco::iterators::*;
use djanco::query::sample::*;

fn stars<'a>(config: &Configuration, log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .group_by_attrib(project::Language)
        .sort_by_attrib(project::Stars)
        .sample(Random(10, Seed(42)))
        .sample(sample::Distinct(sample::Top(5), sample::Ratio(project::Commits, 0.9)))
        .ungroup()
}

// works with downloader from commit 5e4e9d5deb0fe8f9c8bb3bae0ca6947633701346 
// `cargo run --bin example --release -- -o ~/output -d /mnt/data/dataset -c /mnt/data/cache --data-dump=~/output/dump`
fn main() {
    let now = time::now();
    let config = Configuration::from_args();
    let log = Log::new(Verbosity::Debug);

    let store = DatastoreView::new(config.dataset_path(), now);
    let database = Database::from_store(store, config.cache_path(), log.clone());

    stars(&config, &log, &database).into_csv(config.output_csv_path("stars")).unwrap()
}