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

    database.projects()
        .map((|project| project.star_count()))
        .into_csv(config.output_csv_path("stars.csv")).unwrap()
}
