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

use djanco::attrib::*;
use djanco::iterators::*;
use djanco::query::*;
use djanco::attrib::AttributeIterator;

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

    // TODO a priori size estimate for logging
    database.projects().sort_by_attrib(project::Stars).count(); // TODO logging
    database.projects().select_by_attrib(project::Stars).count();
    database.projects().group_by_attrib(project::Stars).count();
    database.projects().filter_by_attrib(require::Exactly(project::Language, objects::Language::C));
    database.projects().filter_by_attrib(require::And(require::AtLeast(project::Stars, 1), require::AtMost(project::Stars, 10))).count();
    database.projects().filter_by_attrib(require::Exists(project::Stars));
    database.projects().filter_by_attrib(require::Same(project::Homepage, "kondziu.github.io"));
    database.projects().filter_by_attrib(require::Matches(project::Homepage, regex!("www.*")));
    database.projects().filter_by_attrib(project::HasIssues);
    database.projects().sort_by_attrib(stats::Count(project::Commits));
    database.projects().select_by_attrib(retrieve::FromEach(project::Commits, commit::Message));

}
