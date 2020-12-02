use structopt::StructOpt;

use dcd::DatastoreView;

use djanco::*;
use djanco::data::*;
use djanco::time;
use djanco::log::*;
use djanco::commandline::*;
use djanco::query::*;
use djanco::attrib::*;
use djanco::csv::CSV;

// works with downloader from commit 5e4e9d5deb0fe8f9c8bb3bae0ca6947633701346
// `cargo run --bin example --release -- -o ~/output -d /mnt/data/dataset -c /mnt/data/cache --data-dump=~/output/dump`
fn main() {
    let now = time::now();
    let config = Configuration::from_args();
    let log = Log::new(Verbosity::Debug);
    let store = DatastoreView::new(config.dataset_path(), now);
    let database =  Database::from_store(store, config.cache_path(), log);

    // TODO a priori size estimate for logging
    database.projects().sort_by_attrib(project::Stars).into_csv(config.output_csv_path("sort_by_stars")).unwrap();
    database.projects().select_by_attrib(project::Stars).into_csv(config.output_csv_path("select_by_stars")).unwrap();
    database.projects().group_by_attrib(project::Stars).ungroup().into_csv(config.output_csv_path("group_by_stars")).unwrap();
    database.projects().filter_by_attrib(require::Exactly(project::Language, objects::Language::C)).into_csv(config.output_csv_path("filter_by_language_c")).unwrap();
    database.projects().filter_by_attrib(require::And(require::AtLeast(project::Stars, 1), require::AtMost(project::Stars, 10))).into_csv(config.output_csv_path("filter_by_between_1_and_10_stars")).unwrap();
    database.projects().filter_by_attrib(require::Exists(project::Stars)).into_csv(config.output_csv_path("filter_by_has_stars")).unwrap();
    database.projects().filter_by_attrib(require::Same(project::Homepage, "http://manasource.org/")).into_csv(config.output_csv_path("filter_by_homepage_exact")).unwrap();
    database.projects().filter_by_attrib(require::Matches(project::Homepage, regex!("\\.org/?$"))).into_csv(config.output_csv_path("filter_by_homepage_regex")).unwrap();
    database.projects().filter_by_attrib(project::HasIssues).into_csv(config.output_csv_path("filter_by_has_issues")).unwrap();
    database.projects().sort_by_attrib(stats::Count(project::Commits)).into_csv(config.output_csv_path("sort_by_commit_count")).unwrap();
    database.projects().select_by_attrib(stats::Mean(get::FromEach(project::Commits, commit::MessageLength))).into_csv(config.output_csv_path("select_mean_commit_messages_length")).unwrap();
    database.projects().select_by_attrib(stats::Median(get::FromEach(project::Commits, commit::MessageLength))).into_csv(config.output_csv_path("select_median_commit_messages_length")).unwrap();

}

