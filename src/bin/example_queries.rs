use structopt::StructOpt;

use dcd::DatastoreView;

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
use djanco::fraction::Fraction;

fn stars<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .group_by_attrib(project::Language)
        .sort_by_attrib(project::Stars)
        .sample(sample::Distinct(sample::Top(50), sample::Ratio(project::Commits, 0.9)))
        .ungroup()
}

fn mean_changed_paths<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .group_by_attrib(project::Language)
        .sort_by_attrib(stats::Mean(get::FromEach(project::Commits, stats::Count(commit::Paths))))
        .sample(sample::Distinct(sample::Top(50), sample::Ratio(project::Commits, 0.9)))
        .ungroup()
}

fn median_changed_paths<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .group_by_attrib(project::Language)
        .sort_by_attrib(stats::Median(get::FromEach(project::Commits, stats::Count(commit::Paths))))
        .sample(sample::Distinct(sample::Top(50), sample::Ratio(project::Commits, 0.9)))
        .ungroup()
}

fn experienced_authors_ratio<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .group_by_attrib(project::Language)
        .filter_by_attrib(require::AtLeast(stats::Count(project::Users), 2))
        //FIXME
        .filter_by_attrib(require::AtLeast(stats::Ratio(with::Requirement(project::Users, require::AtLeast(user::Experience, Duration::from_years(2))), project::Users), Fraction::new(1,2)))
        .sample(sample::Distinct(sample::Random(50, Seed(42)), sample::Ratio(project::Commits, 0.9)))
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

    stars(&config, &log, &database).into_csv(config.output_csv_path("stars")).unwrap();
    mean_changed_paths(&config, &log, &database).into_csv(config.output_csv_path("mean_changed_paths")).unwrap();
    median_changed_paths(&config, &log, &database).into_csv(config.output_csv_path("median_changed_paths")).unwrap();
    experienced_authors_ratio(&config, &log, &database).into_csv(config.output_csv_path("experienced_authors_ratio")).unwrap();

}