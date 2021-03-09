use structopt::StructOpt;

use djanco::*;
use djanco::data::*;
use djanco::time::*;
use djanco::objects::*;
use djanco::csv::*;
use djanco::log::*;
use djanco::commandline::*;
use djanco::fraction::Fraction;

fn stars<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .group_by(project::Language)
        .sort_by(project::Stars)
        .sample(Distinct(Top(50), MinRatio(project::Commits, 0.9)))
        .ungroup()
}

fn mean_changed_paths<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .group_by(project::Language)
        .sort_by(Mean(FromEach(project::Commits, Count(commit::Paths))))
        .sample(Distinct(Top(50), MinRatio(project::Commits, 0.9)))
        .ungroup()
}

fn median_changed_paths<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .group_by(project::Language)
        .sort_by(Median(FromEach(project::Commits, Count(commit::Paths))))
        .sample(Distinct(Top(50), MinRatio(project::Commits, 0.9)))
        .ungroup()
}

fn experienced_author<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .group_by(project::Language)
        //.filter_by_attrib(AtLeast(Count(project::Users), 1))
        .filter_by(AtLeast(Count(FromEachIf(project::Users, AtLeast(user::Experience, Duration::from_years(2)))), 1))
        //.filter_by_attrib(Exists(project::UsersWith(MoreThan(user::Experience, Seconds::from_years(2)))))
        .sort_by(Count(project::Commits))
        .sample(Distinct(Top(50), MinRatio(project::Commits, 0.9)))
        .ungroup()
}

fn experienced_authors_ratio<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .group_by(project::Language)
        .filter_by(AtLeast(Count(project::Users), 2))
        .filter_by(AtLeast(Ratio(FromEachIf(project::Users, AtLeast(user::Experience, Duration::from_years(2))), project::Users), Fraction::new(1, 2)))
        //.sample(Distinct(Random(50, Seed(42)), MinRatio(project::Commits, 0.9)))
        .sample(Distinct(Top(50), MinRatio(project::Commits, 0.9)))
        .ungroup()
}

fn mean_commit_message_sizes<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .group_by(project::Language)
        .sort_by(Mean(FromEach(project::Commits, commit::MessageLength)))
        .sample(Distinct(Top(50), MinRatio(project::Commits, 0.9)))
        .ungroup()
}

fn median_commit_message_sizes<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .group_by(project::Language)
        .sort_by(Median(FromEach(project::Commits, commit::MessageLength)))
        .sample(Distinct(Top(50), MinRatio(project::Commits, 0.9)))
        .ungroup()
}

fn commits<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .group_by(project::Language)
        .sort_by(Count(project::Commits))
        .sample(Distinct(Top(50), MinRatio(project::Commits, 0.9)))
        .ungroup()
}

// `cargo run --bin example_queries --release -- -o ~/output -d /mnt/data/dataset -c /mnt/data/cache --data-dump=~/output/dump`
fn main() {
    let config = Configuration::from_args();
    let log = Log::new(Verbosity::Debug);

    macro_rules! path { ($name:expr) => { config.output_csv_path($name) } }

    let database =
        Djanco::from_spec(config.dataset_path(), config.cache_path(),
                          timestamp!(December 2020), stores!(All), log.clone())
            .expect("Error initializing datastore.");

    stars(&config, &log, &database).into_csv(path!("stars")).unwrap();
    mean_changed_paths(&config, &log, &database).into_csv(path!("mean_changed_paths")).unwrap();
    median_changed_paths(&config, &log, &database).into_csv(path!("median_changed_paths")).unwrap();
    experienced_author(&config, &log, &database).into_csv(path!("experienced_author")).unwrap();
    experienced_authors_ratio(&config, &log, &database).into_csv(path!("experienced_authors_ratio")).unwrap();
    mean_commit_message_sizes(&config, &log, &database).into_csv(path!("mean_commit_message_sizes")).unwrap();
    median_commit_message_sizes(&config, &log, &database).into_csv(path!("median_commit_message_sizes")).unwrap();
    commits(&config, &log, &database).into_csv(path!("commits")).unwrap();
}