use structopt::StructOpt;

use djanco::*;
use djanco::data::*;
use djanco::objects::*;
use djanco::csv::*;
use djanco::log::*;
use djanco::commandline::*;

// rm -rf ~/djanco_cache && cargo run --bin clones --release -- -o ~/output -d /home/peta/devel/codedj-2/datasets/java-1k5-merged -c ~/djanco_cache --data-dump ~/output/dump > out.txt

fn main() {
    let config = Configuration::from_args();
    let log = Log::new(Verbosity::Debug);

    macro_rules! path { ($name:expr) => { config.output_csv_path($name) } }

    let database =
        Djanco::from_spec(config.dataset_path(), config.cache_path(),
                          timestamp!(December 2020), stores!(Generic), log.clone())
            .expect("Error initializing datastore.");

    //snapshots_by_num_projects(&config, &log, &database).into_csv(path!("snapshots_by_projects")).unwrap();
    projects_by_unique_files(&config, &log, &database).into_csv(path!("projects_by_unique_files")).unwrap();
    projects_by_original_files(&config, &log, &database).into_csv(path!("projects_by_original_files")).unwrap();
    projects_by_impact(&config, &log, &database).into_csv(path!("projects_by_impact")).unwrap();
    projects_by_files(&config, &log, &database).into_csv(path!("projects_by_files")).unwrap();
    //projects_by_major_language_ratio(&config, &log, &database).into_csv(path!("projects_by_major_language_ratio")).unwrap();
    projects_by_major_language_changes(&config, &log, &database).into_csv(path!("projects_by_major_language_changes")).unwrap();    
    projects_by_all_forks(&config, &log, &database).into_csv(path!("projects_by_all_forks")).unwrap();
    projects_by_loc(&config, &log, &database).into_csv(path!("projects_by_loc")).unwrap();
}

/*
fn snapshots_by_num_projects<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Snapshot>> {
    database
        .snapshots()
        .sort_by(snapshot::NumProjects)
        .sample(Top(50))
}
*/

fn projects_by_unique_files<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .sort_by(project::UniqueFiles)
        .sample(Top(50))
}

fn projects_by_original_files<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .sort_by(project::OriginalFiles)
        .sample(Top(50))
}

fn projects_by_impact<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .sort_by(project::Impact)
        .sample(Top(50))
}

fn projects_by_files<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .sort_by(project::Files)
        .sample(Top(50))
}

/*
fn projects_by_major_language_ratio<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .sort_by(project::MajorLanguageRatio)
        .sample(Top(50))
}
*/

fn projects_by_major_language_changes<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .sort_by(project::MajorLanguageChanges)
        .sample(Top(50))
}

fn projects_by_all_forks<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .sort_by(Count(project::AllForks))
        .sample(Top(50))
}

fn projects_by_loc<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .sort_by(project::Locs)
//        .sample(Top(50))
}


