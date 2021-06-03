use structopt::StructOpt;

use djanco::*;
use djanco::data::*;
use djanco::objects::*;
use djanco::csv::*;
use djanco::log::*;
use djanco::commandline::*;


// rm -rf ~/djanco_cache && cargo run --bin stefanie --release -- -o ~/output -d ~//Documents/prague/work/codedj-parasite/example-dataset -c ~/djanco_cache --data-dump ~/output/dump > out.txt

fn main() {
    let config = Configuration::from_args();
    let log = Log::new(Verbosity::Debug);

    macro_rules! path { ($name:expr) => { config.output_csv_path($name) } }

    let database =
        Djanco::from_spec(config.dataset_path(), config.cache_path(),
                          timestamp!(March 2021), stores!(Python), log.clone())
            .expect("Error initializing datastore.");

    projects_by_duplicated_code(&config, &log, &database).into_csv(path!("projects_by_duplicated_code")).unwrap();
}

fn projects_by_duplicated_code<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        //.sort_by(project::DuplicatedCode)
        .sample(Top(50))
}


