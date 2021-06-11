use clap::Clap;

use djanco::*;
use djanco::database::*;
use djanco::objects::*;
use djanco::csv::*;
use djanco::log::*;

// rm -rf ~/djanco_cache && cargo run --bin stefanie --release -- -o ~/output -d ~//Documents/prague/work/codedj-parasite/example-dataset -c ~/djanco_cache --data-dump ~/output/dump > out.txt

fn main() {
    let config = Configuration::parse();
    let log = Log::new(Verbosity::Debug);

    let database =
        Djanco::from_config(&config, timestamp!(March 2021), stores!(Python), log.clone())
            .expect("Error initializing datastore.");

    projects_by_duplicated_code(&config, &log, &database).into_csv_in_dir(&config.output_path, "projects_by_duplicated_code").unwrap();
}

fn projects_by_duplicated_code<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        //.sort_by(project::DuplicatedCode)
        .sample(Top(50))
}


