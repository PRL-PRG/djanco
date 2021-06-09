use clap::Clap;

use djanco::*;
use djanco::csv::*;
use djanco::log::*;

fn main() {
    let config = Configuration::parse();
    let log = Log::new(Verbosity::Debug);

    let database =
        Djanco::from_options(&config, timestamp!(December 2020), stores!(All), log.clone())
            .expect("Error initializing datastore.");

    database.projects().into_csv_in_dir(&config.output_path, "projects").unwrap();
}