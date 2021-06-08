use structopt::StructOpt;

use djanco::*;
use djanco::csv::*;
use djanco::log::*;
use djanco::commandline::*;

fn main() {
    let config = Configuration::from_args();
    let log = Log::new(Verbosity::Debug);

    macro_rules! path { ($name:expr) => { config.output_csv_path($name) } }

    let database =
        Djanco::from_config(&config, timestamp!(December 2020), stores!(All), log.clone())
            .expect("Error initializing datastore.");

    database.projects().into_csv(path!("projects")).unwrap();
}