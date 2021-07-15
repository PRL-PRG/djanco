use clap::Clap;

use djanco::*;
use djanco::csv::*;
use djanco::log::*;

fn main() {
    let config = Configuration::parse();
    let database =
        Djanco::from_config(&config, timestamp!(December 2020), store!(JavaScript, TypeScript, Python), Log::new(Verbosity::Log)).unwrap();

    database.commits()
        .sample(Random(100, Seed(42)))
        .map_into(commit::Tree)
        .into_files_in_dir(&config.output_path).unwrap();
}