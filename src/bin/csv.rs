use clap::Parser;

use djanco::*;
use djanco::csv::*;
use djanco::log::*;

fn main() {
    let config = Configuration::parse();
    let database =
        Djanco::from_config(&config, timestamp!(December 2020), store!(JavaScript, TypeScript, Python), Log::new(Verbosity::Log)).unwrap();

    database.projects().into_csv_in_dir(&config.output_path, "projects.csv").unwrap();
    database.projects().into_extended_csv_in_dir(&config.output_path, "projects-extended.csv").unwrap();

    database.commits().into_csv_in_dir(&config.output_path, "commits.csv").unwrap();
    database.commits().into_extended_csv_in_dir(&config.output_path, "commits-extended.csv").unwrap();

    database.users().into_csv_in_dir(&config.output_path, "users.csv").unwrap();
    database.users().into_extended_csv_in_dir(&config.output_path, "users-extended.csv").unwrap();

    database.paths().into_csv_in_dir(&config.output_path, "paths.csv").unwrap();
    database.paths().into_extended_csv_in_dir(&config.output_path, "paths-extended.csv").unwrap();

    database.snapshots().into_csv_in_dir(&config.output_path, "snapshots.csv").unwrap();
    database.snapshots().into_extended_csv_in_dir(&config.output_path, "snapshots-extended.csv").unwrap();

    database.commits().map_into(commit::Tree).into_csv_in_dir(&config.output_path, "trees.csv").unwrap();
    database.commits().map_into(commit::Tree).into_extended_csv_in_dir(&config.output_path, "trees-extended.csv").unwrap();

    database.commits().map_into(commit::Changes).flat_map(|e| e).flat_map(|e| e).into_csv_in_dir(&config.output_path, "changes.csv").unwrap();
    database.commits().map_into(commit::Changes).flat_map(|e| e).flat_map(|e| e).into_extended_csv_in_dir(&config.output_path, "changes-extended.csv").unwrap();
}
