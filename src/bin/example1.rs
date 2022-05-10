use std::path::PathBuf;

use clap::Parser;

use djanco::*;
use djanco::objects::*;
use djanco::csv::*;
use djanco::log::{Verbosity, Log};

// `cargo run --bin example1 --release -- -o ~/output -d /mnt/data/dataset -c /mnt/data/cache --data-dump=~/output/dump`
fn main() {
    let config = Configuration::parse();
    let database =
        Djanco::from_config(&config, timestamp!(December 2020), vec![], Log::new(Verbosity::Log))
            .expect("Error initializing datastore.");

    // If file does not exist, filter snapshots with required string and save to file.
    if !PathBuf::from(config.path_in_output_dir("snapshots_with_memory_resource", "csv").unwrap()).exists() {
        database.snapshots_with_data()
            .filter_by(Contains(snapshot::Contents, "#include <memory_resource>"))
            .map_into(snapshot::Id)
            .into_csv_in_dir(&config.output_path, "snapshots_with_memory_resource").unwrap();
    }

    // Read snapshot IDs from file.
    let selected_snapshot_ids: Vec<SnapshotId> =
        SnapshotId::from_csv_in_dir(&config.output_path, "snapshots_with_memory_resource").unwrap();

    // Select projects with at least one snapshot IDs from the list, sort them by stars and write to
    // CSV.
    database.projects()
        .filter_by(AnyIn(project::SnapshotIds, selected_snapshot_ids))
        .sort_by(project::Stars)
        .into_csv_in_dir(&config.output_path, "projects_with_memory_resource").unwrap()
}
