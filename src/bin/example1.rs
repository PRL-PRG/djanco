use std::path::PathBuf;

use structopt::StructOpt;

use parasite::DatastoreView;

use djanco::*;
use djanco::objects::*;
use djanco::csv::*;
use djanco::commandline::*;

// `cargo run --bin example1 --release -- -o ~/output -d /mnt/data/dataset -c /mnt/data/cache --data-dump=~/output/dump`
fn main() {
    let config = Configuration::from_args();
    let database =
        Djanco::from_spec(config.dataset_path(), config.cache_path(), timestamp!(December 2020), vec![]);

    // If file does not exist, filter snapshots with required string and save to file.
    if !PathBuf::from(config.output_csv_path("snapshots_with_memory_resource")).exists() {
        database.snapshots_with_data()
            .filter_by(Contains(snapshot::Contents, "#include <memory_resource>"))
            .map_into(snapshot::Id)
            .into_csv(config.output_csv_path("snapshots_with_memory_resource")).unwrap();
    }

    // Read snapshot IDs from file.
    let selected_snapshot_ids: Vec<SnapshotId> =
        SnapshotId::from_csv(config.output_csv_path("snapshots_with_memory_resource")).unwrap();

    // Select projects with at least one snapshot IDs from the list, sort them by stars and write to
    // CSV.
    database.projects()
        .filter_by(AnyIn(project::SnapshotIds, selected_snapshot_ids))
        .sort_by(project::Stars)
        .into_csv(config.output_csv_path("projects_with_memory_resource")).unwrap()
}
