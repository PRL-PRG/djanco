use structopt::StructOpt;

use dcd::DatastoreView;

use djanco::*;
use djanco::data::*;
use djanco::objects::*;
use djanco::csv::*;
use djanco::log::*;
use djanco::commandline::*;
use std::path::PathBuf;

// `cargo run --bin example1 --release -- -o ~/output -d /mnt/data/dataset -c /mnt/data/cache --data-dump=~/output/dump`
fn main() {
    let config = Configuration::from_args();
    let log = Log::new(Verbosity::Debug);
    let store = DatastoreView::new(config.dataset_path(), timestamp!(December 2020));
    let database = Database::from_store(store, config.cache_path(), log);

    // If file does not exist, filter snapshots with required string and save to file.
    if !PathBuf::from(config.output_csv_path("snapshots_with_memory_resource")).exists() {
        database.snapshots_with_data()
            .filter_by_attrib(Contains(snapshot::Contents, "#include <memory_resource>"))
            .map_into_attrib(snapshot::Id)
            .into_csv(config.output_csv_path("snapshots_with_memory_resource")).unwrap();
    }

    // Read snapshot IDs from file.
    let selected_snapshot_ids: Vec<SnapshotId> =
        SnapshotId::from_csv(config.output_csv_path("snapshots_with_memory_resource")).unwrap();

    // Select projects with at least one snapshot IDs from the list, sort them by stars and write to
    // CSV.
    database.projects()
        .filter_by_attrib(AnyIn(project::SnapshotIds, selected_snapshot_ids))
        .sort_by_attrib(project::Stars)
        .into_csv(config.output_csv_path("projects_with_memory_resource")).unwrap()
}
