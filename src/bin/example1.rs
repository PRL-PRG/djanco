use structopt::StructOpt;

use dcd::DatastoreView;

//use djanco::*;
use djanco::data::*;
use djanco::time;
use djanco::objects::*;
use djanco::query::*;
use djanco::csv::*;
use djanco::log::*;
use djanco::commandline::*;
use djanco::attrib::*;
use djanco::iterators::*;
use std::path::PathBuf;

// `cargo run --bin example1 --release -- -o ~/output -d /mnt/data/dataset -c /mnt/data/cache --data-dump=~/output/dump`
fn main() {
    let now = time::now();
    let config = Configuration::from_args();
    let log = Log::new(Verbosity::Debug);
    let store = DatastoreView::new(config.dataset_path(), now);
    let database = Database::from_store(store, config.cache_path(), log);

    // If file does not exist, filter snapshots with required string and save to file.
    if !PathBuf::from(config.output_csv_path("snapshots_with_memory_resource")).exists() {
        database.snapshots()
            .attach_data_to_each(&database) // dirty hack!
            .filter_by_attrib(require::Contains(snapshot::Contents, "#include <memory_resource>"))
            .map_into_attrib(snapshot::Id)
            .into_csv(config.output_csv_path("snapshots_with_memory_resource")).unwrap();
    }

    // Read snapshot IDs from file.
    let selected_snapshot_ids: Vec<SnapshotId> =
        SnapshotId::from_csv(config.output_csv_path("snapshots_with_memory_resource")).unwrap();

    // Select projects with at least one snapshot IDs from the list, sort them by stars and write to
    // CSV.
    database.projects()
        // hack
        .filter(|project| {
            project.snapshot_ids().map_or(false, |snapshot_ids| {
                snapshot_ids.iter().any(|snapshot_id| {
                    selected_snapshot_ids.contains(snapshot_id)
                })
            })
        })
        // end of hack
        //.filter_by_attrib(require::AnyIn(get::FromEach(project::Snapshots, snapshot::Id), selected_snapshot_ids))
        .sort_by_attrib(project::Stars)
        .into_csv(config.output_csv_path("projects_with_memory_resource")).unwrap()
}
