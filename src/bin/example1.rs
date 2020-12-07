use std::collections::BTreeSet;
use std::iter::FromIterator;

use structopt::StructOpt;
use itertools::Itertools;

use dcd::DatastoreView;

use djanco::*;
use djanco::data::*;
use djanco::time;
use djanco::objects::*;
use djanco::csv::*;
use djanco::log::*;
use djanco::commandline::*;
use std::fs::File;
use std::io::Write;

// `cargo run --bin example1 --release -- -o ~/output -d /mnt/data/dataset -c /mnt/data/cache --data-dump=~/output/dump`
fn main() {
    let now = time::now();
    let config = Configuration::from_args();
    let log = Log::new(Verbosity::Debug);

    let (store, store_secs) = with_elapsed_secs!("open data store", {
        DatastoreView::new(config.dataset_path(), now)
    });


    let (database, database_secs) = with_elapsed_secs!("open database", {
        Database::from_store(store, config.cache_path(), log)
    });

    // database.snapshots()
    //     .filter_by_attrib(require::Contains(snapshot::Contents, "#include <memory_resource>"))
    //     .select_attib(snapshot::Id)
    //     .into_csv(config.output_csv_path("snapshots_with_memory_resource"));

    if config.grep_snapshots {
        let (snapshot_ids, _) = with_elapsed_secs!("find snapshots", {
            let snapshot_ids = database.snapshots().flat_map(|snapshot| {
                if snapshot.contains("#include <memory_resource>") {
                    Some(snapshot.id())
                } else {
                    None
                }
            }).collect::<Vec<SnapshotId>>();
            snapshot_ids
        });

        let _ = elapsed_secs!("save snapshots", {
            snapshot_ids.into_iter()
                .into_csv(config.output_csv_path("snapshots_with_memory_resource")).unwrap()
        });
    }

    let (selected_snapshot_ids, load_snapshots_secs) = with_elapsed_secs!("load snapshots", {
        let selected_snapshot_ids: Vec<SnapshotId> =
            SnapshotId::from_csv(config.output_csv_path("snapshots_with_memory_resource")).unwrap();
        BTreeSet::from_iter(selected_snapshot_ids.into_iter())
    });

    let export_snapshots_secs = elapsed_secs!("export snapshots", {
        for snapshot_id in selected_snapshot_ids.iter() {
            if let Some(snapshot) = database.snapshot(snapshot_id) {
                //snapshot.raw_contents()
                let path = config.output_path_in_subdir("snapshots", snapshot_id.to_string(), "txt").unwrap();
                let mut file = File::create(path).unwrap();
                file.write(snapshot.raw_contents()).unwrap();
            }
        }
    });



    let (selected_projects, select_projects_secs) = with_elapsed_secs!("select projects", {
        database.projects().filter(|project| {
            project.snapshot_ids()
                .map_or(false, |snapshot_ids| {
                    snapshot_ids.iter().any(|snapshot_id| {
                        selected_snapshot_ids.contains(snapshot_id)
                    })
                })
        }).sorted_by_key(|project| project.star_count())
    });

    let save_selected_projects_secs = elapsed_secs!("save selected projects", {
        selected_projects
            .into_csv(config.output_csv_path("projects_with_memory_resource")).unwrap()
    });

    eprintln!("Summary");
    eprintln!("   open data store:        {}s", store_secs);
    eprintln!("   open database:          {}s", database_secs);
    //eprintln!("   find snapshots:         {}s", find_snapshots_secs);
    //eprintln!("   save snapshots:         {}s", save_snapshots_secs);
    eprintln!("   load snapshots:         {}s", load_snapshots_secs);
    eprintln!("   export snapshots:       {}s", export_snapshots_secs);
    eprintln!("   select projects:        {}s", select_projects_secs);
    eprintln!("   save selected projects: {}s", save_selected_projects_secs);
}
