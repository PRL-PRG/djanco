use structopt::StructOpt;

use dcd::DatastoreView;

use djanco::data::*;
use djanco::time;
use djanco::csv::*;
use djanco::log::*;
use djanco::commandline::*;
use djanco::objects::SnapshotId;

fn main() {
    let now = time::now();
    let config = Configuration::from_args();
    let log = Log::new(Verbosity::Debug);

    let store = DatastoreView::new(config.dataset_path(), now);
    let database = Database::from_store(store, config.cache_path(), log);

    database.commits().take(10).map(|c| c.hash()).for_each(|e| println!("{:?}", e))

    //database.projects().filter(|p| p.id() == )
    // let x: Vec<(String, String, /*String,*/ SnapshotId)> =
    // database.projects().take(10).flat_map(|project| {
    //     let project_id = project.url().clone();
    //     project.commits().map_or(vec![], |v| {
    //         v.iter().flat_map(|c| {
    //             let hash = c.hash(project.data).unwrap_or(String::new());
    //             let changes = c.change_ids(project.data).unwrap_or(vec!());
    //             changes.iter().map(|(path_id, snapshot_id)|{
    //                (project_id.clone(), hash.clone(),
    //                 //database.path(path_id).map_or(String::new(), |p| p.location()),
    //                 snapshot_id.clone())
    //             }).collect::<Vec<(String, String, /*String,*/ SnapshotId)>>()
    //         }).collect::<Vec<(String, String, /*String,*/ SnapshotId)>>()
    //     })
    // })
    // .collect();
    //
    // for (project, commit, /*path,*/ snapshot) in x {
    //     println!("{},{},{}", project, commit, /*path,*/ snapshot);
    // }


    // let snapshot1 =
    //     database.snapshot(&SnapshotId(375603357u64))
    //         .map(|s| s.contents().to_string());
    // eprintln!("snapshot1\n{:?}\n------------------------------------------------------", snapshot1);
    //
    // let snapshot2: Vec<String> =
    //     database.snapshots()
    //         .filter(|s| s.id() == SnapshotId(375603357u64))
    //         .map(|s| s.contents().to_string())
    //         .collect();
    // for string in snapshot2 {
    //     eprintln!("snapshot2\n{:?}\n------------------------------------------------------", string);
    // }
}
