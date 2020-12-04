use structopt::StructOpt;

use dcd::DatastoreView;

use djanco::*;
use djanco::data::*;
use djanco::time;
use djanco::csv::*;
use djanco::log::*;
use djanco::commandline::*;
use djanco::csv::CSV;
use djanco::objects::*;
use djanco::iterators::ItemWithData;
use itertools::Itertools;


fn main() {
    let now = time::now();
    let config = Configuration::from_args();
    let log = Log::new(Verbosity::Debug);

    let store = DatastoreView::new(config.dataset_path(), now);
    let database = Database::from_store(store, config.cache_path(), log);

    // database.projects()
    //     .filter(|p| p.id() == ProjectId::from(23usize))
    //     .flat_map(|p| p.snapshot_ids().unwrap())
    //     .into_csv("snapshots.csv")
    //     .unwrap();

    let selected_snapshot_ids: Vec<SnapshotId> =
        SnapshotId::from_csv("snapshots.csv").unwrap();

    // for id in selected_snapshot_ids {
        database.projects()
             .filter(|p| p.id() == ProjectId::from(23usize))
             .flat_map(|p| p.commits().unwrap().into_iter().map(|c| ItemWithData::new(p.data, c)).collect::<Vec<ItemWithData<Commit>>>())
             .for_each(|c| {
                 for change in c.changes().unwrap() {
                     println!("change: {} {:?} {} {} {:?}", c.id(), c.hash(), change.path_id(), database.path(&change.path_id()).unwrap().location(), change.snapshot_id());
                 }
             });

    println!("XXXX{:?}XXXX", selected_snapshot_ids);

    database.projects()
        .filter(|p| p.id() == ProjectId::from(23usize)).for_each(|p| {
        println!("project: {} {} ", p.id(), p.url());
        p.snapshot_ids().unwrap().into_iter().sorted().into_csv("snapshot_ids_from_23.csv").unwrap();
        p.commits().unwrap().into_iter().flat_map(|c| {
            c.changes(p.data).map(|changes| {
                changes.into_iter().flat_map(|c| c.snapshot_id()).collect::<Vec<SnapshotId>>()
            }).unwrap()
        }).sorted().unique().into_csv("snapshot_ids_from_23_via_commits.csv").unwrap();
    })
    // }
}
