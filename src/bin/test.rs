use structopt::StructOpt;

use dcd::DatastoreView;

use djanco::data::*;
use djanco::time;
use djanco::csv::*;
use djanco::log::*;
use djanco::commandline::*;

fn main() {
    let now = time::now();
    let config = Configuration::from_args();
    let log = Log::new(Verbosity::Debug);

    let store = DatastoreView::new(config.dataset_path(), now);
    let database = Database::from_store(store, config.cache_path(), log);




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
