use structopt::StructOpt;

use djanco::*;
use djanco::commandline::*;
use djanco::objects::*;
use djanco::csv::*;
use djanco::log::{Log, Verbosity};
use parasite::{DatastoreView, StoreKind};
use barrier::commits_iter;
// `cargo run --bin dsl --release -- -o ~/output -d /mnt/data/dataset -c /mnt/data/cache --data-dump=~/output/dump`
fn main() {
    let config = Configuration::from_args();

    let store = DatastoreView::new(config.dataset_path());
    let savepoint = store.current_savepoint();

    let js_commits = commits_iter(&store,StoreKind::JavaScript, &savepoint);
    let c_commits = commits_iter(&store,StoreKind::C, &savepoint);

    println!("JS commits: {}", js_commits.count());
    println!("C commits: {}", c_commits.count());
}