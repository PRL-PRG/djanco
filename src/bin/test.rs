use structopt::StructOpt;
//
use dcd::DatastoreView;

use djanco::*;
use djanco::data::*;
use djanco::time;
use djanco::csv::*;
use djanco::log::*;
use djanco::commandline::*;
use djanco::csv::CSV;
use djanco::objects::*;
use itertools::Itertools;


fn main() {
    let now = time::now();
    let config = Configuration::from_args();
    let log = Log::new(Verbosity::Debug);

    let store = DatastoreView::new(config.dataset_path(), now);
    //let database = Database::from_store(store, config.cache_path(), log);

    store.project_urls().for_each(|(id, url)| println!("({}, \"{}\"),", id, url));
}
