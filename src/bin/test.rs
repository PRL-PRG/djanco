use structopt::StructOpt;
//
use dcd::DatastoreView;

use djanco::*;
use djanco::data::*;
use djanco::time;
use djanco::csv::*;
use djanco::log::*;
use djanco::commandline::*;
use itertools::Itertools;
//use djanco::objects::*;


fn main() {
    let now = time::now();
    let config = Configuration::from_args();
    let log = Log::new(Verbosity::Debug);

    let store = DatastoreView::new(config.dataset_path(), now);
    let database = Database::from_store(store, config.cache_path(), log);

    //store.project_urls().for_each(|(id, url)| println!("({}, \"{}\"),", id, url));
    let path = config.dump_path().unwrap();
    database.projects().dump_all_info_to(path).unwrap();

    //println!("{:?}", database.projects().map_into_attrib(project::Language).unique().collect::<Vec<Option<objects::Language>>>());

    // let x = store.content_data(0).unwrap();
    // println!("{}", x.to_str_lossy().to_string());
}
