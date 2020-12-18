use structopt::StructOpt;
//
use dcd::DatastoreView;

use djanco::*;
use djanco::data::*;
use djanco::time;
use djanco::csv::*;
use djanco::log::*;
use djanco::commandline::*;
use std::f64::{NAN, INFINITY, NEG_INFINITY};
use std::convert::TryInto;
use std::io::Write;
//use itertools::Itertools;
//use djanco::objects::*;
//
// enum Bucket {
//     Range(n, n),
// }

fn main() {
    let now = time::now();
    let config = Configuration::from_args();
    // let log = Log::new(Verbosity::Debug);
    //
    let store = DatastoreView::new(config.dataset_path(), now);
    // let database = Database::from_store(store, config.cache_path(), log);

    //store.project_urls().for_each(|(id, url)| println!("({}, \"{}\"),", id, url));
    //let path = config.dump_path().unwrap();
    //database.projects().dump_all_info_to(path).unwrap();

    //println!("{:?}", database.projects().map_into_attrib(project::Language).unique().collect::<Vec<Option<objects::Language>>>());

    for n in vec![0,1,2,3,4,5,6,7,8,9] {
        let x = store.content_data(n).unwrap();
        let mut f = std::fs::File::create(format!("output/{}.json", n)).unwrap();
        f.write_all(x.as_slice()).unwrap();
    }

    let n = 100usize;
    for i in vec![1f64, 0f64, -1f64, 100f64, -100f64, 99f64, 101f64, 0.5f64, NAN] {
        let b: i64 = (i / n as f64).floor() as i64; //.try_into().map_or(None, |e| Some(e));

        // b == NAN;
        // b == INFINITY;
        // b == NEG_INFINITY;

        let min = b * n as i64;
        let max = ((b + 1) * n as i64);

        println!("{} -> {:?}                    [{}, {})", i, b, min, max);
    }
}
