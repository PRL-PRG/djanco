use dcd::{DCD, Database};
use loader::selectors::*;

fn main() {
    let database = DCD::new("/dejavuii/dejacode/dataset-tiny".to_string());
    let projects = group_by_language_order_by_stars_top_n(&database, 100);
    for project in projects {
        println!("{:?}", project)
    }
}