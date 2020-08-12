use dcd::{DCD, Database};
use loader::selectors::*;

fn main() {
    let database = DCD::new("/dejavuii/dejacode/dataset-small".to_string());
    let projects = group_by_language_order_by_stars_top_n(&database, 1);
    for project_id in projects {
        let project = database.get_project(project_id).unwrap();
        println!("{:?} {:?} {:?}", project_id, project.get_language(), project.get_stars())
    }
}