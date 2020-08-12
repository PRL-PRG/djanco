use dcd::{DCD, Database};
use selector::selectors::*;
use std::time::Instant;
use selector::meta::ProjectMeta;

macro_rules! with_elapsed_seconds {
    ($thing:expr) => {{
        let start = Instant::now();
        let result = { $thing };
        (result, start.elapsed().as_secs())
    }}
}

fn main() {
    let (database, loading_time) =
        with_elapsed_seconds!(DCD::new("/dejavuii/dejacode/dataset-tiny".to_string()));

    let (projects, query_execution_time) =
        with_elapsed_seconds!(group_by_language_order_by_stars_top_n(&database, 10));

    for project_id in projects {
        let project = database.get_project(project_id).unwrap();
        println!("{:?} {:?} {:?}", project_id, project.get_language(), project.get_stars())
    }

    eprintln!("elapsed time: loading = {}s, query = {}s", loading_time, query_execution_time);
}