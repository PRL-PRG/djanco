use structopt::StructOpt;

use dcd::{DCD, Database};

use select::selectors::*;
use select::meta::ProjectMeta;

macro_rules! with_elapsed_seconds {
    ($thing:expr) => {{
        let start = std::time::Instant::now();
        let result = { $thing };
        (result, start.elapsed().as_secs())
    }}
}

#[derive(StructOpt)]
struct Configuration {
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: std::path::PathBuf,

    #[structopt(parse(from_os_str), short = "d", long = "dataset")]
    dataset_path: std::path::PathBuf,
}

impl Configuration {
    fn dataset_path_as_string(&self) -> String {
        self.dataset_path.as_os_str().to_str().unwrap().to_string()
    }
}

fn main() {
    let configuration = Configuration::from_args();

    let (database, loading_time) =
        with_elapsed_seconds!(DCD::new(configuration.dataset_path_as_string()));

    let (projects, query_time) =
        with_elapsed_seconds!(group_by_language_order_by_stars_top_n(&database, 10));

    for project_id in projects {
        let project = database.get_project(project_id).unwrap();
        println!("{:?} {:?} {:?}", project_id, project.get_language(), project.get_stars())
    }

    eprintln!("(elapsed time: loading={}s, query={}s)", loading_time, query_time);
}