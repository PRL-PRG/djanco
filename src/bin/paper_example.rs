use djanco::*;
use dcd::*;
use djanco::csv::*;

const DATASET_PATH: &'static str = "";
const CACHE_PATH: &'static str = "";
const OUTPUT_PATH: &'static str = "";

fn main() {
    DatastoreView::new(DATASET_PATH, timestamp!(December 2020))
        .with_cache(CACHE_PATH)
        .projects()
        .group_by_attrib(project::Language)
        .filter_by_attrib(AtLeast(Count(project::Users), 5))
        .sort_by_attrib(project::Stars)
        .sample(Top(50))
        .map_into_attrib(Select!(project::Id, project::URL))
        .into_csv(OUTPUT_PATH).unwrap();
}
