use djanco::*;
use dcd::*;
use djanco::csv::*;
use djanco::time::Duration;

const DATASET_PATH: &'static str = "/dejacode/tiny-mk2/";
const OUTPUT_PATH: &'static str = "/dejacode/output-tiny-mk2/";

fn main() {
    Djanco::from(DATASET_PATH)
        //.with_cache(CACHE_PATH)
        .projects()
        .group_by(project::Language)
        .filter_by(AtLeast(Count(project::Users), 5))
        .sort_by(project::Stars)
        .sample(Top(50))
        .map_into(Select!(project::Id, project::URL))
        .into_csv(OUTPUT_PATH).unwrap();

    Djanco::from(DATASET_PATH)
        //.with_cache(CACHE_PATH)
        .projects()
        .filter_by(AtLeast(Count(FromEachIf(project::Users, AtLeast(user::Experience, Duration::from_years(5)))), 2))
        .sample(Distinct(Random(50, Seed(42)), MinRatio(project::Commits, 0.95)))
        .into_csv(OUTPUT_PATH).unwrap();
}
