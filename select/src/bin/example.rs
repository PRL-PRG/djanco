use select::{Djanco, project, sample, require};
use select::objects::*;
use select::csv::*;
//use select::dump::*;
use select::prototype::api::*;

// TODO
// * CommitsWhere, PathsWhere, UsersWhere, etc.
// * snapshots
// * keep and produce receipt snippets
// * fix load filters, maybe base on git commit hash of query
// * CSV output if not squashed
// * logging everywhere

fn main() {
    let database = Djanco::from("/dejavuii/dejacode/dataset-tiny", 0, Month::August(2020))
        .with_cache("/dejavuii/dejacode/cache-tiny");
    //.with_filter(require::AtLeast(project::Commits, 10));

    database.projects()
        //.filter_by_attrib(require::AtLeast(project::Commits, 28))
        .group_by_attrib(project::Language)
        //.filter_by_attrib(require::AtLeast(project::Stars, 1))
        //.filter_by_attrib(require::AtLeast(project::Commits, 25))
        //.filter_by_attrib(require::AtLeast(project::Users, 2))
        //.filter_by_attrib(require::Same(project::Language, "Rust"))
        //.filter_by_attrib(require::Matches(project::URL, regex!("^https://github.com/PRL-PRG/.*$")))
        .sort_by_attrib(project::Age)
        .filter_by_attrib(require::Contains::Item(project::Users, User::with_name("Konrad Siek")))
        .sample(sample::Top(1))
        .squash()
        .flat_map_to_attrib(project::Commits)
        .to_csv("commits.csv").unwrap();
        //.dump_all_info_to("dump").unwrap();
}