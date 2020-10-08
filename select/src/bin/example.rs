use select::{Djanco};
//use select::objects::*;
use select::csv::*;
use select::stats;
use select::project;
use select::retrieve;
use select::user;
use select::commit;
use select::sample;
use select::require;
use select::message;
//use select::dump::*;
//use select::prototype::api::*;
use select::attrib::sort::Direction::*;
use select::time::{Month, Seconds};
use select::dump::Dump;

// TODO
// * snapshots aka file contents
// * keep and produce receipt snippets
// * fix load filters, maybe base on git commit hash of query
// * CSV output if not squashed
// * logging everywhere
// * look into replacing Vecs in Iterators with VecDeques to preserve order

fn _stars(path: &str) {
    Djanco::from(path, 0, Month::August(2020))
        .with_cache("/dejavuii/dejacode/examples/cache")
        .projects()
        .group_by_attrib(project::Language)

        .sort_by_attrib(Descending, project::Stars)

        .sample(sample::Top(50))
        .squash()
        .to_csv("/dejavuii/dejacode/examples/output/stars.csv").unwrap();
}

fn _touched_files(path: &str) { //. FIXME
    Djanco::from(path, 0, Month::August(2020))
        .with_cache("/dejavuii/dejacode/examples/cache")
        .projects()
        .group_by_attrib(project::Language)

        .sort_by_attrib(Descending, stats::Median(retrieve::From(project::Commits, commit::Paths)))

        .sample(sample::Top(50))
        .squash()
        .to_csv("/dejavuii/dejacode/examples/output/touched_files.csv").unwrap();
}

fn _experienced_author(path: &str) {
    Djanco::from(path, 0, Month::August(2020))
        .with_cache("/dejavuii/dejacode/examples/cache")
        .projects()
        .group_by_attrib(project::Language)

        .filter_by_attrib(require::Exists(project::UsersWith(require::AtLeast(user::Experience, Seconds::from_years(2)))))

        .sample(sample::Random(50))
        .squash()
        .to_csv("/dejavuii/dejacode/examples/output/experienced_author.csv").unwrap();
}

fn _fifty_percent_experienced(path: &str) {
    Djanco::from(path, 0, Month::August(2020))
        .with_cache("/dejavuii/dejacode/examples/cache")
        .projects()
        .group_by_attrib(project::Language)

        .filter_by_attrib(require::AtLeast(stats::Count(project::Users), 2))
        .filter_by_attrib(require::AtLeast(stats::Ratio(project::UsersWith(require::AtLeast(user::Experience, Seconds::from_years(2)))), 0.5))

        .sample(sample::Random(50))
        .squash()
        .to_csv("/dejavuii/dejacode/examples/output/50%_experienced.csv").unwrap();
}

fn _message_size(path: &str) {
    Djanco::from(path, 0, Month::August(2020))
        .with_cache("/dejavuii/dejacode/examples/cache")
        .projects()
        .group_by_attrib(project::Language)

        .sort_by_attrib(Descending, stats::Median(retrieve::From(retrieve::From(project::Commits, commit::Message), message::Length)))

        .sample(sample::Top(50))
        .squash()
        .to_csv("/dejavuii/dejacode/examples/output/message_size.csv").unwrap();
}

fn _number_of_commits(path: &str) {
    Djanco::from(path, 0, Month::August(2020))
        .with_cache("/dejavuii/dejacode/examples/cache")
        .projects()
        .group_by_attrib(project::Language)

        .sort_by_attrib(Descending, project::Commits)

        .sample(sample::Top(50))
        .squash()
        .to_csv("/dejavuii/dejacode/examples/output/number_of_commits.csv").unwrap();
}

fn _issues(path: &str) {
    Djanco::from(path, 0, Month::August(2020))
        .with_cache("/dejavuii/dejacode/examples/cache")
        .projects()
        .group_by_attrib(project::Language)

        .sort_by_attrib(Descending, project::AllIssues)

        .sample(sample::Top(50))
        .squash()
        .to_csv("/dejavuii/dejacode/examples/output/issues.csv").unwrap();
}

fn _dump_all(path: &str) {
    Djanco::from(path, 0, Month::August(2020))
        .with_cache("/dejavuii/dejacode/examples/cache")
        .projects()
        .dump_all_info_to("/dejavuii/dejacode/examples/output/data").unwrap();
}

// works with downloader from commit  146e55e34ca1f4cc5b826e0c909deac96afafc17
fn main() {
    let database = Djanco::from("/dejavuii/dejacode/dataset-tiny", 0, Month::August(2020));

    //.with_filter(require::AtLeast(project::Commits, 10));

    //database.clone().projects().for_each(|e| println!("{}", e.id));
    //database.clone().projects().to_csv("test0.csv").unwrap();
    //database.clone().projects().sort_by_attrib(Descending, project::Stars).to_csv("test1.csv").unwrap();
    database.projects().sort_by_attrib(Descending, project::Stars).sample(sample::Top(10)).to_csv("test2.csv").unwrap();

    // database.projects()
    //     //.filter_by_attrib(require::AtLeast(project::Commits, 28))
    //     .group_by_attrib(project::Language)
    //     .filter_by_attrib(require::Exists(project::UsersWith(require::AtLeast(user::Experience, Seconds::from_years(1)))))
    //     //.filter_by_attrib(require::AtLeast(project::Commits, 25))
    //     //.filter_by_attrib(require::AtLeast(project::Users, 2))
    //     //.filter_by_attrib(require::Same(project::Language, "Rust"))
    //     //.filter_by_attrib(require::Matches(project::URL, regex!("^https://github.com/PRL-PRG/.*$")))
    //     //.sort_by_attrib(project::Age)
    //     //.sort_by_attrib(stats::Median(retrieve::From(project::Commits, commit::Message)))
    //     .filter_by_attrib(require::Contains::Item(project::Users, User::with_name("Konrad Siek")))
    //     .sample(sample::Top(1))
    //     .squash()
    //     //.flat_map_to_attrib(project::Commits)
    //     //.to_csv("commits.csv").unwrap();
    //     .to_csv("projects.csv").unwrap();
    //     //.dump_all_info_to("dump").unwrap();

    // _stars("/dejavuii/dejacode/dataset-tiny");
    // _touched_files("/dejavuii/dejacode/dataset-tiny");
    // _experienced_author("/dejavuii/dejacode/dataset-tiny");
    // _fifty_percent_experienced("/dejavuii/dejacode/dataset-tiny");
    // _message_size("/dejavuii/dejacode/dataset-tiny");
    // _number_of_commits("/dejavuii/dejacode/dataset-tiny");
    // _issues("/dejavuii/dejacode/dataset-tiny");
    // _dump_all("/dejavuii/dejacode/dataset-tiny")
}