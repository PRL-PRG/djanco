use select::{Djanco};
use select::objects::*;
use select::csv::*;
use select::stats;
use select::project;
//use select::attrib;
use select::retrieve;
use select::user;
use select::commit;
use select::sample;
use select::require;
//use select::dump::*;
use select::prototype::api::*;
use select::time::{Month, Seconds};

// TODO
// * snapshots
// * keep and produce receipt snippets
// * fix load filters, maybe base on git commit hash of query
// * CSV output if not squashed
// * logging everywhere

fn _stars(path: &str) {
    Djanco::from(path, 0, Month::August(2020))
        .with_cache("/dejavuii/dejacode/examples/cache")
        .projects()
        .group_by_attrib(project::Language)
        .sort_by_attrib(project::Stars)
        .sample(sample::Top(50))
        .squash()
        .to_csv("/dejavuii/dejacode/examples/output/stars.csv").unwrap();
}

fn _touched_files(path: &str) { //. FIXME
    Djanco::from(path, 0, Month::August(2020))
        .with_cache("/dejavuii/dejacode/examples/cache")
        .projects()
        .group_by_attrib(project::Language)

        //.sort_by_attrib(stats::Median(project::CommitsWith(require::Exists(commit::Paths))))

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

        .sort_by_attrib(stats::Median(retrieve::From(project::Commits, commit::Message)))

        .sample(sample::Top(50))
        .squash()
        .to_csv("/dejavuii/dejacode/examples/output/message_size.csv").unwrap();
}

fn _number_of_commits(path: &str) {
    Djanco::from(path, 0, Month::August(2020))
        .with_cache("/dejavuii/dejacode/examples/cache")
        .projects()
        .group_by_attrib(project::Language)

        .sort_by_attrib(project::Commits)

        .sample(sample::Top(50))
        .squash()
        .to_csv("/dejavuii/dejacode/examples/output/number_of_commits.csv").unwrap();
}

fn _issues(path: &str) {
    Djanco::from(path, 0, Month::August(2020))
        .with_cache("/dejavuii/dejacode/examples/cache")
        .projects()
        .group_by_attrib(project::Language)

        .sort_by_attrib(project::AllIssues)

        .sample(sample::Top(50))
        .squash()
        .to_csv("/dejavuii/dejacode/examples/output/issues.csv").unwrap();
}

// works with downloader from commit  146e55e34ca1f4cc5b826e0c909deac96afafc17
fn main() {
    let database = Djanco::from("/dejavuii/dejacode/dataset-tiny", 0, Month::August(2020));

    //.with_filter(require::AtLeast(project::Commits, 10));

    database.projects()
        //.filter_by_attrib(require::AtLeast(project::Commits, 28))
        .group_by_attrib(project::Language)
        .filter_by_attrib(require::Exists(project::UsersWith(require::AtLeast(user::Experience, Seconds::from_years(1)))))
        //.filter_by_attrib(require::AtLeast(project::Commits, 25))
        //.filter_by_attrib(require::AtLeast(project::Users, 2))
        //.filter_by_attrib(require::Same(project::Language, "Rust"))
        //.filter_by_attrib(require::Matches(project::URL, regex!("^https://github.com/PRL-PRG/.*$")))
        //.sort_by_attrib(project::Age)
        .sort_by_attrib(stats::Median(retrieve::From(project::Commits, commit::Message)))
        .filter_by_attrib(require::Contains::Item(project::Users, User::with_name("Konrad Siek")))
        .sample(sample::Top(1))
        .squash()
        //.flat_map_to_attrib(project::Commits)
        //.to_csv("commits.csv").unwrap();
        .to_csv("projects.csv").unwrap();
        //.dump_all_info_to("dump").unwrap();
}