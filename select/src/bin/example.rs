use select::Djanco;
use select::objects::Month;
use select::csv::CSV;

fn main() {
    let database = Djanco::from("/dejavuii/dejacode/dataset-tiny", 0, Month::August(2020))
        .with_cache("/dejavuii/dejacode/cache-tiny");
    //.with_filter(require::AtLeast(project::Commits, 10));

    database.projects()
        //.filter_by_attrib(require::AtLeast(project::Commits, 28))
        //.group_by_attrib(project::Stars)
        //.filter_by_attrib(require::AtLeast(project::Stars, 1))
        //.filter_by_attrib(require::AtLeast(project::Commits, 25))
        //.filter_by_attrib(require::AtLeast(project::Users, 2))
        //.filter_by_attrib(require::Same(project::Language, "Rust"))
        //.filter_by_attrib(require::Matches(project::URL, regex!("^https://github.com/PRL-PRG/.*$")))
        //.sort_by_attrib(project::Stars)
        //.sample(sample::Top(2))
        //.squash()
        //.select_attrib(project::Id)
        .to_csv("projects.csv").unwrap();
}