use structopt::StructOpt;

use dcd::DatastoreView;

use djanco::*;
use djanco::data::*;
use djanco::objects::*;
use djanco::csv::*;
use djanco::log::*;
use djanco::commandline::*;

// `cargo run --bin example3 --release -- -o ~/output -d /mnt/data/dataset -c /mnt/data/cache --data-dump=~/output/dump`
fn main() {
    let now = timestamp!(December 2020);
    let config = Configuration::from_args();

    let store = DatastoreView::new(config.dataset_path(), now);
    let database = Database::from_store(store, config.cache_path());

    let bug_regex = regex!("(close|closes|closed|fix|fixes|fixed|resolve|resolves|resolved)\\s+#[0-9]+");

    with_elapsed_secs!("executing query", {
        database.projects()
            .filter_by_attrib(Equal(project::Language, Language::Python))
            .filter_by_attrib(AtLeast(Count(FromEachIf(project::Commits, Matches(commit::Message, bug_regex.clone()))), 1))
            .sort_by_attrib(project::Stars)
            .map_into_attrib(Select!(project::Itself, FromEachIf(project::Commits, Matches(commit::Message, bug_regex.clone()))))

            // no hack!

            .into_csv(config.output_csv_path("project_issue_closers")).unwrap();
    });
}
