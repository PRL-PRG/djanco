use structopt::StructOpt;

use dcd::DatastoreView;

use djanco::*;
use djanco::data::*;
use djanco::time;
use djanco::objects::*;
use djanco::csv::*;
use djanco::log::*;
use djanco::commandline::*;
use djanco::attrib::*;
use djanco::query::*;

// `cargo run --bin example3 --release -- -o ~/output -d /mnt/data/dataset -c /mnt/data/cache --data-dump=~/output/dump`
fn main() {
    let now = time::now();
    let config = Configuration::from_args();
    let log = Log::new(Verbosity::Debug);

    let store = DatastoreView::new(config.dataset_path(), now);
    let database = Database::from_store(store, config.cache_path(), log);

    with_elapsed_secs!("executing query", {
        database.projects()
            .filter_by_attrib(Equal(project::Language, Language::Python))
            .filter_by_attrib(AtLeast(Count(FromEachIf(project::Commits, Contains(commit::Message, "performance"))), 1))
            .sort_by_attrib(project::Stars)
            .map_into_attrib(Select!(project::Itself, FromEachIf(project::Commits, Contains(commit::Message, "performance"))))

            // no hack!

            .into_csv(config.output_csv_path("project_commits_with_the_word_performance_C")).unwrap();
    });
}
