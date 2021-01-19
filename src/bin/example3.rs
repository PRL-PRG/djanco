use structopt::StructOpt;

use parasite::DatastoreView;

use djanco::*;
use djanco::objects::*;
use djanco::csv::*;
use djanco::commandline::*;

// `cargo run --bin example3 --release -- -o ~/output -d /mnt/data/dataset -c /mnt/data/cache --data-dump=~/output/dump`
fn main() {
    let config = Configuration::from_args();

    let database =
        Djanco::from_spec(config.dataset_path(), config.cache_path(), timestamp!(December 2020), vec![]).unwrap();

    with_elapsed_secs!("executing query", {
        database.projects()
            .filter_by(Equal(project::Language, Language::Python))
            .filter_by(AtLeast(Count(FromEachIf(project::Commits, Contains(commit::Message, "performance"))), 1))
            .sort_by(project::Stars)
            .map_into(Select!(project::Itself, FromEachIf(project::Commits, Contains(commit::Message, "performance"))))

            // no hack!

            .into_csv(config.output_csv_path("project_commits_with_the_word_performance_C")).unwrap();
    });
}
