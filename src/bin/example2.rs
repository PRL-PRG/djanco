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
        Djanco::from_spec(config.dataset_path(), config.cache_path(), timestamp!(December 2020), vec![]);

    let bug_regex = regex!("(close|closes|closed|fix|fixes|fixed|resolve|resolves|resolved)\\s+#[0-9]+");

    with_elapsed_secs!("executing query", {
        database.projects()
            .filter_by(Equal(project::Language, Language::Python))
            .filter_by(AtLeast(Count(FromEachIf(project::Commits, Matches(commit::Message, bug_regex.clone()))), 1))
            .sort_by(project::Stars)
            .map_into(Select!(project::Itself, FromEachIf(project::Commits, Matches(commit::Message, bug_regex.clone()))))

            // no hack!

            .into_csv(config.output_csv_path("project_issue_closers")).unwrap();
    });
}
