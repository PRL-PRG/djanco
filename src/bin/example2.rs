use clap::Clap;

use djanco::*;
use djanco::objects::*;
use djanco::csv::*;
use djanco::log::{Log, Verbosity};

// `cargo run --bin example3 --release -- -o ~/output -d /mnt/data/dataset -c /mnt/data/cache --data-dump=~/output/dump`
fn main() {
    let config = Configuration::parse();

    let database =
        Djanco::from_config(&config, timestamp!(December 2020), vec![], Log::new(Verbosity::Log))
            .expect("Error initializing datastore.");

    let bug_regex = regex!("(close|closes|closed|fix|fixes|fixed|resolve|resolves|resolved)\\s+#[0-9]+");

    with_elapsed_secs!("executing query", {
        database.projects()
            .filter_by(Equal(project::Language, Language::Python))
            .filter_by(AtLeast(Count(FromEachIf(project::Commits, Matches(commit::Message, bug_regex.clone()))), 1))
            .sort_by(project::Stars)
            .map_into(Select!(project::Itself, FromEachIf(project::Commits, Matches(commit::Message, bug_regex.clone()))))

            // no hack!

            .into_csv_in_dir(&config.output_path, "project_issue_closers").unwrap();
    });
}
