use structopt::StructOpt;
use itertools::Itertools;

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
use djanco::iterators::ItemWithData;

// `cargo run --bin example3 --release -- -o ~/output -d /mnt/data/dataset -c /mnt/data/cache --data-dump=~/output/dump`
fn main() {
    let now = time::now();
    let config = Configuration::from_args();
    let log = Log::new(Verbosity::Debug);

    let store = DatastoreView::new(config.dataset_path(), now);
    let database = Database::from_store(store, config.cache_path(), log);

    let bug_regex = regex!("(close|closes|closed|fix|fixes|fixed|resolve|resolves|resolved)\\s+#[0-9]+");

    with_elapsed_secs!("executing query", {
        database.projects()
            .filter_by_attrib(require::Equal(project::Language, Language::Python))
            .flat_map(|project| {
                let all_commits =
                    project.commits().unwrap_or(vec![]);

                let issues_closers = all_commits.into_iter()
                    .filter(|commit| {
                        commit.message(project.data)
                            .map_or(false, |message| bug_regex.is_match(&message))
                    });

                let project_commit_mapping = issues_closers
                    .map(|commit| (project.clone(), ItemWithData::new(&project.data, commit)))
                    .collect::<Vec<(ItemWithData<Project>, ItemWithData<Commit>)>>();

                project_commit_mapping
            })
            .sorted_by_key(|(project, commit)| {
                (project.star_count(), project.id(), commit.author_timestamp(), commit.id())
            })
            .into_iter().into_csv(config.output_csv_path("project_issue_closers")).unwrap();
    });
}
