use structopt::StructOpt;

use dcd::DatastoreView;

use djanco::*;
use djanco::data::*;
use djanco::time;
use djanco::log::*;
use djanco::commandline::*;
use djanco::csv::CSV;
use djanco::objects::Language;

// `cargo run --bin dsl --release -- -o ~/output -d /mnt/data/dataset -c /mnt/data/cache --data-dump=~/output/dump`
fn main() {
    let now = time::now();
    let config = Configuration::from_args();
    let log = Log::new(Verbosity::Debug);
    let store = DatastoreView::new(config.dataset_path(), now);
    let database =  Database::from_store(store, config.cache_path(), log);

    macro_rules! path { ($name:expr) => { config.output_csv_path($name) } }

    // TODO a priori size estimate for logging
    database.projects().sort_by_attrib(project::Stars).into_csv(path!("sort_by_stars")).unwrap();
    database.projects().map_into_attrib(project::Stars).into_csv(path!("select_by_stars")).unwrap();
    database.projects().group_by_attrib(project::Stars).ungroup().into_csv(path!("group_by_stars")).unwrap();
    database.projects().filter_by_attrib(Equal(project::Language, objects::Language::C)).into_csv(path!("filter_by_language_c")).unwrap();
    database.projects().filter_by_attrib(And(AtLeast(project::Stars, 1), AtMost(project::Stars, 10))).into_csv(path!("filter_by_between_1_and_10_stars")).unwrap();
    database.projects().filter_by_attrib(Exists(project::Stars)).into_csv(path!("filter_by_has_stars")).unwrap();
    database.projects().filter_by_attrib(Same(project::Homepage, "http://manasource.org/")).into_csv(path!("filter_by_homepage_exact")).unwrap();
    database.projects().filter_by_attrib(Matches(project::Homepage, regex!("\\.org/?$"))).into_csv(path!("filter_by_homepage_regex")).unwrap();
    database.projects().filter_by_attrib(project::HasIssues).into_csv(path!("filter_by_has_issues")).unwrap();
    database.projects().sort_by_attrib(Count(project::Commits)).into_csv(path!("sort_by_commit_count")).unwrap();
    database.projects().map_into_attrib(Mean(FromEach(project::Commits, commit::MessageLength))).into_csv(path!("select_mean_commit_messages_length")).unwrap();
    database.projects().map_into_attrib(Median(FromEach(project::Commits, commit::MessageLength))).into_csv(path!("select_median_commit_messages_length")).unwrap();
    database.projects().map_into_attrib(Count(FromEachIf(project::Commits, Equal(commit::MessageLength, 0)))).into_csv(path!("select_projects_with_empty_commits")).unwrap();
    database.users().sort_by_attrib(user::Experience).sample(Top(100)).into_csv(path!("sample_top_100_experienced_users")).unwrap();
    database.paths().filter_by_attrib(Equal(path::Language, Language::Haskell)).into_csv(path!("filter_haskell_paths")).unwrap();
    database.commits().sample(Random(100, Seed(42))).into_csv(path!("sample_100_commits")).unwrap();
    database.projects().map_into_attrib(Ratio(project::Authors, project::Users)).into_csv(path!("select_project_ratio_of_authors_to_users")).unwrap();
    database.projects().map_into_attrib(Select!(project::Id, project::URL)).into_csv(path!("select_project_ids_and_urls")).unwrap();
    database.commits().map_into_attrib(commit::Author)/*TODO .unique().map_into_attrib(user::IdTODO Experience*/.into_csv(path!("commit_author_experience")).unwrap();
    database.commits().map_into_attrib(commit::Committer)/*TODO .unique().map_into_attrib(user::IdTODO Experience*/.into_csv(path!("commit_committer_experience")).unwrap();
    database.commits().map_into_attrib(commit::Parents).into_csv(path!("commit_parents")).unwrap();
    database.projects().map_into_attrib(FromEach(project::Commits, commit::MessageLength)).into_csv(path!("project_commit_message_length")).unwrap();
    database.users().sort_by_attrib(user::Experience).map_into_attrib(user::Experience).into_csv(path!("user_experience")).unwrap();
    database.projects().group_by_attrib(project::Language).map_into_attrib(FromEach(project::Commits, commit::MessageLength)).into_csv(path!("language/project_commit_message_length")).unwrap();
    database.projects().filter_by_attrib(Member(project::Homepage, vec!["http://manasource.org/"].iter().map(|e| e.to_string()).collect::<Vec<String>>()));
    database.projects().filter_by_attrib(AnyIn(FromEach(project::Commits, commit::Id), vec![objects::CommitId::from(42u64), objects::CommitId::from(666u64)]));
    database.projects().filter_by_attrib(AllIn(FromEach(project::Commits, commit::Id), vec![objects::CommitId::from(42u64), objects::CommitId::from(666u64)]));
}

// TODO features
// CSV export
// dump
// receipts
// Git commit as version
// commit frequency
// fill in CSV-capable objects
// maybe length for all strings
// maybe non-empty precicate for vectors
// buckets
// Fraction vs f64
// unit tests
// print out fractions as decimals
// flat_map select
// explore parallelism
// prefiltering