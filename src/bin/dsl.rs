use structopt::StructOpt;

use djanco::*;
use djanco::commandline::*;
use djanco::objects::*;
use djanco::csv::*;
use djanco::log::{Log, Verbosity};

// `cargo run --bin dsl --release -- -o ~/output -d /mnt/data/dataset -c /mnt/data/cache --data-dump=~/output/dump`
fn main() {
    let config = Configuration::from_args();
    let database =
        Djanco::from_spec(config.dataset_path(), config.cache_path(), timestamp!(December 2020), vec![], Log::new(Verbosity::Log)).unwrap();

    macro_rules! path { ($name:expr) => { config.output_csv_path($name) } }

    // TODO a priori size estimate for logging
    database.projects().sort_by(project::Stars).into_csv(path!("sort_by_stars")).unwrap();
    database.projects().map_into(project::Stars).into_csv(path!("select_by_stars")).unwrap();
    database.projects().group_by(project::Stars).ungroup().into_csv(path!("group_by_stars")).unwrap();
    database.projects().filter_by(Equal(project::Language, objects::Language::C)).into_csv(path!("filter_by_language_c")).unwrap();
    database.projects().filter_by(And(AtLeast(project::Stars, 1), AtMost(project::Stars, 10))).into_csv(path!("filter_by_between_1_and_10_stars")).unwrap();
    database.projects().filter_by(Exists(project::Stars)).into_csv(path!("filter_by_has_stars")).unwrap();
    database.projects().filter_by(Same(project::Homepage, "http://manasource.org/")).into_csv(path!("filter_by_homepage_exact")).unwrap();
    database.projects().filter_by(Matches(project::Homepage, regex!("\\.org/?$"))).into_csv(path!("filter_by_homepage_regex")).unwrap();
    database.projects().filter_by(project::HasIssues).into_csv(path!("filter_by_has_issues")).unwrap();
    database.projects().sort_by(Count(project::Commits)).into_csv(path!("sort_by_commit_count")).unwrap();
    database.projects().map_into(Mean(FromEach(project::Commits, commit::MessageLength))).into_csv(path!("select_mean_commit_messages_length")).unwrap();
    database.projects().map_into(Median(FromEach(project::Commits, commit::MessageLength))).into_csv(path!("select_median_commit_messages_length")).unwrap();
    database.projects().map_into(Count(FromEachIf(project::Commits, Equal(commit::MessageLength, 0)))).into_csv(path!("select_projects_with_empty_commits")).unwrap();
    database.users().sort_by(user::Experience).sample(Top(100)).into_csv(path!("sample_top_100_experienced_users")).unwrap();
    database.paths().filter_by(Equal(path::Language, Language::Haskell)).into_csv(path!("filter_haskell_paths")).unwrap();
    database.commits().sample(Random(100, Seed(42))).into_csv(path!("sample_100_commits")).unwrap();
    database.projects().map_into(Ratio(project::Authors, project::Users)).into_csv(path!("select_project_ratio_of_authors_to_users")).unwrap();
    database.projects().map_into(Select!(project::Id, project::URL)).into_csv(path!("select_project_ids_and_urls")).unwrap();
    database.commits().map_into(commit::Author)/*TODO .unique().map_into_attrib(user::IdTODO Experience*/.into_csv(path!("commit_author_experience")).unwrap();
    database.commits().map_into(commit::Committer)/*TODO .unique().map_into_attrib(user::IdTODO Experience*/.into_csv(path!("commit_committer_experience")).unwrap();
    database.commits().map_into(commit::Parents).into_csv(path!("commit_parents")).unwrap();
    database.projects().map_into(FromEach(project::Commits, commit::MessageLength)).into_csv(path!("project_commit_message_length")).unwrap();
    database.users().sort_by(user::Experience).map_into(user::Experience).into_csv(path!("user_experience")).unwrap();
    database.projects().group_by(project::Language).map_into(FromEach(project::Commits, commit::MessageLength)).into_csv(path!("language/project_commit_message_length")).unwrap();
    database.projects().filter_by(Member(project::Homepage, vec!["http://manasource.org/"].iter().map(|e| e.to_string()).collect::<Vec<String>>()));
    database.projects().filter_by(AnyIn(FromEach(project::Commits, commit::Id), vec![objects::CommitId::from(42u64), objects::CommitId::from(666u64)]));
    database.projects().filter_by(AllIn(FromEach(project::Commits, commit::Id), vec![objects::CommitId::from(42u64), objects::CommitId::from(666u64)]));
    // database.projects().map_into_attrib(Bucket(Count(project::Commits), Interval(1000))).into_csv(path!("bucket_1000")).unwrap();
}