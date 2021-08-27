use clap::Clap;

use djanco::*;
use djanco::objects::*;
use djanco::csv::*;
use djanco::log::*;

// `cargo run --bin dsl --release -- -o ~/output -d /mnt/data/dataset -c /mnt/data/cache --data-dump=~/output/dump`
fn main() {
    let config = Configuration::parse();
    let database =
        Djanco::from_config(&config, timestamp!(December 2020), store!(JavaScript, TypeScript, Python), Log::new(Verbosity::Log)).unwrap();

    // TODO a priori size estimate for logging
    database.projects().sort_by(project::Stars).into_csv_in_dir(&config.output_path,  "sort_by_stars").unwrap();
    database.projects().map_into(project::Stars).into_csv_in_dir(&config.output_path,  "select_by_stars").unwrap();
    database.projects().group_by(project::Stars).ungroup().into_csv_in_dir(&config.output_path,  "group_by_stars").unwrap();
    database.projects().filter_by(Equal(project::Language, objects::Language::C)).into_csv_in_dir(&config.output_path,  "filter_by_language_c").unwrap();
    database.projects().filter_by(Equal(project::Language, objects::Language::JavaScript)).into_csv_in_dir(&config.output_path,  "filter_by_language_js").unwrap();
    database.projects().filter_by(And(AtLeast(project::Stars, 20000), AtMost(project::Stars, 80000))).into_csv_in_dir(&config.output_path,  "filter_by_between_20K_and_80K_stars").unwrap();
    database.projects().filter_by(Exists(project::Stars)).into_csv_in_dir(&config.output_path,  "filter_by_has_stars").unwrap();
    database.projects().filter_by(Exists(project::Homepage)).into_csv_in_dir(&config.output_path,  "filter_by_has_homepage").unwrap(); // recheck
    database.projects().filter_by(Same(project::Homepage, "http://vuejs.org")).into_csv_in_dir(&config.output_path,  "filter_by_homepage_exact").unwrap(); // recheck
    database.projects().filter_by(Matches(project::Homepage, regex!("\\.org/?$"))).into_csv_in_dir(&config.output_path,  "filter_by_homepage_regex").unwrap();
    database.projects().filter_by(project::HasIssues).into_csv_in_dir(&config.output_path,  "filter_by_has_issues").unwrap();
    database.projects().sort_by(Count(project::Commits)).into_csv_in_dir(&config.output_path,  "sort_by_commit_count").unwrap();
    database.projects().map_into(Mean(FromEach(project::Commits, commit::MessageLength))).into_csv_in_dir(&config.output_path,  "select_mean_commit_messages_length").unwrap();
    database.projects().map_into(Median(FromEach(project::Commits, commit::MessageLength))).into_csv_in_dir(&config.output_path,  "select_median_commit_messages_length").unwrap();
    database.projects().map_into(Count(FromEachIf(project::Commits, Equal(commit::MessageLength, 0)))).into_csv_in_dir(&config.output_path,  "select_projects_with_empty_commits").unwrap();
    database.users().sort_by(user::Experience).sample(Top(100)).into_csv_in_dir(&config.output_path,  "sample_top_100_experienced_users").unwrap();
    database.paths().filter_by(Equal(path::Language, Language::Haskell)).into_csv_in_dir(&config.output_path,  "filter_haskell_paths").unwrap();
    database.paths().filter_by(Equal(path::Language, Language::JavaScript)).into_csv_in_dir(&config.output_path,  "filter_javascript_paths").unwrap(); // recheck
    database.commits().sample(Random(100, Seed(42))).into_csv_in_dir(&config.output_path,  "sample_100_commits").unwrap();
    database.projects().map_into(Ratio(project::Authors, project::Users)).into_csv_in_dir(&config.output_path,  "select_project_ratio_of_authors_to_users").unwrap();
    database.projects().map_into(Select!(project::Id, project::URL)).into_csv_in_dir(&config.output_path,  "select_project_ids_and_urls").unwrap();
    database.commits().map_into(commit::Author)/*TODO .unique().map_into_attrib(user::IdTODO Experience*/.into_csv_in_dir(&config.output_path,  "commit_author_experience").unwrap();
    database.commits().map_into(commit::Committer)/*TODO .unique().map_into_attrib(user::IdTODO Experience*/.into_csv_in_dir(&config.output_path,  "commit_committer_experience").unwrap();
    database.commits().map_into(commit::Parents).into_csv_in_dir(&config.output_path,  "commit_parents").unwrap();
    database.projects().map_into(FromEach(project::Commits, commit::MessageLength)).into_csv_in_dir(&config.output_path,  "project_commit_message_length").unwrap();
    database.users().sort_by(user::Experience).map_into(user::Experience).into_csv_in_dir(&config.output_path,  "user_experience").unwrap();
    database.projects().group_by(project::Language).map_into(FromEach(project::Commits, commit::MessageLength)).into_csv_in_dir(&config.output_path,  "language/project_commit_message_length").unwrap();
    database.projects().filter_by(Member(project::Homepage, vec!["http://manasource.org/"].iter().map(|e| e.to_string()).collect::<Vec<String>>()));
    database.projects().filter_by(AnyIn(FromEach(project::Commits, commit::Id), vec![objects::CommitId::from(42u64), objects::CommitId::from(666u64)]));
    database.projects().filter_by(AllIn(FromEach(project::Commits, commit::Id), vec![objects::CommitId::from(42u64), objects::CommitId::from(666u64)]));
    database.projects().filter_by(Within(FromEach(project::Commits, commit::Id), objects::CommitId::from(666u64)));
    database.snapshots_with_data().sample(Random(10, Seed(42))).into_files_in_dir(&config.output_path).unwrap();
    // database.projects().map_into_attrib(Bucket(Count(project::Commits), Interval(1000))).into_csv_in_dir(&config.output_path,  "bucket_1000").unwrap();
    database.commits().map_into(commit::Tree);//.into_csv_in_dir(&config.output_path, "commit_trees.csv").unwrap();
    database.projects().sample(Stratified(project::Size, Strata!("big" -> Random(5, Seed(42)), "small" -> Random(10, Seed(42))), Custom(|size: Option<&usize>| match size { None => "NA", Some(n) if *n >= 10000 => "big", Some(n) => "small" }))).into_csv_in_dir(&config.output_path,  "stratified_1").unwrap();
    database.projects().sample(Stratified(project::Size, Strata!("big" -> Random(5, Seed(42)), "small" -> Random(10, Seed(42))), Threshold::Inclusive(10000, "big", "small"))).into_csv_in_dir(&config.output_path,  "stratified_2").unwrap();
    database.projects().sample(Stratified(project::Size, Strata!("big" -> Random(5, Seed(42)), "medium" -> Random(10, Seed(42)), "small" -> Random(10, Seed(42))), Thresholds::Inclusive(Conditions!("big" -> 10000, "medium" -> 1000), "small"))).into_csv_in_dir(&config.output_path,  "stratified_3").unwrap();
        
    database.projects().sample(Distinct(Top(10), ByAttribute(project::License))).into_csv_in_dir(&config.output_path,  "distinct_licenses").unwrap();
    database.projects().sample(Distinct(Top(10), ByAttribute(project::Language))).into_csv_in_dir(&config.output_path,  "distinct_languages").unwrap();
    database.projects().sample(Distinct(Top(10), ByAttribute(project::Locs))).into_csv_in_dir(&config.output_path,  "distinct_locs").unwrap();

}