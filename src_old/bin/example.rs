use structopt::StructOpt;
use std::path::PathBuf;

use select::Djanco;
use select::djanco;
use select::attrib;
use select::csv::*;
use select::stats;
use select::project;
use select::retrieve;
use select::user;
use select::commit;
use select::sample;
use select::require;
use select::message;
use select::objects;
use select::attrib::sort::Direction::*;
use select::time::{Month, Seconds};
use select::dump::Dump;

// TODO
// * snapshots aka file contents
// * keep and produce receipt snippets
// * fix load filters, maybe base on git commit hash of query
// * logging everywhere

#[derive(StructOpt,Debug)]
pub struct Configuration {
    #[structopt(parse(from_os_str), short = "o", long = "output", name = "OUTPUT_PATH")]
    pub output_path: PathBuf,

    #[structopt(parse(from_os_str), short = "d", long = "dataset", name = "DATASET_PATH")]
    pub dataset_path: PathBuf,

    // #[structopt(parse(from_os_str), short = "l", long = "timing-log", name = "TIMING_LOG_PATH", default_value = "timing.log")]
    // pub timing_log: PathBuf,

    // #[structopt(long = "experiment-group", short = "g", name = "EXPERIMENT_NAME", default_value = "")]
    // pub group: String,

    #[structopt(parse(from_os_str), short = "c", long = "cache", name = "PERSISTENT_CACHE_PATH")]
    pub cache_path: Option<PathBuf>,

    #[structopt(parse(from_os_str), long = "data-dump", name = "DATA_DUMP_PATH")]
    pub dump_path: Option<PathBuf>
}

macro_rules! with_elapsed_secs {
    ($name:expr,$thing:expr) => {{
        eprintln!("Starting task {}...", $name);
        let start = std::time::Instant::now();
        let result = { $thing };
        let secs = start.elapsed().as_secs();
        eprintln!("Finished task {} in {}s.", $name, secs);
        (result, secs)
    }}
}

macro_rules! elapsed_secs {
    ($name:expr,$thing:expr) => {{
        eprintln!("Starting task {}...", $name);
        let start = std::time::Instant::now();
        { $thing };
        let secs = start.elapsed().as_secs();
        eprintln!("Finished task {} in {}s.", $name, secs);
        secs
    }}
}

type Projects = djanco::QuincunxIter<objects::Project>;
fn load_projects(config: &Configuration, seed: u128, timestamp: Month) -> Projects {
    let mut db = Djanco::from(config.dataset_path.to_str().unwrap(), seed, timestamp);
    if let Some(path) = &config.cache_path { db = db.with_cache(path.to_str().unwrap()) }
    db.projects()//.filter_by_attrib(require::AtLeast(project::Commits, 28))
}

type Groups = djanco::GroupIter<attrib::AttributeValue<project::Language, String>, objects::Project>;
fn group_projects_by_languages(projects: Projects) -> Groups {
    projects.group_by_attrib(project::Language)
}

fn stars(config: &Configuration, groups: Groups) {  // This is "stars" in the paper
    groups
        .sort_by_attrib(Descending, project::Stars)
        .sample(sample::Distinct(sample::Top(50), sample::Ratio(project::Commits, 0.9)))
        .squash()
        .to_id_list(format!("{}/stars.csv", config.output_path.to_str().unwrap())).unwrap();
}

fn mean_changes_in_commits(config: &Configuration, groups: Groups) {
    groups
        .sort_by_attrib(Descending, stats::Mean(retrieve::From(project::Commits, commit::Paths)))
        .sample(sample::Distinct(sample::Top(50), sample::Ratio(project::Commits, 0.9)))
        .squash()
        .to_id_list(format!("{}/mean_changes_in_commits.csv", config.output_path.to_str().unwrap())).unwrap();
}

fn median_changes_in_commits(config: &Configuration, groups: Groups) { // This is "touched files" in the paper
    groups
        .sort_by_attrib(Descending, stats::Median(retrieve::From(project::Commits, commit::Paths)))
        .sample(sample::Distinct(sample::Top(50), sample::Ratio(project::Commits, 0.9)))
        .squash()
        .to_id_list(format!("{}/median_changes_in_commits.csv", config.output_path.to_str().unwrap())).unwrap();
}

#[allow(dead_code)]
fn experienced_authors_random(config: &Configuration, groups: Groups) { // This is "experienced author" in the paper
    groups
        .filter_by_attrib(require::Exists(project::UsersWith(require::MoreThan(user::Experience, Seconds::from_years(2)))))
        .sample(sample::Distinct(sample::Random(50), sample::Ratio(project::Commits, 0.9)))
        .squash()
        .to_id_list(format!("{}/experienced_authors_random.csv", config.output_path.to_str().unwrap())).unwrap();
}

#[allow(dead_code)]
fn experienced_authors_sorted(config: &Configuration, groups: Groups) { // This is "experienced author" in the paper
    groups
        .filter_by_attrib(require::Exists(project::UsersWith(require::MoreThan(user::Experience, Seconds::from_years(2)))))
        .sort_by_attrib(Descending, project::Commits)
        .sample(sample::Distinct(sample::Top(50), sample::Ratio(project::Commits, 0.9)))
        .squash()
        .to_id_list(format!("{}/experienced_authors_sorted.csv", config.output_path.to_str().unwrap())).unwrap();
}

fn experienced_authors_ratio_random(config: &Configuration, groups: Groups) { // This is "50% experienced" in the paper
    groups
        .filter_by_attrib(require::AtLeast(stats::Count(project::Users), 2))
        .filter_by_attrib(require::AtLeast(stats::Ratio(project::UsersWith(require::MoreThan(user::Experience, Seconds::from_years(2)))), 0.5))
        .sample(sample::Distinct(sample::Random(50), sample::Ratio(project::Commits, 0.9)))
        .squash()
        .to_id_list(format!("{}/experienced_authors_ratio_random.csv", config.output_path.to_str().unwrap())).unwrap();
}

#[allow(dead_code)]
fn experienced_authors_ratio_sorted(config: &Configuration, groups: Groups) { // This is "50% experienced" in the paper
    groups
        .filter_by_attrib(require::AtLeast(stats::Count(project::Users), 2))
        .sort_by_attrib(Descending, stats::Ratio(project::UsersWith(require::MoreThan(user::Experience, Seconds::from_years(2)))))
        .sample(sample::Distinct(sample::Top(50), sample::Ratio(project::Commits, 0.9)))
        .squash()
        .to_id_list(format!("{}/experienced_authors_ratio_sorted.csv", config.output_path.to_str().unwrap())).unwrap();
}

#[allow(dead_code)]
fn experienced_authors_ratio_sorted2(config: &Configuration, groups: Groups) { // This is "50% experienced" in the paper
    groups
        //.filter_by_attrib(require::AtLeast(stats::Count(project::Users), 2))
        .filter_by_attrib(require::AtLeast(stats::Ratio(project::UsersWith(require::MoreThan(user::Experience, Seconds::from_years(2)))), 0.5))
        .sort_by_attrib(Descending, project::Commits)
        .sample(sample::Distinct(sample::Top(50), sample::Ratio(project::Commits, 0.9)))
        .squash()
        .to_id_list(format!("{}/experienced_authors_ratio_sorted2.csv", config.output_path.to_str().unwrap())).unwrap();
}

fn mean_commit_message_sizes(config: &Configuration, groups: Groups) {
    groups
        .sort_by_attrib(Descending, stats::Mean(retrieve::From(retrieve::From(project::Commits, commit::Message), message::Length)))
        .sample(sample::Distinct(sample::Top(50), sample::Ratio(project::Commits, 0.9)))
        .squash()
        .to_id_list(format!("{}/mean_commit_message_sizes.csv", config.output_path.to_str().unwrap())).unwrap();
}

fn median_commit_message_sizes(config: &Configuration, groups: Groups) { // This is "message size" in the paper
    groups
        .sort_by_attrib(Descending, stats::Median(retrieve::From(retrieve::From(project::Commits, commit::Message), message::Length)))
        .sample(sample::Distinct(sample::Top(50), sample::Ratio(project::Commits, 0.9)))
        .squash()
        .to_id_list(format!("{}/median_commit_message_sizes.csv", config.output_path.to_str().unwrap())).unwrap();
}

fn commits(config: &Configuration, groups: Groups) { // This is "number of commits" in the paper
    groups
        .sort_by_attrib(Descending, project::Commits)
        .sample(sample::Distinct(sample::Top(50), sample::Ratio(project::Commits, 0.9)))
        .squash()
        .to_id_list(format!("{}/commits.csv", config.output_path.to_str().unwrap())).unwrap();
}

fn all_issues(config: &Configuration, groups: Groups) { // This is "issues" in the paper
    groups
        .sort_by_attrib(Descending, project::AllIssues)
        .sample(sample::Distinct(sample::Top(50), sample::Ratio(project::Commits, 0.9)))
        .squash()
        .to_id_list(format!("{}/all_issues.csv", config.output_path.to_str().unwrap())).unwrap();
}

fn buggy_issues(config: &Configuration, groups: Groups) {
    groups
        .sort_by_attrib(Descending, project::BuggyIssues)
        //.sample(sample::Top(50))
        .sample(sample::Distinct(sample::Top(50), sample::Ratio(project::Commits, 0.9)))
        .squash()
        .to_id_list(format!("{}/buggy_issues.csv", config.output_path.to_str().unwrap())).unwrap();
}

fn issues(config: &Configuration, groups: Groups) {
    groups
        .sort_by_attrib(Descending, project::Issues)
        //.sample(sample::Top(50))
        .sample(sample::Distinct(sample::Top(50), sample::Ratio(project::Commits, 0.9)))
        .squash()
        .to_id_list(format!("{}/issues.csv", config.output_path.to_str().unwrap())).unwrap();
}

fn debug_dump(config: &Configuration, projects: &Projects) {
    projects.clone()
        .map_to_attrib(attrib::ID::with(project::Language))
        .to_csv(format!("{}/languages.debug.csv", config.output_path.to_str().unwrap())).unwrap();

    projects.clone()
        .map_to_attrib(attrib::ID::with(stats::Mean(retrieve::From(project::Commits, commit::Paths))))
        .to_csv(format!("{}/mean_changes_in_commits.debug.csv", config.output_path.to_str().unwrap())).unwrap();

    projects.clone()
        .map_to_attrib(attrib::ID::with(stats::Median(retrieve::From(project::Commits, commit::Paths))))
        .to_csv(format!("{}/median_changes_in_commits.debug.csv", config.output_path.to_str().unwrap())).unwrap();

    projects.clone()
        .map_to_attrib(attrib::ID::with(stats::Mean(retrieve::From(retrieve::From(project::Commits, commit::Message), message::Length))))
        .to_csv(format!("{}/mean_commit_message_sizes.debug.csv", config.output_path.to_str().unwrap())).unwrap();

    projects.clone()
        .map_to_attrib(attrib::ID::with(stats::Median(retrieve::From(retrieve::From(project::Commits, commit::Message), message::Length))))
        .to_csv(format!("{}/median_commit_message_sizes.debug.csv", config.output_path.to_str().unwrap())).unwrap();

    projects.clone()
        .map_to_attrib(attrib::ID::with(stats::Count(project::UsersWith(require::AtLeast(user::Experience, Seconds::from_years(2))))))
        .to_csv(format!("{}/experienced_authors.debug.csv", config.output_path.to_str().unwrap())).unwrap();

    projects.clone()
        .map_to_attrib(attrib::ID::with(stats::Count(project::UsersWith(require::AtLeast(user::Experience, Seconds::from_years(2))))))
        .to_csv(format!("{}/experienced_authors_sum.debug.csv", config.output_path.to_str().unwrap())).unwrap();

    projects.clone()
        .map_to_attrib(attrib::ID::with(stats::Ratio(project::UsersWith(require::AtLeast(user::Experience, Seconds::from_years(2))))))
        .to_csv(format!("{}/experienced_authors_ratio.debug.csv", config.output_path.to_str().unwrap())).unwrap();
}

#[allow(dead_code)]
fn dump_all(config: &Configuration, projects: Projects) {
    match &config.dump_path {
        Some(path) => projects.dump_all_info_to(path.to_str().unwrap()).unwrap(),
        None => ()
    }
}

// works with downloader from commit  146e55e34ca1f4cc5b826e0c909deac96afafc17
// cargo run --bin example --release -- -o /dejacode/query_results_old/artifact_testing/output -d /dejacode/dataset -c /dejacode/query_results_old/artifact_testing/cache --data-dump=/dejacode/query_results_old/artifact_testing/dump
fn main() {
    // let dataset_path = "/dejacode/dataset";
    // let cache_path = "examples/cache";
    // let output_path = "examples/output";
    // let _dump_path = "examples/dump";
    let config = Configuration::from_args();

    let (projects, load_projects) = with_elapsed_secs!("load_projects",
        load_projects(&config, 42, Month::August(2020))
    );
    let (groups, group_projects_by_languages) = with_elapsed_secs!("group_projects_by_languages",
        group_projects_by_languages(projects.clone())
    );

    let stars                       = elapsed_secs!("stars",                       stars                      (&config, groups.clone()));
    let mean_changes_in_commits     = elapsed_secs!("mean_changes_in_commits",     mean_changes_in_commits    (&config, groups.clone()));
    let median_changes_in_commits   = elapsed_secs!("median_changes_in_commits",   median_changes_in_commits  (&config, groups.clone()));
    let experienced_authors         = elapsed_secs!("experienced_authors_random",         experienced_authors_random (&config, groups.clone()));
    //let experienced_authors_sorted         = elapsed_secs!("experienced_authors_sorted",         experienced_authors_sorted (&config, groups.clone()));
    let experienced_authors_ratio   = elapsed_secs!("experienced_authors_ratio_random",   experienced_authors_ratio_random (&config, groups.clone()));
    //let experienced_authors_ratio_sorted   = elapsed_secs!("experienced_authors_ratio_sorted",   experienced_authors_ratio_sorted (&config, groups.clone()));
    let mean_commit_message_sizes   = elapsed_secs!("mean_commit_message_sizes",   mean_commit_message_sizes  (&config, groups.clone()));
    let median_commit_message_sizes = elapsed_secs!("median_commit_message_sizes", median_commit_message_sizes(&config, groups.clone()));
    let commits                     = elapsed_secs!("commits",                     commits                    (&config, groups.clone()));
    let all_issues                  = elapsed_secs!("all_issues",                  all_issues                 (&config, groups.clone()));
    let issues                      = elapsed_secs!("issues",                      issues                     (&config, groups.clone()));
    let buggy_issues                = elapsed_secs!("buggy_issues",                buggy_issues               (&config, groups.clone()));
    let debug_dump                  = elapsed_secs!("debug_dump",                  debug_dump                 (&config, &projects));
    let dump                        = elapsed_secs!("dump_all",                    dump_all                   (&config, projects));

    eprintln!("Summary:");
    eprintln!("  dataset: `{}`", config.dataset_path.to_str().unwrap());
    eprintln!("   output: `{}`", config.output_path.to_str().unwrap());
    eprintln!("    cache: `{}`", config.cache_path.unwrap_or(PathBuf::new()).to_str().unwrap());
    eprintln!("     dump: `{}`", config.dump_path.unwrap_or(PathBuf::new()).to_str().unwrap());
    eprintln!();
    eprintln!("+---------+----------------------------------+--------------------+-----------------+");
    eprintln!("| section | task                             | query in paper     | elapsed seconds |");
    eprintln!("+---------+----------------------------------+--------------------+-----------------+");
    eprintln!("| init    | load_projects                    |                    | {:>15} |", load_projects);
    eprintln!("| init    | group_projects_by_language       |                    | {:>15} |", group_projects_by_languages);
    eprintln!("+---------+----------------------------------+--------------------+-----------------+");
    eprintln!("| queries | stars                            | stars              | {:>15} |", stars);
    eprintln!("| queries | mean_changes_in_commits          |                    | {:>15} |", mean_changes_in_commits);
    eprintln!("| queries | median_changes_in_commits        | touched files      | {:>15} |", median_changes_in_commits);
    eprintln!("| queries | experienced_authors              | experienced author | {:>15} |", experienced_authors);
    eprintln!("| queries | experienced_authors_ratio        | 50% experienced    | {:>15} |", experienced_authors_ratio);
    eprintln!("| queries | mean_commit_message_sizes        |                    | {:>15} |", mean_commit_message_sizes);
    eprintln!("| queries | median_commit_message_sizes      | message size       | {:>15} |", median_commit_message_sizes);
    eprintln!("| queries | commits                          | number of commits  | {:>15} |", commits);
    eprintln!("| queries | all_issues                       | issues             | {:>15} |", all_issues);
    eprintln!("| queries | issues                           |                    | {:>15} |", issues);
    eprintln!("| queries | buggy_issues                     |                    | {:>15} |", buggy_issues);
    eprintln!("+---------+----------------------------------+--------------------+-----------------+");
    eprintln!("| dump    | debug_dump                       |                    | {:>15} |", debug_dump);
    eprintln!("| dump    | dump_all                         |                    | {:>15} |", dump);
    eprintln!("+---------+----------------------------------+--------------------+-----------------+");
}
