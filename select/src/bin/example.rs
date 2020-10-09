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
// * CSV output if not squashed
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
    db.projects()
}

type Groups = djanco::GroupIter<attrib::AttributeValue<project::Language, String>, objects::Project>;
fn group_projects_by_languages(projects: Projects) -> Groups {
    projects.group_by_attrib(project::Language)
}

fn stars(config: &Configuration, groups: Groups) {
    groups
        .sort_by_attrib(Descending, project::Stars)
        .sample(sample::Top(50))
        .squash()
        .to_csv(format!("{}/stars.csv", config.output_path.to_str().unwrap())).unwrap();
}

fn touched_files(config: &Configuration, groups: Groups) {
    groups
        .sort_by_attrib(Descending, stats::Median(retrieve::From(project::Commits, commit::Paths)))
        .sample(sample::Top(50))
        .squash()
        .to_csv(format!("{}/touched_files.csv", config.output_path.to_str().unwrap())).unwrap();
}

fn experienced_author(config: &Configuration, groups: Groups) {
    groups
        .filter_by_attrib(require::Exists(project::UsersWith(require::AtLeast(user::Experience, Seconds::from_years(2)))))
        .sample(sample::Random(50))
        .squash()
        .to_csv(format!("{}/experienced_author.csv", config.output_path.to_str().unwrap())).unwrap();
}

fn fifty_percent_experienced(config: &Configuration, groups: Groups) {
    groups
        .filter_by_attrib(require::AtLeast(stats::Count(project::Users), 2))
        .filter_by_attrib(require::AtLeast(stats::Ratio(project::UsersWith(require::AtLeast(user::Experience, Seconds::from_years(2)))), 0.5))
        .sample(sample::Random(50))
        .squash()
        .to_csv(format!("{}/50%_experienced.csv", config.output_path.to_str().unwrap())).unwrap();
}

fn message_size(config: &Configuration, groups: Groups) {
    groups
        .sort_by_attrib(Descending, stats::Median(retrieve::From(retrieve::From(project::Commits, commit::Message), message::Length)))
        .sample(sample::Top(50))
        .squash()
        .to_csv(format!("{}/message_size.csv", config.output_path.to_str().unwrap())).unwrap();
}

fn number_of_commits(config: &Configuration, groups: Groups) {
    groups
        .sort_by_attrib(Descending, project::Commits)
        .sample(sample::Top(50))
        .squash()
        .to_csv(format!("{}/number_of_commits.csv", config.output_path.to_str().unwrap())).unwrap();
}

fn issues(config: &Configuration, groups: Groups) {
    groups
        .sort_by_attrib(Descending, project::AllIssues)
        .sample(sample::Top(50))
        .squash()
        .to_csv(format!("{}/issues.csv", config.output_path.to_str().unwrap())).unwrap();
}

#[allow(dead_code)]
fn dump_all(config: &Configuration, projects: Projects) {
    match &config.dump_path {
        Some(path) => projects.dump_all_info_to(path.to_str().unwrap()).unwrap(),
        None => ()
    }
}

// works with downloader from commit  146e55e34ca1f4cc5b826e0c909deac96afafc17
fn main() {
    // let dataset_path = "/dejacode/dataset";
    // let cache_path = "examples/cache";
    // let output_path = "examples/output";
    // let _dump_path = "examples/dump";
    let config = Configuration::from_args();

    let (projects, load_projects) = with_elapsed_secs!("load_projects",
        load_projects(&config, 0, Month::August(2020))
    );
    let (groups, group_projects_by_languages) = with_elapsed_secs!("group_projects_by_languages",
        group_projects_by_languages(projects.clone())
    );

    let stars                     = elapsed_secs!("stars",                     stars                    (&config, groups.clone()));
    let touched_files             = elapsed_secs!("touched_files",             touched_files            (&config, groups.clone()));
    let experienced_author        = elapsed_secs!("experienced_author",        experienced_author       (&config, groups.clone()));
    let fifty_percent_experienced = elapsed_secs!("fifty_percent_experienced", fifty_percent_experienced(&config, groups.clone()));
    let message_size              = elapsed_secs!("message_size",              message_size             (&config, groups.clone()));
    let number_of_commits         = elapsed_secs!("number_of_commits",         number_of_commits        (&config, groups.clone()));
    let issues                    = elapsed_secs!("issues",                    issues                   (&config, groups.clone()));
    let dump                      = elapsed_secs!("dump_all",                   dump_all                (&config, projects));

    eprintln!("Summary:");
    eprintln!();
    eprintln!("+---------+-------------------------------+-----------------+");
    eprintln!("| section | task                          | elapsed seconds |");
    eprintln!("+---------+-------------------------------+-----------------+");
    eprintln!("| init    | load_projects                 | {:>15} |", load_projects);
    eprintln!("| init    | group_projects_by_language    | {:>15} |", group_projects_by_languages);
    eprintln!("+---------+-------------------------------|-----------------+");
    eprintln!("| queries | stars                         | {:>15} |", stars);
    eprintln!("| queries | touched_files                 | {:>15} |", touched_files);
    eprintln!("| queries | experienced_author            | {:>15} |", experienced_author);
    eprintln!("| queries | fifty_percent_experienced     | {:>15} |", fifty_percent_experienced);
    eprintln!("| queries | message_size                  | {:>15} |", message_size);
    eprintln!("| queries | number_of_commits             | {:>15} |", number_of_commits);
    eprintln!("| queries | issues                        | {:>15} |", issues);
    eprintln!("+---------+-------------------------------|-----------------+");
    eprintln!("| dump    | dump_all                      | {:>15} |", dump);
    eprintln!("+---------+-------------------------------+-----------------+");
}