use clap::Clap;

use djanco::*;
use djanco::database::*;
use djanco::objects::*;
use djanco::csv::*;
use djanco::log::*;

// rm -rf ~/djanco_cache && cargo run --bin clones --release -- -o ~/output -d /home/peta/devel/codedj-2/datasets/java-1k5-merged -c ~/djanco_cache --data-dump ~/output/dump > out.txt

fn main() {
    let config = Configuration::parse();
    let log = Log::new(Verbosity::Debug);

    let database =
        Djanco::from_config(&config, timestamp!(December 2020), stores!(Generic), log.clone())
            .expect("Error initializing datastore.");

    /*
    projects_all(&config, &log, &database).map(|project| {
        return (
            project.id(),
            project.url(),
            project.star_count(),
            project.created(),
            project.size(),
            project.fork_count(),
            project.language(),
        );        
    }).into_csv_with_headers_in_dir(
        vec!["id","url", "stars","created", "forks","language"],
        &config.output_path,
        "projects_codedj_2_quick"
    ).unwrap();
    */    

    //projects_all(&config, &log, &database).into_extended_csv_in_dir(&config.output_path, "projects_all").unwrap();
    // for lack of better names
    // for lack of better tools
    // for lack of better languages
    // variadic stuff rulez!!!!!!!!! 
    // but this helps the memory down
    // constructive comment: We split the table into multiple tables as the macros & traits required for the thing to be glued together properly are only defined up to certain length
    projects_all(&config, &log, &database).map(|project| {
        return (
            project.id(),
            project.url(),
            project.is_archived(), 
            project.star_count(),
            project.subscriber_count(),
            project.size(),
            project.commit_count(),
            project.license(),
            project.language(),
            project.major_language(),
        );        
    }).into_csv_with_headers_in_dir(
        vec!["id","url", "archived","stars","subscribers", "size","commits","license","language","major_language"],
        &config.output_path,
        "projects_codedj_2_a"
    ).unwrap();
    projects_all(&config, &log, &database).map(|project| {
        return (
            project.id(),
            project.created(),
            project.lifetime(),
            project.user_count(),
            project.author_count(),
            project.committer_count(),
            project.max_commit_delta(),
            project.avg_commit_delta(),
            project.project_max_experience(),
            project.project_experience(),
        );        
    }).into_csv_with_headers_in_dir(
        vec!["id","created","age","users","authors","committers","max_c_delta", "avg_c_delta", "max_exp", "exp"],
        &config.output_path,
        "projects_codedj_2_b"
    ).unwrap();
    projects_all(&config, &log, &database).map(|project| {
        return (
            project.id(),
            project.duplicated_code(),
            project.authors_contributing_commits_count(95),
            project.authors_contributing_commits_count(80),
            project.authors_contributing_commits_count(50),
            project.authors_contributing_changes_count(95),
            project.authors_contributing_changes_count(80),
            project.authors_contributing_changes_count(50),
            project.unique_files(),
            project.original_files(),
        );        
    }).into_csv_with_headers_in_dir(
        vec!["id", "dup","a_commits_95","a_commits_80","a_commits_50", "a_changes_95", "a_changes_80", "a_changes_50", "unique", "original"],
        &config.output_path,
        "projects_codedj_2_c"
    ).unwrap();
    projects_all(&config, &log, &database).map(|project| {
        return (
            project.id(),
            project.path_count(),
            project.snapshot_count(),
            project.major_language_ratio(),
            //project.all_forks(),
            project.project_locs(),
            project.impact(),
            project.latest_update_time(),
            project.oldest_commit_with_data().map(|x| x.committer_timestamp()),
            project.newest_commit_with_data().map(|x| x.committer_timestamp()),
            project.main_branch_commits_with_data().map(|x| x.len()),
        );        
    }).into_csv_with_headers_in_dir(
        vec!["id", "paths", "snapshots","major_language_ratio", /*"all_forks",*/ "locs", "impact", "latestUpdateTime","oldestCommitTime","newestCommitTime", "mbrCommits"],
        &config.output_path,
        "projects_codedj_2_d"
    ).unwrap();
    /*
    //snapshots_by_num_projects(&config, &log, &database).into_csv(path!("snapshots_by_projects")).unwrap();
    projects_by_unique_files(&config, &log, &database).into_csv(path!("projects_by_unique_files")).unwrap();
    projects_by_original_files(&config, &log, &database).into_csv(path!("projects_by_original_files")).unwrap();
    projects_by_impact(&config, &log, &database).into_csv(path!("projects_by_impact")).unwrap();
    projects_by_files(&config, &log, &database).into_csv(path!("projects_by_files")).unwrap();
    //projects_by_major_language_ratio(&config, &log, &database).into_csv(path!("projects_by_major_language_ratio")).unwrap();
    projects_by_major_language_changes(&config, &log, &database).into_csv(path!("projects_by_major_language_changes")).unwrap();    
    projects_by_all_forks(&config, &log, &database).into_csv(path!("projects_by_all_forks")).unwrap();
    projects_by_loc(&config, &log, &database).into_csv(path!("projects_by_loc")).unwrap();
    */
}

/*
fn snapshots_by_num_projects<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Snapshot>> {
    database
        .snapshots()
        .sort_by(snapshot::NumProjects)
        .sample(Top(50))
}
*/

fn projects_all<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
}

#[allow(dead_code)]
fn projects_by_unique_files<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .sort_by(project::UniqueFiles)
        .sample(Top(50))
}

#[allow(dead_code)]
fn projects_by_original_files<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .sort_by(project::OriginalFiles)
        .sample(Top(50))
}

#[allow(dead_code)]
fn projects_by_impact<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .sort_by(project::Impact)
        .sample(Top(50))
}

#[allow(dead_code)]
fn projects_by_files<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .sort_by(project::Files)
        .sample(Top(50))
}

/*
fn projects_by_major_language_ratio<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .sort_by(project::MajorLanguageRatio)
        .sample(Top(50))
}
*/

#[allow(dead_code)]
fn projects_by_major_language_changes<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .sort_by(project::MajorLanguageChanges)
        .sample(Top(50))
}

#[allow(dead_code)]
fn projects_by_all_forks<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .sort_by(Count(project::AllForks))
        .sample(Top(50))
}

#[allow(dead_code)]
fn projects_by_loc<'a>(_config: &Configuration, _log: &Log, database: &'a Database) -> impl Iterator<Item=ItemWithData<'a, Project>> {
    database
        .projects()
        .sort_by(project::Locs)
//        .sample(Top(50))
}


