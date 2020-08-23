use dcd::{DCD, Database};
use select::meta::ProjectMeta;
use std::fs::File;
use ghql::ast::Parameter::Path;
use std::path::PathBuf;
use std::io::Write;

fn main() {
    let database = DCD::new("/dejavuii/dejacode/dataset-small".to_string());

    let mut file = File::create(PathBuf::from("dump.csv")).unwrap();
    writeln!(file, "id,url,language,stars,issues,buggy_issues,commits");

    for project in (&database).projects() {

        let id = &project.id;
        let url = &project.url;
        let stars = &project.get_stars_or_zero();
        let issues = &project.get_issue_count_or_zero();
        let buggy_issues = &project.get_buggy_issue_count_or_zero();
        let language = &project.get_language_or_empty();
        let commits = &project.get_commit_count_in(&database);

        writeln!(file, "{},{},{},{},{},{},{}", id,url,language,stars,issues,buggy_issues,commits);
    }
}