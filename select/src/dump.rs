use crate::meta::MetaDatabase;
use std::io::Error;
use dcd::{Project, Commit, User, ProjectId, CommitId, UserId};
use std::collections::HashSet;
use std::fs::{create_dir_all, File};
use std::path::PathBuf;

trait DumpFrom {
    fn dump_all_info_about<I>(&self, projects: I, dir: &PathBuf) -> Result<(), Error> where I: Iterator<Item=Project>;
}

impl<D> DumpFrom for D where D: MetaDatabase {
    fn dump_all_info_about<I>(&self, source: I, dir: &PathBuf) -> Result<(), Error> where I: Iterator<Item=Project> {

        create_dir_all(dir)?;

        let project_sink = { let mut path = dir.clone(); path.push("projects.csv"); File::create(path) };
        let commit_sink  = { let mut path = dir.clone(); path.push("commits.csv");  File::create(path) };
        let user_sink    = { let mut path = dir.clone(); path.push("users.csv");    File::create(path) };

        let mut visited_projects: HashSet<ProjectId> = HashSet::new();
        let mut visited_commits:  HashSet<CommitId>  = HashSet::new();
        let mut visited_users:    HashSet<UserId>   = HashSet::new();

        for project in source {

        }

        unimplemented!()
    }
}
