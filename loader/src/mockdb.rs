use std::collections::HashMap;

use dcd::Database;
use dcd::{Commit,   Project,   User, };
use dcd::{CommitId, ProjectId, UserId};

pub struct MockDatabase {
    projects:  HashMap<ProjectId, Project>,
    commits:   HashMap<CommitId, Commit>,
    users:     HashMap<UserId, User>,
    // snapshots: HashMap<BlobId, Snapshot>,
    // paths:     HashMap<PathId, FilePath>,
}

impl Database for MockDatabase {
    fn num_projects(&self) -> u64 {
        self.projects.len() as u64
    }

    fn num_commits(&self) -> u64 {
        self.commits.len() as u64
    }

    fn num_users(&self) -> u64 {
        self.users.len() as u64
    }

    fn get_project(&self, id: ProjectId) -> Option<Project> {
        self.projects.get(&id).map(|project| project.clone())
    }

    fn get_commit(&self, id: CommitId) -> Option<Commit> {
        self.commits.get(&id).map(|commit| commit.clone())
    }

    fn get_user(&self, id: UserId) -> Option<&User> {
        self.users.get(&id)
    }

    // fn get_snapshot(&self, id: BlobId) -> Option<Snapshot> {
    //     self.snapshots.get(&id).map(|snapshot| snapshot.clone())
    // }

    // fn get_file_path(&self, id: PathId) -> Option<FilePath> {
    //     self.paths.get(&id).map(|path| path.clone())
    // }
}