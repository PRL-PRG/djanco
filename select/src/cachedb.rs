use dcd::{Database, FilePath, Project, User, Commit, ProjectId, CommitId, UserId, PathId};
use std::cell::RefCell;
use std::collections::HashMap;

pub struct CachedDatabase<'a> {
    database: &'a dyn Database,
    bypass:   bool,

    projects: RefCell<HashMap<ProjectId, Project>>,
    commits:  RefCell<HashMap<CommitId,  Commit>>,
    paths:    RefCell<HashMap<PathId,    FilePath>>,
}

impl<'a> CachedDatabase<'a> {
    pub fn from(database: &'a impl Database, bypass: bool) -> Self {
        CachedDatabase {
            database,
            bypass,

            projects: RefCell::new(HashMap::new()),
            commits:  RefCell::new(HashMap::new()),
            paths:    RefCell::new(HashMap::new()),
        }
    }
}

impl<'a> Database for CachedDatabase<'a> {
    fn num_projects(&self) -> u64 {
        self.database.num_projects()
    }

    fn num_commits(&self) -> u64 {
        self.database.num_commits()
    }

    fn num_users(&self) -> u64 {
        self.database.num_users()
    }

    fn num_file_paths(&self) -> u64 {
        self.database.num_file_paths()
    }

    fn get_project(&self, id: ProjectId) -> Option<Project> {
        if self.bypass {
            return self.database.get_project(id)
        }

        let mut projects = self.projects.borrow_mut();

        if let Some(project) = projects.get(&id) {
            return Some(project.clone())
        };

        if let Some(project) = self.database.get_project(id) {
            let returned_project = project.clone(); // stuff from db stay here,
                                                            // always return clone
            projects.insert(id, project);
            return Some(returned_project)
        }

        return None;
    }

    fn get_commit(&self, id: CommitId) -> Option<Commit> {
        if self.bypass {
            return self.database.get_commit(id)
        }

        let mut commits = self.commits.borrow_mut();

        if let Some(commit) = commits.get(&id) {
            return Some(commit.clone())
        };

        if let Some(commit) = self.database.get_commit(id) {
            let returned_commit = commit.clone(); // stuff from db stay here,
                                                          // always return clone
            commits.insert(id, commit);
            return Some(returned_commit)
        }

        return None;
    }

    fn get_user(&self, id: UserId) -> Option<&User> {
        // Already cached.
        self.database.get_user(id)
    }

    fn get_file_path(&self, id: PathId) -> Option<FilePath> {
        if self.bypass {
            return self.database.get_file_path(id)
        }

        let mut paths = self.paths.borrow_mut();

        if let Some(path) = paths.get(&id) {
            return Some(path.clone())
        };

        if let Some(path) = self.database.get_file_path(id) {
            let returned_path = path.clone(); // stuff from db stay here,
                                                      // always return clone
            paths.insert(id, path);
            return Some(returned_path)
        }

        return None;
    }
}