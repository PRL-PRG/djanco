use std::collections::{HashSet, HashMap};
use std::iter::FromIterator;
use crate::api::*;

pub struct ProjectIter<'a> {
    current:  ProjectId,
    total:    ProjectId,
    database: &'a dyn Database,
}

impl<'a> ProjectIter<'a> {
    pub fn from(database: &impl Database) -> ProjectIter {  // TODO This would probably work better as .iter() in Database
        let total = database.num_projects();
        ProjectIter { current: 0, total, database }
    }
}

impl<'a> Iterator for ProjectIter<'a> {
    type Item = Project;

    fn next(&mut self) -> Option<Self::Item> {
        if !(self.current < self.total) { // TODO I think the if is probably unnecessary.
            return None;
        }
        let project = self.database.get_project(self.current);
        self.current += 1;
        return project;
    }
}

pub struct CommitIter<'a> {
    current_project:  ProjectId,
    total_projects:   ProjectId,
    visited_commits:  HashSet<CommitId>,
    commits_to_visit: HashSet<CommitId>,
    database:         &'a dyn Database,
}

impl<'a> CommitIter<'a> {
    pub fn from(database: &impl Database) -> CommitIter {  // TODO This would probably work better as .iter() in Database
        let current_project = 0;
        let total_projects = database.num_projects();
        let project = database.get_project(current_project);
        let visited_commits = HashSet::new();

        let heads: Option<HashMap<String, CommitId>> = project.map(|project| {
            match project.heads {
                Some(heads) => heads,
                None => HashMap::new(),
            }
        });

        let commits_to_visit = match heads {
            Some(heads) => HashSet::from_iter(heads.values().map(|e| *e)),
            None => HashSet::new(),
        };

        CommitIter { visited_commits, commits_to_visit, database, current_project, total_projects }
    }

    fn next_commit(&mut self) -> Option<Commit> {
        loop {
            let next_commit = self.commits_to_visit.iter().next();
            return match next_commit { // Blergh.
                Some(commit_id) => {
                    if !self.visited_commits.insert(*commit_id) {
                        continue; // Commit already visited - ignoring, going to the next one.
                    }
                    self.database.get_commit(*commit_id)
                },
                None => None, // commits_to_visit is empty
            }
        }
    }

    fn ensure_something_to_visit(&mut self) -> bool {
        // If no commits to visit, find some.
        while self.commits_to_visit.is_empty() {
            if !(self.current_project < self.total_projects) { // TODO I think the if is probably unnecessary.
                return false;
            }

            // Pour project heads into commits_to_visit.
            self.current_project += 1;
            if let Some(project) = self.database.get_project(self.current_project) {
                if let Some(heads) = project.heads {
                    self.commits_to_visit.extend(heads.values())
                }
            }
            // If the project had no heads, go again.
        }
        return true;
    }
}

impl<'a> Iterator for CommitIter<'a> {
    type Item = Commit;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if !(self.ensure_something_to_visit()) {
                return None;
            }
            if let Some(commit) = self.next_commit() {
                return Some(commit)
            }
            // If returned None, then self.commits_to_visit is empty again, so go around again.
        }
        unreachable!()
    }
}

pub struct ProjectCommitIter<'a> {
    visited:  HashSet<CommitId>,
    to_visit: HashSet<CommitId>,
    database: &'a dyn Database,
}

impl<'a> ProjectCommitIter<'a> {
    pub(crate) fn from(database: &'a impl Database, project: &Project) -> ProjectCommitIter<'a> { // TODO This would probably work better as .iter() in Project
        let visited = HashSet::new();
        let to_visit = match &project.heads {
            Some(heads) => HashSet::from_iter(heads.values().map(|e| *e)),
            None => HashSet::new(),
        };
        ProjectCommitIter { visited, to_visit, database }
    }
}

impl<'a> Iterator for ProjectCommitIter<'a> {
    type Item = Commit;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            return match self.to_visit.iter().next() { // Blergh.
                Some(commit_id) => {
                    if !self.visited.insert(*commit_id) {
                        continue; // Commit already visited - ignoring, going to the next one.
                    }
                    self.database.get_commit(*commit_id)
                },
                None => None, // Iterator is empty
            };
        }
    }
}

struct MockDatabase {
    projects:  HashMap<ProjectId, Project>,
    commits:   HashMap<CommitId, Commit>,
    snapshots: HashMap<BlobId, Snapshot>,
    paths:     HashMap<PathId, FilePath>,
    users:     HashMap<UserId, User>,
}

impl Database for MockDatabase {
    fn num_projects(&self) -> u64 {
        self.projects.len() as u64
    }

    fn get_user(&self, id: UserId) -> Option<&User> {
        self.users.get(&id)
    }

    fn get_snapshot(&self, id: BlobId) -> Option<Snapshot> {
        self.snapshots.get(&id).map(|snapshot| snapshot.clone())
    }

    fn get_file_path(&self, id: PathId) -> Option<FilePath> {
        self.paths.get(&id).map(|path| path.clone())
    }

    fn get_commit(&self, id: CommitId) -> Option<Commit> {
        self.commits.get(&id).map(|commit| commit.clone())
    }

    fn get_project(&self, id: ProjectId) -> Option<Project> {
        self.projects.get(&id).map(|project| project.clone())
    }
}