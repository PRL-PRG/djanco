use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::fs::File;
use std::io::Error;

use itertools::Itertools;
use byteorder::{WriteBytesExt, BigEndian, ReadBytesExt};

use dcd::{Database, FilePath, Project, User, Commit, ProjectId, CommitId, UserId, PathId, ProjectAllCommitIdsIter, ProjectCommitIter};

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

    fn get_commit_bare(&self, id: CommitId) -> Option<Commit> {
        // there is little point in caching bare commits.
        return self.database.get_commit_bare(id);
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

    // fn commits_from(&self, project: &Project)  -> ProjectCommitIter {
    //     ProjectCommitIter::from(self, project)
    // }
    //
    // fn bare_commits_from(&self, project: &Project) -> ProjectBareCommitIter {
    //     ProjectBareCommitIter::from(self, project)
    // }
    //
    // fn user_ids_from(&self, project: &Project) -> ProjectUserIdIter {
    //     ProjectUserIdIter::from(self, project)
    // }
}

#[derive(Debug)]
pub struct PersistentProjectCommitIndex {
    project_ids_and_their_commit_ids: Vec<Vec<CommitId>>,
}

impl PersistentProjectCommitIndex {

    pub fn write_to(path: &Path, data: &mut dyn Iterator<Item=(ProjectId, Vec<CommitId>)>) -> Result<(), Error> {
        let iterator = data
            .sorted_by_key(|(project_id, _commit_ids)| *project_id)
            .map(|(_project_id, commit_ids)| commit_ids);

        let mut file = File::create(path)?;
        file.write_u64::<BigEndian>(iterator.len() as u64)?;
        for commit_ids_of_a_single_project in iterator {
            file.write_u64::<BigEndian>(commit_ids_of_a_single_project.len() as u64)?;
            for commit_id in commit_ids_of_a_single_project.iter() {
                file.write_u64::<BigEndian>(*commit_id)?;
            }
        }

        Ok(())
    }

    pub fn read_from(path: &Path) -> Result<Self, Error> {
        let mut file = File::open(path)?;
        let mut project_ids_and_their_commit_ids = Vec::new();
        let num_projects = file.read_u64::<BigEndian>()?;
        for _ in 0..num_projects {
            match file.read_u64::<BigEndian>() {
                Ok(num_commits_in_this_project) => {
                    let mut this_projects_commit_ids = Vec::new();
                    for _ in 0..num_commits_in_this_project {
                        let commit_id = file.read_u64::<BigEndian>()?;
                        this_projects_commit_ids.push(commit_id);
                    }
                    project_ids_and_their_commit_ids.push(this_projects_commit_ids);
                },
                Err(e) => return Err(e),
            }
        }
        Ok(PersistentProjectCommitIndex { project_ids_and_their_commit_ids })
    }

    pub fn get_commit_ids_for(&self, project_id: ProjectId) -> Option<&Vec<CommitId>> {
        self.project_ids_and_their_commit_ids.get(project_id as usize)
    }
}

pub struct IndexedDatabase<'a> {
    database: &'a dyn Database,
    bypass:   bool,

    project_commit_index: PersistentProjectCommitIndex,
}

impl<'a> IndexedDatabase<'a> {
    pub fn from(database: &'a impl Database, path: &Path, bypass: bool) -> Result<Self, Error> {
        let project_commit_index = if path.is_file() {
            PersistentProjectCommitIndex::read_from(path)?
        } else {
            PersistentProjectCommitIndex::write_to(path, &mut database.project_ids_and_their_commit_ids())?;
            PersistentProjectCommitIndex::read_from(path)?
        };

        Ok(IndexedDatabase {
            database,
            bypass,
            project_commit_index,
        })
    }
}

trait IndexedCommits {
    fn commits_from(&self, project: &Project) -> Vec<Commit> where Self: Sized;
}

impl<'a> IndexedCommits for IndexedDatabase<'a> {
    fn commits_from(&self, project: &Project) -> Vec<Commit> where Self: Sized {
        self.project_commit_index
            .get_commit_ids_for(project.id).unwrap()
            .into_iter()
            .map(|commit_id| self.database.get_commit(*commit_id).unwrap())
            .collect()
    }
}

// impl<'a> Database for IndexedDatabase<'a> {
//     fn num_projects(&self) -> u64 { self.database.num_projects() }
//     fn num_commits(&self) -> u64 { self.database.num_commits() }
//     fn num_users(&self) -> u64 { self.database.num_users() }
//     fn num_file_paths(&self) -> u64 { self.database.num_file_paths() }
//     fn get_project(&self, id: u64) -> Option<Project> { self.database.get_project(id) }
//     fn get_commit(&self, id: u64) -> Option<Commit> { self.database.get_commit(id) }
//     fn get_commit_bare(&self, id: u64) -> Option<Commit> { self.database.get_commit_bare(id) }
//     fn get_user(&self, id: u64) -> Option<&User> { self.database.get_user(id) }
//     fn get_file_path(&self, id: u64) -> Option<FilePath> { self.database.get_file_path(id) }
//
//     fn commits_from(&self, project: &Project) -> ProjectCommitIter<'a> where Self: Sized {
//         self.project_commit_index.get_commit_ids_for(project.id)
//     }
//
//     fn project_ids_and_their_commit_ids(&self) -> ProjectAllCommitIdsIter<'_, Self> where Self: Sized {
//         self.project_commit_index.
//     }
// }

// struct LiveProjectCommitIndex<'a> {
//     database: &'a dyn Database,
// }
//
// impl<'a> LiveProjectCommitIndex<'a> {
//     pub fn from(database: &'a impl Database) -> Self {
//         LiveProjectCommitIndex { database }
//     }
// }
//
// // impl<'a> Iterator for LiveProjectCommitIndex<'a> {
// //     type Item = Vec<CommitId>;
// //
// //     fn next(&mut self) -> Option<Self::Item> {
// //         let project_opt = self.database.get_project(self.project_cursor);
// //         if project_opt.is_none() { return None }
// //         self.project_cursor += 1;
// //         Some(self.database.bare_commits_from(&project_opt.unwrap()).map(|c| c.id).collect())
// //     }
// // }
//
// impl<'a> Serialize for LiveProjectCommitIndex<'a> {
//     fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error> where
//         S: Serializer {
//         serializer.collect_seq(self.database.project_ids_and_their_commit_ids())
//     }
// }
//
// struct PersistentProjectCommitIndex {
//     project_ids_and_their_commit_ids: Vec<(ProjectId, Vec<CommitId>)>
// }
//
// impl<'a> Serialize for PersistentProjectCommitIndex {
//     fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error> where
//         S: Serializer {
//         serializer.collect_seq(self.project_ids_and_their_commit_ids.iter())
//     }
// }
//
// impl Visitor<'de> for PersistentProjectCommitIndex {
//
// }
//
// impl Deserialize for PersistentProjectCommitIndex {
//     fn deserialize<'de, D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error> where
//         D: Deserializer<'de> {
//         let v = unimplemented!();
//         let result = deserializer.deserialize_seq(v);
//     }
//
//     fn deserialize_in_place<'de, D>(deserializer: D, place: &mut Self) -> Result<(), <D as Deserializer<'de>>::Error> where
//         D: Deserializer<'de>, {
//         unimplemented!()
//     }
// }


