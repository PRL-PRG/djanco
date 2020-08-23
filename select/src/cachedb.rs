use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::io::Error;
use std::cell::RefCell;
use std::time::Duration;

use itertools::{Itertools, MinMaxResult};

use dcd::{Database, FilePath, Project, User, Commit, ProjectId, CommitId, UserId, PathId};

use crate::meta::MetaDatabase;
use crate::io::*;

type Seconds = u64;

pub struct CachedDatabase<'a> {
    database: &'a dyn MetaDatabase,
    bypass:   bool,

    projects:   RefCell<HashMap<ProjectId, Project>>,
    commits:    RefCell<HashMap<CommitId,  Commit>>,
    paths:      RefCell<HashMap<PathId,    FilePath>>,

    author_experience:   RefCell<HashMap<UserId, Seconds>>,
}

impl<'a> CachedDatabase<'a> {
    pub fn from(database: &'a impl MetaDatabase, bypass: bool) -> Self {
        CachedDatabase {
            database,
            bypass,

            projects: RefCell::new(HashMap::new()),
            commits:  RefCell::new(HashMap::new()),
            paths:    RefCell::new(HashMap::new()),

            author_experience: RefCell::new(HashMap::new()),
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

impl MetaDatabase for CachedDatabase<'_> {
    fn author_experience_time(&self, id: UserId) -> Option<Duration> {
        if self.bypass {
            return self.database.author_experience_time(id)
        }

        let mut author_experiences = self.author_experience.borrow_mut();

        if author_experiences.is_empty() {
            let iterator = self.bare_commits()
                .map(|c| (c.author_id, c.author_time))
                .into_group_map()
                .into_iter()
                .map(|(author_id, author_times)| {(
                    author_id,
                    match author_times.iter().minmax() {
                        MinMaxResult::NoElements       => 0u64,
                        MinMaxResult::OneElement(_)    => 0u64,
                        MinMaxResult::MinMax(min, max) => (max - min) as u64,
                    }
                )});
            author_experiences.extend(iterator);
        }

        author_experiences.get(&id).map(|seconds| Duration::from_secs(*seconds))
    }
}

pub struct PersistentIndex<'a, D: Database + MetaDatabase + Sized> {
    database: &'a D,
    bypass: bool,
    //path: PathBuf,

    project_commit_mapping: Vec<Vec<CommitId>>,
    author_experiences:     Vec<Seconds>,
}

impl<'a, D> PersistentIndex<'a, D> where D: Database + MetaDatabase + Sized {
    pub fn from(database: &'a D, path: Option<PathBuf>) -> Result<PersistentIndex<'a, D>, Error> {
        Ok(match path {
            None => PersistentIndex {
                database,
                bypass: true,
                project_commit_mapping: vec![],
                author_experiences: vec![],
            },
            Some(path) => {
                let project_commit_mapping =
                    Self::load_project_commit_mapping(database, &path)?;
                let author_experiences =
                    Self::load_author_experience_times(database, &path)?;
                PersistentIndex {
                    database,
                    bypass: false,
                    project_commit_mapping,
                    author_experiences,
                }
            }
        })
    }
}

impl<'a, D> Database for PersistentIndex<'a, D> where D: Database + MetaDatabase + Sized {
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
        self.database.get_project(id)
    }
    fn get_commit(&self, id: CommitId) -> Option<Commit> {
        self.database.get_commit(id)
    }
    fn get_commit_bare(&self, id: CommitId) -> Option<Commit> {
        self.database.get_commit_bare(id)
    }
    fn get_user(&self, id: UserId) -> Option<&User> {
        self.database.get_user(id)
    }
    fn get_file_path(&self, id: PathId) -> Option<FilePath> {
        self.database.get_file_path(id)
    }

    fn projects(&self) -> Box<dyn Iterator<Item=Project> + '_> where Self: Sized {
        self.database.projects()
    }
    fn commits(&self) -> Box<dyn Iterator<Item=Commit> + '_> where Self: Sized {
        self.database.commits()
    }
    fn bare_commits(&self) -> Box<dyn Iterator<Item=Commit> + '_> where Self: Sized {
        self.database.bare_commits()
    }
    fn users(&self) -> Box<dyn Iterator<Item=&User> + '_> where Self: Sized {
        self.database.users()
    }

    fn commits_from(&self, project: &Project) -> Box<dyn Iterator<Item=Commit> + '_> where Self: Sized {
        if self.bypass {
            return self.database.commits_from(project)
        }
        let commit_ids =
            self.project_commit_mapping.get(project.id as usize).unwrap().clone();
        let iterator: ProjectCommitIter<'a,D> = ProjectCommitIter::from(self.database, commit_ids);
        Box::new(iterator)
    }

    fn bare_commits_from(&self, project: &Project) -> Box<dyn Iterator<Item=Commit> + '_> where Self: Sized {
        if self.bypass {
            return self.database.bare_commits_from(project)
        }

        let commit_ids =
            self.project_commit_mapping.get(project.id as usize).unwrap().clone();
        let iterator: ProjectBareCommitIter<'a, D> = ProjectBareCommitIter::from(self.database,commit_ids);
        Box::new(iterator)
    }

    fn user_ids_from(&self, project: &Project) -> Box<dyn Iterator<Item=UserId> + '_> where Self: Sized {
        self.database.user_ids_from(project) //TODO
    }

    fn project_ids_and_their_commit_ids(&self) -> Box<dyn Iterator<Item=(ProjectId,Vec<CommitId>)> + '_> where Self: Sized {
        if self.bypass {
            return self.database.project_ids_and_their_commit_ids()
        }

        fn convert(e: (usize, &Vec<CommitId>)) -> (ProjectId, Vec<CommitId>) {
            let (project_id, commit_ids) = e;
            (project_id as u64, commit_ids.clone())
        }

        let iter = self.project_commit_mapping
             .iter()
             .enumerate()
             .map(convert as fn((usize, &Vec<CommitId>)) -> (ProjectId, Vec<CommitId>));
        Box::new(iter)
    }
}

impl<'a, D> MetaDatabase for PersistentIndex<'a, D> where D: Database + MetaDatabase + Sized {
    fn author_experience_time(&self, id: u64) -> Option<Duration> {
        if self.bypass {
            return self.database.author_experience_time(id)
        }
        if (id as usize) >= self.author_experiences.len() {
            return None
        }
        self.author_experiences.get(id as usize).map(|experience: &Seconds| {
            Duration::from_secs(*experience)
        })
    }
}

impl<'a, D> PersistentIndex<'a, D> where D: MetaDatabase + Sized {
    fn load_author_experience_times(database: &'a impl MetaDatabase, path: &Path) -> Result<Vec<Seconds>, Error> {
        let mut full_path = PathBuf::new();
        full_path.push(path.clone());
        full_path.push("author_experiences");
        full_path.set_extension("bin");

        if full_path.exists() {
            return read_vec_u64(&full_path)
        }

        let mut vector: Vec<Seconds> = vec![];
        for user_id in 0..database.num_users() as UserId {
            vector.push(database.author_experience_time(user_id).map_or(0, |d| d.as_secs()))
        }

        write_vec_u64(&full_path, &vector)?;
        return Ok(vector);
    }

    fn load_project_commit_mapping(database: &'a impl Database, path: &Path) -> Result<Vec<Vec<CommitId>>, Error> {
        let mut full_path = PathBuf::new();
        full_path.push(path.clone());
        full_path.push("project_commit_mapping");
        full_path.set_extension("bin");

        if full_path.exists() {
            return read_vec_vec_u64(&full_path)
        }

        let vector: Vec<Vec<CommitId>> = database
            .project_ids_and_their_commit_ids()
            //.sorted_by_key(|(row_id, _row)| { println!("* {}", row_id); *row_id})
            .map(|(_row_id, row)| row)
            .collect();

        write_vec_vec_u64(&full_path, &vector)?;
        return Ok(vector);
    }
}

pub struct ProjectCommitIter<'a, D> where D: Database + Sized {
    database: &'a D,
    commit_ids: Vec<CommitId>,
}

impl<'a, D> ProjectCommitIter<'a, D> where D: Database + Sized {
    pub fn from(database: &'a D, commit_ids: Vec<CommitId>) -> ProjectCommitIter<'a, D> {
        ProjectCommitIter { database, commit_ids }
    }
}

impl<'a, D> Iterator for ProjectCommitIter<'a, D> where D: Database + Sized {
    type Item = Commit;
    fn next(&mut self) -> Option<Self::Item> {
        if self.commit_ids.is_empty() {
            return None;
        }
        let commit_id = self.commit_ids.pop().unwrap();
        let commit = self.database.get_commit(commit_id).unwrap();
        // Commit_id should be correct, so i unwrap it and wrap it again. If this explodes,
        // something is very wrong indeed, so it's better to stop the program than just the
        // iterator.
        return Some(commit);
    }
}

pub struct ProjectBareCommitIter<'a, D: Database + Sized> {
    database: &'a D,
    commit_ids: Vec<CommitId>,
}

impl<'a, D> ProjectBareCommitIter<'a, D> where D: Database + Sized {
    pub fn from(database: &'a D, commit_ids: Vec<CommitId>) -> ProjectBareCommitIter<'a, D> {
        ProjectBareCommitIter { database, commit_ids }
    }
}

impl<'a, D> Iterator for ProjectBareCommitIter<'a, D> where D: Database + Sized {
    type Item = Commit;
    fn next(&mut self) -> Option<Self::Item> {
        if self.commit_ids.is_empty() {
            return None;
        }
        let commit_id = self.commit_ids.pop().unwrap();
        let commit = self.database.get_commit_bare(commit_id).unwrap();
        // Commit_id should be correct, so i unwrap it and wrap it again. If this explodes,
        // something is very wrong indeed, so it's better to stop the program than just the
        // iterator.
        return Some(commit);
    }
}

// #[derive(Debug)]
// pub struct PersistentProjectCommitIndex {
//     project_ids_and_their_commit_ids: Vec<Vec<CommitId>>,
// }

// impl PersistentProjectCommitIndex {
//
//     pub fn write_to(path: &Path, data: &mut dyn Iterator<Item=(ProjectId, Vec<CommitId>)>) -> Result<(), Error> {
//         let iterator = data
//             .sorted_by_key(|(project_id, _commit_ids)| *project_id)
//             .map(|(_project_id, commit_ids)| commit_ids);
//
//         let mut file = File::create(path)?;
//         file.write_u64::<BigEndian>(iterator.len() as u64)?;
//         for commit_ids_of_a_single_project in iterator {
//             file.write_u64::<BigEndian>(commit_ids_of_a_single_project.len() as u64)?;
//             for commit_id in commit_ids_of_a_single_project.iter() {
//                 file.write_u64::<BigEndian>(*commit_id)?;
//             }
//         }
//
//         Ok(())
//     }
//
//     pub fn read_from(path: &Path) -> Result<Self, Error> {
//         let mut file = File::open(path)?;
//         let mut project_ids_and_their_commit_ids = Vec::new();
//         let num_projects = file.read_u64::<BigEndian>()?;
//         for _ in 0..num_projects {
//             match file.read_u64::<BigEndian>() {
//                 Ok(num_commits_in_this_project) => {
//                     let mut this_projects_commit_ids = Vec::new();
//                     for _ in 0..num_commits_in_this_project {
//                         let commit_id = file.read_u64::<BigEndian>()?;
//                         this_projects_commit_ids.push(commit_id);
//                     }
//                     project_ids_and_their_commit_ids.push(this_projects_commit_ids);
//                 },
//                 Err(e) => return Err(e),
//             }
//         }
//         Ok(PersistentProjectCommitIndex { project_ids_and_their_commit_ids })
//     }
//
//     pub fn get_commit_ids_for(&self, project_id: ProjectId) -> Option<&Vec<CommitId>> {
//         self.project_ids_and_their_commit_ids.get(project_id as usize)
//     }
// }



// pub struct IndexedDatabase<'a> {
//     database: &'a dyn Database,
// //    bypass:   bool,
//
//     project_commit_index: PersistentProjectCommitIndex,
// }
//
// impl<'a> IndexedDatabase<'a> {
//     pub fn from(database: &'a impl Database, path: &Path, _bypass: bool) -> Result<Self, Error> {
//         let project_commit_index = if path.is_file() {
//             PersistentProjectCommitIndex::read_from(path)?
//         } else {
//             PersistentProjectCommitIndex::write_to(path, &mut database.project_ids_and_their_commit_ids())?;
//             PersistentProjectCommitIndex::read_from(path)?
//         };
//
//         Ok(IndexedDatabase {
//             database,
//             //bypass,
//             project_commit_index,
//         })
//     }
// }
//
// trait IndexedCommits {
//     fn commits_from(&self, project: &Project) -> Vec<Commit> where Self: Sized;
// }
//
// impl<'a> IndexedCommits for IndexedDatabase<'a> {
//     fn commits_from(&self, project: &Project) -> Vec<Commit> where Self: Sized {
//         self.project_commit_index
//             .get_commit_ids_for(project.id).unwrap()
//             .into_iter()
//             .map(|commit_id| self.database.get_commit(*commit_id).unwrap())
//             .collect()
//     }
// }