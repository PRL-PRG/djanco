//mod pythagorean;
#[macro_use] mod log;
pub mod data;
pub mod objects;
pub mod csv;
//pub mod dump;
//mod io;
//pub mod query;
//pub mod cachedb;
pub mod meta;
//pub mod mockdb;
//pub mod selectors;

use std::path::PathBuf;
use dcd::DCD;
use std::marker::PhantomData;
use itertools::Itertools;
//use crate::meta::*;
use std::hash::Hash;
use std::rc::{Rc, Weak};
use std::cell::{RefCell};
use std::ops::Range;
use std::borrow::Borrow;
use std::iter::Map;
use std::collections::{HashSet, VecDeque};
use std::time::Duration;
use crate::csv::WithNames;
use crate::objects::{Project, Commit, User, Path, ProjectId, CommitId, UserId, PathId};
use crate::log::LogLevel;
use crate::data::Data;
use std::fmt;


trait DataSource {
    fn project_count(&self) -> usize;
    fn commit_count(&self)  -> usize;
    fn user_count(&self)    -> usize;
    fn path_count(&self)    -> usize;

    fn project(&self, id: &ProjectId)    -> Option<&Project>;
    fn commit(&self, id: &CommitId)      -> Option<&Commit>;
    fn user(&self, id: &UserId)          -> Option<&User>;
    fn path(&self, id: &PathId)          -> Option<&Path>;

    fn project_ids(&self) -> Map<Range<usize>, fn(usize) -> ProjectId>;
    fn commit_ids(&self)  -> Map<Range<usize>, fn(usize) -> CommitId>;
    fn user_ids(&self)    -> Map<Range<usize>, fn(usize) -> UserId>;
    fn path_ids(&self)    -> Map<Range<usize>, fn(usize) -> PathId>;

    fn projects(&self)     -> EntityIter<ProjectId, Project>;
    fn commits(&self)      -> EntityIter<CommitId,  Commit>;
    fn users(&self)        -> EntityIter<UserId,    User>;
    fn paths(&self)        -> EntityIter<PathId,    Path>;

    fn commits_from(&self, project: &ProjectId)      -> ProjectEntityIter<Commit>;
    fn paths_from(&self, project: &ProjectId)        -> ProjectEntityIter<Path>;
    fn users_from(&self, project: &ProjectId)        -> ProjectEntityIter<User>;
    fn authors_from(&self, project: &ProjectId)      -> ProjectEntityIter<User>;
    fn committers_from(&self, project: &ProjectId)   -> ProjectEntityIter<User>;

    fn commit_count_from(&self, project: &ProjectId)    -> usize;
    fn path_count_from(&self, project: &ProjectId)      -> usize;
    fn user_count_from(&self, project: &ProjectId)      -> usize;
    fn author_count_from(&self, project: &ProjectId)    -> usize;
    fn committer_count_from(&self, project: &ProjectId) -> usize;

    fn age_of(&self, project: &ProjectId) -> Option<Duration>;

    fn seed(&self) -> u128;
}

type DatabasePtr = Rc<RefCell<Djanco>>;

pub struct Djanco_ {
    seed: u128,
    timestamp: i64,
    verbosity: LogLevel,
    path: PathBuf,
}

impl Djanco_ {
    fn open(&self) -> DatabasePtr {
        //Djanco::load(self.seed, self.timestamp, self.)
        unimplemented!()
    }
}

pub struct Djanco {
    //warehouse: Option<DCD>,
    me: Option<Weak<RefCell<Djanco>>>, // Thanks for the help, Colette.
    seed: u128,
    timestamp: i64,
    verbosity: LogLevel,
    path: PathBuf,

    filters: Vec<Box<dyn LoadFilter>>,
    data: Option<Data>,
}

impl Djanco {
    pub fn from<S: Into<String>, T: Into<i64>>(path: S, seed: u128, timestamp: T) -> DatabasePtr {
        assert_eq!(std::u64::MAX.to_be_bytes(), std::usize::MAX.to_be_bytes());

        let string_path = path.into();
        //let warehouse = DCD::new(string_path.clone());
        let database = Djanco {
            //warehouse: Some(warehouse),
            verbosity: LogLevel::Verbose,
            me: None,
            path: PathBuf::from(string_path),
            timestamp: timestamp.into(),
            seed,
            filters: vec![],
            data: None,
        };
        let pointer: DatabasePtr = Rc::new(RefCell::new(database));

        // Things we do to avoid unsafe.
        pointer.borrow_mut().me = Some(Rc::downgrade(&pointer));
        pointer
    }

    pub fn instantiate(self) -> DatabasePtr {
        let warehouse = DCD::new(self.path_as_string());

        unimplemented!()
        // let data = if self.filters.is_empty() {
        //     Data::from(&warehouse, &self.verbosity)
        // } else {
        //     Data::from_filtered(&warehouse, &self.filters, &self.verbosity)
        // };

        // let database = Djanco {
        //     //warehouse: Some(warehouse),
        //     verbosity: self.verbosity,
        //     me: None,
        //     path: self.path,
        //     timestamp: self.timestamp,
        //     seed: self.seed,
        //     filters: self.filters,
        //     data: Some(data),
        // };
        //
        // let pointer: DatabasePtr = Rc::new(RefCell::new(database));
        //
        // // Things we do to avoid unsafe.
        // pointer.borrow_mut().me = Some(Rc::downgrade(&pointer));
        // pointer
    }

    pub fn me(&self) -> DatabasePtr {
        self.me.as_ref().unwrap().upgrade().unwrap()
    }

    // fn load_from_warehouse(&self) -> Data {
    //     let warehouse = DCD::new(self.path_as_string());
    //     if self.filters.is_empty() {
    //         Data::from(&warehouse, &self.verbosity)))
    //     } else {
    //         Data::from_filtered(&warehouse, &self.filters, &self.verbosity))
    //     }
    // }

    pub fn path_as_string(&self) -> String {
        self.path.as_os_str().to_str().unwrap().to_string()
    }

    pub fn with_log_level(mut self, level: LogLevel) -> Self {
        self.verbosity = level;
        self
    }
    pub fn with_project_filter<F>(mut self, filter: F) -> Self where F: LoadFilter + 'static {
        self.filters.push(Box::new(filter));
        self
    }
}

impl DataSource for Djanco {
    fn project_count(&self) -> usize {
        unimplemented!()
        //let db = self.clone().instantiate();
        //let db2: &RefCell<Djanco> = db.borrow();
        //db.project_count()
    }

    fn commit_count(&self) -> usize {
        unimplemented!()
    }

    fn user_count(&self) -> usize {
        unimplemented!()
    }

    fn path_count(&self) -> usize {
        unimplemented!()
    }

    fn project(&self, id: &ProjectId) -> Option<&Project> {
        unimplemented!()
    }

    fn commit(&self, id: &CommitId) -> Option<&Commit> {
        unimplemented!()
    }

    fn user(&self, id: &UserId) -> Option<&User> {
        unimplemented!()
    }

    fn path(&self, id: &PathId) -> Option<&Path> {
        unimplemented!()
    }

    fn project_ids(&self) -> Map<Range<usize>, fn(usize) -> ProjectId> {
        unimplemented!()
    }

    fn commit_ids(&self) -> Map<Range<usize>, fn(usize) -> CommitId> {
        unimplemented!()
    }

    fn user_ids(&self) -> Map<Range<usize>, fn(usize) -> UserId> {
        unimplemented!()
    }

    fn path_ids(&self) -> Map<Range<usize>, fn(usize) -> PathId> {
        unimplemented!()
    }

    fn projects(&self) -> EntityIter<ProjectId, Project> {
        unimplemented!()
    }

    fn commits(&self) -> EntityIter<CommitId, Commit> {
        unimplemented!()
    }

    fn users(&self) -> EntityIter<UserId, User> {
        unimplemented!()
    }

    fn paths(&self) -> EntityIter<PathId, Path> {
        unimplemented!()
    }

    fn commits_from(&self, project: &ProjectId) -> ProjectEntityIter<Commit> {
        unimplemented!()
    }

    fn paths_from(&self, project: &ProjectId) -> ProjectEntityIter<Path> {
        unimplemented!()
    }

    fn users_from(&self, project: &ProjectId) -> ProjectEntityIter<User> {
        unimplemented!()
    }

    fn authors_from(&self, project: &ProjectId) -> ProjectEntityIter<User> {
        unimplemented!()
    }

    fn committers_from(&self, project: &ProjectId) -> ProjectEntityIter<User> {
        unimplemented!()
    }

    fn commit_count_from(&self, project: &ProjectId) -> usize {
        unimplemented!()
    }

    fn path_count_from(&self, project: &ProjectId) -> usize {
        unimplemented!()
    }

    fn user_count_from(&self, project: &ProjectId) -> usize {
        unimplemented!()
    }

    fn author_count_from(&self, project: &ProjectId) -> usize {
        unimplemented!()
    }

    fn committer_count_from(&self, project: &ProjectId) -> usize {
        unimplemented!()
    }

    fn age_of(&self, project: &ProjectId) -> Option<Duration> {
        unimplemented!()
    }

    fn seed(&self) -> u128 {
        unimplemented!()
    }
}

// impl DataSource for Djanco {
//     fn project_count(&self) -> usize { unimplemented!() }
//     fn commit_count(&self)  -> usize { self.warehouse.num_commits()    as usize }
//     fn user_count(&self)    -> usize { self.warehouse.num_users()      as usize }
//     fn path_count(&self)    -> usize { self.warehouse.num_file_paths() as usize }
//
//     fn project(&self, id: ProjectId)    -> Option<Project>  { self.warehouse.get_project(id.into())     }
//     fn commit(&self, id: CommitId)      -> Option<Commit>   { self.warehouse.get_commit(id.into())      }
//     fn user(&self, id: UserId)          -> Option<User>     { self.warehouse.get_user(id.into()).map(|u| u.clone()) }
//     fn path(&self, id: PathId)          -> Option<Path>     { self.warehouse.get_file_path(id.into())   }
//
//     fn project_ids(&self) -> Map<Range<usize>, fn(usize) -> ProjectId> {
//         (0..self.project_count()).map(|e| ProjectId::from(e))
//     }
//     fn commit_ids(&self) -> Map<Range<usize>, fn(usize) -> CommitId> {
//         (0..self.commit_count()).map(|e| CommitId::from(e))
//     }
//     fn user_ids(&self) -> Map<Range<usize>, fn(usize) -> UserId> {
//         (0..self.user_count()).map(|e| UserId::from(e))
//     }
//     fn path_ids(&self) -> Map<Range<usize>, fn(usize) -> PathId> {
//         (0..self.path_count()).map(|e| PathId::from(e))
//     }
//
//     fn projects(&self) -> EntityIter<ProjectId, Project> {
//         EntityIter::from(self.me(), self.project_ids())
//     }
//     fn commits(&self) -> EntityIter<CommitId, Commit> {
//         EntityIter::from(self.me(), self.commit_ids())
//     }
//     fn users(&self) -> EntityIter<UserId, User> {
//         EntityIter::from(self.me(), self.user_ids())
//     }
//     fn paths(&self) -> EntityIter<PathId, Path> {
//         EntityIter::from(self.me(), self.path_ids())
//     }
//
//     fn commits_from(&self, project: &Project) -> ProjectEntityIter<Commit> {
//         ProjectEntityIter::<Commit>::from(self.me(), &project)
//     }
//     fn paths_from(&self, project: &Project) -> ProjectEntityIter<Path> {
//         ProjectEntityIter::<Path>::from(self.me(), &project)
//     }
//     fn users_from(&self, project: &Project) -> ProjectEntityIter<User> {
//         ProjectEntityIter::<User>::from(self.me(), &project)
//     }
//     fn authors_from(&self, project: &Project) -> ProjectEntityIter<User> {
//         ProjectEntityIter::<User>::from(self.me(), &project).and_skip_committers()
//     }
//     fn committers_from(&self, project: &Project) -> ProjectEntityIter<User> {
//         ProjectEntityIter::<User>::from(self.me(), &project).and_skip_authors()
//     }
//
//
//     fn commit_count_from(&self, project: &Project) -> usize {
//         self.bare_commits_from(project).count()
//     }
//     fn path_count_from(&self, project: &Project) -> usize {
//         self.paths_from(project).count()
//     }
//     fn user_count_from(&self, project: &Project) -> usize {
//         self.users_from(project).count()
//     }
//     fn author_count_from(&self, project: &Project) -> usize {
//         self.authors_from(project).count()
//     }
//     fn committer_count_from(&self, project: &Project) -> usize {
//         self.committers_from(project).count()
//     }
//
//     fn age_of(&self, project: &Project) -> Option<Duration> {
//         let minmax = self.commits_from(project)
//             .minmax_by(|c1, c2| {
//                 if c1.author_time < c2.author_time { return Ordering::Less }
//                 if c1.author_time > c2.author_time { return Ordering::Greater }
//                 return Ordering::Equal
//             });
//         match minmax {
//             MinMaxResult::NoElements => None,
//             MinMaxResult::OneElement(_commit) => None,
//             MinMaxResult::MinMax(first_commit, last_commit) => {
//                 assert!(last_commit.author_time > first_commit.author_time);
//                 let elapsed_seconds: u64 =
//                     (last_commit.author_time - first_commit.author_time) as u64;
//                 Some(Duration::from_secs(elapsed_seconds))
//             }
//         }
//     }
//
//     fn seed(&self) -> u128 {
//         self.seed
//     }
// }

pub trait WithDatabase { fn get_database_ptr(&self) -> DatabasePtr; }
impl WithDatabase for Djanco { fn get_database_ptr(&self) -> DatabasePtr { self.me() } }

pub struct ProjectEntityIter<T> {
    database: DatabasePtr,
    visited_commits: HashSet<u64>,
    to_visit_commits: VecDeque<u64>,

    authors: bool,
    committers: bool,

    seen_entities: HashSet<u64>,
    entity_cache: VecDeque<u64>,

    _entity: PhantomData<T>,
    desired_cache_size: usize,
}

impl<T> ProjectEntityIter<T> {
    pub fn from(database: DatabasePtr, project: &Project) -> ProjectEntityIter<T> {
        let visited_commits: HashSet<u64> = HashSet::new();
        let to_visit_commits: VecDeque<u64> =
            project.heads.iter().map(|(_, id)| id.into()).collect();

        ProjectEntityIter {
            visited_commits, to_visit_commits, database,
            committers: true, authors: true,
            _entity: PhantomData, desired_cache_size: 100,
            entity_cache: VecDeque::new(), seen_entities: HashSet::new(),
        }
    }

    pub fn and_skip_committers(self) -> Self {
        ProjectEntityIter {
            visited_commits: self.visited_commits,
            to_visit_commits: self.to_visit_commits,
            database: self.database,
            _entity: PhantomData,
            committers: false,
            authors: self.authors,
            desired_cache_size: self.desired_cache_size,
            entity_cache: self.entity_cache,
            seen_entities: self.seen_entities,
        }
    }

    pub fn and_skip_authors(self) -> Self {
        ProjectEntityIter {
            visited_commits: self.visited_commits,
            to_visit_commits: self.to_visit_commits,
            database: self.database,
            _entity: PhantomData,
            committers: self.committers,
            authors: false,
            desired_cache_size: self.desired_cache_size,
            entity_cache: self.entity_cache,
            seen_entities: self.seen_entities,
        }
    }

    pub fn next_commit(&mut self) -> Option<Commit> {
        unimplemented!()
        // loop {
        //     if self.to_visit_commits.is_empty() {
        //         return None;
        //     }
        //     let commit_id = self.to_visit_commits.pop_back().unwrap();
        //     if ! self.visited_commits.insert(commit_id) {
        //         continue;
        //     }
        //     let commit = self.database.commit(CommitId::from(commit_id)).unwrap();
        //     self.to_visit_commits.extend(commit.parents.iter());
        //     return Some(commit);
        // }
    }

    fn next_id_from_cache(&mut self) -> Option<u64> {
        self.entity_cache.pop_front()
    }
}

impl ProjectEntityIter<User> {
    fn populate_cache(&mut self) -> bool {
        assert!(self.authors || self.committers);
        loop {
            return match self.next_commit() {
                Some(commit) => {
                    if self.authors {
                        if self.seen_entities.insert(commit.author.into()) {
                            self.entity_cache.push_back(commit.author.into()); // User not yet seen.
                        }
                    }

                    if self.committers {
                        if self.seen_entities.insert(commit.committer.into()) {
                            self.entity_cache.push_back(commit.committer.into()); // User not yet seen.
                        }
                    }

                    if self.entity_cache.len() < self.desired_cache_size {
                        continue;
                    }

                    true
                },
                None => self.entity_cache.len() != 0
            }
        }
    }
}

impl ProjectEntityIter<Path> {
    fn populate_cache(&mut self) -> bool {
        loop {
            //return match self.next_commit() {
                unimplemented!()
                // Some(commit) => {
                //     let changes: Vec<u64> =
                //         commit.changes.map_or(vec![],
                //             |map| {
                //                 map.iter()
                //                     .map(|(path_id, _)| *path_id)
                //                     .filter(|path_id| {
                //                         !self.seen_entities.contains(path_id)
                //                     })
                //                     .collect()
                //             });
                //
                //     self.seen_entities.extend(changes);
                //
                //     if self.entity_cache.len() < self.desired_cache_size {
                //         continue;
                //     }
                //
                //     true
                // },
                // None => self.entity_cache.len() != 0
            //}
        }
    }
}

impl Iterator for ProjectEntityIter<Commit> {
    type Item = Commit;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_commit()
    }
}

impl Iterator for ProjectEntityIter<User> {
    type Item = User;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let id_opt =
                self.next_id_from_cache();

            if let Some(id) = id_opt {
                return self.database.user(&UserId::from(id)).map(|e| e.clone())
            }

            if !self.populate_cache() {
                return None
            }
        }
    }
}

impl Iterator for ProjectEntityIter<Path> {
    type Item = Path;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let id_opt =
                self.next_id_from_cache();

            if let Some(id) = id_opt {
                return self.database.path(&PathId::from(id)).map(|e| e.clone())
            }

            if !self.populate_cache() {
                return None
            }
        }
    }
}

pub struct EntityIter<TI: From<usize> + Into<u64>, T> {
    database: DatabasePtr,
    ids: Box<dyn Iterator<Item=TI>>,
    _entity: PhantomData<T>,
}

impl<TI, T> EntityIter<TI, T> where TI: From<usize> + Into<u64> {
    pub fn from(database: DatabasePtr, ids: impl Iterator<Item=TI> + 'static) -> EntityIter<TI, T> {
        EntityIter { ids: Box::new(ids), database, _entity: PhantomData }
    }
}

impl Iterator for EntityIter<ProjectId, Project> {
    type Item = Project;
    fn next(&mut self) -> Option<Self::Item> {
        self.ids.next().map(|id| self.database.project(&id.into()).map(|e| e.clone())).flatten()
    }
}

impl Iterator for EntityIter<CommitId, Commit> { // FIXME also bare commit
    type Item = Commit;
    fn next(&mut self) -> Option<Self::Item> {
        self.ids.next().map(|id| self.database.commit(&id.into()).map(|e| e.clone())).flatten()
    }
}

macro_rules! untangle { ($self:expr) => {{ let db: &RefCell<Djanco> = $self.borrow(); db.borrow() }} }

impl DataSource for DatabasePtr {
    fn project_count(&self) -> usize { untangle!(self).path_count()   }
    fn commit_count(&self)  -> usize { untangle!(self).commit_count() }
    fn user_count(&self)    -> usize { untangle!(self).user_count()   }
    fn path_count(&self)    -> usize { untangle!(self).path_count()   }

    fn project(&self, id: &ProjectId) -> Option<&Project> {
        unimplemented!()
    }

    fn commit(&self, id: &CommitId) -> Option<&Commit> {
        unimplemented!()
    }

    fn user(&self, id: &UserId) -> Option<&User> {
        unimplemented!()
    }

    fn path(&self, id: &PathId) -> Option<&Path> {
        unimplemented!()
    }

    fn project_ids(&self) -> Map<Range<usize>, fn(usize) -> ProjectId> {
        unimplemented!()
    }

    fn commit_ids(&self) -> Map<Range<usize>, fn(usize) -> CommitId> {
        unimplemented!()
    }

    fn user_ids(&self) -> Map<Range<usize>, fn(usize) -> UserId> {
        unimplemented!()
    }

    fn path_ids(&self) -> Map<Range<usize>, fn(usize) -> PathId> {
        unimplemented!()
    }

    fn projects(&self) -> EntityIter<ProjectId, Project> {
        unimplemented!()
    }

    fn commits(&self) -> EntityIter<CommitId, Commit> {
        unimplemented!()
    }

    fn users(&self) -> EntityIter<UserId, User> {
        unimplemented!()
    }

    fn paths(&self) -> EntityIter<PathId, Path> {
        unimplemented!()
    }

    fn commits_from(&self, project: &ProjectId) -> ProjectEntityIter<Commit> {
        unimplemented!()
    }

    fn paths_from(&self, project: &ProjectId) -> ProjectEntityIter<Path> {
        unimplemented!()
    }

    fn users_from(&self, project: &ProjectId) -> ProjectEntityIter<User> {
        unimplemented!()
    }

    fn authors_from(&self, project: &ProjectId) -> ProjectEntityIter<User> {
        unimplemented!()
    }

    fn committers_from(&self, project: &ProjectId) -> ProjectEntityIter<User> {
        unimplemented!()
    }

    fn commit_count_from(&self, project: &ProjectId) -> usize {
        unimplemented!()
    }

    fn path_count_from(&self, project: &ProjectId) -> usize {
        unimplemented!()
    }

    fn user_count_from(&self, project: &ProjectId) -> usize {
        unimplemented!()
    }

    fn author_count_from(&self, project: &ProjectId) -> usize {
        unimplemented!()
    }

    fn committer_count_from(&self, project: &ProjectId) -> usize {
        unimplemented!()
    }

    fn age_of(&self, project: &ProjectId) -> Option<Duration> {
        unimplemented!()
    }

    fn seed(&self) -> u128 {
        unimplemented!()
    }

    // fn project(&self, id: &ProjectId)    -> Option<&Project> { untangle!(self).project(id) }
    // fn commit(&self, id: &CommitId)      -> Option<&Commit>  { untangle!(self).commit(id)  }
    // fn user(&self, id: &UserId)          -> Option<&User>    { untangle!(self).user(id)    }
    // fn path(&self, id: &PathId)          -> Option<&Path>    { untangle!(self).path(id)    }
    //
    // fn project_ids(&self) -> Map<Range<usize>, fn(usize)->ProjectId> { untangle!(self).project_ids() }
    // fn commit_ids(&self)  -> Map<Range<usize>, fn(usize)->CommitId>  { untangle!(self).commit_ids()  }
    // fn user_ids(&self)    -> Map<Range<usize>, fn(usize)->UserId>    { untangle!(self).user_ids()    }
    // fn path_ids(&self)    -> Map<Range<usize>, fn(usize)->PathId>    { untangle!(self).path_ids()    }
    //
    // fn projects(&self)     -> EntityIter<ProjectId, Project> { untangle!(self).projects()     }
    // fn commits(&self)      -> EntityIter<CommitId, Commit>   { untangle!(self).commits()      }
    // fn users(&self)        -> EntityIter<UserId, User>       { untangle!(self).users()        }
    // fn paths(&self)        -> EntityIter<PathId, Path>   { untangle!(self).paths()        }
    //
    // fn commits_from(&self, project: &ProjectId) -> ProjectEntityIter<Commit> {
    //     untangle!(self).commits_from(project)
    // }
    // fn paths_from(&self, project: &ProjectId) -> ProjectEntityIter<Path> {
    //     untangle!(self).paths_from(project)
    // }
    // fn users_from(&self, project: &ProjectId) -> ProjectEntityIter<User> {
    //     untangle!(self).users_from(project)
    // }
    // fn authors_from(&self, project: &ProjectId) -> ProjectEntityIter<User> {
    //     untangle!(self).authors_from(project)
    // }
    // fn committers_from(&self, project: &ProjectId) -> ProjectEntityIter<User> {
    //     untangle!(self).committers_from(project)
    // }
    //
    // fn commit_count_from(&self, project: &ProjectId)    -> usize { untangle!(self).commit_count_from(project)    }
    // fn path_count_from(&self, project: &ProjectId)      -> usize { untangle!(self).path_count_from(project)      }
    // fn user_count_from(&self, project: &ProjectId)      -> usize { untangle!(self).user_count_from(project)      }
    // fn author_count_from(&self, project: &ProjectId)    -> usize { untangle!(self).author_count_from(project)    }
    // fn committer_count_from(&self, project: &ProjectId) -> usize { untangle!(self).committer_count_from(project) }
    //
    // fn age_of(&self, project: &ProjectId) -> Option<Duration> { untangle!(self).age_of(project) }
    //
    // fn seed(&self) -> u128 { untangle!(self).seed() }
}

impl Iterator for EntityIter<UserId, User> {
    type Item = User;
    fn next(&mut self) -> Option<Self::Item> {
        self.ids.next().map(move |id| self.database.clone().user(&id).map(|e| e.clone())).flatten()
    }
}

impl Iterator for EntityIter<PathId, Path> {
    type Item = Path;
    fn next(&mut self) -> Option<Self::Item> {
        // XXX helpfulstuff
        // let db: Rc<RefCell<Dejaco>> = self.database.clone();
        // self.ids.next().map(move |id| {
        //     let x = (*(self.database.clone())).borrow();
        //     x.path(id.into())
        // }).flatten()
        self.ids.next().map(move |id| self.database.clone().path(&id).map(|e| e.clone())).flatten()
    }
}

impl<TI, T> WithDatabase for EntityIter<TI, T> where TI: From<usize> + Into<u64> {
    fn get_database_ptr(&self) -> DatabasePtr { self.database.clone() }
}

// Project Attributes
// pub enum Attrib {
//     Language,
//     Stars,
//     Commits,
//     Users,
// }
//
// pub trait RequireOperand {}
// impl RequireOperand for Attrib {}
// impl RequireOperand for Stats  {}
//
// pub trait StatsOperand {}
// impl StatsOperand for Attrib {}
//
// pub enum Stats {
//     Count(),
//     Mean(),
//     Median(),
// }

pub trait Attribute {}

pub struct AttributeValue<A: Attribute, T> {
    value: T,
    attribute_type: PhantomData<A>,
}

impl<A, T> AttributeValue<A, T> where A: Attribute {
    pub fn new(_attribute: &A, value: T) -> AttributeValue<A, T> {
        AttributeValue { value, attribute_type: PhantomData }
    }
}

pub trait LoadFilter {
    fn filter(&self, database: &DCD, /*key: &Self::Key,*/ project_id: &dcd::ProjectId, commit_ids: &Vec<dcd::CommitId>) -> bool;
}

pub trait Group {
    type Key;
    fn select(&self, project: &Project) -> Self::Key;
}

pub trait SortEach {
    /*type Key;*/ // TODO
    fn sort(&self, database: DatabasePtr, /*key: &Self::Key,*/ projects: &mut Vec<Project>);
}

pub trait FilterEach {
    /*type Key;*/ // TODO
    fn filter(&self, database: DatabasePtr, /*key: &Self::Key,*/ project: &Project) -> bool;
}

pub trait SampleEach {
    /*type Key;*/ // TODO
    fn sample(&self, database: DatabasePtr, /*key: &Self::Key,*/ projects: Vec<Project>) -> Vec<Project>;
}



pub trait SelectEach: WithNames {
    type Entity;
    fn select(&self, database: DatabasePtr, /*key: &Self::Key,*/ project: Project) -> Self::Entity;
}

pub trait NumericalAttribute {
    type Entity;
    fn calculate(&self, database: DatabasePtr, entity: &Self::Entity) -> usize;
}

pub trait CollectionAttribute {
    type Entity;
    //fn calculate(&self, database: DatabasePtr, entity: &Self::Entity) -> usize;
}

pub trait StringAttribute {
    type Entity;
    fn extract(&self, database: DatabasePtr, entity: &Self::Entity) -> String;
}

pub mod sample {
    use crate::{DatabasePtr, SampleEach, DataSource};
    use rand::seq::IteratorRandom;
    use rand_pcg::Pcg64Mcg;
    use rand::SeedableRng;
    use crate::objects::Project;

    #[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Top(pub usize);
    #[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Unique<D>(pub usize, D);
    #[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Random(pub usize);

    impl SampleEach for Top {
        fn sample(&self, _database: DatabasePtr, /*key: &Self::Key,*/ projects: Vec<Project>) -> Vec<Project> {
            projects.into_iter().take(self.0).collect()
        }
    }

    impl SampleEach for Random {
        fn sample(&self, database: DatabasePtr, /*key: &Self::Key,*/ projects: Vec<Project>) -> Vec<Project> {
            let mut rng = Pcg64Mcg::from_seed(database.seed().to_be_bytes());
            projects.into_iter().choose_multiple(&mut rng, self.0)
        }
    }
}

pub mod require {
    use crate::{DatabasePtr, FilterEach, NumericalAttribute, StringAttribute};
    use regex::Regex;
    use crate::objects::Project;

    #[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct AtLeast<N>(pub N, pub usize);
    #[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Exactly<N>(pub N, pub usize);
    #[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct AtMost<N> (pub N, pub usize);

    impl<N> FilterEach for AtLeast<N> where N: NumericalAttribute<Entity=Project> {
        fn filter(&self, database: DatabasePtr, project: &Project) -> bool {
            self.0.calculate(database, project) >= self.1
        }
    }

    impl<N> FilterEach for Exactly<N> where N: NumericalAttribute<Entity=Project> {
        fn filter(&self, database: DatabasePtr, project: &Project) -> bool {
            self.0.calculate(database, project) == self.1
        }
    }

    impl<N> FilterEach for AtMost<N> where N: NumericalAttribute<Entity=Project> {
        fn filter(&self, database: DatabasePtr, project: &Project) -> bool {
            self.0.calculate(database, project) <= self.1
        }
    }

    #[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Same<'a, S>(pub S, pub &'a str);
    #[derive(Clone,                          )] pub struct Matches<S>(pub S, pub Regex);

    #[macro_export] macro_rules! regex { ($str:expr) => { regex::Regex::new($str).unwrap() }}

    impl<'a, S> FilterEach for Same<'a, S> where S: StringAttribute<Entity=Project> {
        fn filter(&self, database: DatabasePtr, project: &Project) -> bool {
            self.0.extract(database, project) == self.1.to_string()
        }
    }

    impl<S> FilterEach for Matches<S> where S: StringAttribute<Entity=Project> {
        fn filter(&self, database: DatabasePtr, project: &Project) -> bool {
            self.1.is_match(&self.0.extract(database, project))
        }
    }
}

pub mod project {
    use crate::{Attribute, Group, NumericalAttribute, StringAttribute, SortEach, SelectEach, AttributeValue};
    use crate::{ProjectId, DatabasePtr, DataSource};
    use crate::objects::Project;

    #[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Id;
    #[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct URL;

    #[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Language;
    #[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Stars;
    #[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Issues;
    #[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct BuggyIssues;

    #[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Heads;
    #[derive(Eq, PartialEq,       Clone, Hash)] pub struct Metadata(pub String);

    #[derive(Eq, PartialEq,       Clone, Hash)] pub struct Commits;
    #[derive(Eq, PartialEq,       Clone, Hash)] pub struct Users;
    #[derive(Eq, PartialEq,       Clone, Hash)] pub struct Paths;

    impl Attribute for Id          {}
    impl Attribute for URL         {}

    impl Attribute for Language    {}
    impl Attribute for Stars       {}
    impl Attribute for Issues      {}
    impl Attribute for BuggyIssues {}

    impl Attribute for Heads       {}
    impl Attribute for Metadata    {}

    impl Attribute for Commits     {}
    impl Attribute for Users       {}
    impl Attribute for Paths       {}


    impl StringAttribute for Id {
        type Entity = Project;
        fn extract(&self, _database: DatabasePtr, entity: &Self::Entity) -> String {
            entity.id.to_string()
        }
    }

    impl StringAttribute for URL {
        type Entity = Project;
        fn extract(&self, _database: DatabasePtr, entity: &Self::Entity) -> String {
            entity.url.clone()
        }
    }

    impl StringAttribute for Language {
        type Entity = Project;
        fn extract(&self, _database: DatabasePtr, entity: &Self::Entity) -> String {
            entity.language_or_empty()
        }
    }

    impl StringAttribute for Stars {
        type Entity = Project;
        fn extract(&self, _database: DatabasePtr, entity: &Self::Entity) -> String {
            entity.stars.map_or(String::new(), |e| e.to_string())
        }
    }

    impl StringAttribute for Issues {
        type Entity = Project;
        fn extract(&self, _database: DatabasePtr, entity: &Self::Entity) -> String {
            entity.issues.map_or(String::new(), |e| e.to_string())
        }
    }

    impl StringAttribute for BuggyIssues {
        type Entity = Project;
        fn extract(&self, _database: DatabasePtr, entity: &Self::Entity) -> String {
            entity.buggy_issues.map_or(String::new(), |e| e.to_string())
        }
    }

    impl NumericalAttribute for Id {
        type Entity = Project;
        fn calculate(&self, _database: DatabasePtr, entity: &Self::Entity) -> usize {
            entity.id.into()
        }
    }

    impl NumericalAttribute for Stars {
        type Entity = Project;
        fn calculate(&self, _database: DatabasePtr, entity: &Self::Entity) -> usize {
            entity.stars.map_or(0usize, |n| n as usize)
        }
    }

    impl NumericalAttribute for Issues {
        type Entity = Project;
        fn calculate(&self, _database: DatabasePtr, entity: &Self::Entity) -> usize {
            entity.issues.map_or(0usize, |n| n as usize)
        }
    }

    impl NumericalAttribute for BuggyIssues {
        type Entity = Project;
        fn calculate(&self, _database: DatabasePtr, entity: &Self::Entity) -> usize {
            entity.buggy_issues.map_or(0usize, |n| n as usize)
        }
    }

    impl NumericalAttribute for Heads {
        type Entity = Project;
        fn calculate(&self, _database: DatabasePtr, entity: &Self::Entity) -> usize {
            entity.heads.len()
        }
    }

    impl NumericalAttribute for Metadata {
        type Entity = Project;
        fn calculate(&self, _database: DatabasePtr, entity: &Self::Entity) -> usize {
            entity.metadata.len()
        }
    }

    impl NumericalAttribute for Commits {
        type Entity = Project;
        fn calculate(&self, database: DatabasePtr, entity: &Self::Entity) -> usize {
            database.commit_count_from(&entity.id)
        }
    }

    impl NumericalAttribute for Users {
        type Entity = Project;
        fn calculate(&self, database: DatabasePtr, entity: &Self::Entity) -> usize {
            database.user_count_from(&entity.id)
        }
    }

    impl NumericalAttribute for Paths {
        type Entity = Project;
        fn calculate(&self, database: DatabasePtr, entity: &Self::Entity) -> usize {
            database.path_count_from(&entity.id)
        }
    }

    impl Group for Id {
        type Key = ProjectId;
        fn select(&self, project: &Project) -> Self::Key {
            project.id
        }
    }

    impl Group for Language {
        type Key = String;
        fn select(&self, project: &Project) -> Self::Key {
            project.language_or_empty()
        }
    }

    impl Group for Stars {
        type Key = usize;
        fn select(&self, project: &Project) -> Self::Key {
            project.stars_or_zero()
        }
    }

    impl Group for Issues {
        type Key = usize;
        fn select(&self, project: &Project) -> Self::Key {
            project.issues_or_zero()
        }
    }

    impl Group for BuggyIssues {
        type Key = usize;
        fn select(&self, project: &Project) -> Self::Key {
            project.buggy_issues_or_zero()
        }
    }

    impl SortEach for Id {
        fn sort(&self, _database: DatabasePtr, projects: &mut Vec<Project>) {
            projects.sort_by_key(|p| p.id)
        }
    }

    impl SortEach for URL {
        fn sort(&self, _database: DatabasePtr, projects: &mut Vec<Project>) {
            projects.sort_by(|p1, p2| p1.url.cmp(&p2.url))
        }
    }

    impl SortEach for Language {
        fn sort(&self, _database: DatabasePtr, projects: &mut Vec<Project>) {
            projects.sort_by_key(|p| p.language.clone())
        }
    }

    impl SortEach for Stars {
        fn sort(&self, _database: DatabasePtr, projects: &mut Vec<Project>) {
            projects.sort_by_key(|p| p.stars)
        }
    }

    impl SortEach for Issues {
        fn sort(&self, _database: DatabasePtr, projects: &mut Vec<Project>) {
            projects.sort_by_key(|f| f.issues)
        }
    }

    impl SortEach for BuggyIssues {
        fn sort(&self, _database: DatabasePtr, projects: &mut Vec<Project>) {
            projects.sort_by_key(|p| p.buggy_issues)
        }
    }

    impl SortEach for Heads {
        fn sort(&self, _database: DatabasePtr, projects: &mut Vec<Project>) {
            projects.sort_by_key(|p| p.heads.len())
        }
    }

    impl SortEach for Metadata {
        fn sort(&self, _database: DatabasePtr, projects: &mut Vec<Project>) {
            projects.sort_by(|p1, p2| {
                p1.metadata.get(&self.0).cmp(&p2.metadata.get(&self.0))
            });
        }
    }

    impl SortEach for Commits {
        fn sort(&self, database: DatabasePtr, projects: &mut Vec<Project>) {
            projects.sort_by_key(|p| database.commit_count_from(&p.id))
        }
    }

    impl SortEach for Users {
        fn sort(&self, database: DatabasePtr, projects: &mut Vec<Project>) {
            projects.sort_by_key(|p| database.user_count_from(&p.id))
        }
    }

    impl SortEach for Paths {
        fn sort(&self, database: DatabasePtr, projects: &mut Vec<Project>) {
            projects.sort_by_key(|p| database.path_count_from(&p.id))
        }
    }

    impl SelectEach for Id {
        type Entity = AttributeValue<Id, ProjectId>;
        fn select(&self, _database: DatabasePtr, project: Project) -> Self::Entity {
            AttributeValue::new(self, ProjectId::from(project.id))
        }
    }

    impl SelectEach for URL {
        type Entity = AttributeValue<URL, String>;
        fn select(&self, _database: DatabasePtr, project: Project) -> Self::Entity {
            AttributeValue::new(self, project.url)
        }
    }

    impl SelectEach for Language {
        type Entity = AttributeValue<Language, Option<String>>;
        fn select(&self, _database: DatabasePtr, project: Project) -> Self::Entity {
            AttributeValue::new(self, project.language)
        }
    }

    impl SelectEach for Stars {
        type Entity = AttributeValue<Stars, Option<usize>>;
        fn select(&self, _database: DatabasePtr, project: Project) -> Self::Entity {
            AttributeValue::new(self, project.stars)
        }
    }

    impl SelectEach for Issues {
        type Entity = AttributeValue<Issues, Option<usize>>;
        fn select(&self, _database: DatabasePtr, project: Project) -> Self::Entity {
            AttributeValue::new(self, project.issues)
        }
    }

    impl SelectEach for BuggyIssues {
        type Entity = AttributeValue<BuggyIssues, Option<usize>>;
        fn select(&self, _database: DatabasePtr, project: Project) -> Self::Entity {
            AttributeValue::new(self, project.buggy_issues)
        }
    }

    impl SelectEach for Heads {
        type Entity = AttributeValue<Heads, usize>;
        fn select(&self, _database: DatabasePtr, project: Project) -> Self::Entity {
            AttributeValue::new(self, project.heads.len())
        }
    }

    impl SelectEach for Metadata {
        //type Entity = AttributeValue<Metadata, Option<String>>;
        type Entity = Option<String>;
        fn select(&self, _database: DatabasePtr, project: Project) -> Self::Entity {
            //AttributeValue::new(self, project.metadata.get(&self.0).map(|s| s.clone()))
            project.metadata.get(&self.0).map(|s| s.clone())
        }
    }

    impl SelectEach for Commits {
        type Entity = AttributeValue<Commits, usize>;
        fn select(&self, database: DatabasePtr, project: Project) -> Self::Entity {
            AttributeValue::new(self, database.commit_count_from(&project.id))
        }
    }

    impl SelectEach for Users {
        type Entity = AttributeValue<Users, usize>;
        fn select(&self, database: DatabasePtr, project: Project) -> Self::Entity {
            AttributeValue::new(self, database.user_count_from(&project.id))
        }
    }

    impl SelectEach for Paths {
        type Entity = AttributeValue<Paths, usize>;
        fn select(&self, database: DatabasePtr, project: Project) -> Self::Entity {
            AttributeValue::new(self, database.path_count_from(&project.id))
        }
    }
}

trait ProjectGroup {
    fn group_by_attrib<TK>(self, attrib: impl Group<Key=TK>) -> GroupIter<Project, TK> // FIXME can I make this &self?
        where TK: PartialEq + Eq + Hash;
}

impl ProjectGroup for EntityIter<ProjectId, Project> {
    fn group_by_attrib<TK>(self, attrib: impl Group<Key=TK>) -> GroupIter<Project, TK>
        where TK: PartialEq + Eq + Hash {

        let names: Vec<String> =
            <Project as csv::WithStaticNames>::names().into_iter().map(|e| e.to_owned()).collect();

        GroupIter::from(self.get_database_ptr(),
                        self.map(|p: Project| { (attrib.select(&p), p) })
                            .into_group_map().into_iter().collect::<Vec<(TK, Vec<Project>)>>(), names) // FIXME
    }
}

/**
 * There's two thing that can happen in NormilIter. One is to sort the list of things and then
 * return as you go. The other is to pre-group into a map and then yield from that. The second thing
 * happens because there's only so much time I can spend wrangling lifetimes.
 */
pub struct NormilIter<T> {
    database: DatabasePtr,
    data: Vec<T>, // TODO There's gotta be a better way to do this.
    entity_type: PhantomData<T>,
    names: Vec<String>,
}

impl<T> NormilIter<T> {
    pub fn from(database: DatabasePtr, data: impl Into<Vec<T>>, names: Vec<String>) -> NormilIter<T> {
        NormilIter {
            database,
            data: data.into(),
            entity_type: PhantomData,
            names,
        }
    }
}

impl<T> WithDatabase for NormilIter<T> {
    fn get_database_ptr(&self) -> DatabasePtr { self.database.clone() }
}

impl<T> WithNames for NormilIter<T> {
    fn names(&self) -> Vec<String> { self.names.clone() }
}

impl<T> Iterator for NormilIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> { self.data.pop() }
}

/**
 * There's two thing that can happen in GroupIter. One is to sort the list of things and then
 * return as you go. The other is to pre-group into a map and then yield from that. The second thing
 * happens because there's only so much time I can spend wrangling lifetimes (for now).
 */
pub struct GroupIter<T, TK: PartialEq + Eq + Hash> {
    database: DatabasePtr,
    map: Vec<(TK, Vec<T>)>,
    entity_type: PhantomData<T>,
    key_type: PhantomData<TK>,
    names: Vec::<String>,
}

impl<T, TK> GroupIter<T, TK> where TK: PartialEq + Eq + Hash {
    pub fn from(database: DatabasePtr, data: impl Into<Vec<(TK, Vec<T>)>>, names: Vec::<String>) -> GroupIter<T, TK> {
        GroupIter {
            database,
            map: data.into(),
            entity_type: PhantomData,
            key_type: PhantomData,
            names,
        }
    }
}

impl<T, TK> WithDatabase for GroupIter<T, TK> where TK: PartialEq + Eq + Hash {
    fn get_database_ptr(&self) -> DatabasePtr { self.database.clone() }
}

impl<TK, T> Iterator for GroupIter<T, TK> where TK: PartialEq + Eq + Hash {
    type Item = (TK, Vec<T>);
    fn next(&mut self) -> Option<Self::Item> { self.map.pop() }
}

impl<T, TK> WithNames for GroupIter<T, TK> where TK: PartialEq + Eq + Hash {
    fn names(&self) -> Vec<String> { self.names.clone() }
}

trait Ops {
    fn filter_by_attrib(self, attrib: impl FilterEach + Clone) -> NormilIter<Project>; // tombstones?
    fn sort_by_attrib(self, attrib: impl SortEach + Clone) -> NormilIter<Project>;
    fn sample_by(self, attrib: impl SampleEach + Clone) -> NormilIter<Project>;
    fn select<IntoT>(self, attrib: impl SelectEach<Entity=IntoT> + Clone) -> NormilIter<IntoT>;
}

impl Ops for NormilIter<Project> {
    fn filter_by_attrib(self, attrib: impl FilterEach + Clone) -> NormilIter<Project> {
        let database = self.get_database_ptr();
        let inherited_database = self.get_database_ptr();
        let names = self.names();
        let iterator= self.into_iter().filter(|p| {
            let database = database.clone();
            // FIXME giving up on laziness for now
            attrib.filter(database, /*&key,*/ p)
        });
        NormilIter::from(inherited_database, iterator.collect::<Vec<Project>>(), names)
    }

    fn sort_by_attrib(mut self, attrib: impl SortEach + Clone) -> NormilIter<Project> {
        let database = self.get_database_ptr();
        let inherited_database = self.get_database_ptr();
        let names = self.names();
        attrib.sort(database, &mut self.data);
        NormilIter::from(inherited_database, self.data, names)
    }

    fn sample_by(self, attrib: impl SampleEach + Clone) -> NormilIter<Project> {
        let database = self.get_database_ptr();
        let inherited_database = self.get_database_ptr();
        let names = self.names();
        let sample = attrib.sample(database, self.data);
        NormilIter::from(inherited_database,sample, names)
    }

    fn select<IntoT>(self, attrib: impl SelectEach<Entity=IntoT> + Clone) -> NormilIter<IntoT> {
        let database = self.get_database_ptr();
        let inherited_database = self.get_database_ptr();
        let iterator =
            self.data.into_iter().map(|p| attrib.select(database.clone(), p));
        NormilIter::from(inherited_database, iterator.collect::<Vec<IntoT>>(), attrib.names())
    }
}

trait GroupOps<TK> where TK: PartialEq + Eq + Hash {
    fn filter_each_by_attrib(self, attrib: impl FilterEach + Clone) -> GroupIter<Project, TK>;
    fn sort_each_by_attrib(self, attrib: impl SortEach + Clone) -> GroupIter<Project, TK>;
    fn sample_each(self, attrib: impl SampleEach + Clone) -> GroupIter<Project, TK>;
    fn select_each<IntoT>(self, attrib: impl SelectEach<Entity=IntoT> + Clone) -> GroupIter<IntoT, TK>;
    fn drop_key(self) -> Map<GroupIter<Project, TK>, fn((TK, Vec<Project>)) -> Vec<Project>>;
    fn squash(self) -> NormilIter<Project>;
}

impl<TK> GroupOps<TK> for GroupIter<Project, TK> where TK: PartialEq + Eq + Hash + Clone {
    fn filter_each_by_attrib(self, attrib: impl FilterEach + Clone) -> GroupIter<Project, TK> {
        let database = self.get_database_ptr();
        let inherited_database = self.get_database_ptr();
        let names = self.names();
        let iterator= self.into_iter()
            .map(|(key, projects)| {
                let database = database.clone();
                (key.clone(), projects.into_iter().filter(|p| {
                    let database = database.clone();
                    // FIXME giving up on laziness for now
                    attrib.filter(database, /*&key,*/ p)
                }).collect::<Vec<Project>>())
            });
        GroupIter::from(inherited_database, iterator.collect::<Vec<(TK, Vec<Project>)>>(), names)
    }

    fn sort_each_by_attrib(self, attrib: impl SortEach + Clone) -> GroupIter<Project, TK> {
        let database = self.get_database_ptr();
        let inherited_database = self.get_database_ptr();
        let names = self.names();
        let iterator = self.into_iter()
            .map(|(key, mut projects)| {
                let database = database.clone();
                attrib.sort(database, &mut projects);
                (key, projects)
            });
        GroupIter::from(inherited_database,iterator.collect::<Vec<(TK, Vec<Project>)>>(), names)
    }

    fn sample_each(self, attrib: impl SampleEach + Clone) -> GroupIter<Project, TK> {
        let database = self.get_database_ptr();
        let names = self.names();
        let inherited_database = self.get_database_ptr();
        let iterator = self.into_iter()
            .map(|(key, projects)| {
                let database = database.clone();
                (key, attrib.sample(database, projects))
            });
        GroupIter::from(inherited_database,iterator.collect::<Vec<(TK, Vec<Project>)>>(), names)
    }

    fn select_each<IntoT>(self, attrib: impl SelectEach<Entity=IntoT> + Clone) -> GroupIter<IntoT, TK> {
        let database = self.get_database_ptr();
        let inherited_database = self.get_database_ptr();
        let iterator = self.into_iter()
            .map(|(key, projects)| {
                let database = database.clone();
                (key, projects.into_iter().map(|p| attrib.select(database.clone(), p)).collect())
            });
        GroupIter::from(inherited_database,iterator.collect::<Vec<(TK, Vec<IntoT>)>>(), attrib.names())
    }

    fn drop_key(self) -> Map<GroupIter<Project, TK>, fn((TK, Vec<Project>)) -> Vec<Project>> {
        self.into_iter().map(|tupple| tupple.1)
    }

    fn squash(self) -> NormilIter<Project> {
        let inherited_database = self.get_database_ptr();
        let names = self.names();
        let iterator = self.into_iter().map(|tupple| tupple.1).flatten();
        NormilIter::from(inherited_database, iterator.collect::<Vec<Project>>(), names)
    }
}

// pub trait WithCSVFormat {
//     fn header_line(&self) -> String;
// }

// impl<I> WithCSVFormat for I where I: Iterator<Item=Project> {
//     fn header_line(&self) -> String {
//         "id,url,last_update,language,\
//          stars,issues,buggy_issues,\
//          head_count,commit_count,user_count,path_count,author_count,committer_count,\
//          age".to_owned()
//     }
// }

// impl<A,T> WithCSVFormat for NormilIter<AttributeValue<A,T>> where A: NamedAttribute, T: CSVItem {
//     fn header_line(&self) -> String { A::name() }
// }

// impl<TK> WithCSVFormat for GroupIter<Project, TK> where TK: PartialEq + Eq + Hash + CSVItem { // FIXME TK?
//     fn header_line(&self) -> String {
//         "key,\
//          id,url,last_update,language,\
//          stars,issues,buggy_issues,\
//          head_count,commit_count,user_count,path_count,author_count,committer_count,\
//          age".to_owned()
//     }
// }





// impl<T, A> CSVItem for AttributeValue<A, T> where T: CSVItem, A: NamedAttribute {
//     //fn header_line() -> String { A::name() }
//     fn to_csv_line(&self, db: DatabasePtr) -> String { self.value.to_csv_line(db) }
// }
//
// impl<T1, T2> CSVItem for (T1, T2) where T1: CSVItem, T2: CSVItem {
//     fn to_csv_line(&self, db: DatabasePtr) -> String {
//         format!("{},{}", self.0.to_csv_line(db.clone()), self.1.to_csv_line(db))
//     }
// }

struct Error {
    message: String
}

impl Error {
    pub fn new<S>(message: S) -> Self where S: Into<String> { Error { message: message.into() } }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.message) }
}

#[cfg(test)]
mod tests {
    use crate::{Djanco, DataSource, ProjectGroup, Ops, GroupOps, regex, project, require, sample, csv::*, objects::*};

    #[test]
    fn example() {
        let database = Djanco::from("/dejavuii/dejacode/dataset-tiny", 0,
                                               Month::August(2020));
        database
            .projects()
            .group_by_attrib(project::Stars)
            .filter_each_by_attrib(require::AtLeast(project::Stars, 1))
            .filter_each_by_attrib(require::AtLeast(project::Commits, 25))
            .filter_each_by_attrib(require::AtLeast(project::Users, 2))
            .filter_each_by_attrib(require::Same(project::Language, "Rust"))
            .filter_each_by_attrib(require::Matches(project::URL, regex!("^https://github.com/PRL-PRG/.*$")))
            .sort_each_by_attrib(project::Stars)
            .sample_each(sample::Top(2))
            .squash()
            .select(project::Id)
            .to_csv("projects.csv").unwrap()
    }
}