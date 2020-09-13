use std::collections::BTreeMap;
use std::time::Duration;
use std::path::PathBuf;
use std::rc::Weak;
use std::rc::Rc;
use std::cell::RefCell;
use std::fmt;

use itertools::Itertools;

use dcd::{DCD, Database};

use crate::log::LogLevel;
use crate::objects::*;
use crate::attrib::*;
use crate::djanco;

pub type DataPtr = Rc<RefCell<Data>>;

pub trait WithData { fn get_database_ptr(&self) -> DataPtr; }
impl WithData for Data { fn get_database_ptr(&self) -> DataPtr { self.me() } }

impl Clone for Box<dyn LoadFilter> {
    fn clone(&self) -> Box<dyn LoadFilter> {
        self.clone_box()
    }
}

pub struct Data {
    me: Option<Weak<RefCell<Data>>>, // Thanks for the help, Colette.
    filters: Vec<Box<dyn LoadFilter>>,

    warehouse: DCD,

    //cache_path: PathBuf, //TODO
    projects: BTreeMap<ProjectId, Project>, // TODO internal mutability + laziness
    commits:  BTreeMap<CommitId,  Commit>,
    users:    BTreeMap<UserId,    User>,
    paths:    BTreeMap<PathId,    Path>,
    //pub(crate) snapshots:    BTreeMap<SnapshotId,    Snapshot>,

    commits_from_project: BTreeMap<ProjectId, Vec<CommitId>>,
    users_from_project:   BTreeMap<ProjectId, Vec<UserId>>,
    //pub(crate) authors_from_project:   BTreeMap<ProjectId, Vec<UserId>>,
    //pub(crate) committers_from_project:   BTreeMap<ProjectId, Vec<UserId>>,
    //pub(crate) paths_from_project:   RefCell<BTreeMap<ProjectId, Vec<PathId>>>,

    paths_from_commit:   BTreeMap<CommitId, Vec<PathId>>,
    //pub(crate) snapshots_from_commit:   BTreeMap<CommitId, HashMap<PathId, SnapshotId>>,
    message_from_commit: BTreeMap<CommitId, Message>,                                // To be able to load them separately.
    //pub(crate) metadata_for_project:   RefCell<BTreeMap<ProjectId, HashMap<String, String>>>,
    // TODO age

    seed: u128,
}

impl From<djanco::Spec> for DataPtr {
    fn from(spec: djanco::Spec) -> Self {
        Data::ptr(&spec.path, &spec.timestamp, spec.seed, &spec.log_level)
    }
}

impl From<&djanco::Spec> for DataPtr {
    fn from(spec: &djanco::Spec) -> Self {
        Data::ptr(&spec.path, &spec.timestamp, spec.seed, &spec.log_level)
    }
}

impl From<djanco::Lazy> for DataPtr {
    fn from(lazy: djanco::Lazy) -> Self {
        let data_ptr = DataPtr::from(&lazy.spec);
        data_ptr.as_ref().borrow_mut().filters = lazy.filters;
        data_ptr
    }
}

impl From<&djanco::Lazy> for DataPtr {
    fn from(lazy: &djanco::Lazy) -> Self {
        let data_ptr = DataPtr::from(&lazy.spec);
        let iter =
            lazy.filters.iter().map(|f| f.clone_box());
        data_ptr.as_ref().borrow_mut().filters.extend(iter);
        data_ptr
    }
}

macro_rules! count_relationships {
    ($data:expr) => {
        $data.iter().map(|(_, v)| v.len()).sum::<usize>()
    }
}

impl Data {
    pub fn me(&self) -> DataPtr {
        self.me.as_ref().unwrap().upgrade().unwrap()
    }

    pub fn seed(&self) -> u128 { self.seed }

    pub fn ptr(warehouse_path: &PathBuf, time: &Month, seed: u128, verbosity: &LogLevel) -> DataPtr {
        let data = Self::from(warehouse_path, time, seed, verbosity);
        let pointer: DataPtr = Rc::new(RefCell::new(data));

        // Things we do to avoid unsafe.
        pointer.borrow_mut().me = Some(Rc::downgrade(&pointer));
        pointer
    }

    pub fn from(warehouse_path: &PathBuf, time: &Month, seed: u128, verbosity: &LogLevel) -> Self {
        let warehouse: DCD = DCD::new(warehouse_path.as_os_str().to_str().unwrap().to_string());
        unimplemented!()
    }

    pub fn from_(warehouse: DCD, seed: u128, verbosity: &LogLevel) -> Self {
        log_header!(verbosity, "Checking out data from warehouse"); // TODO path

        log_item!(verbosity, "loading project data");
        let projects: BTreeMap<ProjectId, Project> =
            warehouse.projects().into_iter()
                .map(|project| (ProjectId::from(project.id), Project::from(project)))
                .collect();
        log_addendum!(verbosity, format!("loaded project data for {} projects", projects.len()));

        log_item!(verbosity, "loading commit data");
        let commits: BTreeMap<CommitId, Commit> =
            warehouse.bare_commits().into_iter()
                .map(|commit| (CommitId::from(commit.id), Commit::from(commit)))
                .collect();
        log_addendum!(verbosity, format!("loaded commit data for {} commits", commits.len()));

        log_item!(verbosity, "loading user data");
        let users: BTreeMap<UserId, User> =
            warehouse.users().into_iter()
                .map(|user| (UserId::from(user.id), User::from(user)))
                .collect();
        log_addendum!(verbosity, format!("loaded user data for {} users", users.len()));

        log_item!(verbosity, "loading path data");
        let paths: BTreeMap<PathId, Path> =
            (0..warehouse.num_file_paths())
                .flat_map(|path_id| warehouse.get_file_path(path_id))
                .map(|path| (PathId::from(path.id), Path::from(path)))
                .collect();
        log_addendum!(verbosity, format!("loaded path data for {} paths", paths.len()));

        log_item!(verbosity, "loading project-commit mapping");
        let commits_from_project: BTreeMap<ProjectId, Vec<CommitId>> =
            warehouse.project_ids_and_their_commit_ids().into_iter()
                .map(|(id, commit_ids)| {
                    (ProjectId::from(id),
                     commit_ids.into_iter()
                         .map(|commit_id| CommitId::from(commit_id))
                         .collect())
                })
                .collect();
        log_item!(verbosity, format!("loaded {} relationships",
                                     count_relationships!(commits_from_project)));

        log_item!(verbosity, "loading project-user mapping");
        let users_from_project: BTreeMap<ProjectId, Vec<UserId>>  =
            commits_from_project.iter()
                .map(|(id, commit_ids)|
                    (*id,
                     commit_ids.into_iter()
                         .flat_map(|commit_id| commits.get(commit_id))
                         .flat_map(|commit| commit.users())
                         .unique()
                         .collect()))
                .collect();
        log_item!(verbosity, format!("loaded {} relationships",
                                     count_relationships!(users_from_project)));

        log_item!(verbosity, "loading commit-path mapping");
        let paths_from_commit: BTreeMap<CommitId, Vec<PathId>> =
            warehouse
                .commits()
                .map(|commit|
                    (CommitId::from(commit.id),
                     commit.changes.as_ref().map_or(vec![], |changes| {
                         changes.into_iter()
                             .map(|(path_id, _)| PathId::from(*path_id))
                             .collect()
                     })))
                .collect();
        log_item!(verbosity, format!("loaded {} relationships",
                                     count_relationships!(paths_from_commit)));

        log_item!(verbosity, "loading commit messages");
        let message_from_commit: BTreeMap<CommitId, Message> =
            warehouse.commits()
                .flat_map(|commit| {
                    commit.message.as_ref().map(|message| {
                        (CommitId::from(commit.id), Message::from(message))
                    })
                })
                .collect();
        log_item!(verbosity, format!("loaded {} messages", message_from_commit.len()));

        Data {
            me: None, warehouse, seed, filters: vec![],
            projects, commits, users, paths,
            commits_from_project, users_from_project, paths_from_commit, message_from_commit,
        }
    }

    pub fn from_filtered(warehouse: DCD, filters: Vec<Box<dyn LoadFilter>>, seed: u128, verbosity: &LogLevel) -> Self {
        log_header!(verbosity, "Checking out data from warehouse"); // TODO path

        log_item!(verbosity, format!("loading project-commit mapping with {} filter{}",
                                     filters.len(),
                                     if filters.len() > 1 {"s"} else {""} ));
        let commits_from_project: BTreeMap<ProjectId, Vec<CommitId>> =
            warehouse.project_ids_and_their_commit_ids()
                .filter(|(project_id, commit_ids)| {
                    filters.iter().all(|filter| {
                        filter.filter(&warehouse, project_id, commit_ids)
                    })
                })
                .map(|(project_id, commit_ids)| {
                    (ProjectId::from(project_id),
                     commit_ids.iter().map(|commit_id|
                         CommitId::from(*commit_id)).collect())
                })
                .collect();
        log_item!(verbosity, format!("loaded {} relationships",
                                     commits_from_project.iter().map(|(_, v)| v.len()).sum::<usize>()));

        log_item!(verbosity, "loading project data");
        let projects: BTreeMap<ProjectId, Project> =
            commits_from_project.iter()
                .flat_map(|(project_id, _)| warehouse.get_project(project_id.into()))
                .map(|project| (ProjectId::from(project.id), Project::from(project)) )
                .collect();
        log_item!(verbosity, format!("loaded {} projects", projects.len()));

        log_item!(verbosity, "loading commit ids");
        let commit_ids: Vec<CommitId> = commits_from_project.iter()
            .flat_map(|(_, commit_ids)| commit_ids)
            .unique()
            .map(|commit_id| *commit_id)
            .collect();
        log_item!(verbosity, format!("loaded {} commit ids", commit_ids.len()));

        log_item!(verbosity, "loading commit data");
        let commits: BTreeMap<CommitId, Commit> =
            commit_ids.iter()
                .flat_map(|commit_id| warehouse.get_commit_bare(commit_id.into()))
                .map(|commit| (CommitId::from(commit.id), Commit::from(commit)))
                .collect();
        log_item!(verbosity, format!("loaded {} commits", commits.len()));

        log_item!(verbosity, "loading project-user mapping");
        let users_from_project: BTreeMap<ProjectId, Vec<UserId>> =
            commits_from_project.iter()
                .map(|(project_id, commit_ids)| {
                    (*project_id,
                     commit_ids.iter().flat_map(|commit_id| {
                         commits.get(commit_id).users()
                     })
                         .unique()
                         .map(|user_id| UserId::from(user_id))
                         .collect::<Vec<UserId>>())
                })
                .collect();
        log_item!(verbosity, format!("loaded {} relationships",
                                     count_relationships!(users_from_project)));

        log_item!(verbosity, "loading user data");
        let users: BTreeMap<UserId, User> =
            commits.iter()
                .flat_map(|(_, commit)| commit.users())
                .unique()
                .flat_map(|user_id| warehouse.get_user(user_id.into()))
                .map(|user| (UserId::from(user.id), User::from(user)))
                .collect();
        log_item!(verbosity, format!("loaded {} users", users.len()));

        log_item!(verbosity, "loading commit-path mapping");
        let paths_from_commit: BTreeMap<CommitId, Vec<PathId>> =
            commit_ids.iter()
                .flat_map(|commit_id| warehouse.get_commit(commit_id.into()))
                .map(|commit| {
                    (CommitId::from(commit.id),
                     commit.changes.as_ref()
                         .map_or(Default::default(), |changes| {
                             changes.iter()
                                 .map(|(path_id, _snapshot_id)| *path_id)
                                 .unique()
                                 .map(|path_id| PathId::from(path_id))
                                 .collect::<Vec<PathId>>()
                         }))
                })
                .collect();
        log_item!(verbosity, format!("loaded {} relationships",
                                     count_relationships!(paths_from_commit)));

        log_item!(verbosity, "loading path data");
        let paths: BTreeMap<PathId, Path> =
            paths_from_commit.iter()
                .flat_map(|(_, path_ids)| path_ids)
                .unique()
                .flat_map(|path_id| warehouse.get_file_path(path_id.into()))
                .map(|path| (PathId::from(path.id), Path::from(path)) )
                .collect();
        log_item!(verbosity, format!("loaded {} paths", paths.len()));


        log_item!(verbosity, "loading commit messages");
        let message_from_commit: BTreeMap<CommitId, Message> =
            commit_ids.iter()
                .flat_map(|commit_id| warehouse.get_commit(commit_id.into()))
                .flat_map(|commit| {
                    commit.message.as_ref().map(|message| {
                        (CommitId::from(commit.id), Message::from(message))
                    })
                })
                .collect();
        log_item!(verbosity, format!("loaded {} messages", message_from_commit.len()));

        Data {
            me: None, warehouse, seed, filters,
            projects, commits, users, paths,
            commits_from_project, users_from_project, paths_from_commit, message_from_commit,
        }
    }

    // pub fn project_count(&self) -> usize { self.projects.len() }
    // pub fn commit_count(&self)  -> usize { self.commits.len()  }
    // pub fn user_count(&self)    -> usize { self.users.len()    }
    // pub fn path_count(&self)    -> usize { self.paths.len()    }
    //
    // pub fn project(&self, id: &ProjectId) -> Option<&Project>  { self.projects.get(id) }
    // pub fn commit(&self, id: &CommitId)   -> Option<&Commit>   { self.commits.get(id)  }
    // pub fn user(&self, id: &UserId)       -> Option<&User>     { self.users.get(id)    }
    // pub fn path(&self, id: &PathId)       -> Option<&Path> { self.paths.get(id)    }
    //
    // pub fn project_ids(&self) -> impl Iterator<Item=&ProjectId> { self.projects.keys().into_iter() }
    // pub fn commit_ids(&self)  -> impl Iterator<Item=&CommitId>  { self.commits.keys().into_iter()  }
    // pub fn user_ids(&self)    -> impl Iterator<Item=&UserId>    { self.users.keys().into_iter()    }
    // pub fn path_ids(&self)    -> impl Iterator<Item=&PathId>    { self.paths.keys().into_iter()    }
    //
    // pub fn projects(&self) -> impl Iterator<Item=&Project> { self.projects.values().into_iter() }
    // pub fn commits(&self)  -> impl Iterator<Item=&Commit>  { self.commits.values().into_iter()  }
    // pub fn users(&self)    -> impl Iterator<Item=&User>    { self.users.values().into_iter()    }
    // pub fn paths(&self)    -> impl Iterator<Item=&Path>    { self.paths.values().into_iter()    }
    //
    // fn commit_ids_from(&self, project_id: &ProjectId) -> Option<impl Iterator<Item=&CommitId>> {
    //     self.commits_from_project.get(project_id).map(|vector| vector.iter())
    // }
    // fn user_ids_from(&self, project_id: &ProjectId)   -> Option<impl Iterator<Item=&UserId>> {
    //     self.users_from_project.get(project_id).map(|vector| vector.iter())
    // }
    //
    // pub fn commits_from(&self, project_id: &ProjectId) -> Option<impl Iterator<Item=&Commit> + '_> {
    //     self.commits_from_project.get(project_id)
    //         .map(move |vector| {
    //             vector.clone().into_iter().map(move |id| {
    //                 self.commits.get(&id).unwrap()                                              // TODO flatmap?
    //             })
    //         })
    // }
    //
    // pub fn users_from(&self, project_id: &ProjectId) -> Option<impl Iterator<Item=&User> + '_> {
    //     self.users_from_project.get(project_id)
    //         .map(move |vector| {
    //             vector.clone().into_iter().map(move |id| {
    //                 self.users.get(&id).unwrap()                                                // TODO flatmap?
    //             })
    //         })
    // }
}

impl Data {
    pub fn project_count(&mut self) -> usize { self.projects.len() }
    pub fn commit_count(&self)  -> usize { self.commits.len()  }
    pub fn user_count(&self)    -> usize { self.users.len()    }
    pub fn path_count(&self)    -> usize { self.paths.len()    }

    // pub fn project(&self, id: &ProjectId) -> Option<Ref<Project>> {
    //     let borrowed = self.projects.borrow();
    //     if !borrowed.contains_key(id) { return None; }
    //     Some(Ref::map(borrowed, |borrowed| borrowed.get(id).unwrap()))
    // }
    pub fn project(&mut self, id: &CommitId)   -> Option<&Commit>  { self.commits.get(id)  }
    pub fn commit(&self, id: &CommitId)   -> Option<&Commit>  { self.commits.get(id)  }
    pub fn user(&self, id: &UserId)       -> Option<&User>    { self.users.get(id)    }
    pub fn path(&self, id: &PathId)       -> Option<&Path>    { self.paths.get(id)    }

    // pub fn project_ids(&self) -> impl Iterator<Item=ProjectId> {
    //     //Ref::map(self.projects.borrow(), |projects| &projects.keys())
    //     Ref::map(self.projects.borrow(), |borrowed| borrowed.keys())
    // }
    pub fn project_ids(&mut self) -> impl Iterator<Item=&ProjectId> { self.projects.keys() }
    pub fn commit_ids(&self)  -> impl Iterator<Item=&CommitId>  { self.commits.keys()  }
    pub fn user_ids(&self)    -> impl Iterator<Item=&UserId>    { self.users.keys()    }
    pub fn path_ids(&self)    -> impl Iterator<Item=&PathId>    { self.paths.keys()    }

    /** Project iterators **/
    pub fn project_iter(&mut self) -> impl Iterator<Item=&Project> {
        self.projects.iter().map(|(_, project)| project)
    }

    pub fn projects(&mut self) -> Vec<Project> {
        self.projects.iter().map(|(_, project)| project.clone()).collect()
    }

    pub fn projects_with_filter<Filter>(&mut self, filter: Filter) -> Vec<Project> where Filter: Fn(&&Project) -> bool {
        self.projects.iter()
            .filter(|(_, project)| filter(project))
            .map(|(_, project)| project.clone())
            .collect()
    }

    pub fn projects_with_map<Map,T>(&mut self, map: Map) -> Vec<T> where Map: Fn(&Project) -> T {
        self.projects.iter()
            .map(|(_, project)| map(project))
            .collect()
    }

    pub fn projects_with_flat_map<Map,T,I>(&mut self, map: Map) -> Vec<T> where Map: Fn(&Project) -> I, I: IntoIterator<Item=T> {
        self.projects.iter()
            .flat_map(|(_, project)| map(project))
            .collect()
    }

    /** Commit iterators **/
    pub fn commit_iter(&self) -> impl Iterator<Item=&Commit> {
        self.commits.iter().map(|(_, commit)| commit)
    }

    pub fn commits(&self) -> Vec<Commit> {
        self.commits.iter().map(|(_, commit)| commit.clone()).collect()
    }

    pub fn commits_with_filter<Filter>(&self, filter: Filter) -> Vec<Commit> where Filter: Fn(&&Commit) -> bool {
        self.commits.iter()
            .filter(|(_, commit)| filter(commit))
            .map(|(_, commit)| commit.clone())
            .collect()
    }

    pub fn commits_with_map<Map,T>(&self, map: Map) -> Vec<T> where Map: Fn(&Commit) -> T {
        self.commits.iter()
            .map(|(_, commit)| map(commit))
            .collect()
    }

    pub fn commits_with_flat_map<Map,T,I>(&self, map: Map) -> Vec<T> where Map: Fn(&Commit) -> I, I: IntoIterator<Item=T> {
        self.commits.iter()
            .flat_map(|(_, commit)| map(commit))
            .collect()
    }

    pub fn commit_refs_from(&self, project: &ProjectId) -> Vec<&Commit> {
        self.commits_from_project.get(project).map_or(Default::default(), |commit_ids| {
            commit_ids.iter()
                .flat_map(|commit_id| self.commits.get(commit_id))
                .collect()
        })
    }

    pub fn commits_from(&self, project: &ProjectId) -> Vec<Commit> {
        self.commits_from_project.get(project).map_or(Default::default(), |commit_ids| {
            commit_ids.iter()
                .flat_map(|commit_id| self.commits.get(commit_id))
                .map(|commit| commit.clone())
                .collect()
        })
    }

    pub fn commit_count_from(&self, project: &ProjectId) -> usize {
        self.commits_from_project.get(project).map_or(0, |v| v.len())
    }

    pub fn commit_conditional_count_from<Filter>(&self, project: &ProjectId, filter: Filter) -> usize where Filter: Fn(&&CommitId) -> bool {
        self.commits_from_project.get(project).map_or(0, |v| v.iter().filter(filter).count())
    }

    /** User iterators **/
    pub fn user_iter(&self) -> impl Iterator<Item=&User> {
        self.users.iter().map(|(_, user)| user)
    }

    pub fn users(&self) -> Vec<User> {
        self.users.iter().map(|(_, user)| user.clone()).collect()
    }

    pub fn users_with_filter<Filter>(&self, filter: Filter) -> Vec<User> where Filter: Fn(&&User) -> bool {
        self.users.iter()
            .filter(|(_, user)| filter(user))
            .map(|(_, user)| user.clone())
            .collect()
    }
    pub fn users_with_map<Map,T>(&self, map: Map) -> Vec<T> where Map: Fn(&User) -> T {
        self.users.iter()
            .map(|(_, user)| map(user))
            .collect()
    }

    pub fn users_with_flat_map<Map,T,I>(&self, map: Map) -> Vec<T> where Map: Fn(&User) -> I, I: IntoIterator<Item=T> {
        self.users.iter()
            .flat_map(|(_, user)| map(user))
            .collect()
    }

    pub fn user_refs_from(&self, project: &ProjectId) -> Vec<&User> {
        self.users_from_project.get(project).map_or(Default::default(), |user_ids| {
            user_ids.iter()
                .flat_map(|user_id| self.users.get(user_id))
                .collect()
        })
    }

    pub fn users_from(&self, project: &ProjectId) -> Vec<User> {
        self.users_from_project.get(project).map_or(Default::default(), |user_ids| {
            user_ids.iter()
                .flat_map(|user_id| self.users.get(user_id))
                .map(|user| user.clone())
                .collect()
        })
    }

    pub fn user_count_from(&self, project: &ProjectId) -> usize {
        self.users_from_project.get(project).map_or(0, |v| v.len())
    }


    pub fn user_conditional_count_from<Filter>(&self, project: &ProjectId, filter: Filter) -> usize where Filter: Fn(&&UserId) -> bool {
        self.users_from_project.get(project).map_or(0, |v| v.iter().filter(filter).count())
    }

    /** Path iterators **/
    pub fn path_iter(&self) -> impl Iterator<Item=&Path> {
        self.paths.iter().map(|(_, path)| path)
    }

    pub fn paths(&self) -> Vec<Path> {
        self.paths.iter().map(|(_, path)| path.clone()).collect()
    }

    pub fn paths_with_filter<Filter>(&self, filter: Filter) -> Vec<Path> where Filter: Fn(&&Path) -> bool {
        self.paths.iter()
            .filter(|(_, path)| filter(path))
            .map(|(_, path)| path.clone())
            .collect()
    }

    pub fn paths_with_map<Map,T>(&self, map: Map) -> Vec<T> where Map: Fn(&Path) -> T {
        self.paths.iter()
            .map(|(_, path)| map(path))
            .collect()
    }

    pub fn paths_with_flat_map<Map,T,I>(&self, map: Map) -> Vec<T> where Map: Fn(&Path) -> I, I: IntoIterator<Item=T> {
        self.paths.iter()
            .flat_map(|(_, path)| map(path))
            .collect()
    }

    // pub fn path_refs_from(&self, project: &ProjectId) -> Vec<&Path> {
    //     self.paths_from_project.get(project).map_or(Default::default(), |path_ids| {
    //         path_ids.iter()
    //             .flat_map(|path_id| self.paths.get(path_id))
    //             .collect()
    //     })
    // }pub fn path_refs_from(&self, project: &ProjectId) -> Vec<&Path> {
    //     self.paths_from_project.get(project).map_or(Default::default(), |path_ids| {
    //         path_ids.iter()
    //             .flat_map(|path_id| self.paths.get(path_id))
    //             .collect()
    //     })
    // }

    // pub fn paths_from(&self, project: &ProjectId) -> Vec<&Path> {
    //     self.paths_from_project.get(project).map_or(Default::default(), |path_ids| {
    //         path_ids.iter()
    //             .flat_map(|path_id| self.paths.get(path_id))
    //             .map(|path| path.clone())
    //             .collect()
    //     })
    // }

    // pub fn paths_from_iter(&self, project: &ProjectId) -> impl Iterator<Item=&Path> {
    //     //self.
    // }

    pub fn path_count_from(&self, project: &ProjectId) -> usize {
        //self.paths_from_project.get(project).map_or(0, |v| v.len())
        unimplemented!()
    }

    pub fn path_conditional_count_from<Filter>(&self, project: &ProjectId, filter: Filter) -> usize where Filter: Fn(&&PathId) -> bool {
        //self.paths_from_project.get(project).map_or(0, |v| v.iter().filter(filter).count() )
        unimplemented!()
    }


    // pub fn authors_from(&self, project: &ProjectId) -> impl Iterator<Item=User> {
    //     unimplemented!()
    // }
    //
    // pub fn committers_from(&self, project: &ProjectId) -> impl Iterator<Item=User> {
    //     unimplemented!()
    // }

    pub fn author_count_from(&self, project: &ProjectId) -> usize {
        unimplemented!()
    }

    pub fn committer_count_from(&self, project: &ProjectId) -> usize {
        unimplemented!()
    }

    pub fn age_of(&self, project: &ProjectId) -> Option<Duration> {
        unimplemented!()
    }
}

pub trait Quincunx {
    fn stream_from(data: &DataPtr) -> Vec<Self> where Self: Sized;
}

impl Quincunx for Project {
    fn stream_from(data: &DataPtr) -> Vec<Self> { data.as_ref().borrow_mut().projects() }
}

impl Quincunx for Commit {
    fn stream_from(data: &DataPtr) -> Vec<Self> { data.as_ref().borrow_mut().commits() }
}

impl Quincunx for Path {
    fn stream_from(data: &DataPtr) -> Vec<Self> { data.as_ref().borrow_mut().paths() }
}

impl Quincunx for User {
    fn stream_from(data: &DataPtr) -> Vec<Self> { data.as_ref().borrow_mut().users() }
}

// impl Quincunx for Snapshot {
//     fn stream_from(data: DataPtr) -> Vec<Self> { data.as_ref().borrow_mut().snapshots() }
// }

pub enum NotFound {
    Project(ProjectId),
    Commit(CommitId),
    Path(PathId),
    User(UserId),
}

impl fmt::Display for NotFound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotFound::Project(id) => write!(f, "Project {} not found", id),
            NotFound::Commit(id) => write!(f, "Commit {} not found", id),
            NotFound::Path(id) => write!(f, "Path {} not found", id),
            NotFound::User(id) => write!(f, "User {} not found", id),
        }
    }
}