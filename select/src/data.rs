use crate::log::LogLevel;
use std::collections::BTreeMap;
use crate::objects::{ProjectId, UserId, CommitId, PathId, Project, Commit, User, Path, Message, Roster, Month};
use dcd::{DCD, Database};
use crate::{LoadFilter, EntityIter, ProjectEntityIter};
use std::time::Duration;
use itertools::Itertools;
use std::path::PathBuf;

pub struct Data {
    //cache_path: PathBuf, //TODO
    pub(crate) projects: BTreeMap<ProjectId, Project>, // TODO internal mutability + laziness
    pub(crate) commits:  BTreeMap<CommitId,  Commit>,
    pub(crate) users:    BTreeMap<UserId,    User>,
    pub(crate) paths:    BTreeMap<PathId,    Path>,
    //pub(crate) snapshots:    BTreeMap<SnapshotId,    Snapshot>,

    pub(crate) commits_from_project: BTreeMap<ProjectId, Vec<CommitId>>,
    pub(crate) users_from_project:   BTreeMap<ProjectId, Vec<UserId>>,
    //pub(crate) authors_from_project:   BTreeMap<ProjectId, Vec<UserId>>,
    //pub(crate) committers_from_project:   BTreeMap<ProjectId, Vec<UserId>>,
    //pub(crate) paths_from_project:   RefCell<BTreeMap<ProjectId, Vec<PathId>>>,

    pub(crate) paths_from_commit:   BTreeMap<CommitId, Vec<PathId>>,
    //pub(crate) snapshots_from_commit:   BTreeMap<CommitId, HashMap<PathId, SnapshotId>>,
    pub(crate) message_from_commit: BTreeMap<CommitId, Message>,                                // To be able to load them separately.
    //pub(crate) metadata_for_project:   RefCell<BTreeMap<ProjectId, HashMap<String, String>>>,
    // TODO age
}

macro_rules! count_relationships {
    ($data:expr) => {
        $data.iter().map(|(_, v)| v.len()).sum::<usize>()
    }
}

impl Data {
    pub fn from(warehouse_path: &PathBuf, time: &Month, verbosity: &LogLevel) -> Self {
        let warehouse: DCD = DCD::new(warehouse_path.as_os_str().to_str().unwrap().to_string());
        unimplemented!()
    }

    pub fn from_(warehouse: &DCD, verbosity: &LogLevel) -> Self {
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
            projects, commits, users, paths,
            commits_from_project, users_from_project, paths_from_commit, message_from_commit,
        }
    }

    pub fn from_filtered(warehouse: &DCD, project_filters: &Vec<Box<dyn LoadFilter>>, verbosity: &LogLevel) -> Self {
        log_header!(verbosity, "Checking out data from warehouse"); // TODO path

        log_item!(verbosity, format!("loading project-commit mapping with {} filter{}",
                                     project_filters.len(),
                                     if project_filters.len() > 1 {"s"} else {""} ));
        let commits_from_project: BTreeMap<ProjectId, Vec<CommitId>> =
            warehouse.project_ids_and_their_commit_ids()
                .filter(|(project_id, commit_ids)| {
                    project_filters.iter().all(|filter| {
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
    pub fn project_count(&self) -> usize { self.projects.len() }
    pub fn commit_count(&self)  -> usize { self.commits.len()  }
    pub fn user_count(&self)    -> usize { self.users.len()    }
    pub fn path_count(&self)    -> usize { self.paths.len()    }

    pub fn project(&self, id: &ProjectId) -> Option<&Project> { self.projects.get(id) }
    pub fn commit(&self, id: &CommitId)   -> Option<&Commit>  { self.commits.get(id)  }
    pub fn user(&self, id: &UserId)       -> Option<&User>    { self.users.get(id)    }
    pub fn path(&self, id: &PathId)       -> Option<&Path>    { self.paths.get(id)    }

    pub fn project_ids(&self) -> impl Iterator<Item=&ProjectId> { self.projects.keys() }
    pub fn commit_ids(&self)  -> impl Iterator<Item=&CommitId>  { self.commits.keys()  }
    pub fn user_ids(&self)    -> impl Iterator<Item=&UserId>    { self.users.keys()    }
    pub fn path_ids(&self)    -> impl Iterator<Item=&PathId>    { self.paths.keys()    }

    pub fn projects(&self) -> EntityIter<ProjectId, Project> {
        unimplemented!()
    }

    pub fn commits(&self) -> EntityIter<CommitId, Commit> {
        unimplemented!()
    }

    pub fn users(&self) -> EntityIter<UserId, User> {
        unimplemented!()
    }

    pub fn paths(&self) -> EntityIter<PathId, Path> {
        unimplemented!()
    }

    pub fn commits_from(&self, project: &ProjectId) -> ProjectEntityIter<Commit> {
        unimplemented!()
    }

    pub fn paths_from(&self, project: &ProjectId) -> ProjectEntityIter<Path> {
        unimplemented!()
    }

    pub fn users_from(&self, project: &ProjectId) -> ProjectEntityIter<User> {
        unimplemented!()
    }

    pub fn authors_from(&self, project: &ProjectId) -> ProjectEntityIter<User> {
        unimplemented!()
    }

    pub fn committers_from(&self, project: &ProjectId) -> ProjectEntityIter<User> {
        unimplemented!()
    }

    pub fn commit_count_from(&self, project: &ProjectId) -> usize {
        unimplemented!()
    }

    pub fn path_count_from(&self, project: &ProjectId) -> usize {
        unimplemented!()
    }

    pub fn user_count_from(&self, project: &ProjectId) -> usize {
        unimplemented!()
    }

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