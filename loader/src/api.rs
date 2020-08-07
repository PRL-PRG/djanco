use std::collections::HashMap;

#[derive(Copy, Clone, Debug)]
pub enum Source {
    NA,
    GHTorrent,
    GitHub,
}

#[derive(Debug, Clone)]
pub struct Snapshot {
    // snapshot id and its hash
    id : u64,
    hash : String,
    // file path to the snapshot
    path : Option<String>,
    // metadata
    metadata : HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct FilePath {
    // path id
    id : u64,
    // the actual path
    path : String,
}

#[derive(Debug, Clone)]
pub struct User {
    // id of the user
    pub id : UserId,
    // email for the user
    pub email : String,
    // name of the user
    pub name : String,
}

#[derive(Clone)]
pub struct Project {
    // id of the project
    pub id : ProjectId,
    // url of the project (latest used)
    pub url : String,
    // time at which the project was updated last (i.e. time for which its data are valid)
    pub last_update: u64,
    // head refs of the project at the last update time
    pub heads : Option<HashMap<String, u64>>,
    // source the project data comes from
    pub source : Source,
}

#[derive(Debug, Clone)]
pub struct Commit {
    // commit id
    pub id : CommitId,
    // id of parents
    pub parents : Vec<CommitId>,
    // committer id and time
    pub committer_id : u64,
    pub committer_time : u64,
    // author id and time
    pub author_id : u64,
    pub author_time : u64,
    // source the commit has been obtained from
    pub source : Source,
    pub files : Vec<PathId>,
}

pub type UserId = u64;
pub type BlobId = u64;
pub type PathId = u64;
pub type CommitId = u64;
pub type ProjectId = u64;

pub trait Database {
    fn num_projects(& self) -> u64;
    fn get_user(& self, id : UserId) -> Option<& User>;
    fn get_snapshot(& self, id : BlobId) -> Option<Snapshot>;
    fn get_file_path(& self, id : PathId) -> Option<FilePath>;
    fn get_commit(& self, id : CommitId) -> Option<Commit>;
    fn get_project(& self, id : ProjectId) -> Option<Project>;
    // TODO get commit changes and get commit message functions
}
