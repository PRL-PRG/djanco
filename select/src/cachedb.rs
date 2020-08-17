use dcd::{Database, FilePath, Project, User, Commit};

struct CachedDatabase<'a> {
    database: &'a dyn Database,
}

impl<'a> CachedDatabase<'a> {
    pub fn from(database: &'a impl Database) -> Self {
        CachedDatabase {
            database
        }
    }
}

impl<'a> Database for CachedDatabase<'a> {
    fn num_projects(&self) -> u64 {
        unimplemented!()
    }

    fn num_commits(&self) -> u64 {
        unimplemented!()
    }

    fn num_users(&self) -> u64 {
        unimplemented!()
    }

    fn num_file_paths(&self) -> u64 {
        unimplemented!()
    }

    fn get_project(&self, id: u64) -> Option<Project> {
        unimplemented!()
    }

    fn get_commit(&self, id: u64) -> Option<Commit> {
        unimplemented!()
    }

    fn get_user(&self, id: u64) -> Option<&User> {
        unimplemented!()
    }

    fn get_file_path(&self, id: u64) -> Option<FilePath> {
        unimplemented!()
    }
}