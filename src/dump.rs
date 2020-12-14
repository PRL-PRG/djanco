use std::io::Error;

pub trait Dump {
    fn dump_all_info_to<S>(self, location: S) -> Result<(), std::io::Error> where S: Into<String>;
}

impl<I,T> Dump for I where I: Iterator<Item=T>, T: Dumpable {
    fn dump_all_info_to<S>(self, location: S) -> Result<(), Error> where S: Into<String> {

        unimplemented!()

        // macro_rules! create_file {
        //     ($location:expr) => {{
        //         let path = std::path::PathBuf::from($location.clone());
        //         let dir_path = {
        //             let mut dir_path = path.clone();
        //             dir_path.push($location/into());
        //             dir_path
        //         };
        //         std::fs::create_dir_all(&dir_path)?;
        //         std::fs::File::create(path)
        //     }}
        // }

        // let mut project_sink            = create_file!("projects.csv")?;
        // let mut commit_sink             = create_file!("commits.csv")?;
        // let mut commit_message_sink     = create_file!("commit_message.csv")?;
        // let mut user_sink               = create_file!("users.csv")?;
        //
        // let mut project_commit_map_sink = create_file!("project_commit_map.csv")?;
        // let mut commit_commit_map_sink  = create_file!("commit_parents.csv")?;
        // let mut commit_path_map_sink    = create_file!("commit_path_map.csv")?;
    }
}

struct DumpMeta {
    pub project_sink: std::fs::File,
    pub commit_sink: std::fs::File,
    pub commit_message_sink: std::fs::File,
    pub user_sink: std::fs::File,
    pub project_commit_map_sink: std::fs::File,
    pub commit_commit_map_sink: std::fs::File,
    pub commit_path_map_sink: std::fs::File,
}

pub trait Dumpable {
    fn dump(&self) -> Result<(), std::io::Error>;
}

