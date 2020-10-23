use std::io::{Write, Error};
use std::fs::create_dir_all;
use std::fs::File;
use std::path::PathBuf;

use crate::objects::*;
use crate::data::*;
use crate::csv::*;
use crate::names::*;

use std::collections::HashSet;
use itertools::Itertools;

#[allow(non_snake_case)]
pub trait Dump {
    fn dump_all_info_to<S>(self, location: S) -> Result<(), Error> where S: Into<String>;
}

impl<I> Dump for I where I: Iterator<Item=Project> + WithData {
    fn dump_all_info_to<S>(self, location: S) -> Result<(), Error> where S: Into<String> {
        let dir: PathBuf = PathBuf::from(location.into());

        macro_rules! create_file {
            ($filename:expr) => {{
                let mut path = dir.clone();
                path.push($filename);
                File::create(path)
            }}
        }

        create_dir_all(&dir)?;

        let data = self.get_database_ptr();

        let mut project_sink            = create_file!("projects.csv")?;
        let mut commit_sink             = create_file!("commits.csv")?;
        let mut commit_message_sink     = create_file!("commit_message.csv")?;
        let mut user_sink               = create_file!("users.csv")?;

        let mut project_commit_map_sink = create_file!("project_commit_map.csv")?;
        let mut commit_commit_map_sink  = create_file!("commit_parents.csv")?;
        let mut commit_path_map_sink    = create_file!("commit_path_map.csv")?;

        writeln!(project_sink, "{}", Project::names().iter().map(|s| s.to_owned()).join(","))?;
        writeln!(commit_sink,  "{}", Commit::names().iter().map(|s| s.to_owned()).join(","))?;
        writeln!(user_sink,    "{}", User::names().iter().map(|s| s.to_owned()).join(","))?;

        writeln!(commit_message_sink,     "commit_id,{}", Message::names().iter().map(|s| s.to_owned()).join(","))?;
        writeln!(commit_commit_map_sink,  "commit_id,parent_id")?;
        writeln!(commit_path_map_sink,    "commit_id,path_id,snapshot_id,path,language")?;
        writeln!(project_commit_map_sink, "project_id,commit_id")?;

        let mut visited_commits: HashSet<CommitId> = HashSet::new();
        let mut visited_users:   HashSet<UserId>   = HashSet::new();

        for project in self {
            writeln!(project_sink, "{}", project.to_csv(data.clone()))?;
            let commits = untangle_mut!(data).commits_from(&project.id);
            for commit in commits {
                if visited_commits.insert(commit.id) {
                    writeln!(commit_sink, "{}", commit.to_csv(data.clone()))?;
                    writeln!(commit_message_sink, "{},{}", &commit.id,
                             untangle_mut!(data).message_of(&commit.id)
                                 .map_or(String::new(), |m| m.to_csv(data.clone())))?;

                    for parent in commit.parents.iter() {
                        writeln!(commit_commit_map_sink, "{},{}", commit.id, parent)?;
                    }

                    for path in untangle_mut!(data).paths_of(&commit.id) {
                        writeln!(commit_path_map_sink, r#"{},{},{},"{}",{}"#,
                                 commit.id, path.id, "", // FIXME snapshot id
                                 path.path,
                                 path.language().unwrap_or(String::new()))?;
                    }
                }

                writeln!(project_commit_map_sink, "{},{}", project.id, commit.id)?;
            }

            let users = untangle_mut!(data).users_from(&project.id);
            for user in users {
                if visited_users.insert(user.id) {
                    writeln!(user_sink, "{}", user.to_csv(data.clone()))?;
                }
            }
        }

        Ok(())
    }
}