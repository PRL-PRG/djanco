use std::io::Error;
use std::collections::HashSet;
use std::fs::{create_dir_all, File};
use std::path::PathBuf;
use std::io::Write;

use std::ffi::CString;

use dcd::{Project, ProjectId, CommitId, UserId, Database};

use crate::meta::{ProjectMeta,UserMeta,MetaDatabase};

pub trait DumpFrom {
    fn dump_all_info_about<'a,I>(&self, projects: I, dir: &PathBuf) -> Result<(), Error> where I: Iterator<Item=&'a Project>;
}

impl<D> DumpFrom for D where D: Database + MetaDatabase {
    fn dump_all_info_about<'a,I>(&self, source: I, dir: &PathBuf) -> Result<(), Error> where I: Iterator<Item=&'a Project> {

        macro_rules! create_file {
            ($filename:expr) => {{
                let mut path = dir.clone(); path.push($filename); File::create(path)
            }}
        }

        create_dir_all(dir)?;

        let mut project_sink            = create_file!("projects.csv")?;
        let mut commit_sink             = create_file!("commits.csv")?;
        let mut commit_message_sink     = create_file!("commit_message.csv")?;
        let mut user_sink               = create_file!("users.csv")?;
        let mut path_sink               = create_file!("paths.csv")?;

        let mut project_commit_map_sink = create_file!("project_commit_map.csv")?;
        let mut commit_commit_map_sink  = create_file!("commit_commit_map.csv")?;
        let mut project_user_map_sink   = create_file!("project_user_map.csv")?;
        let mut commit_path_map_sink    = create_file!("commit_path_map.csv")?;
        //let mut user_commit_map_sink    = create_file!("user_commit_map.csv")?;

        writeln!(project_sink, "id,url,last_update,language,issues,buggy_issues,head_count,\
                                commit_count,user_count,path_count,author_count,committer_count,\
                                age")?;

        writeln!(commit_sink, "id,hash,committer_id,committer_time,author_id,author_time,\
                               additions,deletions")?;

        writeln!(user_sink,    "id,name,email,\
                               author_experience_time,committer_experience_time,\
                               authored_commit_count,committer_commit_count")?;

        writeln!(path_sink,    "id,path")?;

        writeln!(commit_message_sink,     "commit_id,message")?;
        writeln!(commit_commit_map_sink,  "commit_id,parent_id")?;
        writeln!(commit_path_map_sink,    "commit_id,path_id,snapshot_id")?;
        writeln!(project_commit_map_sink, "project_id,commit_id")?;
        writeln!(project_user_map_sink,   "project_id,user_id")?;
        //writeln!(user_commit_map_sink,    "user_id,commit_id")?;

        let mut visited_projects: HashSet<ProjectId> = HashSet::new();
        let mut visited_commits:  HashSet<CommitId>  = HashSet::new();
        let mut visited_users:    HashSet<UserId>   = HashSet::new();

        eprintln!("");
        for project in source {
            eprint!(":");
            if visited_projects.insert(project.id) {
                writeln!(project_sink, r#"{},"{}",{},{},{},{},{},{},{},{},{},{},{}"#,
                         project.id, project.url, project.last_update,
                         project.get_language_or_empty(),
                         project.get_issue_count_or_zero(),
                         project.get_buggy_issue_count_or_zero(),
                         project.get_head_count(),
                         project.get_commit_count_in(self),
                         project.get_user_count_in(self),
                         project.get_path_count_in(self),
                         project.get_author_count_in(self),
                         project.get_committer_count_in(self),
                         project.get_age(self).map_or(0u64, |duration| duration.as_secs())
                )?;
            }

            eprint!(".");
            for commit in project.get_commits_in(self, true) {
                writeln!(project_commit_map_sink, "{},{}", project.id, commit.id)?;
                if visited_commits.insert(commit.id) {
                    writeln!(commit_sink, r#"{},"{}",{},{},{},{},{},{}"#,
                             commit.id, commit.hash,
                             commit.committer_id, commit.committer_time,
                             commit.author_id, commit.author_time,
                             commit.additions.unwrap_or(0u64),
                             commit.deletions.unwrap_or(0u64)
                    )?;

                    for parent_id in commit.parents.iter() {
                        writeln!(commit_commit_map_sink, r#"{},{}"#, commit.id, parent_id)?;
                    }

                    // for (path_id, snapshot_id) in commit.changes.map_or(HashMap::new(), |m| m).iter() {
                    //     writeln!(commit_path_map_sink, r#"{},{},{}"#,
                    //              commit.id, path_id, snapshot_id)?;
                    //
                    //     let path_opt = self.get_file_path(*path_id);
                    //     if let Some(path) = path_opt {
                    //         writeln!(path_sink, r#"{},"{}""#, path.id, path.path)?;
                    //     }
                    // }

                    for user_id in vec![commit.author_id, commit.committer_id].iter() {
                        let user_opt = self.get_user(*user_id);
                        if let Some(user) = user_opt {
                            if visited_users.insert(user.id) {
                                writeln!(user_sink, r#"{},"{}","{}",{},{},{},{}"#,
                                         user.id, user.name, user.email,
                                         user.get_author_experience_time_or_zero_in(self).as_secs(),
                                         user.get_committer_experience_time_or_zero_in(self).as_secs(),
                                         user.get_authored_commit_count_or_zero_in(self),
                                         user.get_committed_commit_count_or_zero_in(self),
                                )?;
                            }
                        }
                        //writeln!(project_user_map_sink, r#"{},{}"#, project.id, user_id);
                    }

                    // FIXME
                    writeln!(commit_message_sink, r#"{},"{:?}""#, commit.id,
                             CString::new(commit.message.unwrap_or(vec![])).unwrap_or(CString::new("").unwrap()))?;
                }
            }
        }

        eprintln!("");
        Ok(())
    }
}
