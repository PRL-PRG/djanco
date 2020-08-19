use dcd::{Project, Database, Commit};
use std::time::Duration;

pub type Language = String;

pub trait ProjectMeta {
    fn get_stars(&self)                                        -> Option<u64>;
    fn get_stars_or_zero(&self)                                -> u64;
    fn get_language(&self)                                     -> Option<String>;
    fn get_language_or_empty(&self)                            -> String;
    fn get_issue_count(&self)                                  -> Option<u64>;
    fn get_issue_count_or_zero(&self)                          -> u64 ;
    fn get_buggy_issue_count(&self)                            -> Option<u64>;
    fn get_buggy_issue_count_or_zero(&self)                    -> u64;
    fn get_head_count(&self)                                   -> usize;

    fn get_commit_count_in(&self,    database: &impl Database) -> usize;
    fn get_user_count_in(&self,      database: &impl Database) -> usize;
    fn get_path_count_in(&self,      database: &impl Database) -> usize;
    fn get_author_count_in(&self,    database: &impl Database) -> usize;
    fn get_committer_count_in(&self, database: &impl Database) -> usize;

    fn get_age(&self, database: &impl Database) -> Option<Duration>;

    fn get_earliest_and_most_recent_commits_in_project_by_author_time  (&self, database: &impl Database) -> Option<(Commit, Commit)>;
    fn get_earliest_and_most_recent_commits_in_project_by_committer_time(&self, database: &impl Database) -> Option<(Commit, Commit)>;
}

impl ProjectMeta for Project {
    fn get_stars(&self) -> Option<u64> {
        self.metadata.get("stars").map(|s| s.parse().unwrap())
    }

    fn get_stars_or_zero(&self) -> u64 {
        match self.metadata.get("stars") {
            Some(s) => s.parse::<u64>().unwrap(),
            None => 0u64,
        }
    }

    fn get_language(&self) -> Option<String> {
        self.metadata.get("ght_language").map(|s| s.trim().to_string())
    }

    fn get_language_or_empty(&self) -> String {
        match self.metadata.get("ght_language") {
            Some(s) => s.trim().to_string(),
            None => String::new(),
        }
    }

    fn get_issue_count(&self) -> Option<u64> {
        self.metadata.get("ght_issue").map(|e| e.parse::<u64>().unwrap())
    }

    fn get_issue_count_or_zero(&self) -> u64 {
        self.metadata.get("ght_issue_bug").map_or(0u64, |e| e.parse::<u64>().unwrap())
    }

    fn get_buggy_issue_count(&self) -> Option<u64> {
        self.metadata.get("ght_issue").map(|e| e.parse::<u64>().unwrap())
    }

    fn get_buggy_issue_count_or_zero(&self) -> u64 {
        self.metadata.get("ght_issue_bug").map_or(0u64, |e| e.parse::<u64>().unwrap())
    }

    fn get_head_count(&self) -> usize {
        self.heads.len()
    }

    fn get_commit_count_in(&self, database: &impl Database) -> usize {
        database.commits_from(self).count()
    }

    fn get_user_count_in(&self, _database: &impl Database) -> usize {
        unimplemented!()
    }

    fn get_path_count_in(&self, _database: &impl Database) -> usize {
        unimplemented!()
    }

    fn get_author_count_in(&self, _database: &impl Database) -> usize {
        unimplemented!()
    }

    fn get_committer_count_in(&self, _database: &impl Database) -> usize {
        unimplemented!()
    }

    fn get_age(&self, database: &impl Database) -> Option<Duration> {
        self.get_earliest_and_most_recent_commits_in_project_by_author_time(database).map(
            |(earliest_commit, latest_commit)| {
                let difference = latest_commit.author_time - earliest_commit.author_time;
                assert!(difference >= 0);
                Duration::from_secs(difference as u64)
            }
        )
    }

    fn get_earliest_and_most_recent_commits_in_project_by_author_time(&self, database: &impl Database) -> Option<(Commit, Commit)> {
        database.commits_from(self).fold(None, | collector, commit| {
            match collector {
                None => Some((commit.clone(), commit/*.clone()*/)),
                Some((min, max)) => {
                    if min.author_time < commit.author_time {
                        if max.author_time > commit.author_time {
                            Some((min, max)) // FIXME BAD
                        } else {
                            Some((min, commit))
                        }
                    } else {
                        if max.author_time > commit.author_time {
                            Some((commit, max))
                        } else {
                            unreachable!()
                        }
                    }
                }
            }
        })
    }

    fn get_earliest_and_most_recent_commits_in_project_by_committer_time(&self, database: &impl Database) -> Option<(Commit, Commit)> {
        database.commits_from(self).fold(None, | collector, commit| {
            match collector {
                None => Some((commit.clone(), commit/*.clone()*/)),
                Some((min, max)) => {
                    if min.committer_time < commit.committer_time {
                        if max.committer_time > commit.committer_time {
                            Some((min, max)) // FIXME BAD
                        } else {
                            Some((min, commit))
                        }
                    } else {
                        if max.committer_time > commit.committer_time {
                            Some((commit, max))
                        } else {
                            unreachable!()
                        }
                    }
                }
            }
        })
    }
}