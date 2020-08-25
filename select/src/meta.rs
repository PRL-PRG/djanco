use dcd::{Project, Database, Commit, User, UserId, DCD, CommitId};
use std::time::Duration;
use itertools::{MinMaxResult, Itertools};
use std::cmp::Ordering;

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

    fn get_commits_in(&self, database: &impl Database, load_messages_and_changes: bool) -> Vec::<Commit>;

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
        self.metadata.get("ght_issue").map_or(0u64, |e| e.parse::<u64>().unwrap())
    }

    fn get_buggy_issue_count(&self) -> Option<u64> {
        self.metadata.get("ght_issue_bug").map(|e| e.parse::<u64>().unwrap())
    }

    fn get_buggy_issue_count_or_zero(&self) -> u64 {
        self.metadata.get("ght_issue_bug").map_or(0u64, |e| e.parse::<u64>().unwrap())
    }

    fn get_head_count(&self) -> usize {
        self.heads.len()
    }

    fn get_commits_in(&self, database: &impl Database, load_messages_and_changes: bool) -> Vec<Commit> {
        if load_messages_and_changes{
            database.commits_from(self).collect()
        } else {
            database.bare_commits_from(self).collect()
        }
    }

    fn get_commit_count_in(&self, database: &impl Database) -> usize {
        database.bare_commits_from(self).count()
    }

    fn get_user_count_in(&self, database: &impl Database) -> usize {
        database.user_ids_from(self).count()
    }

    fn get_path_count_in(&self, database: &impl Database) -> usize {
        database.commits_from(self)
            .flat_map(|c| {
                c.changes.map_or(vec![], |changes| changes.iter().map(|(path_id, _)|  *path_id).collect())
            }).sorted().dedup().count()
    }

    fn get_author_count_in(&self, database: &impl Database) -> usize {
        database.commits_from(self)
            .map(|c| c.author_id)
            .sorted().dedup().count()
    }

    fn get_committer_count_in(&self, database: &impl Database) -> usize {
        database.commits_from(self)
            .map(|c| c.committer_id)
            .sorted().dedup().count()
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
        database.commits_from(self).minmax_by(|c1, c2| {
            if c1.author_time < c2.author_time { return Ordering::Less }
            if c1.author_time > c2.author_time { return Ordering::Greater }
            return Ordering::Equal
        }).into_option()
    }

    fn get_earliest_and_most_recent_commits_in_project_by_committer_time(&self, database: &impl Database) -> Option<(Commit, Commit)> {
        database.commits_from(self).minmax_by(|c1, c2| {
            if c1.committer_time < c2.committer_time { return Ordering::Less }
            if c1.committer_time > c2.committer_time { return Ordering::Greater }
            return Ordering::Equal
        }).into_option()
    }
}

pub trait UserMeta {
    fn get_authored_commit_ids_in(&self, database: &impl MetaDatabase) -> Vec<CommitId>;
    fn get_committed_commit_ids_in(&self, database: &impl MetaDatabase) -> Vec<CommitId>;

    fn get_author_experience_time_in(&self, database: &impl MetaDatabase) -> Option<Duration>;
    fn get_author_experience_time_or_zero_in(&self, database: &impl MetaDatabase) -> Duration;
    fn get_committer_experience_time_in(&self, database: &impl MetaDatabase) -> Option<Duration>;
    fn get_committer_experience_time_or_zero_in(&self, database: &impl MetaDatabase) -> Duration;

    fn get_authored_commit_count_in(&self, database: &impl MetaDatabase) -> Option<u64>;
    fn get_authored_commit_count_or_zero_in(&self, database: &impl MetaDatabase) -> u64;
    fn get_committed_commit_count_in(&self, database: &impl MetaDatabase) -> Option<u64>;
    fn get_committed_commit_count_or_zero_in(&self, database: &impl MetaDatabase) -> u64;
}

impl UserMeta for User {
    fn get_authored_commit_ids_in(&self, database: &impl MetaDatabase) -> Vec<CommitId> {
        database.commit_ids_authored_by(self.id).collect()
    }

    fn get_committed_commit_ids_in(&self, database: &impl MetaDatabase) -> Vec<CommitId> {
        database.commit_ids_committed_by(self.id).collect()
    }

    fn get_author_experience_time_in(&self, database: &impl MetaDatabase) -> Option<Duration> {
        database.get_experience_time_as_author(self.id)
    }
    fn get_author_experience_time_or_zero_in(&self, database: &impl MetaDatabase) -> Duration {
        database.get_experience_time_as_author(self.id).unwrap_or(Duration::from_secs(0))
    }

    fn get_committer_experience_time_in(&self, database: &impl MetaDatabase) -> Option<Duration> {
        database.get_experience_time_as_committer(self.id)
    }

    fn get_committer_experience_time_or_zero_in(&self, database: &impl MetaDatabase) -> Duration {
        database.get_experience_time_as_committer(self.id).unwrap_or(Duration::from_secs(0))
    }

    fn get_authored_commit_count_in(&self, database: &impl MetaDatabase) -> Option<u64> {
        database.get_commit_count_authored_by(self.id)
    }

    fn get_authored_commit_count_or_zero_in(&self, database: &impl MetaDatabase) -> u64 {
        database.get_commit_count_authored_by(self.id).unwrap_or(0u64)
    }

    fn get_committed_commit_count_in(&self, database: &impl MetaDatabase) -> Option<u64> {
        database.get_commit_count_committed_by(self.id)
    }

    fn get_committed_commit_count_or_zero_in(&self, database: &impl MetaDatabase) -> u64 {
        database.get_commit_count_committed_by(self.id).unwrap_or(0u64)
    }
}

pub trait MetaDatabase: Database {
    fn commit_ids_authored_by(&self, user: UserId) -> Box<dyn Iterator<Item=CommitId> + '_>;
    fn commit_ids_committed_by(&self, user: UserId) -> Box<dyn Iterator<Item=CommitId> + '_>;

    fn get_experience_time_as_author(&self, id: UserId) -> Option<Duration>;
    fn get_experience_time_as_committer(&self, id: UserId) -> Option<Duration>;

    fn get_commit_count_authored_by(&self, id: UserId) -> Option<u64>;
    fn get_commit_count_committed_by(&self, id: UserId) -> Option<u64>;
}

impl MetaDatabase for DCD {
    fn commit_ids_authored_by(&self, user: UserId) -> Box<dyn Iterator<Item=CommitId> + '_> {
        Box::new(self.commits().filter(|c| c.author_id == user).map(|c| c.id).collect::<Vec<CommitId>>().into_iter())
    }

    fn commit_ids_committed_by(&self, user: UserId) -> Box<dyn Iterator<Item=CommitId> + '_> {
        Box::new(self.commits().filter(|c| c.committer_id == user).map(|c| c.id).collect::<Vec<CommitId>>().into_iter())
    }

    fn get_experience_time_as_author(&self, id: u64) -> Option<Duration> {
        // eprintln!("Hi. If you're seeing this, you're computing author experience times in the \
        //            slowest possible way and you should consider using one of the caching database \
        //            implementations instead, which won't traverse all commits in the dataset \
        //            repeatedly each single user. --k");

        let result =
            self.bare_commits()
            .filter(|c| c.author_id == id)
            .map(|c| c.author_time)
            .minmax();

        match result {
            MinMaxResult::NoElements       => None,
            MinMaxResult::OneElement(_)    => None,
            MinMaxResult::MinMax(min, max) => {
                Some(Duration::from_secs((max - min) as u64))
            }
        }
    }

    fn get_experience_time_as_committer(&self, id: u64) -> Option<Duration> {
        // eprintln!("Hi. If you're seeing this, you're computing committer experience times in the \
        //            slowest possible way and you should consider using one of the caching database \
        //            implementations instead, which won't traverse all commits in the dataset \
        //            repeatedly each single user. --k");

        let result =
            self.bare_commits()
                .filter(|c| c.committer_id == id)
                .map(|c| c.committer_time)
                .minmax();

        match result {
            MinMaxResult::NoElements       => None,
            MinMaxResult::OneElement(_)    => None,
            MinMaxResult::MinMax(min, max) => {
                Some(Duration::from_secs((max - min) as u64))
            }
        }
    }

    fn get_commit_count_authored_by(&self, id: u64) -> Option<u64> {
        Some(self.commit_ids_authored_by(id).count() as u64)
    }

    fn get_commit_count_committed_by(&self, id: u64) -> Option<u64> {
        Some(self.commit_ids_committed_by(id).count() as u64)
    }
}