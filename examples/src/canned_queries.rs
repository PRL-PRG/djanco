use std::cmp::Ordering;
use std::collections::{HashMap};

use itertools::Itertools;

use select::selectors::{sort_and_sample, filter_sort_and_sample, CompareProjectsByRatioOfIdenticalCommits};
use select::meta::{ProjectMeta, MetaDatabase};

use dcd::Project;
use dcd::Commit;

use crate::sort_by_numbers;
//use crate::top;
use crate::top_distinct;
use crate::Direction;

#[derive(Debug)]
pub enum QueryParameter {
    Int(u64),
    Float(f64),
    String(String),
}

impl QueryParameter {
    pub fn as_u64(&self) -> u64 {
        match self {
            QueryParameter::Int(v) => *v,
            QueryParameter::Float(_) => panic!("Query parameter is f64, but requested u64"),
            QueryParameter::String(_) => panic!("Query parameter is String, but requested u64"),
        }
    }

    pub fn as_f64(&self) -> f64 {
        match self {
            QueryParameter::Int(_) => panic!("Query parameter is u64, but requested f64"),
            QueryParameter::Float(v) => *v,
            QueryParameter::String(_) => panic!("Query parameter is String, but requested f64"),
        }
    }

    pub fn as_string(&self) -> &str {
        match self {
            QueryParameter::Int(_) => panic!("Query parameter is u64, but requested string"),
            QueryParameter::Float(_) => panic!("Query parameter is f64, but requested string"),
            QueryParameter::String(s) => s,
        }
    }
}

pub struct Queries;
impl Queries {
    fn stars(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let n = parameters["n"].as_u64(); // 50
        let similarity = parameters["similarity"].as_f64(); // 0.9
        let how_sort =
            sort_by_numbers!(Direction::Descending, |p: &Project| p.get_stars_or_zero());
        let how_deduplicate =
            |p: &Project| { CompareProjectsByRatioOfIdenticalCommits::new(database, p, similarity) };
        let how_sample = top_distinct!(how_deduplicate, n as usize);

        sort_and_sample(database, how_sort, how_sample)
    }

    fn issues(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let n = parameters["n"].as_u64(); // 50
        let similarity = parameters["similarity"].as_f64(); // 0.9
        let how_sort = sort_by_numbers ! (Direction::Descending, | p: & Project | p.get_issue_count_or_zero());
        let how_deduplicate =
            |p: &Project| { CompareProjectsByRatioOfIdenticalCommits::new(database, p, similarity) };
        let how_sample = top_distinct!(how_deduplicate, n as usize);
        sort_and_sample(database, how_sort, how_sample)
    }

    fn buggy_issues(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let n = parameters["n"].as_u64(); // 50
        let similarity = parameters["similarity"].as_f64(); // 0.9
        let how_sort = sort_by_numbers!(Direction::Descending, |p: &Project| p.get_buggy_issue_count_or_zero());
        let how_deduplicate =
            |p: &Project| { CompareProjectsByRatioOfIdenticalCommits::new(database, p, similarity) };
        let how_sample = top_distinct!(how_deduplicate, n as usize);
        sort_and_sample(database, how_sort, how_sample)
    }

    fn changes_in_commits(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let n = parameters["n"].as_u64(); // 50
        let similarity = parameters["similarity"].as_f64(); // 0.9
        let how_sort = sort_by_numbers!(Direction::Descending, |p: &Project| {
            let changes_per_commit: Vec<usize> =
                database.commits_from(p).map(|c: Commit| {
                    c.changes.map_or(0, |m| m.len())
                }).collect();

            let average_changes_per_commit = if changes_per_commit.len() == 0 {
                0u64
            } else {
                changes_per_commit.iter().fold(0u64, |s: u64, c: &usize| (*c as u64) + s)
                / changes_per_commit.len() as u64
            };

            average_changes_per_commit
        });
        let how_deduplicate =
            |p: &Project| { CompareProjectsByRatioOfIdenticalCommits::new(database, p, similarity) };
        let how_sample = top_distinct!(how_deduplicate, n as usize);

        sort_and_sample(database, how_sort, how_sample)
    }

    fn commit_message_sizes(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let n = parameters["n"].as_u64(); // 50
        let similarity = parameters["similarity"].as_f64(); // 0.9
        let how_sort = sort_by_numbers!(Direction::Descending, |p: &Project| {
            let message_sizes: Vec<usize> = database
                .commits_from(p)
                .map(|c: Commit| c.message.map_or(0usize, |s| s.len()))
                .collect();

            let avg_message_size = if message_sizes.len() == 0usize {
                0usize
            } else {
                message_sizes.iter().fold(0usize, |c, e| c + e) / message_sizes.len()
            };

            avg_message_size
        });
        let how_deduplicate =
            |p: &Project| { CompareProjectsByRatioOfIdenticalCommits::new(database, p, similarity) };
        let how_sample = top_distinct!(how_deduplicate, n as usize);

        sort_and_sample(database, how_sort, how_sample)
    }

    fn commits(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let n = parameters["n"].as_u64(); // 50
        let similarity = parameters["similarity"].as_f64(); // 0.9
        let how_sort = sort_by_numbers!(Direction::Descending, |p: &Project| p.get_commit_count_in(database));
        let how_deduplicate =
            |p: &Project| { CompareProjectsByRatioOfIdenticalCommits::new(database, p, similarity) };
        let how_sample = top_distinct!(how_deduplicate, n as usize);
        sort_and_sample(database, how_sort, how_sample)
    }

    fn experienced_authors(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let required_experience = parameters["experience"].as_u64();
        let required_number_of_commits_by_experienced_authors: u64 = parameters["min_commits"].as_u64();
        let n = parameters["n"].as_u64(); // 50
        let similarity = parameters["similarity"].as_f64(); // 0.9

        let how_filter = |p: &Project| {
            let commits_with_experienced_authors: usize =
                database
                    .bare_commits_from(p)
                    .map(|c| { database.get_experience_time_as_author(c.author_id).map_or(0u64, |e| e.as_secs()) })
                    .filter(|experience_in_seconds| *experience_in_seconds > required_experience)
                    .count();

            commits_with_experienced_authors as u64 > required_number_of_commits_by_experienced_authors
        };

        let how_sort = sort_by_numbers!(Direction::Descending,
                                                  |p: &Project| p.get_commit_count_in(database));
        let how_deduplicate =
            |p: &Project| { CompareProjectsByRatioOfIdenticalCommits::new(database, p, similarity) };
        let how_sample = top_distinct!(how_deduplicate, n as usize);

        filter_sort_and_sample(database, how_filter, how_sort, how_sample)
    }

    fn experienced_authors_ratio(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let required_experience = parameters["experience"].as_u64();
        //2/*yrs*/ * 365/*days*/ * 24/*hrs*/ * 60/*mins*/ * 60/*secs*/
        //let required_number_of_commits_by_experienced_authors: u64 = 1;
        let required_ratio_of_commits_by_experienced_authors = parameters["min_ratio"].as_f64(); //0.5
        let n = parameters["n"].as_u64(); // 50
        let similarity = parameters["similarity"].as_f64(); // 0.9

        let how_filter = |p: &Project| {
            let commit_has_experienced_author: Vec<bool> =
                database
                    .bare_commits_from(p)
                    .map(|c| { database.get_experience_time_as_author(c.author_id).map_or(0u64, |e| e.as_secs()) })
                    .map(|experience_in_seconds| experience_in_seconds > required_experience)
                    .collect();

            let ratio_of_commits_by_experienced_authors: f64 =
                (commit_has_experienced_author.iter().filter(|b| **b).count() as f64)
                    / (commit_has_experienced_author.iter().count() as f64);

            ratio_of_commits_by_experienced_authors >
                required_ratio_of_commits_by_experienced_authors
        };

        let how_sort = sort_by_numbers!(Direction::Descending,
                                                 |p: &Project| p.get_commit_count_in(database));
        let how_deduplicate =
            |p: &Project| { CompareProjectsByRatioOfIdenticalCommits::new(database, p, similarity) };
        let how_sample = top_distinct!(how_deduplicate, n as usize);

        filter_sort_and_sample(database, how_filter, how_sort, how_sample)
    }

    pub fn all() -> Vec<String> {
        vec!["stars","issues","buggy_issues","changes_in_commits","commit_message_sizes","commits",
             "experienced_authors","experienced_authors_ratio"]
            .iter().map(|s| s.to_string()).collect()
    }

    pub fn run(database: &impl MetaDatabase, key: &str, parameters: Vec<(String,QueryParameter)>) -> Option<Vec<Project>> {
        let parameters: HashMap<String, QueryParameter> = parameters.into_iter().collect();
        match key {
            "stars"                     => Some(Self::stars(database, parameters)),
            "issues"                    => Some(Self::issues(database, parameters)),
            "buggy_issues"              => Some(Self::buggy_issues(database, parameters)),
            "changes_in_commits"        => Some(Self::changes_in_commits(database, parameters)),
            "commit_message_sizes"      => Some(Self::commit_message_sizes(database, parameters)),
            "commits"                   => Some(Self::commits(database, parameters)),
            "experienced_authors"       => Some(Self::experienced_authors(database, parameters)),
            "experienced_authors_ratio" => Some(Self::experienced_authors_ratio(database, parameters)),

            _ => None
        }
    }

    pub fn default_parameters(key: &str) -> Vec<(String, QueryParameter)> {
        match key {
            "stars"                     => vec![("n".to_string(), QueryParameter::Int(50)),
                                                ("similarity".to_string(), QueryParameter::Float(0.9))],
            "issues"                    => vec![("n".to_string(), QueryParameter::Int(50)),
                                                ("similarity".to_string(), QueryParameter::Float(0.9))],
            "buggy_issues"              => vec![("n".to_string(), QueryParameter::Int(50)),
                                                ("similarity".to_string(), QueryParameter::Float(0.9))],
            "changes_in_commits"        => vec![("n".to_string(), QueryParameter::Int(50)),
                                                ("similarity".to_string(), QueryParameter::Float(0.9))],
            "commit_message_sizes"      => vec![("n".to_string(), QueryParameter::Int(50)),
                                                ("similarity".to_string(), QueryParameter::Float(0.9))],
            "commits"                   => vec![("n".to_string(), QueryParameter::Int(50)),
                                                ("similarity".to_string(), QueryParameter::Float(0.9))],
            "experienced_authors"       => vec![("n".to_string(), QueryParameter::Int(50)),
                                                ("similarity".to_string(), QueryParameter::Float(0.9)),
                                                ("experience".to_string(),
                                                 QueryParameter::Int(2/*yrs*/ * 365/*days*/ * 24/*hrs*/ * 60/*mins*/ * 60/*secs*/)),
                                                ("min_commits".to_string(),
                                                 QueryParameter::Int(1))],
            "experienced_authors_ratio" => vec![("n".to_string(), QueryParameter::Int(50)),
                                                ("similarity".to_string(), QueryParameter::Float(0.9)),
                                                ("experience".to_string(),
                                                 QueryParameter::Int(2/*yrs*/ * 365/*days*/ * 24/*hrs*/ * 60/*mins*/ * 60/*secs*/)),
                                                ("min_ratio".to_string(),
                                                 QueryParameter::Float(0.5))],
            _ => vec![]
        }
    }
}