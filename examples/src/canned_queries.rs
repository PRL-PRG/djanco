use std::cmp::Ordering;
use std::collections::HashMap;

use itertools::{Itertools, MinMaxResult};

use select::selectors::{sort_and_sample, filter_sort_and_sample};
use select::meta::ProjectMeta;

use dcd::UserId;
use dcd::Project;
use dcd::Commit;
use dcd::Database;

use crate::sort_by_numbers;
use crate::top;
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
    fn stars(database: &impl Database, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let n = parameters["n"].as_u64(); // 50
        let how_sort =
            sort_by_numbers!(Direction::Descending, |p: &Project| p.get_stars_or_zero());
        let how_sample = top!(n as usize);
        sort_and_sample(database, how_sort, how_sample)
    }

    fn issues(database: &impl Database, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let n = parameters["n"].as_u64(); // 50
        let how_sort = sort_by_numbers ! (Direction::Descending, | p: & Project | p.get_issue_count_or_zero());
        let how_sample = top ! (n as usize);
        sort_and_sample(database, how_sort, how_sample)
    }

    fn buggy_issues(database: &impl Database, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let n = parameters["n"].as_u64(); // 50
        let how_sort = sort_by_numbers!(Direction::Descending, |p: &Project| p.get_buggy_issue_count_or_zero());
        let how_sample = top!(n as usize);
        sort_and_sample(database, how_sort, how_sample)
    }

    fn changes_in_commits(database: &impl Database, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let n = parameters["n"].as_u64(); // 50

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
        let how_sample = top!(n as usize);

        sort_and_sample(database, how_sort, how_sample)
    }

    fn commit_message_sizes(database: &impl Database, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let n = parameters["n"].as_u64(); // 50

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
        let how_sample = top!(n as usize);

        sort_and_sample(database, how_sort, how_sample)
    }

    fn commits(database: &impl Database, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let n = parameters["n"].as_u64(); // 50
        let how_sort = sort_by_numbers!(Direction::Descending, |p: &Project| p.get_commit_count_in(database));
        let how_sample = top!(n as usize);
        sort_and_sample(database, how_sort, how_sample)
    }

    fn experienced_authors(database: &impl Database, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let required_experience = parameters["experience"].as_u64();
        let required_number_of_commits_by_experienced_authors: u64 = parameters["min_commits"].as_u64();
        let n = parameters["n"].as_u64(); // 50

        let author_experience: HashMap<UserId, u64> =
            database.bare_commits()
                .map(|c| (c.author_id, c.author_time))
                .into_group_map()
                .into_iter()
                .map(|(author_id, author_times)| {(
                    author_id,
                    match author_times.iter().minmax() {
                        MinMaxResult::NoElements       => 0u64,
                        MinMaxResult::OneElement(_)    => 0u64,
                        MinMaxResult::MinMax(min, max) => (max - min) as u64,
                    }
                )})
                .collect();

        println!("Experienced authors: {} out of {}",
                 author_experience.iter().filter(|(_,t)| **t > required_experience).count(),
                 author_experience.len());

        let how_filter = |p: &Project| {
            let commits_with_experienced_authors: u64 =
                database
                    .bare_commits_from(p)
                    .map(|c| { author_experience.get(&c.author_id).map_or(0u64, |e| *e) })
                    .filter(|experience_in_seconds| *experience_in_seconds > required_experience)
                    .count() as u64;

            commits_with_experienced_authors > required_number_of_commits_by_experienced_authors
        };

        let how_sort = sort_by_numbers!(Direction::Descending,
                                                  |p: &Project| p.get_commit_count_in(database));
        let how_sample = top!(n as usize);

        filter_sort_and_sample(database, how_filter, how_sort, how_sample)
    }

    fn experienced_authors_ratio(database: &impl Database, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let required_experience = parameters["experience"].as_u64();
        //2/*yrs*/ * 365/*days*/ * 24/*hrs*/ * 60/*mins*/ * 60/*secs*/
        //let required_number_of_commits_by_experienced_authors: u64 = 1;
        let required_ratio_of_commits_by_experienced_authors = parameters["min_ratio"].as_f64(); //0.5
        let n = parameters["n"].as_u64(); // 50

        let author_experience: HashMap<UserId, u64> =
            database.bare_commits()
                .map(|c| (c.author_id, c.author_time))
                .into_group_map()
                .into_iter()
                .map(|(author_id, author_times)| {(
                    author_id,
                    match author_times.iter().minmax() {
                        MinMaxResult::NoElements       => 0u64,
                        MinMaxResult::OneElement(_)    => 0u64,
                        MinMaxResult::MinMax(min, max) => (max - min) as u64,
                    }
                )})
                .collect();

        println!("Experienced authors: {} out of {}",
                 author_experience.iter().filter(|(_,t)| **t > required_experience).count(),
                 author_experience.len());

        let how_filter = |p: &Project| {
            let commit_has_experienced_author: Vec<bool> =
                database
                    .bare_commits_from(p)
                    .map(|c| { author_experience.get(&c.author_id).map_or(0u64, |e| *e) })
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
        let how_sample = top!(n as usize);

        filter_sort_and_sample(database, how_filter, how_sort, how_sample)
    }

    pub fn run(database: &impl Database, key: &str, parameters: Vec<(String,QueryParameter)>) -> Option<Vec<Project>> {
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
            "stars"                     => vec![("n".to_string(), QueryParameter::Int(50))],
            "issues"                    => vec![("n".to_string(), QueryParameter::Int(50))],
            "buggy_issues"              => vec![("n".to_string(), QueryParameter::Int(50))],
            "changes_in_commits"        => vec![("n".to_string(), QueryParameter::Int(50))],
            "commit_message_sizes"      => vec![("n".to_string(), QueryParameter::Int(50))],
            "commits"                   => vec![("n".to_string(), QueryParameter::Int(50))],
            "experienced_authors"       => vec![("n".to_string(), QueryParameter::Int(50)),
                                                ("experience".to_string(),
                                                 QueryParameter::Int(2/*yrs*/ * 365/*days*/ * 24/*hrs*/ * 60/*mins*/ * 60/*secs*/)),
                                                ("min_number".to_string(),
                                                 QueryParameter::Int(1))],
            "experienced_authors_ratio" => vec![("n".to_string(), QueryParameter::Int(50)),
                                                ("experience".to_string(),
                                                 QueryParameter::Int(2/*yrs*/ * 365/*days*/ * 24/*hrs*/ * 60/*mins*/ * 60/*secs*/)),
                                                ("min_ratio".to_string(),
                                                 QueryParameter::Float(0.5))],
            _ => vec![]
        }
    }
}