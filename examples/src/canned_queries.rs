use std::cmp::Ordering;
use std::collections::{HashMap};

use itertools::Itertools;

use select::selectors::{sort_and_sample, filter_sort_and_sample, CompareProjectsByRatioOfIdenticalCommits};
use select::meta::{ProjectMeta, MetaDatabase};

use dcd::Project;
use dcd::Commit;

use crate::sort_by_numbers;
use crate::sort_by_numbers_opt;
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

    fn all_issues(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let n = parameters["n"].as_u64(); // 50
        let similarity = parameters["similarity"].as_f64(); // 0.9
        let how_sort = sort_by_numbers ! (Direction::Descending, | p: & Project | {
            p.get_issue_count_or_zero() + p.get_buggy_issue_count_or_zero()
        });
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

    fn mean_changes_in_commits(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let n = parameters["n"].as_u64(); // 50
        let similarity = parameters["similarity"].as_f64(); // 0.9
        let how_sort = sort_by_numbers_opt!(Direction::Descending, |p: &Project| {
            let changes_per_commit: Vec<usize> =
                database.commits_from(p).map(|c: Commit| {
                    c.changes.map_or(0, |m| m.len())
                }).collect();

            Self::mean(&changes_per_commit)
        });
        let how_deduplicate =
            |p: &Project| { CompareProjectsByRatioOfIdenticalCommits::new(database, p, similarity) };
        let how_sample = top_distinct!(how_deduplicate, n as usize);

        sort_and_sample(database, how_sort, how_sample)
    }

    fn median_changes_in_commits(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let n = parameters["n"].as_u64(); // 50
        let similarity = parameters["similarity"].as_f64(); // 0.9
        let how_sort = sort_by_numbers_opt!(Direction::Descending, |p: &Project| {
            let mut changes_per_commit: Vec<usize> =
                database.commits_from(p).map(|c: Commit| {
                    c.changes.map_or(0, |m| m.len())
                }).collect();

            Self::median(&mut changes_per_commit)
        });
        let how_deduplicate =
            |p: &Project| { CompareProjectsByRatioOfIdenticalCommits::new(database, p, similarity) };
        let how_sample = top_distinct!(how_deduplicate, n as usize);

        sort_and_sample(database, how_sort, how_sample)
    }

    fn mean_commit_message_sizes(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let n = parameters["n"].as_u64(); // 50
        let similarity = parameters["similarity"].as_f64(); // 0.9
        let how_sort = sort_by_numbers_opt!(Direction::Descending, |p: &Project| {
            let message_sizes: Vec<usize> = database
                .commits_from(p)
                .map(|c: Commit| c.message.map_or(0usize, |s| s.len()))
                .collect();

            Self::mean(&message_sizes)
        });
        let how_deduplicate =
            |p: &Project| { CompareProjectsByRatioOfIdenticalCommits::new(database, p, similarity) };
        let how_sample = top_distinct!(how_deduplicate, n as usize);

        sort_and_sample(database, how_sort, how_sample)
    }

    fn median_commit_message_sizes(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let n = parameters["n"].as_u64(); // 50
        let similarity = parameters["similarity"].as_f64(); // 0.9
        let how_sort = sort_by_numbers_opt!(Direction::Descending, |p: &Project| {
            let mut message_sizes: Vec<usize> = database
                .commits_from(p)
                .map(|c: Commit| c.message.map_or(0usize, |s| s.len()))
                .collect();

            Self::median(&mut message_sizes)
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
        vec!["stars","all_issues","issues","buggy_issues",
             "median_changes_in_commits","median_commit_message_sizes","commits",
             "experienced_authors","experienced_authors_ratio",
             "mean_changes_in_commits","mean_commit_message_sizes"]
            .iter().map(|s| s.to_string()).collect()
    }

    pub fn run(database: &impl MetaDatabase, key: &str, parameters: Vec<(String,QueryParameter)>) -> Option<Vec<Project>> {
        let parameters: HashMap<String, QueryParameter> = parameters.into_iter().collect();
        match key {
            "stars"                       => Some(Self::stars(database, parameters)),
            "all_issues"                  => Some(Self::all_issues(database, parameters)),
            "issues"                      => Some(Self::issues(database, parameters)),
            "buggy_issues"                => Some(Self::buggy_issues(database, parameters)),
            "mean_changes_in_commits"     => Some(Self::mean_changes_in_commits(database, parameters)),
            "mean_commit_message_sizes"   => Some(Self::mean_commit_message_sizes(database, parameters)),
            "median_changes_in_commits"   => Some(Self::median_changes_in_commits(database, parameters)),
            "median_commit_message_sizes" => Some(Self::median_commit_message_sizes(database, parameters)),
            "commits"                     => Some(Self::commits(database, parameters)),
            "experienced_authors"         => Some(Self::experienced_authors(database, parameters)),
            "experienced_authors_ratio"   => Some(Self::experienced_authors_ratio(database, parameters)),

            _ => None
        }
    }

    pub fn default_parameters(key: &str) -> Vec<(String, QueryParameter)> {
        match key {
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
            _                           => vec![("n".to_string(), QueryParameter::Int(50)),
                                                ("similarity".to_string(), QueryParameter::Float(0.9))],
        }
    }

    fn median(vector: &mut Vec<usize>) -> Option<f64> {
        vector.sort();
        match vector.len() {
            0usize => None,
            1usize => vector.get(0).map(|e| *e as f64),
            even if even % 2 == 0usize => {
                let left = vector.get((even / 2) - 1);
                let right = vector.get(even / 2);
                if left.is_none() || right.is_none() {
                    None
                } else {
                    Some((*left.unwrap() + *right.unwrap()) as f64 / 2f64)
                }
            }
            odd  if odd % 2 != 0usize => {
                vector.get(odd / 2).map(|e| *e as f64)
            }
            _ => unreachable!()
        }
    }

    fn mean(vector: &Vec<usize>) -> Option<f64> {
        match vector.len() {
            0usize => None,
            len => Some(vector.iter().sum::<usize>() as f64 / len as f64),
        }
    }
}


#[cfg(test)]
mod tests {
    use crate::canned_queries::Queries;

    #[test]
    fn mean() {
        assert_eq!(Queries::mean(&vec![1,5,3,4,2]), Some(3f64));
        assert_eq!(Queries::mean(&vec![1]), Some(1f64));
        assert_eq!(Queries::mean(&vec![]), None);
    }

    #[test]
    fn median() {
        assert_eq!(Queries::median(&mut vec![1,5,3,4,2,7]), Some(3.5f64));
        assert_eq!(Queries::median(&mut vec![1,5,3,4,2]), Some(3f64));
        assert_eq!(Queries::median(&mut vec![1]), Some(1f64));
        assert_eq!(Queries::median(&mut vec![]), None);
    }
}