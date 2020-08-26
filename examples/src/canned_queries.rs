use std::cmp::Ordering;
use std::collections::{HashMap};

use itertools::Itertools;

use select::selectors::{filter_and_sample, sort_and_sample, filter_sort_and_sample, CompareProjectsByRatioOfIdenticalCommits};
use select::meta::{ProjectMeta, MetaDatabase, UserMeta};
use rand_pcg::Pcg64Mcg;

use dcd::Project;
use dcd::Commit;
use dcd::User;
use dcd::UserId;

use crate::sort_by_numbers;
use crate::sort_by_numbers_opt;
use crate::sort_by_numbers_desc;
use crate::sort_by_numbers_opt_desc;
//use crate::top;
use crate::top_distinct;
//use crate::random;
use crate::random_distinct;
use crate::Direction;
use rand::SeedableRng;
use rand::seq::IteratorRandom;

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

#[allow(unused_macros)]
macro_rules! maybe_filter_then_sort_and_sample {
    ($database:expr,$min_commits:expr,$how_sort:expr,$how_sample:expr) => {{
        if $min_commits == 0 {
            sort_and_sample($database, $how_sort, $how_sample)
        } else {
            let how_filter = |p: &Project| {
                p.get_commit_count_in($database) >= $min_commits as usize
            };
            filter_sort_and_sample($database, how_filter, $how_sort, $how_sample)
        }
    }}
}


macro_rules! maybe_filter_then_sort_and_sample {
    ($database:expr,$min_commits:expr,$how_sort:expr,$how_sample:expr) => {{
        if $min_commits == 0 {
            sort_and_sample($database, $how_sort, $how_sample)
        } else {
            let how_filter = |p: &Project| {
                p.get_commit_count_in($database) >= $min_commits as usize
            };
            filter_sort_and_sample($database, how_filter, $how_sort, $how_sample)
        }
    }}
}


macro_rules! maybe_extra_filter_then_sort_and_sample {
    ($database:expr,$min_commits:expr,$how_filter:expr, $how_sort:expr,$how_sample:expr) => {{
        if $min_commits == 0 {
            filter_sort_and_sample($database, $how_filter, $how_sort, $how_sample)
        } else {
            let how_filter = |p: &Project| {
                if p.get_commit_count_in($database) >= $min_commits as usize {
                    false
                } else {
                    $how_filter(p)
                }
            };
            filter_sort_and_sample($database, how_filter, $how_sort, $how_sample)
        }
    }}
}

macro_rules! maybe_extra_filter_then_sample {
    ($database:expr,$min_commits:expr,$how_filter:expr, $how_sample:expr) => {{
        if $min_commits == 0 {
            filter_and_sample($database, $how_filter, $how_sample)
        } else {
            let how_filter = |p: &Project| {
                if p.get_commit_count_in($database) >= $min_commits as usize {
                    false
                } else {
                    $how_filter(p)
                }
            };
            filter_and_sample($database, how_filter, $how_sample)
        }
    }}
}

macro_rules! random_distinct_by_commits {
    ($database:expr,$similarity:expr,$rng:expr, $n:expr) => {{
        let how_deduplicate = |p: &Project| {
            CompareProjectsByRatioOfIdenticalCommits::new($database, p, $similarity)
        };
        random_distinct!(how_deduplicate, $rng, $n)
    }}
}

macro_rules! top_distinct_by_commits {
    ($database:expr,$similarity:expr,$n:expr) => {{
        let how_deduplicate = |p: &Project| {
            CompareProjectsByRatioOfIdenticalCommits::new($database, p, $similarity)
        };
        top_distinct!(how_deduplicate, $n)
    }}
}

macro_rules! bog_standard_sort {
    ($database:expr, $parameters:expr, $how_sort:expr) => {{
        let n = $parameters["n"].as_u64();
        let similarity = $parameters["similarity"].as_f64();
        let min_commits = $parameters["min_commits"].as_u64();

        let mut how_sample = top_distinct_by_commits!($database, similarity, n);

        maybe_filter_then_sort_and_sample!($database, min_commits, $how_sort, &mut how_sample)
    }}
}

macro_rules! commits_with_experienced_authors_in_project {
    ($database:expr,$p:expr,$required_experience:expr) => {
        $database
            .bare_commits_from($p)
            .map(|c| { $database.get_experience_time_as_author(c.author_id).map_or(0u64, |e| e.as_secs()) })
            .filter(|experience_in_seconds| *experience_in_seconds > $required_experience)
            .count() as u64
        }
}

pub struct Queries;
impl Queries {
    fn sort_by_stars(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let how_sort   = sort_by_numbers_desc!(|p: &Project| p.get_stars_or_zero());
        bog_standard_sort!(database, parameters, how_sort)
    }

    fn sort_by_all_issues(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let how_sort = sort_by_numbers_desc! (|p: & Project| {
            p.get_issue_count_or_zero() + p.get_buggy_issue_count_or_zero()
        });
        bog_standard_sort!(database, parameters, how_sort)
    }

    fn sort_by_issues(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let how_sort = sort_by_numbers_desc!(|p: & Project| p.get_issue_count_or_zero());
        bog_standard_sort!(database, parameters, how_sort)
    }

    fn sort_by_buggy_issues(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let how_sort = sort_by_numbers_desc!(|p: &Project| p.get_buggy_issue_count_or_zero());
        bog_standard_sort!(database, parameters, how_sort)
    }

    fn sort_by_mean_changes_in_commits(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let how_sort = sort_by_numbers_opt_desc!(|p: &Project| {
            let changes_per_commit: Vec<usize> =
                database.commits_from(p).map(|c: Commit| {
                    c.changes.map_or(0, |m| m.len())
                }).collect();
            Self::mean(&changes_per_commit)
        });
        bog_standard_sort!(database, parameters, how_sort)
    }

    fn sort_by_median_changes_in_commits(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let how_sort = sort_by_numbers_opt_desc!(|p: &Project| {
            let mut changes_per_commit: Vec<u64> =
                database.commits_from(p).map(|c: Commit| {
                    c.changes.map_or(0, |m| m.len()) as u64
                }).collect();
            Self::median(&mut changes_per_commit)
        });
        bog_standard_sort!(database, parameters, how_sort)
    }

    fn sort_by_mean_commit_message_sizes(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let how_sort = sort_by_numbers_opt_desc!(|p: &Project| {
            let message_sizes: Vec<usize> = database
                .commits_from(p)
                .map(|c: Commit| c.message.map_or(0usize, |s| s.len()))
                .collect();
            Self::mean(&message_sizes)
        });
        bog_standard_sort!(database, parameters, how_sort)
    }

    fn sort_by_median_commit_message_sizes(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let how_sort = sort_by_numbers_opt_desc!(|p: &Project| {
            let mut message_sizes: Vec<u64> = database
                .commits_from(p)
                .map(|c: Commit| c.message.map_or(0usize, |s| s.len()) as u64)
                .collect();
            Self::median(&mut message_sizes)
        });
        bog_standard_sort!(database, parameters, how_sort)
    }

    fn sort_by_commits(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let how_sort = sort_by_numbers_desc!(|p: &Project| p.get_commit_count_in(database));
        bog_standard_sort!(database, parameters, how_sort)
    }

    #[allow(dead_code)]
    fn filter_by_commits_by_experienced_authors_sort_by_commits(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let required_experience = parameters["experience"].as_u64();
        let required_number_of_commits_by_experienced_authors = parameters["min_exp_commits"].as_u64();
        let n = parameters["n"].as_u64();
        let similarity = parameters["similarity"].as_f64();
        let min_commits = parameters["min_commits"].as_u64();

        let how_filter = |p: &Project| {
            let commits_with_experienced_authors =
                commits_with_experienced_authors_in_project!(database,p,required_experience);
            commits_with_experienced_authors > required_number_of_commits_by_experienced_authors
        };
        let how_sort = sort_by_numbers_desc!(|p: &Project| p.get_commit_count_in(database));
        let mut how_sample = top_distinct_by_commits!(database, similarity, n);

        maybe_extra_filter_then_sort_and_sample!(database, min_commits, how_filter, how_sort, &mut how_sample)
    }

    #[allow(dead_code)]
    fn filter_by_commits_by_experienced_authors_random_sample(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let required_experience = parameters["experience"].as_u64();
        let required_number_of_commits_by_experienced_authors = parameters["min_exp_commits"].as_u64();
        let n = parameters["n"].as_u64();
        let similarity = parameters["similarity"].as_f64();
        let min_commits = parameters["min_commits"].as_u64();
        let seed = parameters["seed"].as_u64();

        let how_filter = |p: &Project| {
            let commits_with_experienced_authors =
                commits_with_experienced_authors_in_project!(database,p,required_experience);
            commits_with_experienced_authors > required_number_of_commits_by_experienced_authors
        };
        let mut rng = Pcg64Mcg::seed_from_u64(seed);
        let mut how_sample = random_distinct_by_commits!(database, similarity, rng, n);

        maybe_extra_filter_then_sample!(database, min_commits, how_filter, &mut how_sample)
    }

    fn calculate_ratio_of_commits_by_experienced_authors(database: &impl MetaDatabase, project: &Project, required_experience: u64) -> f64 {
        let commit_has_experienced_author: Vec<bool> =
            database
                .bare_commits_from(project)
                .map(|c| { database.get_experience_time_as_author(c.author_id).map_or(0u64, |e| e.as_secs()) })
                .map(|experience_in_seconds| experience_in_seconds > required_experience)
                .collect();

        let ratio_of_commits_by_experienced_authors: f64 =
            (commit_has_experienced_author.iter().filter(|b| **b).count() as f64)
                / (commit_has_experienced_author.iter().count() as f64);

        ratio_of_commits_by_experienced_authors
    }

    #[allow(dead_code)]
    fn filter_by_commits_by_experienced_authors_ratio_sort_by_commits(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let required_experience = parameters["experience"].as_u64();
        let required_ratio_of_commits_by_experienced_authors = parameters["min_ratio"].as_f64();
        let n = parameters["n"].as_u64();
        let min_commits = parameters["min_commits"].as_u64();
        let similarity = parameters["similarity"].as_f64();

        let how_filter = |p: &Project| {
            required_ratio_of_commits_by_experienced_authors <=
                Self::calculate_ratio_of_commits_by_experienced_authors(database, p, required_experience)
        };
        let how_sort = sort_by_numbers_desc!(|p: &Project| p.get_commit_count_in(database));
        let mut how_sample = top_distinct_by_commits!(database, similarity, n);

        maybe_extra_filter_then_sort_and_sample!(database, min_commits, how_filter, how_sort, &mut how_sample)
    }

    #[allow(dead_code)]
    fn filter_by_commits_by_experienced_authors_ratio_random_sample(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let required_experience = parameters["experience"].as_u64();
        let required_ratio_of_commits_by_experienced_authors = parameters["min_ratio"].as_f64();
        let n = parameters["n"].as_u64();
        let min_commits = parameters["min_commits"].as_u64();
        let similarity = parameters["similarity"].as_f64();
        let seed = parameters["seed"].as_u64();

        let how_filter = |p: &Project| {
            required_ratio_of_commits_by_experienced_authors <=
                Self::calculate_ratio_of_commits_by_experienced_authors(database, p, required_experience)
        };
        let mut rng = Pcg64Mcg::seed_from_u64(seed);
        let mut how_sample = random_distinct_by_commits!(database, similarity, rng, n);

        maybe_extra_filter_then_sample!(database, min_commits, how_filter, &mut how_sample)
    }

    fn sort_by_ratio_of_commits_by_experienced_authors(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let required_experience = parameters["min_experience"].as_u64();
        let n = parameters["n"].as_u64();
        let min_commits = parameters["min_commits"].as_u64();
        let similarity = parameters["similarity"].as_f64();

        let how_sort = sort_by_numbers_desc!(|p: &Project| Self::calculate_ratio_of_commits_by_experienced_authors(database, p, required_experience));
        let mut how_sample = top_distinct_by_commits!(database, similarity, n);

        maybe_filter_then_sort_and_sample!(database, min_commits, how_sort, &mut how_sample)
    }

    fn calculate_experience_of_all_users(database: &impl MetaDatabase, project: &Project) -> Vec<u64> {
        database.user_ids_from(project).map(|user_id: UserId| {
            database.get_user(user_id).map(|user: &User| {
                user.get_author_experience_time_or_zero_in(database).as_secs()
            }).unwrap_or(0u64)
        }).collect()
    }

    fn sort_by_sum_of_user_experience(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let n = parameters["n"].as_u64();
        let min_commits = parameters["min_commits"].as_u64();
        let similarity = parameters["similarity"].as_f64();

        let how_sort = sort_by_numbers_desc!(|project: &Project| {
            let experience: Vec<u64> = Self::calculate_experience_of_all_users(database, project);
            experience.iter().sum::<u64>()
        });
        let mut how_sample = top_distinct_by_commits!(database, similarity, n);

        maybe_filter_then_sort_and_sample!(database, min_commits, how_sort, &mut how_sample)
    }

    fn sort_by_median_user_experience(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let n = parameters["n"].as_u64();
        let min_commits = parameters["min_commits"].as_u64();
        let similarity = parameters["similarity"].as_f64();

        let how_sort = sort_by_numbers_desc!(|project: &Project| {
            let mut experience: Vec<u64> = Self::calculate_experience_of_all_users(database, project);
            Self::median(&mut experience)
        });
        let mut how_sample = top_distinct_by_commits!(database, similarity, n);

        maybe_filter_then_sort_and_sample!(database, min_commits, how_sort, &mut how_sample)
    }

    fn filter_by_at_least_one_experienced_user_random_sample(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let n = parameters["n"].as_u64();
        let min_commits = parameters["min_commits"].as_u64();
        let similarity = parameters["similarity"].as_f64();
        let seed = parameters["seed"].as_u64();
        let min_experience = parameters["min_experience"].as_u64();

        let how_filter = |project: &Project| {
            Self::calculate_experience_of_all_users(database, project)
                .iter().any(|experience| *experience >= min_experience )
        };
        let mut rng = Pcg64Mcg::seed_from_u64(seed);
        let mut how_sample = random_distinct_by_commits!(database, similarity, rng, n);

        maybe_extra_filter_then_sample!(database, min_commits, how_filter, &mut how_sample)
    }

    fn filter_by_at_least_two_users_sort_by_ratio_of_experienced_users(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let n = parameters["n"].as_u64();
        let min_commits = parameters["min_commits"].as_u64();
        let similarity = parameters["similarity"].as_f64();
        let min_experience = parameters["min_experience"].as_u64();

        let how_filter = |project: &Project| project.get_user_count_in(database) > 1;
        let how_sort = sort_by_numbers_desc!(|project: &Project| {
            let all_users = project.get_user_count_in(database);
            let experience = Self::calculate_experience_of_all_users(database, project);
            let experienced_users = experience.iter().filter(|experience| **experience >= min_experience).count();
            experienced_users as f64 / all_users as f64
        });
        let mut how_sample = top_distinct_by_commits!(database, similarity, n);

        maybe_extra_filter_then_sort_and_sample!(database, min_commits, how_filter, how_sort, &mut how_sample)
    }

    fn filter_by_at_least_two_users_and_at_least_50perc_experienced_users_random_sample(database: &impl MetaDatabase, parameters: HashMap<String,QueryParameter>) -> Vec<Project> {
        let n = parameters["n"].as_u64();
        let min_commits = parameters["min_commits"].as_u64();
        let similarity = parameters["similarity"].as_f64();
        let min_experience = parameters["min_experience"].as_u64();
        let min_experience_ratio = 0.5; //parameters["min_experience_ratio"].as_f64();
        let seed = parameters["seed"].as_u64();


        let how_filter = |project: &Project| {
            if project.get_user_count_in(database) > 1 {
                let all_users = project.get_user_count_in(database);
                let experience = Self::calculate_experience_of_all_users(database, project);
                let experienced_users = experience.iter().filter(|experience| **experience >= min_experience).count();
                (experienced_users as f64 / all_users as f64) >= min_experience_ratio
            } else {
                false
            }
        };
        let mut rng = Pcg64Mcg::seed_from_u64(seed);
        let mut how_sample = random_distinct_by_commits!(database, similarity, rng, n);

        maybe_extra_filter_then_sample!(database, min_commits, how_filter, &mut how_sample)
    }

    pub fn all() -> Vec<String> {
        vec!["sort_by_stars",
             "sort_by_issues", "sort_by_buggy_issues", "sort_by_all_issues",
             "sort_by_commits",
             "sort_by_mean_changes_in_commits",
             "sort_by_mean_commit_message_sizes",
             "sort_by_median_changes_in_commits",
             "sort_by_median_commit_message_sizes",
             "filter_by_commits_by_experienced_authors_sort_by_commits",
             "filter_by_commits_by_experienced_authors_random_sample",
             "filter_by_commits_by_experienced_authors_ratio_sort_by_commits",
             "filter_by_commits_by_experienced_authors_ratio_random_sample",
             "sort_by_ratio_of_commits_by_experienced_authors",
             "sort_by_sum_of_user_experience",
             "sort_by_median_user_experience",
             "filter_by_at_least_one_experienced_user_random_sample",
             "filter_by_at_least_two_users_sort_by_ratio_of_experienced_users",
             "filter_by_at_least_two_users_and_at_least_50perc_experienced_users_random_sample"
        ].iter().rev().map(|s| s.to_string()).collect()
    }


    pub fn run(database: &impl MetaDatabase, key: &str, parameters: Vec<(String,QueryParameter)>) -> Option<Vec<Project>> {
        let parameters: HashMap<String, QueryParameter> = parameters.into_iter().collect();
        match key {
            //"filter_by_commits_by_experienced_authors_sort_by_commits"       => Some(Self::filter_by_commits_by_experienced_authors_sort_by_commits(database, parameters)),            //old experienced_authors
            //"filter_by_commits_by_experienced_authors_random_sample"         => Some(Self::filter_by_commits_by_experienced_authors_random_sample(database, parameters)),
            //"filter_by_commits_by_experienced_authors_ratio_sort_by_commits" => Some(Self::filter_by_commits_by_experienced_authors_ratio_sort_by_commits(database, parameters)),      //old experienced_authors_ratio
            //"filter_by_commits_by_experienced_authors_ratio_random_sample"   => Some(Self::filter_by_commits_by_experienced_authors_ratio_random_sample(database, parameters)),

            "sort_by_stars"                       => Some(Self::sort_by_stars(database, parameters)),
            "sort_by_all_issues"                  => Some(Self::sort_by_all_issues(database, parameters)),
            "sort_by_issues"                      => Some(Self::sort_by_issues(database, parameters)),
            "sort_by_buggy_issues"                => Some(Self::sort_by_buggy_issues(database, parameters)),
            "sort_by_commits"                     => Some(Self::sort_by_commits(database, parameters)),

            "sort_by_mean_changes_in_commits"     => Some(Self::sort_by_mean_changes_in_commits(database, parameters)),
            "sort_by_mean_commit_message_sizes"   => Some(Self::sort_by_mean_commit_message_sizes(database, parameters)),
            "sort_by_median_changes_in_commits"   => Some(Self::sort_by_median_changes_in_commits(database, parameters)),
            "sort_by_median_commit_message_sizes" => Some(Self::sort_by_median_commit_message_sizes(database, parameters)),

            "filter_by_at_least_one_experienced_user_random_sample"           => Some(Self::filter_by_at_least_one_experienced_user_random_sample(database, parameters)),
            "filter_by_at_least_two_users_sort_by_ratio_of_experienced_users" => Some(Self::filter_by_at_least_two_users_sort_by_ratio_of_experienced_users(database, parameters)),
            "filter_by_at_least_two_users_and_at_least_50perc_experienced_users_random_sample" => Some(Self::filter_by_at_least_two_users_and_at_least_50perc_experienced_users_random_sample(database, parameters)),

            "sort_by_ratio_of_commits_by_experienced_authors" => Some(Self::sort_by_ratio_of_commits_by_experienced_authors(database, parameters)),
            "sort_by_sum_of_user_experience"                  => Some(Self::sort_by_sum_of_user_experience(database, parameters)),
            "sort_by_median_user_experience"                  => Some(Self::sort_by_median_user_experience(database, parameters)),

            _ => None
        }
    }

    pub fn default_parameters(_key: &str) -> Vec<(String, QueryParameter)> {
       vec![("min_commits".to_string(), QueryParameter::Int(0)),
            ("seed".to_string(), QueryParameter::Int(42)),
            ("min_experience".to_string(),
             QueryParameter::Int(2/*yrs*/ * 365/*days*/ * 24/*hrs*/ * 60/*mins*/ * 60/*secs*/)),
            ("n".to_string(), QueryParameter::Int(50)),
            ("similarity".to_string(), QueryParameter::Float(0.9))]
    }

    fn median(vector: &mut Vec<u64>) -> Option<f64> {
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