use dcd::{ProjectId, Database, Project, CommitId};
use std::cmp::Ordering;
use itertools::Itertools;
//use rand::SeedableRng;
//use rand::seq::SliceRandom;
use crate::meta::ProjectMeta;
use rand_pcg::Pcg64Mcg;
use rand::SeedableRng;
use rand::seq::IteratorRandom;
use std::collections::BTreeSet;
use std::hash::{Hasher, Hash};

#[derive(Clone)]
pub struct Query {
    sorter:  Sorter,        /*sorts*/
    sampler: Sampler,       /*samples*/
    filter:  Filter,        /*filtes*/
}

impl Query {
    pub fn new() -> Query {
        Query {
            sorter:   Sorter::AsIs,
            sampler:  Sampler::Everything,
            filter:   Filter::Everything,
        }
    }

    pub fn from(filter:  Filter, sorter:  Sorter, sampler: Sampler) -> Query {
        Query { filter, sampler, sorter }
    }

    pub fn execute(&self, database: &impl Database) -> Vec<ProjectId> {
        let filter = self.filter.create(database);
        let sorter = self.sorter.create(database);
        let mut sampler = self.sampler.create();

        database.projects()
            .map(|p| (p.get_language(), p))
            .into_group_map()
            .into_iter()
            .map(|(_language, projects)| {
                projects.into_iter().filter(&filter).collect::<Vec<Project>>()
            })
            .flat_map(|mut projects| {
                projects.sort_by(&sorter);
                sampler(projects).iter().map(|p| p.id).collect::<Vec<ProjectId>>()
            })
            .collect()
    }
}

#[derive(Debug,Clone,Copy)]
pub enum Sampler {
    Everything,
    Head(usize),
    Random { seed: u128, sample_size: usize },
}

impl Sampler {
    pub fn create(&self) -> Box<dyn FnMut(Vec<Project>) -> Vec<Project>> {
        match self {
            Sampler::Everything => Box::new(
                move |projects: Vec<Project>| { projects }
            ),
            Sampler::Head(n) => {
                let n = *n;
                Box::new(move |projects: Vec<Project>| {
                    projects.into_iter().take(n).collect::<Vec<Project>>()
                })
            },
            Sampler::Random { seed, sample_size } => {
                let seed_bytes = seed.to_be_bytes();
                let mut rng = Pcg64Mcg::from_seed(seed_bytes);
                let size = *sample_size;
                Box::new(move |projects: Vec<Project>| {
                     projects
                         .into_iter()
                         .choose_multiple(&mut rng, size)
                })
            },
        }
    }
}

#[derive(Copy,Debug,Clone)]
pub enum Direction {
    Descending,
    Ascending,
}

#[derive(Copy,Debug,Clone)]
pub enum Sorter {
    AsIs,
    ByStars  (Direction),
    ByCommits(Direction), // FIXME expensive!
    ByUser   (Direction), // FIXME expensive!
}

impl Sorter {
    pub fn create<'a>(&self, database: &'a impl Database) -> Box<dyn Fn(&Project, &Project) -> Ordering + 'a> {
        match *self {
            Sorter::ByStars(direction) => Box::new(
                move |p1: &Project, p2: &Project| {
                    let ascending_ordering =
                        match (p1.get_stars(), p2.get_stars()) {
                            (Some(n1), Some(n2)) =>
                                     if n1 < n2 { Ordering::Less    }
                                else if n1 > n2 { Ordering::Greater }
                                else            { Ordering::Equal   },

                            (None, None) =>       Ordering::Equal,
                            (None,    _) =>       Ordering::Less,
                            (_,    None) =>       Ordering::Greater,
                        };

                    match direction {
                        Direction::Ascending  => ascending_ordering,
                        Direction::Descending => ascending_ordering.reverse(),
                    }
                }
            ),
            Sorter::ByCommits(direction) => Box::new(
                move |p1: &Project, p2: &Project| {

                    let c1 = database.commits_from(p1).count();
                    let c2 = database.commits_from(p2).count();

                    let ascending_ordering =
                             if c1 < c2 { Ordering::Less    }
                        else if c1 > c2 { Ordering::Greater }
                        else            { Ordering::Equal   };

                    match direction {
                        Direction::Ascending  => ascending_ordering,
                        Direction::Descending => ascending_ordering.reverse(),
                    }
                }
            ),
            Sorter::ByUser(direction) => Box::new(
                move |p1: &Project, p2: &Project| {

                    let u1 = database.user_ids_from(p1).count();
                    let u2 = database.user_ids_from(p2).count();

                    let ascending_ordering =
                             if u1 < u2 { Ordering::Less    }
                        else if u1 > u2 { Ordering::Greater }
                        else            { Ordering::Equal   };

                    match direction {
                        Direction::Ascending  => ascending_ordering,
                        Direction::Descending => ascending_ordering.reverse(),
                    }
                }
            ),
            Sorter::AsIs => Box::new(
                |_p1: &Project, _p2: &Project| Ordering::Equal
            ),
        }
    }
}

#[derive(Copy,Debug,Clone)]
pub enum Filter {
    Everything,
    ByCommits(Relation), // FIXME expensive
    ByUsers(Relation),   // FIXME expensive
    ByStars(Relation),   // FIXME expensive
}

#[derive(Copy,Debug,Clone)]
pub enum Relation {
    Equal          (usize),
    EqualOrMoreThan(usize),
    EqualOrLessThan(usize),
    MoreThan       (usize),
    LessThan       (usize),
}

impl Relation {
    fn apply(&self, value: usize) -> bool {
        match *self {
            Relation::Equal          (threshold) => value == threshold,
            Relation::EqualOrMoreThan(threshold) => value >= threshold,
            Relation::EqualOrLessThan(threshold) => value <= threshold,
            Relation::MoreThan       (threshold) => value >  threshold,
            Relation::LessThan       (threshold) => value <  threshold,
        }
    }
}

impl Filter {
    pub fn create<'a>(&self, database: &'a impl Database) -> Box<dyn Fn(&Project) -> bool + 'a> {
        match *self {
            Filter::Everything => Box::new(|_| { true }),
            Filter::ByStars(operator) => Box::new(
                move |project: &Project| {
                    match project.get_stars() {
                        Some(stars) => operator.apply(stars as usize),
                        None => false,
                    }
                }
            ),
            Filter::ByCommits(operator) => Box::new(
                move |project: &Project| {
                    operator.apply(database.commits_from(&project).count())
                }
            ),
            Filter::ByUsers(operator) => Box::new(
                move |project: &Project| {
                    operator.apply(database.user_ids_from(&project).count())
                }
            ),
        }
    }
}

#[allow(dead_code)]
pub fn filter_sort_and_sample<Filter, Sorter, Sampler>(database: &impl Database,
                                                       filter:   Filter,
                                                       sorter:   Sorter,
                                                       sampler:  Sampler)
                                                       -> Vec<Project>

    where Filter:           Fn(&Project) -> bool,
          Sorter:           Fn(&Project, &Project) -> Ordering,
          Sampler:          Fn(Vec<Project>) -> Vec<Project> {

    database.projects()
        .map(|p| (p.get_language(), p))
        .into_group_map()
        .into_iter()
        .map(|(language, projects)| {
            println!("Filtering projects for language {}...",
                     language.as_deref().unwrap_or("?"));
            println!("    {} projects at the outset", projects.len());
            let filtered_projects =
                projects.into_iter().filter(&filter).collect::<Vec<Project>>();
            println!("    {} projects after filtering", filtered_projects.len());
            (language, filtered_projects)
        })
        .flat_map(|(language, mut projects)| {
            println!("Sorting adn sampling projects for language {}...",
                     language.as_deref().unwrap_or("?"));
            println!("    {} projects to sort", projects.len());
            projects.sort_by(&sorter);
            println!("    {} projects to sample from", projects.len());
            let sampled_projects = sampler(projects);
            println!("    {} projects after sampling", sampled_projects.len());
            sampled_projects
        })
        .collect()
}

pub struct CompareProjectsByRatioOfIdenticalCommits {
    commit_ids: BTreeSet<CommitId>,
    min_ratio_to_consider_duplicate: f64,
}
impl CompareProjectsByRatioOfIdenticalCommits {
    pub fn new (database: &impl Database, p: &Project, min_ratio_to_consider_duplicate: f64) -> Self {
        let commit_ids = database.bare_commits_from(p).map(|c| c.id).collect();
        CompareProjectsByRatioOfIdenticalCommits { min_ratio_to_consider_duplicate, commit_ids }
    }
}
impl Hash for CompareProjectsByRatioOfIdenticalCommits {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(42)
    }
}
impl Eq for CompareProjectsByRatioOfIdenticalCommits {}
impl PartialEq for CompareProjectsByRatioOfIdenticalCommits {
    fn eq(&self, other: &Self) -> bool {
        let my_commits: f64 = self.commit_ids.len() as f64;
        let same_commits: f64 = self.commit_ids.intersection(&other.commit_ids).count() as f64;
        same_commits / my_commits > self.min_ratio_to_consider_duplicate
    }
    fn ne(&self, other: &Self) -> bool { !self.eq(other) }
}

#[allow(dead_code)]
pub fn sort_and_sample<Sorter, Sampler>(database: &impl Database,
                                        sorter:   Sorter,
                                        sampler:  Sampler)
                                        -> Vec<Project>

    where Sorter:           Fn(&Project, &Project) -> Ordering,
          Sampler:          Fn(Vec<Project>) -> Vec<Project> {

    database.projects()
        .map(|p| (p.get_language(), p))
        .into_group_map()
        .into_iter()
        .flat_map(|(language, mut projects)| {
            println!("Sorting and sampling projects for language {}...",
                      language.as_deref().unwrap_or("?"));
            println!("    {} projects to sort", projects.len());
            projects.sort_by(&sorter);
            println!("    {} projects to sample from", projects.len());
            let sampled_projects = sampler(projects);
            println!("    {} projects after sampling", sampled_projects.len());
            sampled_projects
        })
        .collect()
}