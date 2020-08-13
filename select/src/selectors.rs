use dcd::{ProjectId, Database, Project};
use std::cmp::Ordering;
use itertools::Itertools;
//use rand::SeedableRng;
//use rand::seq::SliceRandom;
use crate::meta::ProjectMeta;

#[derive(Debug,Clone)]
pub enum Sampler {
    Everything,
    Head(usize),
    // Random(&'a mut SeedableRng<Seed=u64>, usize),
}

impl Sampler {
    pub fn create(&self) -> Box<dyn Fn(Vec<Project>) -> Vec<Project>> {
        match *self {
            Sampler::Everything => Box::new(
                move |projects: Vec<Project>| { projects }
            ),
            Sampler::Head(n) => Box::new(
                move |projects: Vec<Project>| {
                    projects.into_iter().take(n).collect::<Vec<Project>>()
                }
            ),
            // Sampler::Random(rng, n) => Box::new(
            //     move |projects: Vec<Project>| {
            //         projects.choose_multiple(rng, n).collect()
            //     }
            // ),
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
            Relation::Equal          (spec) => spec == value,
            Relation::EqualOrMoreThan(spec) => spec >  value,
            Relation::EqualOrLessThan(spec) => spec <= value,
            Relation::MoreThan       (spec) => spec >= value,
            Relation::LessThan       (spec) => spec <  value,
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

fn group_by_language_and_select<Filter, Sorter, Sampler>(database: &impl Database,
                                                         filter:   Filter,
                                                         sorter:   Sorter,
                                                         sampler:  Sampler)
                                                         -> Vec<ProjectId>

    where Filter:           Fn(&Project) -> bool,
          Sorter:           Fn(&Project, &Project) -> Ordering,
          Sampler:          Fn(Vec<Project>) -> Vec<Project> {

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

pub fn group_by_language_order_by_stars_top_n(database: &impl Database,
                                              top_n: usize)
                                              -> Vec<ProjectId> {

    group_by_language_and_select(/*database*/       database,
                                 /*project_filter*/ Filter::Everything.create(database),
                                 /*project_sorter*/ Sorter::ByStars(Direction::Descending).create(database),
                                 /*sampler*/        Sampler::Head(top_n).create())
}

pub fn group_by_language_min_commits_order_by_stars_top_n(database: &impl Database,
                                                          min_commits: usize,
                                                          top_n: usize)
                                                          -> Vec<ProjectId> {

    group_by_language_and_select(/*database*/       database,
                                 /*project_filter*/ Filter::ByStars(Relation::EqualOrMoreThan(min_commits)).create(database),
                                 /*project_sorter*/ Sorter::ByStars(Direction::Descending).create(database),
                                 /*sampler*/        Sampler::Head(top_n).create())
}