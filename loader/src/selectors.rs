use std::collections::HashSet;
use ghql::ast::{Query, Expressions, Expression, Feature, Connective};
use crate::api::Database;
use crate::api_extensions::{ProjectIter, ProjectCommitIter};

type Project = u32;

trait Selector {
    fn select(&self, database: &impl Database) -> HashSet<Project>;
}

impl Selector for Query {
    fn select(&self, database: &impl Database) -> HashSet<Project> {
        match self {
            Query {expressions} => expressions.select(database),
        }
    }
}

impl Selector for Expressions {
    fn select(&self, database: &impl Database) -> HashSet<Project> {
        let head_selection = self.head.select(database);
        self.tail.iter().fold(head_selection, |selection, (connective, feature)| {
            connective.combine(selection, feature.select(database))
        })
    }
}

impl Selector for Expression {
    fn select(&self, database: &impl Database) -> HashSet<Project> {
        match self {
            Expression::Simple(feature) => {
                feature.select(database)
            },
            Expression::Compound { operator: _, left: _, right: _ } => {
                unimplemented!();
            },
        }
    }
}

fn commit_number_project_filter(project: &Project,
                                minimum_number_of_commits: usize,
                                path: Option<String>) {


}

// fn get_projects_by_commits(minimum_number_of_commits: usize) -> HashSet<Project> {
//     ProjectIter::from(database).filter(|project| {
//         let commits = ProjectCommitIter::from(database, project).count();
//         commits > minimum_number_of_commits
//     }).collect()
// }

impl Selector for Feature {
    fn select(&self, database: &impl Database) -> HashSet<Project> {
        match self {
            Feature::Commits { parameters: _, property: _ } => {
                unimplemented!();
            },
            Feature::Additions { parameters: _, property: _ } => {
                unimplemented!();
            },
            Feature::Deletions { parameters: _, property: _ } => {
                unimplemented!();
            },
            Feature::Changes { parameters: _, property: _ } => {
                unimplemented!();
            },
        }
    }
}

trait Combinator {
    fn combine(&self, left: HashSet<Project>, right: HashSet<Project>) -> HashSet<Project>;
}

impl Combinator for Connective {
    fn combine(&self, left: HashSet<Project>, right: HashSet<Project>) -> HashSet<Project> {
        match self {
            Connective::Conjunction => {
                left.intersection(&right).map(|e| e.clone()).collect::<HashSet<Project>>()
            }
        }
    }
}