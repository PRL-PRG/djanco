use std::collections::HashSet;

use ghql::ast::{Query, Expression, Feature, Connective};

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub struct Project {} // FIXME
//type Project = u32;

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

pub trait Selector {
    fn select(&self) -> HashSet<Project>;
}

impl Selector for Query {
    fn select(&self) -> HashSet<Project> {
        self.expression.select()
    }
}

impl Selector for Expression {
    fn select(&self) -> HashSet<Project> {
        let head_selection = self.head.select();
        self.tail.iter().fold(head_selection, |selection, (connective, feature)| {
            connective.combine(selection, feature.select())
        })
    }
}

impl Selector for Feature {
    fn select(&self) -> HashSet<Project> {
        HashSet::new() // FIXME
    }
}