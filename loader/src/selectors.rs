use std::collections::HashSet;
use dcd::{ProjectId, Database, Project};
use std::cmp::Ordering;
use itertools::Itertools;

pub type Language = String;

trait MetaAwareProject {
    fn get_stars(&self)             -> Option<u64>;
    fn get_stars_or_zero(&self)     -> u64;
    fn get_language(&self)          -> Option<String>;
    fn get_language_or_empty(&self) -> String;
}

impl MetaAwareProject for Project {
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
}

pub fn group_by_language_order_by_stars_top_n(database: &impl Database,
                                              top_n: usize)
                                              -> HashSet<ProjectId> {

    let star_sorter_descending = |p1: &Project, p2: &Project| {
        match (p1.get_stars(), p2.get_stars()) {
            (Some(n1), Some(n2)) =>
                     if n1 < n2 { Ordering::Greater }
                else if n1 > n2 { Ordering::Less    }
                else            { Ordering::Equal   },

            (None, None) =>       Ordering::Equal,
            (None,    _) =>       Ordering::Greater,
            (_,    None) =>       Ordering::Less,
        }
    };

    database.projects()
        .group_by(|p| p.get_language()).into_iter()
        .flat_map(|(language, group)| {
            let mut projects: Vec<Project> = group.collect();
            projects.sort_by(star_sorter_descending);
            projects.iter().take(top_n).map(|p| p.id).collect::<Vec<ProjectId>>()
        })
        .collect()
}