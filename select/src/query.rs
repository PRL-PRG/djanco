use dcd::{ProjectIter, Database, Project};
use crate::query::project::{Group, Property, TimeResolution, GroupKey};
use itertools::Itertools;
use crate::meta::ProjectMeta;
use std::collections::HashMap;

pub mod project {
    use std::time::{Instant, Duration};

    #[derive(Debug, Copy, Clone)]
    pub enum TimeResolution { Days, Months, Years }

    #[derive(Debug, Copy, Clone)]
    pub enum Property { Commits, Paths, Heads, Authors, Committers, Users }

    #[derive(Debug, Clone)]
    pub enum Group {
        // Things we can read directly
        TimeOfLastUpdate,

        // Things we can read from metadata
        Language,
        Stars,

        // Things we can count
        Count(Property), // TODO resolution equidistant, log2, log10

        // Things we can calculate or derive
        Duration(TimeResolution),

        // Complex groupings
        Conjunction(Vec<Group>),
    }

    #[derive(Debug, Clone)]
    pub enum GroupKey {
        // Things we can read directly
        TimeOfLastUpdate(Instant),

        // Things we can read from metadata
        Language(String),
        Stars(usize),

        // Things we can count
        Commits(usize),
        Paths(usize),
        Heads(usize),
        Authors(usize),
        Committers(usize),
        Users(usize),

        // Things we can calculate or derive
        Duration(Duration),

        // Complex groupings
        Conjunction(Vec<GroupKey>),
    }
}

pub trait ProjectQuery {
    fn group_by(self, group: &project::Group, database: &impl Database) -> Vec<Vec<Project>>;
}

impl ProjectQuery for ProjectIter<'_> {
    fn group_by(self, group: &project::Group, database: &impl Database) -> Vec<Vec<Project>> { // FIXME remove database from parameters... somehow
        macro_rules! group_by {
           ($f:expr) => { self.map($f).into_group_map().into_iter().map(|(_, g)| g).collect() }
        }

        match &group {
            Group::TimeOfLastUpdate => group_by!(|p| (p.last_update, p)),
            Group::Language         => group_by!(|p| (p.get_language_or_empty(), p)),
            Group::Stars            => group_by!(|p| (p.get_stars_or_zero(), p)),

            Group::Duration(TimeResolution::Days)   => unimplemented!(),
            Group::Duration(TimeResolution::Months) => unimplemented!(),
            Group::Duration(TimeResolution::Years)  => unimplemented!(),

            Group::Count(Property::Heads)      => group_by!(|p| (p.get_head_count(), p)),
            Group::Count(Property::Commits)    => group_by!(|p| (p.get_commit_count_in(database), p)),
            Group::Count(Property::Paths)      => group_by!(|p| (p.get_path_count_in(database), p)),
            Group::Count(Property::Committers) => group_by!(|p| (p.get_committer_count_in(database), p)),
            Group::Count(Property::Authors)    => group_by!(|p| (p.get_author_count_in(database), p)),
            Group::Count(Property::Users)      => group_by!(|p| (p.get_user_count_in(database), p)),

            Group::Conjunction(_) => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod test {
    // Gimmes:
    //   * make available the Database trait:
    //     * it provides iterators for commits, projects, users, etc.
    //     * it provides iterators for all commits, users, path, etc. in a project
    //     * it also provides total numbers of commits, projects, users, etc.
    //
    //   * make available the structs from the downloader:
    //     * they provide basic information about objects like users, projects, commits
    //     * they are clunky to use for some queries, but you can construct any query you like using
    //       them and the Database trait, so if the api doesn't cover anything, this is the escape
    //       hatch.
    //
    // Intended use cases:
    //   * we create traits to add convenience methods to the downloader structs
    //     (alternatively, we can mask the downloader structs with these completely)
    //     * they provide methods that interpret metadata like get_stars, get_language
    //     * they provide methods that calculate the likely interesting properties of objects that
    //       are not expressed by their structs, for instance: project lifetime, number of projects
    //       a user committed to
    //
    //   * we provide some templates to run specific functions, this would be the expected way to
    //     construct queries, one that immediately comes to mind is:
    //
    //       group projects by X, for each X, select projects where Y, sort by Z, select N using
    //       method M, flatten to list of T
    //
    //     we provide a higher order function to do this and the user-programmer provides closures
    //     to appropriately achieve X, Y, Z, M, and T. We also provide as many canned closures as we
    //     can.
    //
    //     so an ideal query would fit canned and custom closures into one of the provided moulds,
    //     and should look something like this:
    //
    //         let result: Vec<ProjectID> =
    //            dataset
    //              .projects()
    //              .group_by(Project::Grouper::Language)
    //              .filter_each_by(Project::Filter::CommitFilter(AtLeast, 25))
    //              .sort_each_by(Project::Sorter::Stars)
    //              .sample_each_with(Projects::Sampler::Top(50))
    //              .map_each_to(Project::Selector::ProjectId)
    //              .flatten()
    //
    //     (just a prototype, don't know what I can realistically swing in Rust in terms of these HO
    //      functions)
    //
    //     alternatively, the user-programmer might not be satisfied with our canned "selectors"
    //     and they may substitute them with something else:
    //
    //         let result: Vec<ProjectID> =
    //            dataset
    //              .projects()
    //              .group_custom(|p| p.get_language())
    //              .filter_each_custom(|p| {
    //                  database
    //                      .commits_for_project(p)
    //                      .map(|c| c.committer() == "Linus Torvalds")
    //                      .count()
    //              })
    //              .sort_each_custom(|p1, p2| p1.get_project_name().cmp(p2.get_project_name()))
    //              .sample_each_custom(|projects| projects.choose_multiple(rng, 25).collect())
    //              .map_each_custom(|p| p.id)
    //              .flatten()
    //
    //     furthermore if our template doesn't suffice, the user-programmer can also use standard
    //     rust facilities to get what they need

    use dcd::{DCD, Database};

    // Benchmark Q1:
    // group projects by language
    // for each language
    //   *  select all projects where #commits > N=25
    //   *  sort by stars
    //   *  take top N=50
    // flatten to list of IDs
    #[test] fn top_stars_per_language() {
        let dataset = DCD::new("/dejavuii/dejacode/dataset-tiny".to_string());
        let projects = dataset.projects();
    }


}