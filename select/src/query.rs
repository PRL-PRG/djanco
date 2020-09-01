use dcd::{Database, Project};
use crate::query::project::{Group, Property, GroupKey};
use itertools::Itertools;
use crate::meta::{ProjectMeta, MetaDatabase};
use std::time::Duration;
use std::io::Error;
use std::path::PathBuf;
use std::fs::{create_dir_all, File};
use std::io::Write;
use crate::dump::DumpFrom;

pub mod project {
    #[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
    pub enum TimeResolution { Days, Months, Years }

    impl TimeResolution {
        pub fn as_secs(&self) -> u64 {
            match self {
                TimeResolution::Days   => 60/*sec*/ * 60/*min*/ * 24/*hr*/,
                TimeResolution::Months => 60/*sec*/ * 60/*min*/ * 24/*hr*/ * 30/*days*/,
                TimeResolution::Years  => 60/*sec*/ * 60/*min*/ * 24/*hr*/ * 365/*days*/,
            }
        }
    }

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
        //Conjunction(Group, Group),
        //Conjunction(Vec<Group>),
    }

    #[derive(Debug, Clone)]
    pub enum GroupKey {
        // Things we can read directly
        TimeOfLastUpdate(i64),

        // Things we can read from metadata
        Language(String),
        Stars(u64),

        // Things we can count
        Commits(usize),
        Paths(usize),
        Heads(usize),
        Authors(usize),
        Committers(usize),
        Users(usize),

        // Things we can calculate or derive
        Duration { time: u64, resolution: TimeResolution },

        // Complex groupings
        //Conjunction(GroupKey, GroupKey),
        //Conjunction(Vec<GroupKey>),
    }
}

impl project::Group {

    pub fn group_this<'a, I, D>(&self, source: I /*, database: &'a D*/) -> Box<dyn Iterator<Item=(GroupKey, Vec<Project>)>>
        where I: DatabaseIterator<'a, Project, D>,  D: 'a + Database {

        let database = source.get_database();

        macro_rules! group_by {
           ($f:expr, $key_mapper:expr) => {Box::new(
                source
                  .map(|e: Project| (e, database))
                  .map(|(p, db): (Project, &D)| ($f(&p, db), p))
                  .into_group_map().into_iter()
                  .map(move |(k, g)| ($key_mapper(k), g))
           )}
        }

        let boxed_iter: Box<dyn Iterator<Item=(GroupKey,Vec<Project>)> + '_> =
            match self {
                Group::TimeOfLastUpdate            => group_by!(|p: &Project, _| p.last_update,             |k| GroupKey::TimeOfLastUpdate(k)),
                Group::Language                    => group_by!(|p: &Project, _| p.get_language_or_empty(), |k| GroupKey::Language(k)),
                Group::Stars                       => group_by!(|p: &Project, _| p.get_stars_or_zero(),     |k| GroupKey::Stars(k)),

                Group::Count(Property::Heads)      => group_by!(|p: &Project,  _| p.get_head_count(),           |k| GroupKey::Heads(k)),
                Group::Count(Property::Commits)    => group_by!(|p: &Project, db| p.get_commit_count_in(db),    |k| GroupKey::Commits(k)),
                Group::Count(Property::Paths)      => group_by!(|p: &Project, db| p.get_path_count_in(db),      |k| GroupKey::Paths(k)),
                Group::Count(Property::Committers) => group_by!(|p: &Project, db| p.get_committer_count_in(db), |k| GroupKey::Committers(k)),
                Group::Count(Property::Authors)    => group_by!(|p: &Project, db| p.get_author_count_in(db),    |k| GroupKey::Authors(k)),
                Group::Count(Property::Users)      => group_by!(|p: &Project, db| p.get_user_count_in(db),      |k| GroupKey::Users(k)),

                Group::Duration(resolution) => {
                    let resolution = *resolution;
                    group_by!(|p: &Project, db| p.get_age(db).map_or(0u64,  |d: Duration| d.as_secs()) / resolution.as_secs(),
                          |k| GroupKey::Duration { time: k, resolution: resolution })
                },

                // Group::Conjunction(group1, group2) => {
                //     //group_by!(|(p, db)| (()  ,p)),
                //
                //
                //     unimplemented!()
                // },
            };

        boxed_iter
    }
}

pub trait DatabaseIterator<'a, T, D>: Iterator<Item=T>{
    fn get_database(&self) -> &'a D;
}

pub trait ProjectQuery<'a, I: DatabaseIterator<'a, Project, D>, D> where D: 'a + MetaDatabase {
    fn group_by(self, group: project::Group) -> ProjectGroups<'a, D>;
}

impl<'a, I, D> ProjectQuery<'a, I, D> for I where I: DatabaseIterator<'a, Project, D>, D: 'a + MetaDatabase {
    fn group_by(self, group: project::Group) -> ProjectGroups<'a, D> { // FIXME remove database from parameters... somehow
        let database =  self.get_database(); // grab ref before move
        ProjectGroups { data: group.group_this(self), database }
    }
}

pub struct ProjectGroups<'a, D: MetaDatabase> {
    data: Box<dyn Iterator<Item=(GroupKey,Vec<Project>)>>,
    database: &'a D,
}

impl<'a, D> ProjectGroups<'a, D> where D: MetaDatabase  {
   pub fn new(data: Box<dyn Iterator<Item=(GroupKey,Vec<Project>)>>, database: &'a D) -> ProjectGroups<'a, D> {
       ProjectGroups{ data, database }
   }
}

impl<'a, D> Iterator for ProjectGroups<'a, D> where D: MetaDatabase {
    type Item = (GroupKey, Vec<Project>);
    fn next(&mut self) -> Option<Self::Item> { self.data.next() }
}

impl<'a, D> DatabaseIterator<'a, (GroupKey, Vec<Project>), D> for ProjectGroups<'a, D> where D: MetaDatabase {
    fn get_database(&self) -> &'a D { self.database }
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

        //let vector: Vec<(GroupKey, Vec<Project>)> = vec![];
        //println!("{:?}",vector.iter());
        assert!(false)


    }
}

fn write_projects_to_file<'a, I, D, F>(projects: &mut I, path: &PathBuf, formatter: F) -> Result<(), Error>
    where I: DatabaseIterator<'a, &'a Project, D>,
          D: 'a + MetaDatabase,
          F: Fn(&Project) -> Result<String, Error> {

    let dir_path = {
        let mut dir_path = path.clone();
        dir_path.pop();
        dir_path
    };
    create_dir_all(&dir_path).unwrap();

    let mut file = File::create(path)?;
    for project in projects {
        writeln!(file, "{}", formatter(&project)?)?;
    }
    Ok(())
}

pub trait ProjectOutputFormatter<'a, D> where D: 'a + MetaDatabase {
    fn write_ids_to_csv(&mut self, path: &PathBuf) -> Result<(), Error>;
    fn write_urls_to_csv(&mut self, path: &PathBuf) -> Result<(), Error>;
    fn write_all_you_know_to_dir(&mut self, dir: &PathBuf) -> Result<(), Error>;
    fn write_artifact_input_to_csv(&mut self, path: &PathBuf) -> Result<(), Error>;
}

impl<'a, I, D> ProjectOutputFormatter<'a, D> for I where I: DatabaseIterator<'a, &'a Project, D>, D: 'a + MetaDatabase {
    fn write_ids_to_csv(&mut self, path: &PathBuf) -> Result<(), Error> {
        let formatter = |project: &Project| { Ok(project.id.to_string()) };
        write_projects_to_file(self, path, formatter)
    }

    fn write_urls_to_csv(&mut self, path: &PathBuf) -> Result<(), Error> {
        let formatter = |project: &Project| { Ok(project.url.clone()) };
        write_projects_to_file(self, path, formatter)
    }

    fn write_all_you_know_to_dir(&mut self, dir: &PathBuf) -> Result<(), Error> {
        self.get_database().dump_all_info_about(self, dir) // TODO clean up
    }

    fn write_artifact_input_to_csv(&mut self, _path: &PathBuf) -> Result<(), Error> {
        let _database = self.get_database();
        // TODO Peta's stuff goes here
        unimplemented!()
    }
}