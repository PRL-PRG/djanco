use crate::objects::*;
use crate::attrib::*;
use crate::data::*;
use crate::meta::*;

use crate::helpers;

use dcd::{DCD, Database};
use itertools::Itertools;
use crate::time::Seconds;

#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Id;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct URL;

#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Language;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Stars;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Issues;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct AllIssues;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct BuggyIssues;

#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Heads;
#[derive(Eq, PartialEq,       Clone, Hash)] pub struct Metadata(pub String);

#[derive(Eq, PartialEq,       Clone, Hash)] pub struct Commits;
#[derive(Eq, PartialEq,       Clone, Hash)] pub struct Users;
#[derive(Eq, PartialEq,       Clone, Hash)] pub struct Paths;

#[derive(Eq, PartialEq,       Clone, Hash)] pub struct CommitsWith<F>(pub F) where F: Filter<Entity=Commit>;
#[derive(Eq, PartialEq,       Clone, Hash)] pub struct UsersWith<F>(pub F)   where F: Filter<Entity=User>;
#[derive(Eq, PartialEq,       Clone, Hash)] pub struct PathsWith<F>(pub F)   where F: Filter<Entity=Path>;

#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Age;

impl Attribute for Id          {}
impl Attribute for URL         {}

impl Attribute for Language    {}
impl Attribute for Stars       {}
impl Attribute for Issues      {}
impl Attribute for AllIssues   {}
impl Attribute for BuggyIssues {}

impl Attribute for Heads       {}
impl Attribute for Metadata    {}

impl Attribute for Commits     {}
impl Attribute for Users       {}
impl Attribute for Paths       {}

impl<F> Attribute for CommitsWith<F> where F: Filter<Entity=Commit> {}
impl<F> Attribute for UsersWith<F>   where F: Filter<Entity=User>   {}
impl<F> Attribute for PathsWith<F>   where F: Filter<Entity=Path>   {}

impl Attribute for Age         {}

impl OptionalAttribute for Language {
    type Entity = Project;
    fn unknown(&self, _database: DataPtr, entity: &Self::Entity) -> bool {
        entity.language.is_some()
    }
}

impl OptionalAttribute for Stars {
    type Entity = Project;
    fn unknown(&self, _database: DataPtr, entity: &Self::Entity) -> bool {
        entity.stars.is_some()
    }
}

impl OptionalAttribute for Issues {
    type Entity = Project;
    fn unknown(&self, _database: DataPtr, entity: &Self::Entity) -> bool {
        entity.issues.is_some()
    }
}

impl OptionalAttribute for BuggyIssues {
    type Entity = Project;
    fn unknown(&self, _database: DataPtr, entity: &Self::Entity) -> bool {
        entity.buggy_issues.is_some()
    }
}

impl OptionalAttribute for AllIssues {
    type Entity = Project;
    fn unknown(&self, _database: DataPtr, entity: &Self::Entity) -> bool {
        entity.all_issues().is_some()
    }
}

impl OptionalAttribute for Age {
    type Entity = Project;
    fn unknown(&self, database: DataPtr, entity: &Self::Entity) -> bool {
        entity.age(database).is_some()
    }
}

impl ExistentialAttribute for Language {
    type Entity = Project;
    fn exists(&self, _database: DataPtr, entity: &Self::Entity) -> bool {
        entity.language.is_some()
    }
}

impl ExistentialAttribute for Stars {
    type Entity = Project;
    fn exists(&self, _database: DataPtr, entity: &Self::Entity) -> bool {
        entity.stars_or_zero() > 0
    }
}

impl ExistentialAttribute for Issues {
    type Entity = Project;
    fn exists(&self, _database: DataPtr, entity: &Self::Entity) -> bool {
        entity.issues_or_zero() > 0
    }
}

impl ExistentialAttribute for BuggyIssues {
    type Entity = Project;
    fn exists(&self, _database: DataPtr, entity: &Self::Entity) -> bool {
        entity.buggy_issues_or_zero() > 0
    }
}

impl ExistentialAttribute for AllIssues {
    type Entity = Project;
    fn exists(&self, _database: DataPtr, entity: &Self::Entity) -> bool {
        entity.all_issues_or_zero() > 0
    }
}

impl ExistentialAttribute for Heads {
    type Entity = Project;
    fn exists(&self, _database: DataPtr, entity: &Self::Entity) -> bool {
        !entity.heads.is_empty()
    }
}

// impl ExistentialAttribute for Commits {
//     type Entity = Project;
//     fn exists(&self, database: DataPtr, entity: &Self::Entity) -> bool {
//         untangle_mut!(database).commit_count_from(&entity.id) > 0
//     }
// }
//
// impl ExistentialAttribute for Users {
//     type Entity = Project;
//     fn exists(&self, database: DataPtr, entity: &Self::Entity) -> bool {
//         untangle_mut!(database).user_count_from(&entity.id) > 0
//     }
// }
//
// impl ExistentialAttribute for Paths {
//     type Entity = Project;
//     fn exists(&self, database: DataPtr, entity: &Self::Entity) -> bool {
//         untangle_mut!(database).path_count_from(&entity.id) > 0
//     }
// }

impl ExistentialAttribute for Age {
    type Entity = Project;
    fn exists(&self, database: DataPtr, entity: &Self::Entity) -> bool {
        entity.age(database).map_or(false, |e| e > Seconds(0))
    }
}

impl CollectionAttribute for Commits {
    type Entity = Project;
    type Item = Commit;
    fn items(&self, database: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        entity.commits(database)
    }
    fn len(&self, database: DataPtr, entity: &Self::Entity) -> usize {
        entity.commit_count(database)
    }
    fn parent_len(&self, database: DataPtr, entity: &Self::Entity) -> usize { self.len(database, entity) }
}

impl CollectionAttribute for Users {
    type Entity = Project;
    type Item = User;
    fn items(&self, database: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        entity.users(database)
    }
    fn len(&self, database: DataPtr, entity: &Self::Entity) -> usize {
        entity.user_count(database)
    }
    fn parent_len(&self, database: DataPtr, entity: &Self::Entity) -> usize { self.len(database, entity) }
}

impl CollectionAttribute for Paths {
    type Entity = Project;
    type Item = Path;
    fn items(&self, database: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        entity.paths(database)
    }
    fn len(&self, database: DataPtr, entity: &Self::Entity) -> usize {
        entity.path_count(database)
    }
    fn parent_len(&self, database: DataPtr, entity: &Self::Entity) -> usize { self.len(database, entity)  }
}

impl<F> CollectionAttribute for CommitsWith<F> where F: Filter<Entity=Commit> {
    type Entity = Project;
    type Item = Commit;
    fn items(&self, database: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        entity.commits(database.clone()).into_iter()
            .filter(|c| self.0.filter(database.clone(), c))
            .collect()
    }
    fn len(&self, database: DataPtr, entity: &Self::Entity) -> usize {
        entity.commits(database.clone()).into_iter()
            .filter(|c| self.0.filter(database.clone(), c))
            .count()
    }
    fn parent_len(&self, database: DataPtr, entity: &Self::Entity) -> usize {
        entity.commit_count(database)
    }
}

impl<F> CollectionAttribute for UsersWith<F> where F: Filter<Entity=User> {
    type Entity = Project;
    type Item = User;
    fn items(&self, database: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        entity.users(database.clone()).into_iter()
            .filter(|u| self.0.filter(database.clone(), u))
            .collect()
    }
    fn len(&self, database: DataPtr, entity: &Self::Entity) -> usize {
        entity.users(database.clone()).into_iter()
            .filter(|u| self.0.filter(database.clone(), u))
            .count()
    }
    fn parent_len(&self, database: DataPtr, entity: &Self::Entity) -> usize {
        entity.user_count(database)
    }
}

impl<F> CollectionAttribute for PathsWith<F> where F: Filter<Entity=Path> {
    type Entity = Project;
    type Item = Path;
    fn items(&self, database: DataPtr, entity: &Self::Entity) -> Vec<Self::Item> {
        entity.paths(database.clone()).into_iter()
            .filter(|p| self.0.filter(database.clone(), p))
            .collect()
    }
    fn len(&self, database: DataPtr, entity: &Self::Entity) -> usize {
        entity.paths(database.clone()).into_iter()
            .filter(|p| self.0.filter(database.clone(), p))
            .count()
    }
    fn parent_len(&self, database: DataPtr, entity: &Self::Entity) -> usize {
        entity.path_count(database)
    }
}

impl StringAttribute for Id {
    type Entity = Project;
    fn extract(&self, _database: DataPtr, entity: &Self::Entity) -> String {
        entity.id.to_string()
    }
}

impl StringAttribute for URL {
    type Entity = Project;
    fn extract(&self, _database: DataPtr, entity: &Self::Entity) -> String {
        entity.url.clone()
    }
}

impl StringAttribute for Language {
    type Entity = Project;
    fn extract(&self, _database: DataPtr, entity: &Self::Entity) -> String {
        entity.language_or_empty()
    }
}

impl StringAttribute for Stars {
    type Entity = Project;
    fn extract(&self, _database: DataPtr, entity: &Self::Entity) -> String {
        entity.stars.map_or(String::new(), |e| e.to_string())
    }
}

impl StringAttribute for Issues {
    type Entity = Project;
    fn extract(&self, _database: DataPtr, entity: &Self::Entity) -> String {
        entity.issues.map_or(String::new(), |e| e.to_string())
    }
}

impl StringAttribute for AllIssues {
    type Entity = Project;
    fn extract(&self, _database: DataPtr, entity: &Self::Entity) -> String {
        entity.all_issues().map_or(String::new(), |e| e.to_string())
    }
}

impl StringAttribute for BuggyIssues {
    type Entity = Project;
    fn extract(&self, _database: DataPtr, entity: &Self::Entity) -> String {
        entity.buggy_issues.map_or(String::new(), |e| e.to_string())
    }
}

impl StringAttribute for Age {
    type Entity = Project;
    fn extract(&self, database: DataPtr, entity: &Self::Entity) -> String {
        entity.age(database).as_ref().map_or(String::new(), |e| e.to_string())
    }
}

impl NumericalAttribute for Id {
    type Entity = Project;
    type Number = usize;
    fn calculate(&self, _database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(entity.id.into())
    }
}

impl NumericalAttribute for Stars {
    type Entity = Project;
    type Number = usize;
    fn calculate(&self, _database: DataPtr, entity: &Self::Entity) ->  Option<Self::Number> {
        entity.stars
    }
}

impl NumericalAttribute for Issues {
    type Entity = Project;
    type Number = usize;
    fn calculate(&self, _database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        entity.issues
    }
}

impl NumericalAttribute for AllIssues {
    type Entity = Project;
    type Number = usize;
    fn calculate(&self, _database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        entity.all_issues()
    }
}

impl NumericalAttribute for BuggyIssues {
    type Entity = Project;
    type Number = usize;
    fn calculate(&self, _database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        entity.buggy_issues
    }
}

impl NumericalAttribute for Heads {
    type Entity = Project;
    type Number = usize;
    fn calculate(&self, _database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(entity.heads.len())
    }
}

impl NumericalAttribute for Metadata {
    type Entity = Project;
    type Number = usize;
    fn calculate(&self, _database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(entity.metadata.len())
    }
}

impl NumericalAttribute for Commits {
    type Entity = Project;
    type Number = usize;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(untangle_mut!(database).commit_count_from(&entity.id))
    }
}

impl NumericalAttribute for Users {
    type Entity = Project;
    type Number = usize;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(untangle_mut!(database).user_count_from(&entity.id))
    }
}

impl NumericalAttribute for Paths {
    type Entity = Project;
    type Number = usize;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(untangle_mut!(database).path_count_from(&entity.id))
    }
}

impl<F> NumericalAttribute for CommitsWith<F> where F: Filter<Entity=Commit> {
    type Entity = Project;
    type Number = usize;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        //Some(untangle_mut!(database).path_count_from(&entity.id))
        Some(entity.commits(database.clone()).into_iter()
            .filter(|c| self.0.filter(database.clone(), c))
            .count())
    }
}

impl<F> NumericalAttribute for UsersWith<F> where F: Filter<Entity=User> {
    type Entity = Project;
    type Number = usize;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(entity.users(database.clone()).into_iter()
            .filter(|u| self.0.filter(database.clone(), u))
            .count())
    }
}

impl<F> NumericalAttribute for PathsWith<F> where F: Filter<Entity=Path> {
    type Entity = Project;
    type Number = usize;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        Some(entity.paths(database.clone()).into_iter()
                .filter(|p| self.0.filter(database.clone(), p))
                .count())
    }
}

impl NumericalAttribute for Age {
    type Entity = Project;
    type Number = Seconds;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> Option<Self::Number> {
        entity.age(database)
    }
}

impl Group<Project> for Id {
    type Key = ProjectId;
    fn select(&self, _: DataPtr, project: &Project) -> Self::Key {
        project.id
    }
}

impl Group<Project> for Language {
    type Key = AttributeValue<Self, String>;
    fn select(&self, _: DataPtr, project: &Project) -> Self::Key {
        AttributeValue::new(self, project.language_or_empty())
    }
}

impl Group<Project> for Stars {
    type Key = AttributeValue<Self, usize>;
    fn select(&self, _: DataPtr, project: &Project) -> Self::Key {
        AttributeValue::new(self, project.stars_or_zero())
    }
}

impl Group<Project> for Issues {
    type Key = AttributeValue<Self, usize>;
    fn select(&self, _: DataPtr, project: &Project) -> Self::Key {
        AttributeValue::new(self, project.issues_or_zero())
    }
}

impl Group<Project> for AllIssues {
    type Key = AttributeValue<Self, usize>;
    fn select(&self, _: DataPtr, project: &Project) -> Self::Key {
        AttributeValue::new(self, project.all_issues_or_zero())
    }
}

impl Group<Project> for BuggyIssues {
    type Key = AttributeValue<Self, usize>;
    fn select(&self, _: DataPtr, project: &Project) -> Self::Key {
        AttributeValue::new(self, project.buggy_issues_or_zero())
    }
}

impl Group<Project> for Age {
    type Key = AttributeValue<Self, Seconds>;
    fn select(&self, data: DataPtr, project: &Project) -> Self::Key {
        AttributeValue::new(self, project.age(data).unwrap_or(Seconds(0)))
    }
}

impl Sort<Project> for Id {
    fn execute(&mut self, _: DataPtr, vector: Vec<Project>, direction: sort::Direction) -> Vec<Project> {
        sort::Sorter::from(vector, direction).sort_by_key(|p: &Project| p.id)
    }
}

impl Sort<Project> for URL {
    fn execute(&mut self, _: DataPtr, vector: Vec<Project>, direction: sort::Direction) -> Vec<Project> {
        sort::Sorter::from(vector, direction).sort_by( |p1, p2| p1.url.cmp(&p2.url))
    }
}

impl Sort<Project> for Language {
    fn execute(&mut self, _: DataPtr, vector: Vec<Project>, direction: sort::Direction) -> Vec<Project> {
        sort::Sorter::from(vector, direction).sort_by_key(|p| p.language.clone())
    }
}

impl Sort<Project> for Stars {
    fn execute(&mut self, _: DataPtr, vector: Vec<Project>, direction: sort::Direction) -> Vec<Project> {
        //sort::Sorter::from(vector, direction).sort_by_key(|p| p.stars)
        sort::Sorter::from(vector, direction).sort_by(|p1, p2| helpers::opt_cmp(p1.stars,p2.stars))
    }
}

impl Sort<Project> for Issues {
    fn execute(&mut self, _: DataPtr, vector: Vec<Project>, direction: sort::Direction) -> Vec<Project> {
        //sort::Sorter::from(vector, direction).sort_by_key(|f| f.issues)
        sort::Sorter::from(vector, direction).sort_by(|p1, p2| helpers::opt_cmp(p1.issues,p2.issues))
    }
}

impl Sort<Project> for AllIssues {
    fn execute(&mut self, _: DataPtr, vector: Vec<Project>, direction: sort::Direction) -> Vec<Project> {
        //sort::Sorter::from(vector, direction).sort_by_key(|f| f.all_issues())
        sort::Sorter::from(vector, direction).sort_by(|p1, p2| helpers::opt_cmp(p1.all_issues(),p2.all_issues()))
    }
}

impl Sort<Project> for BuggyIssues {
    fn execute(&mut self, _: DataPtr, vector: Vec<Project>, direction: sort::Direction) -> Vec<Project> {
        //sort::Sorter::from(vector, direction).sort_by_key(|p| p.buggy_issues)
        sort::Sorter::from(vector, direction).sort_by(|p1, p2| helpers::opt_cmp(p1.buggy_issues,p2.buggy_issues))
    }
}

impl Sort<Project> for Heads {
    fn execute(&mut self, _: DataPtr, vector: Vec<Project>, direction: sort::Direction) -> Vec<Project> {
        sort::Sorter::from(vector, direction).sort_by_key(|p| p.heads.len())
    }
}

impl Sort<Project> for Metadata {
    fn execute(&mut self, _: DataPtr, vector: Vec<Project>, direction: sort::Direction) -> Vec<Project> {
        sort::Sorter::from(vector, direction).sort_by( |p1, p2| {
            p1.metadata.get(&self.0).cmp(&p2.metadata.get(&self.0))
        })
    }
}

impl Sort<Project> for Commits {
    fn execute(&mut self, data: DataPtr, vector: Vec<Project>, direction: sort::Direction) -> Vec<Project> {
        sort::Sorter::from(vector, direction).sort_by_key(|p| untangle_mut!(data).commit_count_from(&p.id))
    }
}

impl Sort<Project> for Users {
    fn execute(&mut self, data: DataPtr, vector: Vec<Project>, direction: sort::Direction) -> Vec<Project> {
        sort::Sorter::from(vector, direction).sort_by_key(|p| untangle_mut!(data).user_count_from(&p.id))
    }
}

impl Sort<Project> for Paths {
    fn execute(&mut self, data: DataPtr, vector: Vec<Project>, direction: sort::Direction) -> Vec<Project> {
        sort::Sorter::from(vector, direction).sort_by_key(|p| untangle_mut!(data).path_count_from(&p.id))
    }
}

impl Sort<Project> for Age {
    fn execute(&mut self, data: DataPtr, vector: Vec<Project>, direction: sort::Direction) -> Vec<Project> {
        sort::Sorter::from(vector, direction).sort_by_key(|p| p.age(data.clone()))
    }
}

impl<F> Sort<Project> for CommitsWith<F> where F: Filter<Entity=Commit> {
    fn execute(&mut self, data: DataPtr, vector: Vec<Project>, direction: sort::Direction) -> Vec<Project> {
        sort::Sorter::from(vector, direction).sort_by_key(|p| {
           p.commits(data.clone())
               .iter().filter(|c| self.0.filter(data.clone(), c)).count()
        })
    }
}

impl<F> Sort<Project> for UsersWith<F> where F: Filter<Entity=User> {
    fn execute(&mut self, data: DataPtr, vector: Vec<Project>, direction: sort::Direction) -> Vec<Project> {
        sort::Sorter::from(vector, direction).sort_by_key(|p| {
           p.users(data.clone())
                   .iter().filter(|u|self.0.filter(data.clone(), u)).count()
        })
    }
}

impl<F> Sort<Project> for PathsWith<F> where F: Filter<Entity=Path> {
    fn execute(&mut self, data: DataPtr, vector: Vec<Project>, direction: sort::Direction) -> Vec<Project> {
        sort::Sorter::from(vector, direction).sort_by_key(|p| {
           p.paths(data.clone())
               .iter().filter(|p| self.0.filter(data.clone(), p)).count()
        })
    }
}


impl Select<Project> for Id {
    type Entity = AttributeValue<Id, ProjectId>;
    fn select(&self, _: DataPtr, project: Project) -> Self::Entity {
        AttributeValue::new(self, ProjectId::from(project.id))
    }
}

impl Select<Project> for URL {
    type Entity = AttributeValue<URL, String>;
    fn select(&self, _: DataPtr, project: Project) -> Self::Entity {
        AttributeValue::new(self, project.url)
    }
}

impl Select<Project> for Language {
    type Entity = AttributeValue<Language, Option<String>>;
    fn select(&self, _: DataPtr, project: Project) -> Self::Entity {
        AttributeValue::new(self, project.language)
    }
}

impl Select<Project> for Stars {
    type Entity = AttributeValue<Stars, Option<usize>>;
    fn select(&self, _: DataPtr, project: Project) -> Self::Entity {
        AttributeValue::new(self, project.stars)
    }
}

impl Select<Project> for Issues {
    type Entity = AttributeValue<Issues, Option<usize>>;
    fn select(&self, _: DataPtr, project: Project) -> Self::Entity {
        AttributeValue::new(self, project.issues)
    }
}

impl Select<Project> for AllIssues {
    type Entity = AttributeValue<AllIssues, Option<usize>>;
    fn select(&self, _: DataPtr, project: Project) -> Self::Entity {
        AttributeValue::new(self, project.all_issues())
    }
}

impl Select<Project> for BuggyIssues {
    type Entity = AttributeValue<BuggyIssues, Option<usize>>;
    fn select(&self, _: DataPtr, project: Project) -> Self::Entity {
        AttributeValue::new(self, project.buggy_issues)
    }
}

impl Select<Project> for Heads {
    type Entity = AttributeValue<Heads, Vec<(String, CommitId)>>; // TODO maybe make Head object type
    fn select(&self, _: DataPtr, project: Project) -> Self::Entity {
        AttributeValue::new(self, project.heads.clone())
    }
}

// impl Select<Project> for Metadata {
//     //type Entity = AttributeValue<Metadata, Option<String>>;
//     type Entity = Option<String>;
//     fn select(&self, _: DataPtr, project: Project) -> Self::Entity {
//         //AttributeValue::new(self, project.metadata.get(&self.0).map(|s| s.clone()))
//         project.metadata.get(&self.0).map(|s| s.clone())
//     }
// }

impl Select<Project> for Commits {
    type Entity = Vec<Commit>;
    fn select(&self, database: DataPtr, project: Project) -> Self::Entity {
        untangle_mut!(database).commits_from(&project.id)
    }
}

impl<F> Select<Project> for CommitsWith<F> where F: Filter<Entity=Commit> {
    type Entity = Vec<Commit>;
    fn select(&self, database: DataPtr, project: Project) -> Self::Entity {
        untangle_mut!(database).commits_from(&project.id)
            .into_iter()
            .filter(|c| self.0.filter(database.clone(), &c))
            .collect()
    }
}

impl Select<Project> for Users {
    type Entity = Vec<User>;
    fn select(&self, database: DataPtr, project: Project) -> Self::Entity {
        untangle_mut!(database).users_from(&project.id)
    }
}

impl<F> Select<Project> for UsersWith<F> where F: Filter<Entity=User> {
    type Entity = Vec<User>;
    fn select(&self, database: DataPtr, project: Project) -> Self::Entity {
        untangle_mut!(database).users_from(&project.id)
            .into_iter()
            .filter(|u| self.0.filter(database.clone(), &u))
            .collect()
    }
}

impl Select<Project> for Paths {
    type Entity = Vec<Path>;
    fn select(&self, database: DataPtr, project: Project) -> Self::Entity {
        untangle_mut!(database).paths_from(&project.id)
    }
}

impl<F> Select<Project> for PathsWith<F> where F: Filter<Entity=Path> {
    type Entity = Vec<Path>;
    fn select(&self, database: DataPtr, project: Project) -> Self::Entity {
        untangle_mut!(database).paths_from(&project.id)
            .into_iter()
            .filter(|p| self.0.filter(database.clone(), &p))
            .collect()
    }
}

impl Select<Project> for Age {
    type Entity = Seconds;
    fn select(&self, database: DataPtr, project: Project) -> Self::Entity {
        project.age(database).unwrap_or(Seconds(0))
    }
}

impl raw::StringAttribute for Id {
    type Entity = dcd::Project;
    fn extract(&self, _database: &DCD, project_id: &u64, _commit_ids: &Vec<u64>) -> String {
        project_id.to_string()
    }
}

impl raw::StringAttribute for URL {
    type Entity = dcd::Project;
    fn extract(&self, database: &DCD, project_id: &u64, _commit_ids: &Vec<u64>) -> String {
        database.get_project(*project_id)
            .map(|p| p.url)
            .unwrap_or(String::new())
    }
}

impl raw::StringAttribute for Language {
    type Entity = dcd::Project;
    fn extract(&self, database: &DCD, project_id: &u64, _commit_ids: &Vec<u64>) -> String {
        database.get_project(*project_id)
            .map(|p| p.get_language())
            .flatten()
            .unwrap_or(String::new())
    }
}

impl raw::StringAttribute for Stars {
    type Entity = dcd::Project;
    fn extract(&self, database: &DCD, project_id: &u64, _commit_ids: &Vec<u64>) -> String {
        database.get_project(*project_id)
            .map(|p| p.get_stars())
            .flatten()
            .map_or(String::new(), |e| e.to_string())
    }
}

impl raw::StringAttribute for Issues {
    type Entity = dcd::Project;
    fn extract(&self, database: &DCD, project_id: &u64, _commit_ids: &Vec<u64>) -> String {
        database.get_project(*project_id)
            .map(|p| p.get_issue_count())
            .flatten()
            .map_or(String::new(), |e| e.to_string())
    }
}

impl raw::StringAttribute for AllIssues {
    type Entity = dcd::Project;
    fn extract(&self, database: &DCD, project_id: &u64, _commit_ids: &Vec<u64>) -> String {
        database.get_project(*project_id)
            .map(|p| p.get_all_issue_count())
            .flatten()
            .map_or(String::new(), |e| e.to_string())
    }
}

impl raw::StringAttribute for BuggyIssues {
    type Entity = dcd::Project;
    fn extract(&self, database: &DCD, project_id: &u64, _commit_ids: &Vec<u64>) -> String {
        database.get_project(*project_id)
            .map(|p| p.get_buggy_issue_count())
            .flatten()
            .map_or(String::new(), |e| e.to_string())
    }
}

impl raw::NumericalAttribute for Id {
    type Entity = dcd::Project;
    fn calculate(&self, _database: &DCD, project_id: &u64, _commit_ids: &Vec<u64>) -> usize {
        *project_id as usize
    }
}

impl raw::NumericalAttribute for Stars {
    type Entity = dcd::Project;
    fn calculate(&self, database: &DCD, project_id: &u64, _commit_ids: &Vec<u64>) -> usize {
        database.get_project(*project_id)
            .map_or(0usize, |p| p.get_stars_or_zero() as usize)
    }
}

impl raw::NumericalAttribute for Issues {
    type Entity = dcd::Project;
    fn calculate(&self, database: &DCD, project_id: &u64, _commit_ids: &Vec<u64>) -> usize {
        database.get_project(*project_id)
            .map_or(0usize, |p| p.get_issue_count_or_zero() as usize)
    }
}

impl raw::NumericalAttribute for AllIssues {
    type Entity = dcd::Project;
    fn calculate(&self, database: &DCD, project_id: &u64, _commit_ids: &Vec<u64>) -> usize {
        database.get_project(*project_id)
            .map_or(0usize, |p| p.get_all_issue_count_or_zero() as usize)
    }
}

impl raw::NumericalAttribute for BuggyIssues {
    type Entity = Project;
    fn calculate(&self, database: &DCD, project_id: &u64, _commit_ids: &Vec<u64>) -> usize {
        database.get_project(*project_id)
            .map_or(0usize, |p| p.get_buggy_issue_count_or_zero() as usize)
    }
}

impl raw::NumericalAttribute for Heads {
    type Entity = dcd::Project;
    fn calculate(&self, database: &DCD, project_id: &u64, _commit_ids: &Vec<u64>) -> usize {
        database.get_project(*project_id).map_or(0usize, |p| p.heads.len())
    }
}

impl raw::NumericalAttribute for Metadata {
    type Entity = dcd::Project;
    fn calculate(&self, database: &DCD, project_id: &u64, _commit_ids: &Vec<u64>) -> usize {
        database.get_project(*project_id).map_or(0usize, |p| p.metadata.len())
    }
}

impl raw::NumericalAttribute for Commits {
    type Entity = dcd::Project;
    fn calculate(&self, _database: &DCD, _project_id: &u64, commit_ids: &Vec<u64>) -> usize {
        commit_ids.len()
    }
}

impl raw::NumericalAttribute for Users {
    type Entity = Project;
    fn calculate(&self, database: &DCD, _project_id: &u64, commit_ids: &Vec<u64>) -> usize {
        commit_ids.iter()
            .flat_map(|id| database.get_commit(*id))
            .flat_map(|c| vec![c.author_id, c.committer_id])
            .unique()
            .count()
    }
}

impl raw::NumericalAttribute for Paths {
    type Entity = dcd::Project;
    fn calculate(&self, database: &DCD, _project_id: &u64, commit_ids: &Vec<u64>) -> usize {
        commit_ids.iter()
            .flat_map(|id| database.get_commit(*id))
            .flat_map(|c| c.changes.map_or(vec![], |changes| {
                changes.iter().map(|(path, _)| *path).unique().collect()
            }))
            .unique()
            .count()
    }
}