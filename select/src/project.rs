use crate::objects::*;
use crate::attrib::*;
use crate::data::*;
use crate::meta::*;

use dcd::{DCD, Database};
use itertools::Itertools;

#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Id;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct URL;

#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Language;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Stars;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Issues;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct BuggyIssues;

#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Heads;
#[derive(Eq, PartialEq,       Clone, Hash)] pub struct Metadata(pub String);

#[derive(Eq, PartialEq,       Clone, Hash)] pub struct Commits;
#[derive(Eq, PartialEq,       Clone, Hash)] pub struct Users;
#[derive(Eq, PartialEq,       Clone, Hash)] pub struct Paths;

impl Attribute for Id          {}
impl Attribute for URL         {}

impl Attribute for Language    {}
impl Attribute for Stars       {}
impl Attribute for Issues      {}
impl Attribute for BuggyIssues {}

impl Attribute for Heads       {}
impl Attribute for Metadata    {}

impl Attribute for Commits     {}
impl Attribute for Users       {}
impl Attribute for Paths       {}

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

impl StringAttribute for BuggyIssues {
    type Entity = Project;
    fn extract(&self, _database: DataPtr, entity: &Self::Entity) -> String {
        entity.buggy_issues.map_or(String::new(), |e| e.to_string())
    }
}

impl NumericalAttribute for Id {
    type Entity = Project;
    fn calculate(&self, _database: DataPtr, entity: &Self::Entity) -> usize {
        entity.id.into()
    }
}

impl NumericalAttribute for Stars {
    type Entity = Project;
    fn calculate(&self, _database: DataPtr, entity: &Self::Entity) -> usize {
        entity.stars.map_or(0usize, |n| n as usize)
    }
}

impl NumericalAttribute for Issues {
    type Entity = Project;
    fn calculate(&self, _database: DataPtr, entity: &Self::Entity) -> usize {
        entity.issues.map_or(0usize, |n| n as usize)
    }
}

impl NumericalAttribute for BuggyIssues {
    type Entity = Project;
    fn calculate(&self, _database: DataPtr, entity: &Self::Entity) -> usize {
        entity.buggy_issues.map_or(0usize, |n| n as usize)
    }
}

impl NumericalAttribute for Heads {
    type Entity = Project;
    fn calculate(&self, _database: DataPtr, entity: &Self::Entity) -> usize {
        entity.heads.len()
    }
}

impl NumericalAttribute for Metadata {
    type Entity = Project;
    fn calculate(&self, _database: DataPtr, entity: &Self::Entity) -> usize {
        entity.metadata.len()
    }
}

impl NumericalAttribute for Commits {
    type Entity = Project;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> usize {
        untangle_mut!(database).commit_count_from(&entity.id)
    }
}

impl NumericalAttribute for Users {
    type Entity = Project;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> usize {
        untangle_mut!(database).user_count_from(&entity.id)
    }
}

impl NumericalAttribute for Paths {
    type Entity = Project;
    fn calculate(&self, database: DataPtr, entity: &Self::Entity) -> usize {
        untangle_mut!(database).path_count_from(&entity.id)
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

impl Group<Project> for BuggyIssues {
    type Key = AttributeValue<Self, usize>;
    fn select(&self, _: DataPtr, project: &Project) -> Self::Key {
        AttributeValue::new(self, project.buggy_issues_or_zero())
    }
}

impl Sort<Project> for Id {
    fn execute(&mut self, _: DataPtr, mut vector: Vec<Project>) -> Vec<Project> {
        vector.sort_by_key(|p| p.id);
        vector
    }
}

impl Sort<Project> for URL {
    fn execute(&mut self, _: DataPtr, mut vector: Vec<Project>) -> Vec<Project> {
        vector.sort_by(|p1, p2| p1.url.cmp(&p2.url));
        vector
    }
}

impl Sort<Project> for Language {
    fn execute(&mut self, _: DataPtr, mut vector: Vec<Project>) -> Vec<Project> {
        vector.sort_by_key(|p| p.language.clone()); vector
    }
}

impl Sort<Project> for Stars {
    fn execute(&mut self, _: DataPtr, mut vector: Vec<Project>) -> Vec<Project> {
        vector.sort_by_key(|p| p.stars); vector
    }
}

impl Sort<Project> for Issues {
    fn execute(&mut self, _: DataPtr, mut vector: Vec<Project>) -> Vec<Project> {
        vector.sort_by_key(|f| f.issues); vector
    }
}

impl Sort<Project> for BuggyIssues {
    fn execute(&mut self, _: DataPtr, mut vector: Vec<Project>) -> Vec<Project> {
        vector.sort_by_key(|p| p.buggy_issues); vector
    }
}

impl Sort<Project> for Heads {
    fn execute(&mut self, _: DataPtr, mut vector: Vec<Project>) -> Vec<Project> {
        vector.sort_by_key(|p| p.heads.len()); vector
    }
}

impl Sort<Project> for Metadata {
    fn execute(&mut self, _: DataPtr, mut vector: Vec<Project>) -> Vec<Project> {
        vector.sort_by(|p1, p2| {
            p1.metadata.get(&self.0).cmp(&p2.metadata.get(&self.0))
        });
        vector
    }
}

impl Sort<Project> for Commits {
    fn execute(&mut self, data: DataPtr, mut vector: Vec<Project>) -> Vec<Project> {
        vector.sort_by_key(|p| untangle_mut!(data).commit_count_from(&p.id));
        vector
    }
}

impl Sort<Project> for Users {
    fn execute(&mut self, data: DataPtr, mut vector: Vec<Project>) -> Vec<Project> {
        vector.sort_by_key(|p| untangle_mut!(data).user_count_from(&p.id));
        vector
    }
}

impl Sort<Project> for Paths {
    fn execute(&mut self, data: DataPtr, mut vector: Vec<Project>) -> Vec<Project> {
        vector.sort_by_key(|p| untangle_mut!(data).path_count_from(&p.id));
        vector
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

impl Select<Project> for BuggyIssues {
    type Entity = AttributeValue<BuggyIssues, Option<usize>>;
    fn select(&self, _: DataPtr, project: Project) -> Self::Entity {
        AttributeValue::new(self, project.buggy_issues)
    }
}

impl Select<Project> for Heads {
    type Entity = AttributeValue<Heads, usize>;
    fn select(&self, _: DataPtr, project: Project) -> Self::Entity {
        AttributeValue::new(self, project.heads.len())
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
    type Entity = AttributeValue<Commits, usize>;
    fn select(&self, database: DataPtr, project: Project) -> Self::Entity {
        AttributeValue::new(self, untangle_mut!(database).commit_count_from(&project.id))
    }
}

impl Select<Project> for Users {
    type Entity = AttributeValue<Users, usize>;
    fn select(&self, database: DataPtr, project: Project) -> Self::Entity {
        AttributeValue::new(self, untangle_mut!(database).user_count_from(&project.id))
    }
}

impl Select<Project> for Paths {
    type Entity = AttributeValue<Paths, usize>;
    fn select(&self, database: DataPtr, project: Project) -> Self::Entity {
        AttributeValue::new(self, untangle_mut!(database).path_count_from(&project.id))
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