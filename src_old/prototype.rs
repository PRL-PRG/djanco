use regex::Regex;

use crate::objects;
use crate::data::DataPtr;

#[derive(Clone)]
pub enum Stringy { String(String), Regex(Regex) }

impl Stringy {
    pub fn eq(&self, other: &String) -> bool {
        match self {
            Stringy::String(string) => string.eq(other),
            Stringy::Regex(regex) => regex.is_match(other),
        }
    }
}

#[derive(Clone)]
pub struct Project {
    pub id:           Option<objects::ProjectId>,
    pub url:          Option<Stringy>,
    pub last_update:  Option<i64>,
    pub language:     Option<Option<Stringy>>,
    pub stars:        Option<Option<usize>>,
    pub issues:       Option<Option<usize>>,
    pub buggy_issues: Option<Option<usize>>,
}

impl Project {
    pub fn new() -> Self {
        Project {
            id: None, url: None, last_update: None,
            language: None, stars: None,
            issues: None, buggy_issues: None
        }
    }
}

impl Project {
    pub fn with_id<N>(mut self, id: N) -> Project where N: Into<objects::ProjectId> { self.id = Some(id.into()); self }
    pub fn with_url<S>(mut self, url: S) -> Project where S: Into<String> { self.url = Some(Stringy::String(url.into())); self }
    pub fn with_last_update(mut self, last_update: i64) -> Project { self.last_update = Some(last_update); self  }
    pub fn with_language<S>(mut self, language: S) -> Project where S: Into<String> { self.language = Some(Some(Stringy::String(language.into()))); self }
    pub fn with_stars(mut self, stars: usize) -> Project { self.stars = Some(Some(stars)); self }
    pub fn with_issues(mut self, issues: usize) -> Project { self.issues = Some(Some(issues)); self }
    pub fn with_buggy_issues(mut self, buggy_issues: usize) -> Project { self.buggy_issues = Some(Some(buggy_issues)); self }
    pub fn with_unknown_language(mut self) -> Project { self.language = Some(None); self }
    pub fn with_unknown_stars(mut self) -> Project { self.stars = Some(None); self }
    pub fn with_unknown_issues(mut self) -> Project { self.issues = Some(None); self }
    pub fn with_unknown_buggy_issues(mut self) -> Project { self.buggy_issues = Some(None); self }

    pub fn with_url_like<S>(mut self, url: S) -> Project where S: Into<String> {
        self.url = Some(Stringy::Regex(Regex::new(url.into().as_str()).unwrap()));
        self
    }
    pub fn with_language_like<S>(mut self, language: S) -> Project where S: Into<String> {
        self.language = Some(Some(Stringy::Regex(Regex::new(language.into().as_str()).unwrap())));
        self
    }
}

#[derive(Clone)]
pub struct User {
    pub id: Option<objects::UserId>,
    pub email: Option<Stringy>,
    pub name: Option<Stringy>, // TODO maybe also regex option?
}

impl User {
    pub fn new() -> Self { User { id: None, email: None, name: None } }

    pub fn with_id<N>(mut self, id: N) -> User where N: Into<objects::UserId> {
        self.id = Some(id.into());
        self
    }
    pub fn with_email<S>(mut self, email: S) -> User where S: Into<String> {
        self.email = Some(Stringy::String(email.into()));
        self
    }
    pub fn with_name<S>(mut self, name: S) -> User where S: Into<String> {
        self.name = Some(Stringy::String(name.into()));
        self
    }
    pub fn with_email_like<S>(mut self, email: S) -> User where S: Into<String> {
        self.email = Some(Stringy::Regex(Regex::new(email.into().as_str()).unwrap()));
        self
    }
    pub fn with_name_like<S>(mut self, name: S) -> User where S: Into<String> {
        self.name = Some(Stringy::Regex(Regex::new(name.into().as_str()).unwrap()));
        self
    }
}

#[derive(Clone)]
pub struct Commit {
    pub id: Option<objects::CommitId>,
    pub hash: Option<Stringy>,
    pub author: Option<objects::UserId>,
    pub committer: Option<objects::UserId>,
    pub author_time: Option<i64>,
    pub committer_time: Option<i64>,
    pub additions: Option<Option<u64>>,
    pub deletions: Option<Option<u64>>,
    pub message: Option<Option<Stringy>>,
    pub parents: Option<Vec<objects::CommitId>>,
}

impl Commit {
    pub fn new() -> Self {
        Commit {
            id: None, hash: None,
            author: None, committer: None,
            author_time: None, committer_time: None,
            additions: None, deletions: None,
            message: None, parents: None
        }
    }
}

impl Commit {
    pub fn with_id<N>(mut self, id: N) -> Commit where N: Into<objects::CommitId> { self.id = Some(id.into()); self }
    pub fn with_hash<S>(mut self, hash: S) -> Commit where S: Into<String> { self.hash = Some(Stringy::String(hash.into())); self }
    pub fn with_author<N>(mut self, author: N) -> Commit where N: Into<objects::UserId> { self.author = Some(author.into()); self }
    pub fn with_committer<N>(mut self, committer: N) -> Commit where N: Into<objects::UserId> { self.committer = Some(committer.into()); self }
    pub fn with_author_time<N>(mut self, author_time: N) -> Commit where N: Into<i64> { self.author_time = Some(author_time.into()); self }
    pub fn with_committer_time<N>(mut self, committer_time: N) -> Commit where N: Into<i64> { self.committer_time = Some(committer_time.into()); self }
    pub fn with_additions<N>(mut self, additions: N) -> Commit where N: Into<u64> { self.additions = Some(Some(additions.into())); self }
    pub fn with_unknown_additions(mut self) -> Commit { self.additions = Some(None); self }
    pub fn with_deletions<N>(mut self, deletions: N) -> Commit where N: Into<u64> { self.deletions = Some(Some(deletions.into())); self }
    pub fn with_unknown_deletions(mut self) -> Commit { self.deletions = Some(None); self }
    pub fn with_message<S>(mut self, message: S) -> Commit where S: Into<String> { self.message = Some(Some(Stringy::String(message.into()))); self }
    pub fn with_unknown_message(mut self) -> Commit { self.message = Some(None); self }
    pub fn with_no_parents(mut self) -> Commit { self.parents = Some(Vec::new()); self }
    pub fn with_parent<N>(mut self, parent: N) -> Commit where N: Into<objects::CommitId> {
        if self.parents.is_none() { self.parents = Some(Vec::new()) }
        self.parents.as_mut().unwrap().push(parent.into());
        self
    }
    pub fn with_hash_like<S>(mut self, hash: S) -> Commit where S: Into<String> {
        self.hash = Some(Stringy::Regex(Regex::new(hash.into().as_str()).unwrap()));
        self
    }
    pub fn with_message_like<S>(mut self, message: S) -> Commit where S: Into<String> {
        self.message = Some(Some(Stringy::Regex(Regex::new(message.into().as_str()).unwrap())));
        self
    }
}

#[derive(Clone)]
pub struct Path {
    pub id: Option<objects::PathId>,
    pub path: Option<Stringy>,
}

impl Path {
    pub fn new() -> Self { Path { id: None, path: None } }
    pub fn with_id<N>(mut self, id: N) -> Path where N: Into<objects::PathId> { self.id = Some(id.into()); self }
    pub fn with_path<S>(mut self, path: S) -> Path where S: Into<String> { self.path = Some(Stringy::String(path.into())); self }
    pub fn with_path_like<S>(mut self, path: S) -> Path where S: Into<String> {
        self.path = Some(Stringy::Regex(Regex::new(path.into().as_str()).unwrap()));
        self
    }
}

pub trait Prototype<T> {
    fn matches(&self, data: DataPtr, object: &T) -> bool;
}

macro_rules! try_to_reject {
    ($prototype:expr,$object:expr) => {
        if let Some(v) = $prototype.as_ref() {
            if v.eq(&$object) { return false }
        }
    }
}

impl Prototype<objects::Project> for Project {
    fn matches(&self, _: DataPtr, object: &objects::Project) -> bool {
        try_to_reject!(self.id, object.id);
        try_to_reject!(self.url, object.url);
        try_to_reject!(self.last_update, object.last_update);
        try_to_reject!(self.stars, object.stars);
        try_to_reject!(self.issues, object.issues);
        try_to_reject!(self.buggy_issues, object.buggy_issues);
        return true;
    }
}
impl Prototype<objects::Commit> for Commit  {
    fn matches(&self, data: DataPtr, object: &objects::Commit) -> bool {
        try_to_reject!(self.id, object.id);
        try_to_reject!(self.hash, object.hash);
        try_to_reject!(self.author, object.author);
        try_to_reject!(self.committer, object.committer);
        try_to_reject!(self.author_time, object.author_time);
        try_to_reject!(self.committer_time, object.committer_time);
        try_to_reject!(self.additions, object.additions);
        try_to_reject!(self.deletions, object.deletions);

        if let Some(message) = &self.message {
            let object_message = untangle_mut!(data).message_of(&object.id);

            match (&object_message, message) {
                (Some(object), Some(Stringy::String(message))) => {
                    let prototype = message.clone().into_bytes();
                    if object.contents != prototype { return false } else { /*same*/ }
                }
                (Some(object), Some(Stringy::Regex(regex))) => {
                    let string = String::from_utf8_lossy(object.contents.as_slice());
                    if regex.is_match(string.as_ref()) { return false } else { /*same*/ }
                }
                (None, None) => { /*same*/ },
                _ => return false,
            }



            // match message.as_ref() {
            //     Stringy::String(message) => {
            //         match (object_message, prototype_message) {
            //             (Some(object), Some(prototype)) => {
            //                 if object.contents != prototype { return false }
            //             },
            //             (None, None) => { /*same*/ },
            //             _ => return false,
            //         }
            //     }
            //     Stringy::Regex(regex) => {
            //
            //     }
            // }
        }

        if let Some(parents) = self.parents.as_ref() {
            if parents.iter().any(|commit_id| !object.parents.contains(commit_id)) {
                return false;
            }
        }

        return true;
    }
}

impl Prototype<objects::User> for User    {
    fn matches(&self, _: DataPtr, object: &objects::User) -> bool {
        try_to_reject!(self.id, object.id);
        try_to_reject!(self.email, object.email);
        try_to_reject!(self.name, object.name);
        return true;
    }
}

impl Prototype<objects::Path> for Path    {
    fn matches(&self, _: DataPtr, object: &objects::Path) -> bool {
        try_to_reject!(self.id, object.id);
        try_to_reject!(self.path, object.path);
        return true;
    }
}

pub mod api {
    use crate::objects;
    use crate::prototype;
    use serde::export::PhantomData;

    pub trait ProjectPrototype {
        fn with_id<N>(id: N) -> prototype::Project where N: Into<objects::ProjectId> { prototype::Project::new().with_id(id) }
        fn with_last_update(last_update: i64) -> prototype::Project { prototype::Project::new().with_last_update(last_update) }
        fn with_language<S>(language: S) -> prototype::Project where S: Into<String> { prototype::Project::new().with_language(language) }
        fn with_stars(stars: usize) -> prototype::Project { prototype::Project::new().with_stars(stars) }
        fn with_issues(issues: usize) -> prototype::Project { prototype::Project::new().with_issues(issues) }
        fn with_buggy_issues(buggy_issues: usize) -> prototype::Project { prototype::Project::new().with_buggy_issues(buggy_issues) }
        fn with_unknown_language() -> prototype::Project { prototype::Project::new().with_unknown_language() }
        fn with_unknown_stars() -> prototype::Project { prototype::Project::new().with_unknown_stars() }
        fn with_unknown_issues() -> prototype::Project { prototype::Project::new().with_unknown_issues() }
        fn with_unknown_buggy_issues() -> prototype::Project { prototype::Project::new().with_unknown_buggy_issues() }
    }

    pub trait CommitPrototype {
        fn with_id<N>(id: N) -> prototype::Commit where N: Into<objects::CommitId> { prototype::Commit::new().with_id(id) }
        fn with_hash<S>(hash: S) -> prototype::Commit where S: Into<String> { prototype::Commit::new().with_hash(hash) }
        fn with_author<N>(author: N) -> prototype::Commit where N: Into<objects::UserId> { prototype::Commit::new().with_author(author) }
        fn with_committer<N>(committer: N) -> prototype::Commit where N: Into<objects::UserId> { prototype::Commit::new().with_committer(committer) }
        fn with_author_time<N>(author_time: N) -> prototype::Commit where N: Into<i64> { prototype::Commit::new().with_author_time(author_time) }
        fn with_committer_time<N>(committer_time: N) -> prototype::Commit where N: Into<i64> { prototype::Commit::new().with_committer_time(committer_time) }
        fn with_additions<N>(additions: N) -> prototype::Commit where N: Into<u64> { prototype::Commit::new().with_additions(additions) }
        fn with_unknown_additions() -> prototype::Commit { prototype::Commit::new().with_unknown_additions() }
        fn with_deletions<N>(deletions: N) -> prototype::Commit where N: Into<u64> { prototype::Commit::new().with_deletions(deletions) }
        fn with_unknown_deletions() -> prototype::Commit { prototype::Commit::new().with_unknown_deletions() }
        fn with_message<S>(message: S) -> prototype::Commit where S: Into<String> { prototype::Commit::new().with_message(message) }
        fn with_unknown_message() -> prototype::Commit { prototype::Commit::new().with_unknown_message() }
        fn with_parent<N>(parent: N) -> prototype::Commit where N: Into<objects::CommitId> { prototype::Commit::new().with_parent(parent) }
        fn with_no_parents() -> prototype::Commit { prototype::Commit::new().with_no_parents() }
    }

    pub trait UserPrototype {
        fn with_id<N>(id: N) -> prototype::User where N: Into<objects::UserId> { prototype::User::new().with_id(id) }
        fn with_email<S>(email: S) -> prototype::User where S: Into<String> { prototype::User::new().with_email(email) }
        fn with_name<S>(name: S) -> prototype::User where S: Into<String> { prototype::User::new().with_name(name) }
    }

    trait PathPrototype {
        fn with_id<N>(id: N) -> prototype::Path where N: Into<objects::PathId> { prototype::Path::new().with_id(id) }
        fn with_path<S>(path: S) -> prototype::Path where S: Into<String> { prototype::Path::new().with_path(path) }
    }

    impl ProjectPrototype for prototype::Project {}
    impl ProjectPrototype for objects::Project   {}
    impl UserPrototype for prototype::User       {}
    impl UserPrototype for objects::User         {}
    impl CommitPrototype for prototype::Commit   {}
    impl CommitPrototype for objects::Commit     {}
    impl PathPrototype for prototype::Path       {}
    impl PathPrototype for objects::Path         {}

    pub struct PrototypeOf<P, T> { _prototype: PhantomData<P>, _entity: PhantomData<T> }

}
