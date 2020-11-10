use crate::attrib::{Filter, Sort};
use crate::iterators::ItemWithData;
use crate::objects::*;

#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Id;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct URL;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Issues;      // FIXME
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct BuggyIssues; // FIXME
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct IsFork;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct IsArchived;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct IsDisabled;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Stars;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Watchers;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Size;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct OpenIssues;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Forks;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Subscribers;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct License;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Language;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Description;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Homepage;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Heads;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Commits;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Authors;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Committers;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Users;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Paths;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct HasIssues;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct HasDownloads;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct HasWiki;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct HasPages;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Created;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Updated;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Pushed;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct DefaultBranch;
#[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct Age;



// TODO
// #[derive(Eq, PartialEq,       Clone, Hash)] pub struct CommitsWith<F>(pub F) where F: Filter<Entit=Commit>;
// #[derive(Eq, PartialEq,       Clone, Hash)] pub struct UsersWith<F>(pub F)   where F: Filter<Entity=User>;
// #[derive(Eq, PartialEq,       Clone, Hash)] pub struct PathsWith<F>(pub F)   where F: Filter<Entity=Path>;


macro_rules! impl_sort_by_key {
    ($item:ident, $attrib:ident, $key_selection:expr) => {
        impl Sort for $attrib {
            type Item = $item;
            fn sort(&self, vector: &mut Vec<ItemWithData<Self::Item>>) {
                vector.sort_by_key($key_selection)
            }
        }
    }
}

macro_rules! impl_sort_by_key_with_db {
    ($item:ident, $attrib:ident, $method:ident) => {
        impl_sort_by_key!($item, $attrib, | ItemWithData { item, data } | item.$method(data));
    }
}

macro_rules! impl_sort_by_key_sans_db {
    ($item:ident, $attrib:ident, $method:ident) => {
        impl_sort_by_key!($item, $attrib, | ItemWithData { item, data: _ } | item.$method());
    }
}

//impl_sort_by_key_sans_db!(Project, Id,  id);
//impl_sort_by_key_sans_db!(Project, URL, url);
impl_sort_by_key_with_db!(Project, Issues, issue_count);
impl_sort_by_key_with_db!(Project, BuggyIssues, buggy_issue_count);
impl_sort_by_key_with_db!(Project, IsFork, is_fork);
impl_sort_by_key_with_db!(Project, IsArchived, is_archived);
impl_sort_by_key_with_db!(Project, IsDisabled, is_disabled);
impl_sort_by_key_with_db!(Project, Stars, star_count);
impl_sort_by_key_with_db!(Project, Watchers, watcher_count);
impl_sort_by_key_with_db!(Project, Size, size);
impl_sort_by_key_with_db!(Project, OpenIssues, open_issue_count);
impl_sort_by_key_with_db!(Project, Forks, fork_count);
impl_sort_by_key_with_db!(Project, Subscribers, subscriber_count);
impl_sort_by_key_with_db!(Project, License, license);
impl_sort_by_key_with_db!(Project, Language, language);
impl_sort_by_key_with_db!(Project, Description, description);
impl_sort_by_key_with_db!(Project, Homepage, homepage);
impl_sort_by_key_with_db!(Project, Heads, head_count);
impl_sort_by_key_with_db!(Project, Commits, commit_count);
impl_sort_by_key_with_db!(Project, Authors, author_count);
impl_sort_by_key_with_db!(Project, Committers, committer_count);
impl_sort_by_key_with_db!(Project, Users, user_count);
impl_sort_by_key_with_db!(Project, Paths, path_count);
impl_sort_by_key_with_db!(Project, HasIssues, has_issues);
impl_sort_by_key_with_db!(Project, HasDownloads, has_downloads);
impl_sort_by_key_with_db!(Project, HasWiki, has_wiki);
impl_sort_by_key_with_db!(Project, HasPages, has_pages);
impl_sort_by_key_with_db!(Project, Created, created);
impl_sort_by_key_with_db!(Project, Updated, updated);
impl_sort_by_key_with_db!(Project, Pushed, pushed);
impl_sort_by_key_with_db!(Project, DefaultBranch, master_branch);
impl_sort_by_key_with_db!(Project, Age, lifetime);



