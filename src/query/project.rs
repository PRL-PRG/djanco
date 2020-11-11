use crate::attrib::*;
//use crate::iterators::ItemWithData;
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

impl Attribute for Id { type Source = Project; }
impl Attribute for URL { type Source = Project; }
impl Attribute for Issues { type Source = Project; }
impl Attribute for BuggyIssues { type Source = Project; }
impl Attribute for IsFork { type Source = Project; }
impl Attribute for IsArchived { type Source = Project; }
impl Attribute for IsDisabled { type Source = Project; }
impl Attribute for Stars { type Source = Project; }
impl Attribute for Watchers { type Source = Project; }
impl Attribute for Size { type Source = Project; }
impl Attribute for OpenIssues { type Source = Project; }
impl Attribute for Forks { type Source = Project; }
impl Attribute for Subscribers { type Source = Project; }
impl Attribute for License { type Source = Project; }
impl Attribute for Language { type Source = Project; }
impl Attribute for Description { type Source = Project; }
impl Attribute for Homepage { type Source = Project; }
impl Attribute for Heads { type Source = Project; }
impl Attribute for Commits { type Source = Project; }
impl Attribute for Authors { type Source = Project; }
impl Attribute for Committers { type Source = Project; }
impl Attribute for Users { type Source = Project; }
impl Attribute for Paths { type Source = Project; }
impl Attribute for HasIssues { type Source = Project; }
impl Attribute for HasDownloads { type Source = Project; }
impl Attribute for HasWiki { type Source = Project; }
impl Attribute for HasPages { type Source = Project; }
impl Attribute for Created { type Source = Project; }
impl Attribute for Updated { type Source = Project; }
impl Attribute for Pushed { type Source = Project; }
impl Attribute for DefaultBranch { type Source = Project; }
impl Attribute for Age { type Source = Project; }

impl IntegerAttribute for Id {}
impl StringAttribute for URL {}
impl IntegerAttribute for Issues {}
impl IntegerAttribute for BuggyIssues {}
impl LogicalAttribute for IsFork {}
impl LogicalAttribute for IsArchived {}
impl LogicalAttribute for IsDisabled {}
impl IntegerAttribute for Stars {}
impl IntegerAttribute for Watchers {}
impl IntegerAttribute for Size {}
impl IntegerAttribute for OpenIssues {}
impl IntegerAttribute for Forks {}
impl IntegerAttribute for Subscribers {}
impl StringAttribute for License {}
impl LanguageAttribute for Language {}
impl StringAttribute for Description {}
impl StringAttribute for Homepage {}
impl CollectionAttribute for Heads {}
impl CollectionAttribute for Commits {}
impl CollectionAttribute for Authors {}
impl CollectionAttribute for Committers {}
impl CollectionAttribute for Users {}
impl CollectionAttribute for Paths {}
impl LogicalAttribute for HasIssues {}
impl LogicalAttribute for HasDownloads {}
impl LogicalAttribute for HasWiki {}
impl LogicalAttribute for HasPages {}
impl TimestampAttribute for Created {}
impl TimestampAttribute for Updated {}
impl TimestampAttribute for Pushed {}
impl StringAttribute for DefaultBranch {}
impl DurationAttribute for Age {}


// TODO
// #[derive(Eq, PartialEq,       Clone, Hash)] pub struct CommitsWith<F>(pub F) where F: Filter<Entit=Commit>;
// #[derive(Eq, PartialEq,       Clone, Hash)] pub struct UsersWith<F>(pub F)   where F: Filter<Entity=User>;
// #[derive(Eq, PartialEq,       Clone, Hash)] pub struct PathsWith<F>(pub F)   where F: Filter<Entity=Path>;

/*
impl_sort_by_key_sans_db!(Project, Id,  id);
impl_sort_by_key_sans_db!(Project, URL, url);
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
*/


