use crate::djanco;
use crate::stats;
use crate::attrib;
use crate::objects;
use crate::user;
use crate::commit;
use crate::project;
use crate::path;
use crate::attrib::*;
use crate::objects::Identity;

pub trait WithNames {
    fn names() -> Vec<&'static str>;
    fn csv_header() -> String {
        Self::names().join(",")
    }
}

impl WithNames for objects::ProjectId  { fn names() -> Vec<&'static str> { vec!["project_id"]  } }
impl WithNames for objects::CommitId   { fn names() -> Vec<&'static str> { vec!["commit_id"]   } }
impl WithNames for objects::UserId     { fn names() -> Vec<&'static str> { vec!["user_id"]     } }
impl WithNames for objects::PathId     { fn names() -> Vec<&'static str> { vec!["path_id"]     } }
impl WithNames for objects::SnapshotId { fn names() -> Vec<&'static str> { vec!["snapshot_id"] } }

impl WithNames for objects::Project {
    fn names() -> Vec<&'static str> {
        vec!["id", "url", "last_update", "language",
             "stars", "issues", "buggy_issues",
             "head_count", "commit_count", "user_count", "path_count",
             "author_count", "committer_count",
             "age"]
    }
}

impl WithNames for objects::Commit {
    fn names() -> Vec<&'static str> {
        vec!["id","hash",
             "committer_id","committer_time",
             "author_id","author_time",
             "additions","deletions",
             "parents"]
    }
}

impl WithNames for objects::User   {
    fn names() -> Vec<&'static str> {
        vec!["id","name","email",
             "experience",
             "authored","committed"]
    }
}

impl WithNames for objects::Message   {
    fn names() -> Vec<&'static str> {
        vec!["message"]
    }
}

impl WithNames for objects::Path   {
    fn names() -> Vec<&'static str> {
        vec!["id","path"]
    }
}

impl WithNames for f64                   { fn names() -> Vec<&'static str> { vec!["n"] } }
impl WithNames for usize                 { fn names() -> Vec<&'static str> { vec!["n"] } }
impl WithNames for u64                   { fn names() -> Vec<&'static str> { vec!["n"] } }
impl WithNames for i64                   { fn names() -> Vec<&'static str> { vec!["n"] } }
impl WithNames for bool                  { fn names() -> Vec<&'static str> { vec!["condition"] } }

impl WithNames for project::Id           { fn names() -> Vec<&'static str> { vec!["project_id"]  } }
impl WithNames for project::URL          { fn names() -> Vec<&'static str> { vec!["url"]         } }
impl WithNames for project::Language     { fn names() -> Vec<&'static str> { vec!["language"]    } }
impl WithNames for project::Stars        { fn names() -> Vec<&'static str> { vec!["stars"]       } }
impl WithNames for project::Issues       { fn names() -> Vec<&'static str> { vec!["issues"]      } }
impl WithNames for project::AllIssues    { fn names() -> Vec<&'static str> { vec!["all_issues"]  } }
impl WithNames for project::BuggyIssues  { fn names() -> Vec<&'static str> { vec!["buggy_issues"]} }
//impl WithNames for project::Metadata    { fn names() -> Vec<&'static str> { vec![&self.0.]      } }
impl WithNames for project::Heads        { fn names() -> Vec<&'static str> { vec!["heads"]       } }
impl WithNames for project::Commits      { fn names() -> Vec<&'static str> { vec!["commit"]     } }
impl WithNames for project::Users        { fn names() -> Vec<&'static str> { vec!["user"]       } }
impl WithNames for project::Paths        { fn names() -> Vec<&'static str> { vec!["path"]       } }
impl WithNames for project::Age          { fn names() -> Vec<&'static str> { vec!["age"]         } }

impl<F> WithNames for project::PathsWith<F>   where F: Filter<Entity=objects::Path>   { fn names() -> Vec<&'static str> { vec!["path"]} }
impl<F> WithNames for project::UsersWith<F>   where F: Filter<Entity=objects::User>   { fn names() -> Vec<&'static str> { vec!["user"]} }
impl<F> WithNames for project::CommitsWith<F> where F: Filter<Entity=objects::Commit> { fn names() -> Vec<&'static str> { vec!["commit"]} }

impl WithNames for user::Id              { fn names() -> Vec<&'static str> { vec!["user_id"]     } }
impl WithNames for user::Name            { fn names() -> Vec<&'static str> { vec!["name"]        } }
impl WithNames for user::Email           { fn names() -> Vec<&'static str> { vec!["email"]       } }
impl WithNames for user::Experience      { fn names() -> Vec<&'static str> { vec!["experience"]  } }
impl WithNames for user::Commits         { fn names() -> Vec<&'static str> { vec!["commits"]     } }

impl<F> WithNames for user::CommitsWith<F> where F: Filter<Entity=objects::Commit> { fn names() -> Vec<&'static str> { vec!["commit"]} }

impl WithNames for path::Id              { fn names() -> Vec<&'static str> { vec!["path_id"]     } }
impl WithNames for path::Path            { fn names() -> Vec<&'static str> { vec!["path"]        } }
impl WithNames for path::Language        { fn names() -> Vec<&'static str> { vec!["language"]    } }

impl WithNames for commit::Id            { fn names() -> Vec<&'static str> { vec!["commit_id"]   } }
impl WithNames for commit::Hash          { fn names() -> Vec<&'static str> { vec!["hash"]        } }
impl WithNames for commit::Author        { fn names() -> Vec<&'static str> { vec!["author"]      } }
impl WithNames for commit::Committer     { fn names() -> Vec<&'static str> { vec!["committer"]   } }
impl WithNames for commit::AuthorTime    { fn names() -> Vec<&'static str> { vec!["author_time"] } }
impl WithNames for commit::CommitterTime { fn names() -> Vec<&'static str> { vec!["committer_time"] } }
impl WithNames for commit::Additions     { fn names() -> Vec<&'static str> { vec!["additions"]   } }
impl WithNames for commit::Deletions     { fn names() -> Vec<&'static str> { vec!["deletions"]   } }
impl WithNames for commit::Parents       { fn names() -> Vec<&'static str> { vec!["commit"]      } }
impl WithNames for commit::Users         { fn names() -> Vec<&'static str> { vec!["user"]        } }
impl WithNames for commit::Message       { fn names() -> Vec<&'static str> { vec!["message"]     } }
impl WithNames for commit::Paths         { fn names() -> Vec<&'static str> { vec!["path"]        } }

impl<F> WithNames for commit::PathsWith<F>   where F: Filter<Entity=objects::Path>   { fn names() -> Vec<&'static str> { vec!["path"]} }
impl<F> WithNames for commit::ParentsWith<F> where F: Filter<Entity=objects::Commit> { fn names() -> Vec<&'static str> { vec!["commit"]  } }
impl<F> WithNames for commit::UsersWith<F>   where F: Filter<Entity=objects::User>   { fn names() -> Vec<&'static str> { vec!["user"]  } }

macro_rules! join_vec { ($v1:expr, $v2:expr) => {{ vec![$v1, $v2].into_iter().flatten().collect() }} }

impl<T> WithNames for djanco::Iter<T> where T: WithNames {
    fn names() -> Vec<&'static str> { T::names() }
}
impl<T> WithNames for djanco::QuincunxIter<T> where T: WithNames {
    fn names() -> Vec<&'static str> { T::names() }
}
impl<K,T> WithNames for djanco::GroupIter<K,T> where K: WithNames, T: WithNames {
    fn names() -> Vec<&'static str> { join_vec!(K::names(), T::names()) }
}
impl<A,B> WithNames for (A,B) where A: WithNames, B: WithNames {
    fn names() -> Vec<&'static str> { join_vec!(A::names(), B::names()) }
}
impl<A,B,C> WithNames for (A,B,C) where A: WithNames, B: WithNames, C: WithNames {
    fn names() -> Vec<&'static str> { join_vec!(join_vec!(A::names(), B::names()), C::names()) }
}
impl<A,T> WithNames for AttributeValue<A, T> where A: Attribute + WithNames {
    fn names() -> Vec<&'static str> { A::names() }
}
impl<T> WithNames for Option<T> where T: WithNames {
    fn names() -> Vec<&'static str> { T::names() }
}

impl<C> WithNames for stats::Count<C>  { fn names() -> Vec<&'static str> { vec!["count"]  } }
impl<C> WithNames for stats::Min<C>    { fn names() -> Vec<&'static str> { vec!["min"]    } }
impl<C> WithNames for stats::Max<C>    { fn names() -> Vec<&'static str> { vec!["max"]    } }
impl<C> WithNames for stats::Mean<C>   { fn names() -> Vec<&'static str> { vec!["mean"]   } }
impl<C> WithNames for stats::Median<C> { fn names() -> Vec<&'static str> { vec!["median"] } }
impl<C> WithNames for stats::Ratio<C>  { fn names() -> Vec<&'static str> { vec!["ratio"]  } }

impl<A,I> WithNames for attrib::ID<I,A> where A: WithNames, I: Identity {
    fn names() -> Vec<&'static str> { join_vec!(I::names(), A::names()) }
}

// impl WithNames for objects::ProjectId  { fn names() -> Vec<&'static str> { vec!["project_ids"] } }
// impl WithNames for objects::CommitId   { fn names() -> Vec<&'static str> { vec!["commit_ids"] } }
// impl WithNames for objects::UserId     { fn names() -> Vec<&'static str> { vec!["user_ids"] } }
// impl WithNames for objects::PathId     { fn names() -> Vec<&'static str> { vec!["path_ids"] } }
// impl WithNames for objects::SnapshotId { fn names() -> Vec<&'static str> { vec!["snapshot_ids"] } }