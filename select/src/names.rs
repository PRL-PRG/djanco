use crate::{project, djanco};
use crate::objects;
use crate::attrib::{AttributeValue, Attribute};

pub trait WithNames {
    fn names() -> Vec<&'static str>;
    fn csv_header() -> String {
        Self::names().join(",")
    }
}

impl WithNames for objects::ProjectId { fn names() -> Vec<&'static str> { vec!["project_id"] } }
impl WithNames for objects::CommitId  { fn names() -> Vec<&'static str> { vec!["commit_id"]  } }
impl WithNames for objects::UserId    { fn names() -> Vec<&'static str> { vec!["user_id"]    } }
impl WithNames for objects::PathId    { fn names() -> Vec<&'static str> { vec!["path_id"]    } }

impl WithNames for objects::Project {
    fn names() -> Vec<&'static str> {
        vec!["key", "id", "url", "last_update", "language",
             "stars", "issues", "buggy_issues",
             "head_count", "commit_count", "user_count", "path_count",
             "author_count", "committer_count",
             "age"]
    }
}

impl WithNames for objects::Commit { fn names() -> Vec<&'static str> { unimplemented!() } }
impl WithNames for objects::User   { fn names() -> Vec<&'static str> { unimplemented!() } }
impl WithNames for objects::Path   { fn names() -> Vec<&'static str> { unimplemented!() } }

impl WithNames for project::Id          { fn names() -> Vec<&'static str> { vec!["project_id"]  } }
impl WithNames for project::URL         { fn names() -> Vec<&'static str> { vec!["url"] } }
impl WithNames for project::Language    { fn names() -> Vec<&'static str> { vec!["language"]     } }
impl WithNames for project::Stars       { fn names() -> Vec<&'static str> { vec!["stars"]        } }
impl WithNames for project::Issues      { fn names() -> Vec<&'static str> { vec!["issues"]       } }
impl WithNames for project::BuggyIssues { fn names() -> Vec<&'static str> { vec!["buggy_issues"] } }
//impl WithNames for project::Metadata    { fn names() -> Vec<&'static str> { vec![&self.0.]  } }
impl WithNames for project::Heads       { fn names() -> Vec<&'static str> { vec!["heads"]   } }
impl WithNames for project::Commits     { fn names() -> Vec<&'static str> { vec!["commits"] } }
impl WithNames for project::Users       { fn names() -> Vec<&'static str> { vec!["users"]   } }
impl WithNames for project::Paths       { fn names() -> Vec<&'static str> { vec!["paths"]   } }

macro_rules! join_vec { ($v1:expr, $v2:expr) => {{ $v1.extend($v2); $v1 }} }

impl<T> WithNames for djanco::Iter<T> where T: WithNames {
    fn names() -> Vec<&'static str> { T::names() }
}
impl<T> WithNames for djanco::QuincunxIter<T> where T: WithNames {
    fn names() -> Vec<&'static str> { T::names() }
}
impl<K,T> WithNames for djanco::GroupIter<K,T> where K: WithNames, T: WithNames {
    fn names() -> Vec<&'static str> { join_vec!(K::names(), T::names()) }
}
impl<K,T> WithNames for (K,T) where K: WithNames, T: WithNames {
    fn names() -> Vec<&'static str> { join_vec!(K::names(), T::names()) }
}
impl<A,T> WithNames for AttributeValue<A, T> where A: Attribute + WithNames {
    fn names() -> Vec<&'static str> { A::names() }
}