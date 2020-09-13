use std::io::Write;

use crate::djanco;
use crate::attrib::*;
use crate::project::*;
use crate::objects::*;
use crate::data::*;

macro_rules! create_file {
    ($location:expr) => {{
        let path = std::path::PathBuf::from($location.into());
        let dir_path = { let mut dir_path = path.clone(); dir_path.pop(); dir_path };
        std::fs::create_dir_all(&dir_path)?;
        std::fs::File::create(path)
    }}
}

#[allow(non_snake_case)]
pub trait CSV {
    fn to_csv(self, location: impl Into<String>) -> Result<(), std::io::Error>;
}

impl<I, T> CSV for I where I: Iterator<Item=T> + WithData + CSVHeader, T: CSVItem {
    fn to_csv(self, location: impl Into<String>) -> Result<(), std::io::Error> {
        let mut file = create_file!(location)?;
        let database = self.get_database_ptr();
        writeln!(file, "{}", self.header())?;
        for element in self {
            writeln!(file, "{}", element.to_csv(database.clone()))?;
        }
        Ok(())
    }
}

// impl<T, TK> CSV for GroupIter<T, TK> where TK: PartialEq + Eq + std::hash::Hash {
// //I where I: Iterator<Item=T> + WithDatabase, T: CSVItem {
//     fn to_csv(self, location: impl Into<String>) -> Result<(), std::io::Error> {
//         unimplemented!();
//         // let path = std::path::PathBuf::from(location.into());
//         // let dir_path = { let mut dir_path = path.clone(); dir_path.pop(); dir_path };
//         // std::fs::create_dir_all(&dir_path).unwrap();
//         //
//         // let mut file = std::fs::File::create(path)?;
//         let mut file = create_file!(location)?;
//         let database = self.get_database_ptr();
//         writeln!(file, "{}", self.header())?;
//         for element in self {
//             writeln!(file, "{}", element.to_csv(database.clone()))?;
//         }
//
//         Ok(())
//     }
//}

pub trait WithStaticNames { fn names() -> Vec<&'static str>; }

impl WithStaticNames for Id          { fn names() -> Vec<&'static str> { vec!["id"]  } }
impl WithStaticNames for URL         { fn names() -> Vec<&'static str> { vec!["url"] } }

impl WithStaticNames for Language    { fn names() -> Vec<&'static str> { vec!["language"]     } }
impl WithStaticNames for Stars       { fn names() -> Vec<&'static str> { vec!["stars"]        } }
impl WithStaticNames for Issues      { fn names() -> Vec<&'static str> { vec!["issues"]       } }
impl WithStaticNames for BuggyIssues { fn names() -> Vec<&'static str> { vec!["buggy_issues"] } }

//impl WithStaticNames for Metadata    { fn names() -> Vec<&'static str> { vec![&self.0.]  } }
impl WithStaticNames for Heads       { fn names() -> Vec<&'static str> { vec!["heads"]   } }

impl WithStaticNames for Commits     { fn names() -> Vec<&'static str> { vec!["commits"] } }
impl WithStaticNames for Users       { fn names() -> Vec<&'static str> { vec!["users"]   } }
impl WithStaticNames for Paths       { fn names() -> Vec<&'static str> { vec!["paths"]   } }

impl WithStaticNames for Project {
    fn names() -> Vec<&'static str> {
        vec!["key", "id", "url", "last_update", "language",
             "stars", "issues", "buggy_issues",
             "head_count", "commit_count", "user_count", "path_count",
             "author_count", "committer_count",
             "age"]
    }
}

impl WithStaticNames for Commit {
    fn names() -> Vec<&'static str> { unimplemented!() }
}

impl WithStaticNames for User {
    fn names() -> Vec<&'static str> { unimplemented!() }
}

impl WithStaticNames for Path {
    fn names() -> Vec<&'static str> { unimplemented!() }
}

macro_rules! to_owned_vec {
    ($vector:expr) => {
        $vector.into_iter().map(|s| s.to_owned()).collect()
    }
}

pub trait WithNames { fn names(&self) -> Vec<String>; }

impl WithNames for Id            { fn names(&self) -> Vec<String> { to_owned_vec![<Self as WithStaticNames>::names()] } }
impl WithNames for URL           { fn names(&self) -> Vec<String> { to_owned_vec![<Self as WithStaticNames>::names()] } }

impl WithNames for Language      { fn names(&self) -> Vec<String> { to_owned_vec![<Self as WithStaticNames>::names()] } }
impl WithNames for Stars         { fn names(&self) -> Vec<String> { to_owned_vec![<Self as WithStaticNames>::names()] } }
impl WithNames for Issues        { fn names(&self) -> Vec<String> { to_owned_vec![<Self as WithStaticNames>::names()] } }
impl WithNames for BuggyIssues   { fn names(&self) -> Vec<String> { to_owned_vec![<Self as WithStaticNames>::names()] } }

impl WithNames for Metadata      { fn names(&self) -> Vec<String> { vec![self.0.clone()]         } }
impl WithNames for Heads         { fn names(&self) -> Vec<String> { to_owned_vec![<Self as WithStaticNames>::names()] } }

impl WithNames for Commits       { fn names(&self) -> Vec<String> { to_owned_vec![<Self as WithStaticNames>::names()] } }
impl WithNames for Users         { fn names(&self) -> Vec<String> { to_owned_vec![<Self as WithStaticNames>::names()] } }
impl WithNames for Paths         { fn names(&self) -> Vec<String> { to_owned_vec![<Self as WithStaticNames>::names()] } }

impl WithNames for Project       { fn names(&self) -> Vec<String> { to_owned_vec!(<Self as WithStaticNames>::names()) } }
impl WithNames for User          { fn names(&self) -> Vec<String> { to_owned_vec!(<Self as WithStaticNames>::names()) } }
impl WithNames for Path          { fn names(&self) -> Vec<String> { to_owned_vec!(<Self as WithStaticNames>::names()) } }
impl WithNames for Commit        { fn names(&self) -> Vec<String> { to_owned_vec!(<Self as WithStaticNames>::names()) } }

impl<T> WithStaticNames for Vec<T> where T: WithStaticNames {
    fn names() -> Vec<&'static str> {
        T::names()
    }
}

impl<T> WithNames for Vec<T> where T: WithNames + WithStaticNames {
    fn names(&self) -> Vec<String> {
        match self.first() {
            Some(e) => e.names(),
            None => <T as WithStaticNames>::names().iter().map(|s| s.to_string()).collect(),
        }
    }
}

impl<K, T> WithNames for (K, T) where K: WithNames, T: WithNames {
    fn names(&self) -> Vec<String> {
        let mut vector: Vec<String> = vec![];
        vector.extend(self.0.names());
        vector.extend(self.1.names());
        vector
    }
}

impl<K, T> WithStaticNames for (K, T) where K: WithStaticNames, T: WithStaticNames {
    fn names() -> Vec<&'static str> {
        let mut vector: Vec<&'static str> = vec![];
        vector.extend(K::names());
        vector.extend(T::names());
        vector
    }
}

#[allow(non_snake_case)]
pub trait CSVHeader { fn header(&self) -> String; }

// impl<I, T> CSVHeader for I where I: Iterator<Item=T>, T: WithNames {
//     fn header(&self) -> String {
//         self.names().join(",")
//     }
// }

impl<I, T> CSVHeader for I where I: Iterator<Item=T>, T: WithStaticNames {
    fn header(&self) -> String {
        T::names().join(",")
    }
}


// impl<TK> CSVHeader for GroupIter<dcd::Project, TK> where TK: PartialEq + Eq + std::hash::Hash {
//     fn header(&self) -> String {
//         "key,id,url,last_update,language,\
//          stars,issues,buggy_issues,\
//          head_count,commit_count,user_count,path_count,author_count,committer_count,\
//          age".to_owned()
//     }
// }

// impl<T> CSVHeader for djanco::Iter<T> where T: WithNames {
//     fn header(&self) -> String {
//         self.names()
//     }
// }
//
// impl<K, T> CSVHeader for djanco::GroupIter<K, T> where T: WithNames {
//     fn header(&self) -> String {
//
//     }
// }

#[allow(non_snake_case)]
pub trait CSVItem { fn to_csv(&self, db: DataPtr) -> String; }

impl CSVItem for String { fn to_csv(&self, _db: DataPtr) -> String { format!(r#"{}"#, self) } }

impl CSVItem for u64    { fn to_csv(&self, _db: DataPtr) -> String { self.to_string() } }
impl CSVItem for i64    { fn to_csv(&self, _db: DataPtr) -> String { self.to_string() } }
impl CSVItem for usize  { fn to_csv(&self, _db: DataPtr) -> String { self.to_string() } }

impl CSVItem for ProjectId { fn to_csv(&self, db: DataPtr) -> String { self.0.to_csv(db) } }
impl CSVItem for CommitId  { fn to_csv(&self, db: DataPtr) -> String { self.0.to_csv(db) } }
impl CSVItem for UserId    { fn to_csv(&self, db: DataPtr) -> String { self.0.to_csv(db) } }
impl CSVItem for PathId    { fn to_csv(&self, db: DataPtr) -> String { self.0.to_csv(db) } }

impl CSVItem for Project {
    fn to_csv(&self, db: DataPtr) -> String {
        format!(r#"{},{},{},{},{},{},{},{},{},{},{},{},{},{}"#,
                self.id, self.url, self.last_update,
                self.language_or_empty(),
                self.stars.map_or(String::new(), |e| e.to_string()),
                self.issues.map_or(String::new(), |e| e.to_string()),
                self.buggy_issues.map_or(String::new(), |e| e.to_string()),
                self.heads.len(),
                untangle!(db).commit_count_from(&self.id),
                untangle!(db).user_count_from(&self.id),
                untangle!(db).path_count_from(&self.id),
                untangle!(db).author_count_from(&self.id),
                untangle!(db).committer_count_from(&self.id),
                untangle!(db).age_of(&self.id)
                    .map_or(String::new(), |e| e.as_secs().to_string()),
        )
    }
}

impl<A, B> CSVItem for (A, B) where A: CSVItem, B: CSVItem {
    fn to_csv(&self, db: DataPtr) -> String {
        format!(r#"{},{}"#, self.0.to_csv(db.clone()), self.1.to_csv(db))
    }
}

impl<T> CSVItem for Option<T> where T: CSVItem {
    fn to_csv(&self, db: DataPtr) -> String {
        match self {
            Some(value) => value.to_csv(db),
            None => "".to_owned(),
        }
    }
}

impl<A,T> CSVItem for AttributeValue<A, T> where A: Attribute, T: CSVItem {
    fn to_csv(&self, db: DataPtr) -> String {
        self.value.to_csv(db)
    }
}