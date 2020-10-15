use std::io::Write;
use std::ffi::CString;
use itertools::Itertools;

use crate::attrib::*;
use crate::objects::*;
use crate::data::*;
use crate::names::WithNames;

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

#[allow(non_snake_case)]
pub trait SimpleCSV {
    fn to_simple_csv(self, location: impl Into<String>) -> Result<(), std::io::Error>;
}

impl<I, T> CSV for I where I: Iterator<Item=T> + WithData, T: CSVItem + WithNames {
    fn to_csv(self, location: impl Into<String>) -> Result<(), std::io::Error> {
        let mut file = create_file!(location)?;
        let database = self.get_database_ptr();
        writeln!(file, "{}", T::csv_header())?;
        //let _: () = self.map(|e| println!("{}", e.to_csv(database.clone()))).collect();
        for element in self {
            //println!("{}", element.to_csv(database.clone()));
            writeln!(file, "{}", element.to_csv(database.clone()))?;
        }
        Ok(())
    }
}

impl<I, T> SimpleCSV for I where I: Iterator<Item=T>, T: CSVItem + WithNames {
    fn to_simple_csv(self, location: impl Into<String>) -> Result<(), std::io::Error> {
        let mut file = create_file!(location)?;
        writeln!(file, "{}", T::csv_header())?;
        //let _: () = self.map(|e| println!("{}", e.to_csv(database.clone()))).collect();
        for element in self {
            //println!("{}", element.to_csv(database.clone()));
            writeln!(file, "{}", element.to_simple_csv())?;
        }
        Ok(())
    }
}

#[allow(non_snake_case)]
pub trait CSVItem {
    fn to_simple_csv(&self) -> String;
    #[allow(unused_variables)] fn to_csv(&self, db: DataPtr) -> String { self.to_simple_csv() }
}

impl CSVItem for String    { fn to_simple_csv(&self) -> String { format!(r#"{}"#, self) } }

impl CSVItem for u64       { fn to_simple_csv(&self) -> String { self.to_string() } }
impl CSVItem for i64       { fn to_simple_csv(&self) -> String { self.to_string() } }
impl CSVItem for f64       { fn to_simple_csv(&self) -> String { self.to_string() } }
impl CSVItem for usize     { fn to_simple_csv(&self) -> String { self.to_string() } }
impl CSVItem for bool      { fn to_simple_csv(&self) -> String { self.to_string() } }

impl CSVItem for ProjectId { fn to_simple_csv(&self) -> String { self.0.to_simple_csv() } }
impl CSVItem for CommitId  { fn to_simple_csv(&self) -> String { self.0.to_simple_csv() } }
impl CSVItem for UserId    { fn to_simple_csv(&self) -> String { self.0.to_simple_csv() } }
impl CSVItem for PathId    { fn to_simple_csv(&self) -> String { self.0.to_simple_csv() } }

impl CSVItem for Project {
    fn to_simple_csv(&self) -> String {
        format!(r#"{},{},{},{},{},{},{},{},,,,,,"#,
                self.id,
                self.url,
                self.last_update,
                self.language_or_empty(),
                self.stars.map_or(String::new(), |e| e.to_string()),
                self.issues.map_or(String::new(), |e| e.to_string()),
                self.buggy_issues.map_or(String::new(), |e| e.to_string()),
                self.heads.len(),
        )
    }
    fn to_csv(&self, data: DataPtr) -> String {
        let mut data = untangle_mut!(data);
        format!(r#"{},{},{},{},{},{},{},{},{},{},{},{},{},{}"#,
                self.id,
                self.url,
                self.last_update,
                self.language_or_empty(),
                self.stars.map_or(String::new(), |e| e.to_string()),
                self.issues.map_or(String::new(), |e| e.to_string()),
                self.buggy_issues.map_or(String::new(), |e| e.to_string()),
                self.heads.len(),
                data.commit_count_from(&self.id),
                data.user_count_from(&self.id),
                data.path_count_from(&self.id),
                data.author_count_from(&self.id),
                data.committer_count_from(&self.id),
                data.age_of(&self.id)
                    .map_or(String::new(), |e| e.to_string()),
        )
    }
}

impl CSVItem for Commit {
    fn to_simple_csv(&self) -> String {
        format!(r#"{},"{}",{},{},{},{},{},{},{}"#,
                self.id,
                self.hash,
                self.committer,
                self.committer_time,
                self.author,
                self.author_time,
                self.additions.map_or(String::new(), |e| e.to_string()),
                self.deletions.map_or(String::new(), |e| e.to_string()),
                self.parents.iter().join(" "))
    }
}
impl CSVItem for User {

    fn to_simple_csv(&self) -> String {
        format!(r#"{},"{}","{}",,,"#, self.id, self.name, self.email)
    }
    fn to_csv(&self, data: DataPtr) -> String {
        let mut data = untangle_mut!(data);
        format!(r#"{},"{}","{}",{},{},{}"#,
                self.id,
                self.name,
                self.email,
                data.experience_of(&self.id)
                    .map_or(String::new(), |e| e.to_string()),
                data.authored_commit_count_of(&self.id),
                data.committed_commit_count_of(&self.id))
    }
}

impl CSVItem for Message {
    fn to_simple_csv(&self) -> String {
        format!(r#"{:?}"#,
                CString::new(self.contents.clone())
                    .unwrap_or(CString::new("").unwrap()))
    }
}

// impl<I,T> CSVItem for (I, T) where I: Identity, T: CSVItem {
//     fn to_csv(&self, db: DataPtr) -> String {
//         format!(r#"{},{}"#, self.0, self.1.to_csv(db))
//     }
// }

impl CSVItem for Path {
    fn to_simple_csv(&self) -> String {
        format!(r#"{},{}"#, self.id, self.path)
    }
}

impl<A, B> CSVItem for (A, B) where A: CSVItem, B: CSVItem {
    fn to_simple_csv(&self) -> String {
        format!(r#"{},{}"#, self.0.to_simple_csv(), self.1.to_simple_csv())
    }
    fn to_csv(&self, db: DataPtr) -> String {
        format!(r#"{},{}"#, self.0.to_csv(db.clone()), self.1.to_csv(db))
    }
}

impl<A, B, C> CSVItem for (A, B, C) where A: CSVItem, B: CSVItem, C: CSVItem {
    fn to_simple_csv(&self) -> String {
        format!(r#"{},{},{}"#,
                self.0.to_simple_csv(), self.1.to_simple_csv(), self.2.to_simple_csv())
    }
    fn to_csv(&self, db: DataPtr) -> String {
        format!(r#"{},{},{}"#,
                self.0.to_csv(db.clone()), self.1.to_csv(db.clone()), self.1.to_csv(db))
    }
}

impl<T> CSVItem for Option<T> where T: CSVItem {
    fn to_simple_csv(&self) -> String {
        match self {
            Some(value) => value.to_simple_csv(),
            None => "".to_owned(),
        }
    }
    fn to_csv(&self, db: DataPtr) -> String {
        match self {
            Some(value) => value.to_csv(db),
            None => "".to_owned(),
        }
    }
}

impl<A,T> CSVItem for AttributeValue<A, T> where A: Attribute, T: CSVItem {
    fn to_simple_csv(&self) -> String {
        self.value.to_simple_csv()
    }
    fn to_csv(&self, db: DataPtr) -> String {
        self.value.to_csv(db)
    }
}

#[allow(non_snake_case)]
pub trait IDs<T: Identifiable<I>, I: Identity> {
    fn to_id_list(self, location: impl Into<String>) -> Result<(), std::io::Error>;
}

impl<I, C, T> IDs<T, I> for C where C: Iterator<Item=T> + WithData, T: Identifiable<I>, I: Identity  {
    fn to_id_list(self, location: impl Into<String>) -> Result<(), std::io::Error> {
        let mut file = create_file!(location)?;
        for element in self { writeln!(file, "{}", element.id())?; }
        Ok(())
    }
}