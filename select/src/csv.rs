use std::io::Write;

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

impl<I, T> CSV for I where I: Iterator<Item=T> + WithData, T: CSVItem + WithNames {
    fn to_csv(self, location: impl Into<String>) -> Result<(), std::io::Error> {
        let mut file = create_file!(location)?;
        let database = self.get_database_ptr();
        writeln!(file, "{}", T::csv_header())?;
        for element in self {
            writeln!(file, "{}", element.to_csv(database.clone()))?;
        }
        Ok(())
    }
}

#[allow(non_snake_case)]
pub trait CSVItem { fn to_csv(&self, db: DataPtr) -> String; }

impl CSVItem for String    { fn to_csv(&self, _db: DataPtr) -> String { format!(r#"{}"#, self) } }

impl CSVItem for u64       { fn to_csv(&self, _db: DataPtr) -> String { self.to_string() } }
impl CSVItem for i64       { fn to_csv(&self, _db: DataPtr) -> String { self.to_string() } }
impl CSVItem for usize     { fn to_csv(&self, _db: DataPtr) -> String { self.to_string() } }

impl CSVItem for ProjectId { fn to_csv(&self, db: DataPtr) -> String { self.0.to_csv(db) } }
impl CSVItem for CommitId  { fn to_csv(&self, db: DataPtr) -> String { self.0.to_csv(db) } }
impl CSVItem for UserId    { fn to_csv(&self, db: DataPtr) -> String { self.0.to_csv(db) } }
impl CSVItem for PathId    { fn to_csv(&self, db: DataPtr) -> String { self.0.to_csv(db) } }

impl CSVItem for Project {
    fn to_csv(&self, data: DataPtr) -> String {
        format!(r#"{},{},{},{},{},{},{},{},{},{},{},{},{},{}"#,
                self.id, self.url, self.last_update,
                self.language_or_empty(),
                self.stars.map_or(String::new(), |e| e.to_string()),
                self.issues.map_or(String::new(), |e| e.to_string()),
                self.buggy_issues.map_or(String::new(), |e| e.to_string()),
                self.heads.len(),
                untangle!(data).commit_count_from(&self.id),
                untangle!(data).user_count_from(&self.id),
                untangle!(data).path_count_from(&self.id),
                untangle!(data).author_count_from(&self.id),
                untangle!(data).committer_count_from(&self.id),
                untangle!(data).age_of(&self.id)
                    .map_or(String::new(), |e| e.as_secs().to_string()),
        )
    }
}

impl CSVItem for Commit { fn to_csv(&self, data: DataPtr) -> String { unimplemented!() } }
impl CSVItem for User   { fn to_csv(&self, data: DataPtr) -> String { unimplemented!() } }
impl CSVItem for Path   { fn to_csv(&self, data: DataPtr) -> String { unimplemented!() } }

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