use regex::Regex;
use crate::attrib::{FilterEach, NumericalAttribute, StringAttribute, Filter};
use crate::objects::Project;
use crate::data::DataPtr;

#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct AtLeast<N>(pub N, pub usize);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Exactly<N>(pub N, pub usize);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct AtMost<N> (pub N, pub usize);

impl<T, N> Filter<T> for AtLeast<N> where N: NumericalAttribute<Entity=T> {
    fn filter(&self, data: DataPtr, project: &T) -> bool {
        self.0.calculate(data, project) >= self.1
    }
}

impl<T, N> Filter<T> for Exactly<N> where N: NumericalAttribute<Entity=T> {
    fn filter(&self, data: DataPtr, project: &T) -> bool {
        self.0.calculate(data, project) == self.1
    }
}

impl<T, N> Filter<T> for AtMost<N> where N: NumericalAttribute<Entity=T> {
    fn filter(&self, data: DataPtr, project: &T) -> bool {
        self.0.calculate(data, project) <= self.1
    }
}

impl<N> FilterEach for AtLeast<N> where N: NumericalAttribute<Entity=Project> {
    fn filter(&self, database: DataPtr, project: &Project) -> bool {
        self.0.calculate(database, project) >= self.1
    }
}

impl<N> FilterEach for Exactly<N> where N: NumericalAttribute<Entity=Project> {
    fn filter(&self, database: DataPtr, project: &Project) -> bool {
        self.0.calculate(database, project) == self.1
    }
}

impl<N> FilterEach for AtMost<N> where N: NumericalAttribute<Entity=Project> {
    fn filter(&self, database: DataPtr, project: &Project) -> bool {
        self.0.calculate(database, project) <= self.1
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Same<'a, S>(pub S, pub &'a str);
#[derive(Clone,                          )] pub struct Matches<S>(pub S, pub Regex);

#[macro_export] macro_rules! regex { ($str:expr) => { regex::Regex::new($str).unwrap() }}

impl<'a, S> FilterEach for Same<'a, S> where S: StringAttribute<Entity=Project> {
    fn filter(&self, database: DataPtr, project: &Project) -> bool {
        self.0.extract(database, project) == self.1.to_string()
    }
}

impl<S> FilterEach for Matches<S> where S: StringAttribute<Entity=Project> {
    fn filter(&self, database: DataPtr, project: &Project) -> bool {
        self.1.is_match(&self.0.extract(database, project))
    }
}