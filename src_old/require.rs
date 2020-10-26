use regex::Regex;
use dcd::DCD;

use crate::attrib::*;
use crate::data::DataPtr;
use crate::prototype::Prototype;


#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct AtLeast<N,V>(pub N, pub V);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct MoreThan<N,V>(pub N, pub V);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Exactly<N,V>(pub N, pub V);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct LessThan<N,V>(pub N, pub V);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct AtMost<N,V> (pub N, pub V);

impl<T, V, N, X> Filter for AtLeast<N,V> where N: NumericalAttribute<Entity=T, Number=X>, V: From<X> + PartialOrd {
    type Entity=T;
    fn filter(&self, data: DataPtr, project: &T) -> bool {
        self.0.calculate(data, project).map_or(false, |e| V::from(e) >= self.1)
    }
}

impl<T, V, N, X> Filter for MoreThan<N,V> where N: NumericalAttribute<Entity=T, Number=X>, V: From<X> + PartialOrd {
    type Entity=T;
    fn filter(&self, data: DataPtr, project: &T) -> bool {
        self.0.calculate(data, project).map_or(false, |e| V::from(e) > self.1)
    }
}

impl<T, V, N, X> Filter for Exactly<N,V> where N: NumericalAttribute<Entity=T, Number=X>, V: From<X> + PartialEq {
    type Entity=T;
    fn filter(&self, data: DataPtr, project: &T) -> bool {
        self.0.calculate(data, project).map_or(false, |e| V::from(e) == self.1)
    }
}

impl<T, V, N, X> Filter for LessThan<N,V> where N: NumericalAttribute<Entity=T, Number=X>, V: From<X> + PartialOrd {
    type Entity=T;
    fn filter(&self, data: DataPtr, project: &T) -> bool {
        self.0.calculate(data, project).map_or(false, |e| V::from(e) < self.1)
    }
}

impl<T, V, N, X> Filter for AtMost<N,V> where N: NumericalAttribute<Entity=T, Number=X>, V: From<X> + PartialOrd {
    type Entity=T;
    fn filter(&self, data: DataPtr, project: &T) -> bool {
        self.0.calculate(data, project).map_or(false, |e| V::from(e) <= self.1)
    }
}

impl<T, N> LoadFilter for AtLeast<N, usize> where N: raw::NumericalAttribute<Entity=T> + Clone + 'static {
    fn filter(&self, database: &DCD, project_id: &u64, commit_ids: &Vec<u64>) -> bool {
        self.0.calculate(database, project_id, commit_ids) >= self.1
    }
    fn clone_box(&self) -> Box<dyn LoadFilter> { Box::new(AtLeast(self.0.clone(), self.1.clone())) }
}

impl<T, N> LoadFilter for MoreThan<N, usize> where N: raw::NumericalAttribute<Entity=T> + Clone + 'static {
    fn filter(&self, database: &DCD, project_id: &u64, commit_ids: &Vec<u64>) -> bool {
        self.0.calculate(database, project_id, commit_ids) > self.1
    }
    fn clone_box(&self) -> Box<dyn LoadFilter> { Box::new(AtLeast(self.0.clone(), self.1.clone())) }
}

impl<T, N> LoadFilter for Exactly<N, usize> where N: raw::NumericalAttribute<Entity=T> + Clone + 'static {
    fn filter(&self, database: &DCD, project_id: &u64, commit_ids: &Vec<u64>) -> bool {
        self.0.calculate(database, project_id, commit_ids) == self.1
    }
    fn clone_box(&self) -> Box<dyn LoadFilter> { Box::new(Exactly(self.0.clone(), self.1.clone())) }
}

impl<T, N> LoadFilter for LessThan<N, usize> where N: raw::NumericalAttribute<Entity=T> + Clone + 'static {
    fn filter(&self, database: &DCD, project_id: &u64, commit_ids: &Vec<u64>) -> bool {
        self.0.calculate(database, project_id, commit_ids) < self.1
    }
    fn clone_box(&self) -> Box<dyn LoadFilter> { Box::new(AtMost(self.0.clone(), self.1.clone())) }
}

impl<T, N> LoadFilter for AtMost<N, usize> where N: raw::NumericalAttribute<Entity=T> + Clone + 'static {
    fn filter(&self, database: &DCD, project_id: &u64, commit_ids: &Vec<u64>) -> bool {
        self.0.calculate(database, project_id, commit_ids) <= self.1
    }
    fn clone_box(&self) -> Box<dyn LoadFilter> { Box::new(AtMost(self.0.clone(), self.1.clone())) }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Same<'a, S>(pub S, pub &'a str);
#[derive(Clone,                          )] pub struct Matches<S>(pub S, pub Regex);

#[macro_export] macro_rules! regex { ($str:expr) => { regex::Regex::new($str).unwrap() }}

impl<'a, S, T> Filter for Same<'a, S> where S: StringAttribute<Entity=T> {
    type Entity=T;
    fn filter(&self, database: DataPtr, project: &T) -> bool {
        self.0.extract(database, project) == self.1.to_string()
    }
}

impl<S, T> Filter for Matches<S> where S: StringAttribute<Entity=T> {
    type Entity=T;
    fn filter(&self, database: DataPtr, project: &T) -> bool {
        self.1.is_match(&self.0.extract(database, project))
    }
}

impl<S, T> LoadFilter for Same<'static, S> where S: raw::StringAttribute<Entity=T> + Clone + 'static {
    fn filter(&self, database: &DCD, project_id: &u64, commit_ids: &Vec<u64>) -> bool {
        self.0.extract(database, project_id, commit_ids) == self.1.to_string()
    }
    fn clone_box(&self) -> Box<dyn LoadFilter> { Box::new(Same(self.0.clone(), self.1.clone())) }
}

impl<S, T> LoadFilter for Matches<S> where S: raw::StringAttribute<Entity=T> + Clone + 'static {
    fn filter(&self, database: &DCD, project_id: &u64, commit_ids: &Vec<u64>) -> bool {
        self.1.is_match(&self.0.extract(database, project_id, commit_ids))
    }
    fn clone_box(&self) -> Box<dyn LoadFilter> { Box::new(Matches(self.0.clone(), self.1.clone())) }
}

#[derive(Clone, Eq, PartialEq, Hash)] pub enum Contains<C, E> {
    Item(C, E),
    Any(C, Vec<E>),
    All(C, Vec<E>),
}

impl<C,E,P,T> Filter for Contains<C, P> where C: CollectionAttribute<Entity=T,Item=E>, E: Eq, P: Prototype<E> {
    type Entity=T;
    fn filter(&self, data: DataPtr, element: &T) -> bool {
        match self {
            Contains::Item(collection_attribute, prototype) => {
                let objects = collection_attribute.items(data.clone(), element);
                objects.iter().any(|object| prototype.matches(data.clone(), object))
            }
            Contains::Any(collection_attribute, prototypes) => {
                let objects = collection_attribute.items(data.clone(), element);
                prototypes.iter().any(|prototype| {
                    objects.iter().any(|object| prototype.matches(data.clone(), object))
                })
            }
            Contains::All(collection_attribute, prototypes) => {
                let objects = collection_attribute.items(data.clone(), element);
                prototypes.iter().all(|prototype| {
                    objects.iter().any(|object| prototype.matches(data.clone(), object))
                })
            }
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Exists<N> (pub N);
#[derive(Clone, Copy, Eq, PartialEq, Hash)] pub struct Exist<N> (pub N); // basically an alias.

impl<T, N> Filter for Exists<N> where N: ExistentialAttribute<Entity=T> {
    type Entity=T;
    fn filter(&self, data: DataPtr, project: &T) -> bool {
        self.0.exists(data, project)
    }
}

impl<T, N> Filter for Exist<N> where N: ExistentialAttribute<Entity=T> {
    type Entity=T;
    fn filter(&self, data: DataPtr, project: &T) -> bool {
        self.0.exists(data, project)
    }
}