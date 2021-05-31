use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use crate::objects::*;
use crate::Store;

const QUADRILLION: (usize, &'static str) = (TRILLION.0 * 1000, "Q");
const TRILLION:    (usize, &'static str) = (BILLION.0  * 1000, "T");
const BILLION:     (usize, &'static str) = (MILLION.0  * 1000, "B");
const MILLION:     (usize, &'static str) = (THOUSAND.0 * 1000, "M");
const THOUSAND:    (usize, &'static str) = (             1000, "K");

pub struct BigNumbers {

}
impl BigNumbers {
    fn convert(bytes: usize, unit: (usize, &str)) -> String {
        let big_number = bytes / unit.0;
        let small_number = (bytes / (unit.0/100)) % 100;
        format!("{}.{}{}", big_number, small_number, unit.1)
    }
    pub fn big_number_as_human_readable_string(number: usize) -> String {
        match number {
            number if number >= QUADRILLION.0 => Self::convert(number, QUADRILLION),
            number if number >= TRILLION.0    => Self::convert(number, TRILLION),
            number if number >= BILLION.0     => Self::convert(number, BILLION),
            number if number >= MILLION.0     => Self::convert(number, MILLION),
            number if number >= THOUSAND.0    => Self::convert(number, THOUSAND),
            number                            => format!("{}", number),
        }
    }
}

pub trait Countable {
    fn is_simple() -> bool;
    fn count_items(&self) -> usize;
}
impl<T> Countable for Vec<T> {
    fn is_simple() -> bool { false }
    fn count_items(&self) -> usize { self.len() }
}
impl<K,T> Countable for BTreeMap<K,T> where T: Countable {
    fn is_simple() -> bool { false }
    fn count_items(&self) -> usize {
        if T::is_simple() {
            self.len()
        } else {
            self.iter().map(|(_, items)| items.count_items()).sum()
        }
    }
}

macro_rules! quick_impl_countable {
    ($t:ty) => {
        impl Countable for $t {
            fn is_simple() -> bool { true }
            fn count_items(&self) -> usize { 1 }
        }
    }
}

quick_impl_countable!(bool);
quick_impl_countable!(String);
quick_impl_countable!(usize);
quick_impl_countable!(u64);
quick_impl_countable!(u32);
quick_impl_countable!(i64);
quick_impl_countable!(i32);
quick_impl_countable!(u8);

quick_impl_countable!(User);
quick_impl_countable!(Project);
quick_impl_countable!(Commit);
quick_impl_countable!(Path);
quick_impl_countable!(Snapshot);
quick_impl_countable!(Head);
quick_impl_countable!(Change);
quick_impl_countable!(Language);
quick_impl_countable!(Store);

quick_impl_countable!(UserId);
quick_impl_countable!(ProjectId);
quick_impl_countable!(CommitId);
quick_impl_countable!(PathId);
quick_impl_countable!(SnapshotId);

quick_impl_countable!((usize, ProjectId));

//quick_impl_countable!(ChangeTuple);

const PEBI: (usize, &'static str) = (TEBI.0 * 1024, "PB");
const TEBI: (usize, &'static str) = (GIBI.0 * 1024, "TB");
const GIBI: (usize, &'static str) = (MEBI.0 * 1024, "GB");
const MEBI: (usize, &'static str) = (KIBI.0 * 1024, "MB");
const KIBI: (usize, &'static str) = (         1024, "KB");

pub struct Weights;
impl Weights {
    pub fn bytes_as_human_readable_string(bytes: usize) -> String {
        match bytes {
            bytes if bytes >= PEBI.0 => BigNumbers::convert(bytes, PEBI),
            bytes if bytes >= TEBI.0 => BigNumbers::convert(bytes, TEBI),
            bytes if bytes >= GIBI.0 => BigNumbers::convert(bytes, GIBI),
            bytes if bytes >= MEBI.0 => BigNumbers::convert(bytes, MEBI),
            bytes if bytes >= KIBI.0 => BigNumbers::convert(bytes, KIBI),
            bytes                    => format!("{}B", bytes),
        }
    }
}

pub trait Weighed: Sized {
    fn is_static() -> bool { false }
    fn weigh_static_component() -> usize { std::mem::size_of::<Self>()  }
    fn weigh_dynamic_component(&self) -> usize;
    fn weigh(&self) -> usize { Self::weigh_static_component() + self.weigh_dynamic_component() }
    fn weigh_human_readable(&self) -> String {
        Weights::bytes_as_human_readable_string(self.weigh())
    }
}


macro_rules! quick_impl_weighed {
   ($t:ty) => {
        impl Weighed for $t {
            fn is_static() -> bool { true }
            fn weigh_dynamic_component(&self) -> usize { 0usize }
        }
   }
}

quick_impl_weighed!(bool);
quick_impl_weighed!(usize);
quick_impl_weighed!(u64);
quick_impl_weighed!(u32);
quick_impl_weighed!(i64);
quick_impl_weighed!(i32);
quick_impl_weighed!(u8);

quick_impl_weighed!(UserId);
quick_impl_weighed!(ProjectId);
quick_impl_weighed!(CommitId);
quick_impl_weighed!(PathId);
quick_impl_weighed!(SnapshotId);
// quick_impl_weighed!(Change);
quick_impl_weighed!(Language);
quick_impl_weighed!(Store);

macro_rules! quick_impl_weighed_static_collection {
   ($t:ty, $e:ty) => {
        impl Weighed for $t {
            fn weigh_dynamic_component(&self) -> usize {
                self.len() * std::mem::size_of::<$e>()
            }
        }
   }
}

quick_impl_weighed_static_collection!(String, u8);
quick_impl_weighed_static_collection!(&str,   u8);

macro_rules! quick_impl_weighed_generic_collection {
   ($t:ident) => {
        impl<T> Weighed for $t<T> where T: Weighed {
            fn weigh_dynamic_component(&self) -> usize {
                if T::is_static() {
                    self.len() * T::weigh_static_component()
                } else {
                    self.iter().map(|e| e.weigh()).sum()
                }
            }
        }
   }
}

quick_impl_weighed_generic_collection!(Vec);
quick_impl_weighed_generic_collection!(VecDeque);
quick_impl_weighed_generic_collection!(BTreeSet);
quick_impl_weighed_generic_collection!(HashSet);

macro_rules! quick_impl_weighed_generic_map {
   ($t:ident) => {
        impl<K,T> Weighed for $t<K,T> where T: Weighed, K: Weighed {
            fn weigh_dynamic_component(&self) -> usize {
                if K::is_static() && T::is_static() {
                    self.len() * (K::weigh_static_component() + T::weigh_static_component())
                } else {
                    self.iter().map(|(k, v)| k.weigh() + v.weigh()).sum()
                }
            }
        }
   }
}

quick_impl_weighed_generic_map!(BTreeMap);
quick_impl_weighed_generic_map!(HashMap);

impl<Ta,Tb> Weighed for (Ta,Tb) where Ta: Weighed, Tb: Weighed { // I could probably macro those to many lengths, if needed
    fn is_static() -> bool {
        Ta::is_static() && Tb::is_static()
    }
    fn weigh_dynamic_component(&self) -> usize {
        let a = if Ta::is_static() { 0usize } else { self.0.weigh_dynamic_component() };
        let b = if Tb::is_static() { 0usize } else { self.1.weigh_dynamic_component() };
        a + b
    }
}

impl<Ta,Tb,Tc> Weighed for (Ta,Tb,Tc) where Ta: Weighed, Tb: Weighed, Tc: Weighed {
    fn is_static() -> bool {
        Ta::is_static() && Tb::is_static() && Tc::is_static()
    }
    fn weigh_dynamic_component(&self) -> usize {
        let a = if Ta::is_static() { 0usize } else { self.0.weigh_dynamic_component() };
        let b = if Tb::is_static() { 0usize } else { self.1.weigh_dynamic_component() };
        let c = if Tc::is_static() { 0usize } else { self.2.weigh_dynamic_component() };
        a + b + c
    }
}

impl<Ta,Tb,Tc,Td> Weighed for (Ta,Tb,Tc,Td) where Ta: Weighed, Tb: Weighed, Tc: Weighed, Td: Weighed {
    fn is_static() -> bool {
        Ta::is_static() && Tb::is_static() && Tc::is_static() && Td::is_static()
    }
    fn weigh_dynamic_component(&self) -> usize {
        let a = if Ta::is_static() { 0usize } else { self.0.weigh_dynamic_component() };
        let b = if Tb::is_static() { 0usize } else { self.1.weigh_dynamic_component() };
        let c = if Tc::is_static() { 0usize } else { self.2.weigh_dynamic_component() };
        let d = if Td::is_static() { 0usize } else { self.3.weigh_dynamic_component() };
        a + b + c + d
    }
}

impl<T> Weighed for Option<T> where T: Weighed {
    fn weigh_dynamic_component(&self) -> usize {
        if T::is_static() {
            0usize
        } else {
            self.as_ref().map_or(0usize, |e| e.weigh_dynamic_component())
        }
    }
}

macro_rules! quick_impl_weighed_by_fields {
    ($t:ty, $field:ident, $($fields:ident),*) => {
        impl Weighed for $t {
            fn weigh_dynamic_component(&self) -> usize {
                 self.$field.weigh_dynamic_component() $(+ self.$fields.weigh_dynamic_component())*
            }
        }
    }
}

quick_impl_weighed_by_fields!(Project, id, url);
quick_impl_weighed_by_fields!(Commit, id, parents, committer, author);
quick_impl_weighed_by_fields!(User, id, email);
quick_impl_weighed_by_fields!(Path, id, location);
quick_impl_weighed_by_fields!(Snapshot, id, contents);
quick_impl_weighed_by_fields!(Head, name, commit);