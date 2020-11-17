use std::collections::BTreeMap;

use crate::objects::*;

pub trait Countable {
    fn simple() -> bool;
    fn count_items(&self) -> usize;
}
impl<T> Countable for Vec<T> {
    fn simple() -> bool { false }
    fn count_items(&self) -> usize { self.len() }
}
impl<K,T> Countable for BTreeMap<K,T> where T: Countable {
    fn simple() -> bool { false }
    fn count_items(&self) -> usize {
        if T::simple() {
            self.len()
        } else {
            self.iter().map(|(_, items)| items.count_items()).sum()
        }
    }
}

macro_rules! quick_impl_countable {
    ($t:ty) => {
        impl Countable for $t {
            fn simple() -> bool { true }
            fn count_items(&self) -> usize { 1 }
        }
    }
}
quick_impl_countable!(String);
quick_impl_countable!(usize);
quick_impl_countable!(u64);
quick_impl_countable!(u32);
quick_impl_countable!(i64);
quick_impl_countable!(i32);

quick_impl_countable!(User);
quick_impl_countable!(Project);
quick_impl_countable!(Commit);
quick_impl_countable!(Path);
quick_impl_countable!(Snapshot);
quick_impl_countable!(Head);

quick_impl_countable!(UserId);
quick_impl_countable!(ProjectId);
quick_impl_countable!(CommitId);
quick_impl_countable!(PathId);
quick_impl_countable!(SnapshotId);