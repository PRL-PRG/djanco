use std::path::PathBuf;
use crate::objects::*;
use crate::data::*;
use crate::log::LogLevel;
use crate::attrib::{LoadFilter, Group, Filter, Sort, Sample, Select, sort};
use std::hash::Hash;
use crate::time::Month;
use crate::receipt::{Receipt, ReceiptHolder, Task};
use std::collections::VecDeque;



/** Pre-load operations **/
#[derive(Clone)]
pub struct Lazy {
    pub spec: Spec, // TODO probably redundant
    pub receipt: Receipt,
    pub(crate) filters: Vec<Box<dyn LoadFilter + 'static>>,
}

impl ReceiptHolder for Lazy {
    fn get_receipt(&self) -> &Receipt {
        &self.receipt
    }
}

impl From<Spec> for Lazy {
    fn from(spec: Spec) -> Self {
        let mut receipt = Receipt::new();
        receipt.instantaneous(Task::initial(&spec));
        Lazy { filters: vec![], receipt, spec }
    }
}

impl From<&Spec> for Lazy {
    fn from(spec: &Spec) -> Self {
        let mut receipt = Receipt::new();
        receipt.instantaneous(Task::initial(&spec));
        Lazy { filters: vec![], receipt, spec: spec.clone() }
    }
}

impl /* LoadFiltering for */ Lazy {
    pub fn with_filter<F>(mut self, filter: F) -> Self where F: LoadFilter + 'static {
        self.filters.push(Box::new(filter)); self
    }
}

impl /* VerbositySetting for */ Lazy {
    pub fn with_log_level(mut self, log_level: LogLevel) -> Self {
        self.spec.log_level = log_level; self
    }
}

impl /* CachePathSetting for */ Lazy {
    pub fn with_cache<S: Into<String>>(mut self, path: S) -> Self {
        self.spec.database = Some(PathBuf::from(path.into())); self
    }
}

impl /* Quincunx for */ Lazy {
    pub fn projects(self ) -> QuincunxIter<Project> { QuincunxIter::from(self) }
    pub fn commits(self)   -> QuincunxIter<Commit>  { QuincunxIter::from(self) }
    pub fn users(self)     -> QuincunxIter<User>    { QuincunxIter::from(self) }
    pub fn paths(self)     -> QuincunxIter<Path>    { QuincunxIter::from(self) }
    //pub fn snapshots(self) -> Loaded<Snapshot> { Loaded::from(self) }
    // TODO the remainder
}

/** A single strand out of the five main strands in the database **/
#[derive(Clone)]
pub struct QuincunxIter<T> {
    spec: Spec,// TODO redundant?
    receipt: Receipt,
    data: DataPtr,
    source_: Option<VecDeque<T>>, // Serves the iterator: None -> n elements -> ... -> 0 elements
}

impl<T> /* LazyLoad for */ QuincunxIter<T> where T: Quincunx {
    fn borrow_source(&mut self) -> &mut VecDeque<T> {
        if self.source_.is_none() {
            self.source_ = Some(T::stream_from(&self.data).into())
        }
        self.source_.as_mut().unwrap()
    }
    fn consume_source(mut self) -> VecDeque<T> {
        if self.source_.is_none() {
            self.source_ = Some(T::stream_from(&self.data).into())
        }
        self.source_.unwrap()
    }
}

impl<T> WithData for QuincunxIter<T> {
    fn get_database_ptr(&self) -> DataPtr {
        self.data.clone()
    }
}

impl<T> ReceiptHolder for QuincunxIter<T> {
    fn get_receipt(&self) -> &Receipt {
        &self.receipt
    }
}

impl<T> Iterator for QuincunxIter<T> where T: Quincunx {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> { self.borrow_source().pop_front() }
}

impl<T> /* Query for */ QuincunxIter<T> where T: Quincunx {
    pub fn group_by_attrib<K, G>(self, mut attrib: G) -> GroupIter<K, T> where G: Group<T, Key=K>, K: Hash + Eq {

        let mut receipt = self.receipt.clone();
        receipt.start(Task::grouping::<K,T>());

        let data = self.data.clone();
        let spec = self.spec.clone();
        let source = attrib.execute(self.data.clone(), self.consume_source().into()); //FIXME we could switch all attrib to vecdeque, since it mostly doesn't care

        receipt.complete_processing(source.len());

        // complete_task!(conv);
        GroupIter { receipt, spec, data, source: source.into() } //FIXME
    }

    pub fn filter_by_attrib<F>(self, mut attrib: F) -> Iter<T> where F: Filter<Entity=T> {
        let mut receipt = self.receipt.clone();
        receipt.start(Task::filtering::<T>());

        let data = self.data.clone();
        let spec = self.spec.clone();
        let source: Vec<T> = attrib.execute(self.data.clone(), self.consume_source().into());

        receipt.complete_processing(source.len());

        Iter { receipt, spec, data, source: source.into()  }
    }

    pub fn sort_by_attrib<S>(self, direction: sort::Direction, mut attrib: S) -> Iter<T> where S: Sort<T> {
        let mut receipt = self.receipt.clone();
        receipt.start(Task::sorting::<T>());

        let data = self.data.clone();
        let spec = self.spec.clone();
        let source = attrib.execute(self.data.clone(), self.consume_source().into(), direction);

        receipt.complete_processing(source.len());

        Iter { receipt, spec, data, source: source.into()  }
    }

    pub fn map_to_attrib<S, R>(self, mut attrib: S) -> Iter<R> where S: Select<T, Entity=R> {
        let mut receipt = self.receipt.clone();
        receipt.start(Task::mapping::<T,R>());

        let data = self.data.clone();
        let spec = self.spec.clone();
        let source = attrib.execute(self.data.clone(), self.consume_source().into());

        receipt.complete_processing(source.len());

        Iter { receipt, spec, data, source: source.into()  }
    }

    pub fn flat_map_to_attrib<S, R>(self, mut attrib: S) -> Iter<R> where S: Select<T, Entity=Vec<R>> {
        let mut receipt = self.receipt.clone();
        receipt.start(Task::flat_mapping::<T,R>());

        let data = self.data.clone();
        let spec = self.spec.clone();
        let source: Vec<R> = attrib.execute(self.data.clone(), self.consume_source().into())
            .into_iter().flat_map(|e| e).collect();

        receipt.complete_processing(source.len());

        Iter { receipt, spec, data, source: source.into() }
    }

    pub fn sample<S>(self, mut attrib: S) -> Iter<T> where S: Sample<T> {
        let mut receipt = self.receipt.clone();
        receipt.start(Task::sampling::<T>());

        let data = self.data.clone();
        let spec = self.spec.clone();
        let source = attrib.execute(self.data.clone(), self.consume_source().into());

        receipt.complete_processing(source.len());

        Iter { receipt, spec, data, source: source.into()  }
    }

    // pub fn map_with_db<F,R>(self, f: F) -> Iter<R> where F: Fn(DataPtr, T) -> R {
    //     let mut receipt = self.receipt.clone();
    //     //receipt.start(Task::sampling::<T>()); // TODO receipt
    //
    //     let data = self.data.clone();
    //     let spec = self.spec.clone();
    //
    //     let source = self.map(|e| f(data.clone(), e)).collect();
    //
    //     //receipt.complete_processing(source.len());
    //
    //     Iter { receipt, spec, data, source }
    // }
}

impl<T> From<Lazy> for QuincunxIter<T> {
    fn from(lazy: Lazy) -> Self {
        let mut receipt = lazy.receipt.clone();
        receipt.start(Task::prefiltering());

        let data = DataPtr::from(&lazy);

        receipt.complete(); // FIXME
        QuincunxIter { receipt, spec: lazy.spec, data, source_: None }
    }
}

/**
 * A general version of QuincunxITer that is already initialized, and therefore can contain any
 *  element type.
 **/
#[derive(Clone)]
pub struct Iter<T> {
    spec: Spec,// TODO redundant
    receipt: Receipt,
    data: DataPtr,
    source: VecDeque<T>,
}

impl<T> WithData for Iter<T> {
    fn get_database_ptr(&self) -> DataPtr {
        self.data.clone()
    }
}

impl<T> ReceiptHolder for Iter<T> {
    fn get_receipt(&self) -> &Receipt {
        &self.receipt
    }
}

impl<T> Iterator for Iter<T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self.source.pop_front()
    }
}
//
// impl<A,B> Iterator for Iter<(A,B)> {
//     type Item = (A,B);
//     fn next(&mut self) -> Option<Self::Item> {
//         self.source.pop()
//     }
// }

impl<T> /* Query for */ Iter<T> {
    pub fn group_by_attrib<K, G>(self, mut attrib: G) -> GroupIter<K, T> where G: Group<T, Key=K>, K: Hash + Eq {
        let mut receipt = self.receipt.clone();
        receipt.start(Task::grouping::<K,T>());

        let source = attrib.execute(self.data.clone(), self.source.into());

        receipt.complete_processing(source.len());

        GroupIter { receipt, spec: self.spec.clone(), data: self.data.clone(), source: source.into()  }
    }

    pub fn filter_by_attrib<F>(self, mut attrib: F) -> Iter<T> where F: Filter<Entity=T> {
        let mut receipt = self.receipt.clone();
        receipt.start(Task::filtering::<T>());

        let source = attrib.execute(self.data.clone(), self.source.into());

        receipt.complete_processing(source.len());

        Iter { receipt, spec: self.spec.clone(), data: self.data.clone(), source: source.into()  }
    }

    pub fn sort_by_attrib<S>(self, direction: sort::Direction, mut attrib: S) -> Iter<T> where S: Sort<T> {
        let mut receipt = self.receipt.clone();
        receipt.start(Task::sorting::<T>());

        let source = attrib.execute(self.data.clone(), self.source.into(), direction);

        receipt.complete_processing(source.len());

        Iter { receipt, spec: self.spec.clone(), data: self.data.clone(), source: source.into()  }
    }

    pub fn map_to_attrib<S, R>(self, mut attrib: S) -> Iter<R> where S: Select<T, Entity=R> {
        let mut receipt = self.receipt.clone();
        receipt.start(Task::mapping::<T,R>());

        let source = attrib.execute(self.data.clone(), self.source.into());

        receipt.complete_processing(source.len());

        Iter { receipt, spec: self.spec.clone(), data: self.data.clone(), source: source.into()  }
    }

    pub fn flat_map_to_attrib<S, R>(self, mut attrib: S) -> Iter<R> where S: Select<T, Entity=Vec<R>> {
        let mut receipt = self.receipt.clone();
        receipt.start(Task::flat_mapping::<T,R>());

        let source: Vec<R> = attrib.execute(self.data.clone(), self.source.into())
                                .into_iter().flat_map(|e| e).collect();

        receipt.complete_processing(source.len());

        Iter { receipt, spec: self.spec.clone(), data: self.data.clone(), source: source.into()  }
    }

    pub fn sample<S>(self, mut attrib: S) -> Iter<T> where S: Sample<T> {
        let mut receipt = self.receipt.clone();
        receipt.start(Task::sampling::<T>());

        let source = attrib.execute(self.data.clone(), self.source.into());

        receipt.complete_processing(source.len());

        Iter { receipt, spec: self.spec.clone(), data: self.data.clone(), source: source.into()  }
    }

    // pub fn map_with_db<F,R>(self, f: F) -> Iter<R> where F: Fn(DataPtr, T) -> R {
    //     let mut receipt = self.receipt.clone();
    //     //receipt.start(Task::sampling::<T>()); // TODO receipt
    //
    //     let data = self.data.clone();
    //     let spec = self.spec.clone();
    //
    //     let source = self.map(|e| f(data.clone(), e)).collect();
    //
    //     //receipt.complete_processing(source.len());
    //
    //     Iter { receipt, spec, data, source }
    // }
}

// TODO: I think this is a potentially fun idea fror laziness, but I will implement a simple eager
//       solution for now.
// struct TransformedSource<T, Transform> {
//     source: QuincunxIter<T>,
//     transform: Transform,
// }
//
// impl<K, Transform> From<(QuincunxIter<Project>, Transform)> for TransformedSource<Project, Transform> where Transform: Group<Project, Key=K> {
//     fn from(source_and_transform: (QuincunxIter<Project>, Transform)) -> Self {
//         TransformedSource {
//             source: source_and_transform.0,
//             transform: source_and_transform.1,
//         }
//     }
// }

/**
 * Group iterator, probably the most used iterator we build.s
 */
#[derive(Clone)]
pub struct GroupIter<K, T> {
    spec: Spec, // TODO redundant
    receipt: Receipt,
    data: DataPtr,
    source: VecDeque<(K, Vec<T>)>
}

impl<K, T> WithData for GroupIter<K, T> {
    fn get_database_ptr(&self) -> DataPtr {
        self.data.clone()
    }
}

impl<K,T> ReceiptHolder for GroupIter<K, T> {
    fn get_receipt(&self) -> &Receipt {
        &self.receipt
    }
}

impl<K, T> Iterator for GroupIter<K, T> {
    type Item = (K, Vec<T>);
    fn next(&mut self) -> Option<Self::Item> {
        self.source.pop_front()
    }
}

impl<K, T> /* Query for */ GroupIter<K, T> {
    // TODO skipping for now, because it's not expected to be popular and I'm stuck
    // pub fn group_by_attrib<Kb, G>(self, mut attrib: G) -> GroupIter<(K, Kb), T> where G: Group<T, Key=Kb>, Kb: Hash + Eq, (K, Kb): Hash + Eq {
    //     let source: Vec<((K, Kb), Vec<T>)> =
    //         self.source.into_iter()
    //             .map(|(key, vector)| {
    //                 let vector: Vec<((K, Kb), Vec<T>)> =
    //                     attrib.execute(self.data.clone(), vector).into_iter()
    //                         .map(|(key_b, vector)| ((key, key_b), vector))
    //                         .collect();
    //                 vector
    //             })
    //             .into_group_map()
    //             .into_iter()
    //             .collect();
    //     GroupIter { spec: self.spec.clone(), data: self.data.clone(), source }
    // }

    pub fn filter_by_attrib<F>(self, mut attrib: F) -> GroupIter<K, T> where F: Filter<Entity=T> {
        let mut receipt = self.receipt.clone();
        receipt.start(Task::filtering::<T>());

        let data = self.data.clone();
        let source: Vec<(K, Vec<T>)> = self.source.into_iter()
            .map(|(key, vector)| (key, attrib.execute(data.clone(), vector)))
            .collect();

        receipt.complete_processing(source.iter().map(|(_, v)| v.len()).sum());

        GroupIter { receipt, spec: self.spec.clone(), data: self.data.clone(), source: source.into()  }
    }

    pub fn sort_by_attrib<S>(self, direction: sort::Direction, mut attrib: S) -> GroupIter<K, T> where S: Sort<T> {
        let mut receipt = self.receipt.clone();
        receipt.start(Task::sorting::<T>());

        let data = self.data.clone();
        let source: Vec<(K, Vec<T>)> = self.source.into_iter()
            .map(|(key, vector)| (key, attrib.execute(data.clone(), vector, direction)))
            .collect();

        receipt.complete_processing(source.iter().map(|(_, v)| v.len()).sum());

        GroupIter { receipt, spec: self.spec.clone(), data: self.data.clone(), source: source.into()  }
    }

    pub fn map_to_attrib<S, R>(self, mut attrib: S) -> GroupIter<K, R> where S: Select<T, Entity=R> {
        let mut receipt = self.receipt.clone();
        receipt.start(Task::mapping::<T,R>());

        let data = self.data.clone();
        let source: Vec<(K, Vec<R>)> = self.source.into_iter()
            .map(|(key, vector)| (key, attrib.execute(data.clone(), vector)))
            .collect();

        receipt.complete_processing(source.iter().map(|(_, v)| v.len()).sum());

        GroupIter { receipt, spec: self.spec.clone(), data: self.data.clone(), source: source.into()  }
    }

    pub fn flat_map_to_attrib<S, R>(self, mut attrib: S) -> GroupIter<K, R> where S: Select<T, Entity=R> {
        let mut receipt = self.receipt.clone();
        receipt.start(Task::flat_mapping::<T,R>());

        let data = self.data.clone();
        let source: Vec<(K, Vec<R>)> = self.source.into_iter()
            .map(|(key, vector)| (key, attrib.execute(data.clone(), vector)))
            .collect();

        receipt.complete_processing(source.iter().map(|(_, v)| v.len()).sum());

        GroupIter { receipt, spec: self.spec.clone(), data: self.data.clone(), source: source.into()  }
    }

    pub fn sample<S>(self, mut attrib: S) -> GroupIter<K, T> where S: Sample<T> {
        let mut receipt = self.receipt.clone();
        receipt.start(Task::sampling::<T>());

        let data = self.data.clone();
        let source: Vec<(K, Vec<T>)> = self.source.into_iter()
            .map(|(key, vector)| (key, attrib.execute(data.clone(), vector)))
            .collect();

        receipt.complete_processing(source.iter().map(|(_, v)| v.len()).sum());

        GroupIter { receipt, spec: self.spec.clone(), data: self.data.clone(), source: source.into()  }
    }

    pub fn squash(self) -> Iter<T> {
        let mut receipt = self.receipt.clone();
        receipt.start(Task::squashing::<K,T>());

        let source: Vec<T> =
            self.source.into_iter()
                .flat_map(|(_, entity)| entity)
                .collect();

        receipt.complete_processing(source.len());

        Iter { receipt, spec: self.spec.clone(), data: self.data.clone(), source: source.into()  }
    }

    // pub fn map_with_db<F,R>(self, f: F) -> GroupIter<K,R> where F: Fn(DataPtr, (K, Vec<T>)) -> (K, Vec<R>) {
    //     let mut receipt = self.receipt.clone();
    //     //receipt.start(Task::sampling::<T>()); // TODO receipt
    //
    //     let data = self.data.clone();
    //     let spec = self.spec.clone();
    //
    //     let source = self.map(|e| f(data.clone(), e)).collect();
    //
    //     //receipt.complete_processing(source.len());
    //
    //     GroupIter { receipt, spec, data, source }
    // }
}