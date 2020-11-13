use std::iter::FromIterator;
use std::hash::Hash;

use itertools::Itertools;
use chrono::Duration;

use crate::iterators::*;
use crate::objects;

pub trait Attribute { type Object; }
pub trait Getter<T>: Attribute { fn get(object: &ItemWithData<Self::Object>) -> Option<T>;       }
pub trait Counter:   Attribute { fn count(object: &ItemWithData<Self::Object>) -> Option<usize>; }

pub trait LogicalAttribute: Getter<bool> {}
pub trait LanguageAttribute: Getter<objects::Language> {}
pub trait TimestampAttribute: Getter<i64> {}
pub trait DurationAttribute: Getter<Duration> {}
pub trait StringAttribute: Getter<String> {}
pub trait IntegerAttribute: Getter<usize> {}
pub trait FloatAttribute: Getter<f64> {}

pub trait CollectionAttribute<T>: Getter<Vec<T>>            {}
pub trait IdentityAttribute<I:objects::Identity>: Getter<I> {}

pub trait Group {
    type Key: Hash + Eq;
    type Item;
    fn select_key(&self, item_with_data: &ItemWithData<Self::Item>) -> Self::Key;
}

pub trait Select {
    type Item;
    type IntoItem;
    fn select(&self, item_with_data: &ItemWithData<Self::Item>) -> Self::IntoItem;
}

pub trait Filter {
    type Item;
    fn accept(&self, item_with_data: &ItemWithData<Self::Item>) -> bool;
}

pub trait Sort {
    type Item;
    fn sort(&self, vector: &mut Vec<ItemWithData<Self::Item>>);
}

pub trait Sampler {
    type Item;
    fn sample(&self, vector: &mut Vec<ItemWithData<Self::Item>>);
}

trait AttributeIterator<'a, T>: Sized + Iterator<Item=ItemWithData<'a, T>> {
    fn filter_by_attrib<A>(self, attribute: A)
        -> AttributeFilterIter<Self, A>
        where A: Filter<Item=T> {
        AttributeFilterIter { iterator: self, attribute }
    }

    fn select_by_attrib<A, Ta, Tb>(self, attribute: A)
        -> AttributeSelectIter<Self, A>
        where A: Select<Item=Ta, IntoItem=Tb> {
        AttributeSelectIter { iterator: self, attribute }
    }

    fn sort_by_attrib<A: 'a>(self, attribute: A)
        -> std::vec::IntoIter<ItemWithData<'a, T>>
        where A: Sort<Item=T> {
        let mut vector = Vec::from_iter(self);
        attribute.sort(&mut vector);
        vector.into_iter()
    }

    fn sample<S>(self, sampler: S)
        -> std::vec::IntoIter<ItemWithData<'a, T>>
        where S: Sampler<Item=T> {
        let mut vector = Vec::from_iter(self);
        sampler.sample(&mut vector);
        vector.into_iter()
    }

    fn group_by_attrib<A, K>(self, attribute: A)
        -> std::collections::hash_map::IntoIter<K, Vec<ItemWithData<'a, T>>>
        where A: Group<Item=T, Key=K>, K: Hash + Eq {
        self.map(|item_with_data| {
            let key = attribute.select_key(&item_with_data);
            (key, item_with_data)
        }).into_group_map().into_iter()
    }
}

trait AttributeGroupIterator<'a, K, T>: Sized + Iterator<Item=(K, Vec<ItemWithData<'a, T>>)> {
    fn filter_by_attrib<A>(self, attribute: A)
        -> AttributeGroupFilterIter<Self, A>
        where A: Filter<Item=T> {
        AttributeGroupFilterIter { iterator: self, attribute }
    }
    // TODO filter_key

    fn select_by_attrib<A, Ta, Tb>(self, attribute: A)
        -> AttributeGroupSelectIter<Self, A>
        where A: Select<Item=Ta, IntoItem=Tb> {
        AttributeGroupSelectIter { iterator: self, attribute }
    }

    fn sort_by_attrib<A: 'a>(self, attribute: A)
        -> std::vec::IntoIter<(K, Vec<ItemWithData<'a, T>>)>
        where A: Sort<Item=T> {
        let vector: Vec<(K, Vec<ItemWithData<'a, T>>)> =
            self.map(|(key, mut vector)| {
                attribute.sort(&mut vector);
                (key, vector)
            }).collect();
        vector.into_iter()
    }
    // TODO sort_key, sort_key_by, sort_key_with, sort_values, sort_values_by, sort_values_with

    fn sample<S>(self, sampler: S)
        -> std::vec::IntoIter<(K, Vec<ItemWithData<'a, T>>)>
        where S: Sampler<Item=T> {
        let vector: Vec<(K, Vec<ItemWithData<'a, T>>)> =
            self.map(|(key, mut vector)| {
                sampler.sample(&mut vector);
                (key, vector)
            }).collect();
        vector.into_iter()
    }
    // TODO sample_key

    fn ungroup(self) -> std::vec::IntoIter<ItemWithData<'a, T>> {
        let vector: Vec<ItemWithData<'a, T>> =
            self.flat_map(|(_, vector)| vector).collect();
        vector.into_iter()
    }
}

struct AttributeFilterIter<I, A> { iterator: I, attribute: A }
impl<'a,I,A,T> Iterator for AttributeFilterIter<I, A>
    where I: Iterator<Item=ItemWithData<'a, T>>, A: Filter<Item=T> {
    type Item = ItemWithData<'a, T>;
    fn next(&mut self) -> Option<Self::Item> {
        let attribute = &self.attribute;
        self.iterator.find(|item_with_data| {
            attribute.accept(item_with_data)
        })
    }
}

struct AttributeGroupFilterIter<I, A> { iterator: I, attribute: A }
impl<'a,I,A,K,T> Iterator for AttributeGroupFilterIter<I, A>
    where I: Iterator<Item=(K, Vec<ItemWithData<'a, T>>)>, A: Filter<Item=T> {
    type Item = (K, Vec<ItemWithData<'a, T>>);
    fn next(&mut self) -> Option<Self::Item> {
        let attribute = &self.attribute;
        let next_group = self.iterator.next();
        next_group.map(|(key, vector)| {
            let filtered_vector: Vec<ItemWithData<T>> =
                vector.into_iter().filter(|item_with_data| {
                    attribute.accept(item_with_data)
                }).collect();
            (key, filtered_vector)
        })
    }
}

struct AttributeSelectIter<I, A> { iterator: I, attribute: A }
impl<'a,I,A,Ta,Tb> Iterator for AttributeSelectIter<I, A>
    where I: Iterator<Item=ItemWithData<'a, Ta>>, A: Select<Item=Ta, IntoItem=Tb> {
    type Item = ItemWithData<'a, Tb>;
    fn next(&mut self) -> Option<Self::Item> {
        let attribute = &self.attribute;
        self.iterator.next().map(|item_with_data| {
            ItemWithData::new(item_with_data.data,attribute.select(&item_with_data))
        })
    }
}

struct AttributeGroupSelectIter<I, A> { iterator: I, attribute: A }
impl<'a,I,A,K,Ta,Tb> Iterator for AttributeGroupSelectIter<I, A>
    where I: Iterator<Item=(K, Vec<ItemWithData<'a, Ta>>)>, A: Select<Item=Ta, IntoItem=Tb> {
    type Item = (K, Vec<ItemWithData<'a, Tb>>);
    fn next(&mut self) -> Option<Self::Item> {
        let attribute = &self.attribute;
        let next_group = self.iterator.next();
        next_group.map(|(key, vector)| {
            let mapped_vector: Vec<ItemWithData<Tb>> =
                vector.into_iter().map(|item_with_data| {
                    ItemWithData::new(item_with_data.data,attribute.select(&item_with_data))
                }).collect();
            (key, mapped_vector)
        })
    }
}

#[macro_export]
macro_rules! impl_sort_by_key {
    ($item:ident, $attrib:ident, $key_selection:expr) => {
        impl Sort for $attrib {
            type Item = $item;
            fn sort(&self, vector: &mut Vec<ItemWithData<Self::Item>>) {
                vector.sort_by_key($key_selection)
            }
        }
    }
}

#[macro_export]
macro_rules! impl_sort_by_key_with_db {
    ($item:ident, $attrib:ident, $method:ident) => {
        impl_sort_by_key!($item, $attrib, | ItemWithData { item, data } | item.$method(data));
    }
}

#[macro_export]
macro_rules! impl_sort_by_key_sans_db {
    ($item:ident, $attrib:ident, $method:ident) => {
        impl_sort_by_key!($item, $attrib, | ItemWithData { item, data: _ } | item.$method());
    }
}