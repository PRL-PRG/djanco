use std::iter::FromIterator;
use std::hash::Hash;

use itertools::Itertools;
use chrono::Duration;
use serde::{Serialize,Deserialize};

use crate::iterators::*;
use crate::objects;
use std::fs::create_dir;
use std::marker::PhantomData;

pub trait Attribute {
    type Object;
}
pub trait Getter: Attribute {
    type IntoItem;
    fn get(object: &ItemWithData<Self::Object>) -> Self::IntoItem;
    fn get_with_data<'a>(object: &ItemWithData<'a, Self::Object>) -> ItemWithData<'a, Self::IntoItem> {
        ItemWithData::new(object.data, Self::get(object))
    }
}
pub trait OptionGetter: Attribute {
    type IntoItem;
    fn get_opt(object: &ItemWithData<Self::Object>) -> Option<Self::IntoItem>;
    fn get_opt_with_data<'a>(object: &ItemWithData<'a, Self::Object>) -> Option<ItemWithData<'a, Self::IntoItem>> {
        Self::get_opt(object).map(|result| {
            ItemWithData::new(object.data, result)
        })
    }
}

pub trait CollectionGetter<T,I>: Attribute<Object=T> + OptionGetter<IntoItem=Vec<I>> {
    fn get_opt_each_with_data<'a>(object: &ItemWithData<'a, Self::Object>) -> Option<Vec<ItemWithData<'a, I>>> {
        Self::get_opt(object).map(|v| {
            v.into_iter().map(|e| {
                ItemWithData::new(object.data, e)
            }).collect()
        })
    }
}
impl<G, T, I> CollectionGetter<T,I> for G where G: Attribute<Object=T> + OptionGetter<IntoItem=Vec<I>> {}

pub trait Countable: Attribute { // TODO Option?
    fn count(object: &ItemWithData<Self::Object>) -> Option<usize>;
}

// pub trait IdentityAttribute<I> {}
// impl<A,I> IdentityAttribute<I> for A where A: Getter<IntoItem=I>, I: objects::Identity { }
//
// pub trait LogicalAttribute {}
// impl<A> LogicalAttribute for A where A: Getter<IntoItem=bool> {}
//
// pub trait LanguageAttribute {}
// impl<A> LanguageAttribute for A where A: Getter<IntoItem=objects::Language> {}
//
// pub trait TimestampAttribute {}
// impl<A> TimestampAttribute for A where A: Getter<IntoItem=i64> {}
//
// pub trait DurationAttribute {}
// impl<A> DurationAttribute for A where A: Getter<IntoItem=Duration> {}
//
// pub trait StringAttribute {}
// impl<A> StringAttribute for A where A: Getter<IntoItem=String> {}
//
// pub trait IntegerAttribute {}
// impl<A> IntegerAttribute for A where A: Getter<IntoItem=usize> {}
//
// pub trait FloatAttribute {}
// impl<A> FloatAttribute for A where A: Getter<IntoItem=f64> {}
//
// pub trait CollectionAttribute<T> {}
// impl<A,T> CollectionAttribute<T> for A where A: Getter<IntoItem=Vec<T>> + Countable {}

pub trait Group<T, I: Hash + Eq>: Attribute<Object=T> + Getter<IntoItem=I> {
    fn select_key(&self, object: &ItemWithData<T>) -> I {
        Self::get(object)
    }
}
impl<T, I, A> Group<T, I> for A where A: Attribute<Object=T> + Getter<IntoItem=I>, I: Hash + Eq {}

pub trait Select<T, I>: Attribute<Object=T> + Getter<IntoItem=I> {
    fn select(&self, object: &ItemWithData<T>) -> I {
        Self::get(object)
    }
}
impl<T, I, A> Select<T, I> for A where A: Attribute<Object=T> + Getter<IntoItem=I> {}

pub trait Filter {
    type Item;
    fn accept(&self, item_with_data: &ItemWithData<Self::Item>) -> bool;
}

pub trait Sort<T,I: Ord>: Attribute<Object=T> + Getter<IntoItem=I> {
    fn sort_ascending(&self, vector: &mut Vec<ItemWithData<T>>) {
        vector.sort_by_key(|object| Self::get(object))
    }
    fn sort(&self, direction: sort::Direction, vector: &mut Vec<ItemWithData<T>>) {
        self.sort_ascending(vector);
        if direction == sort::Direction::Descending {
            vector.reverse()
        }
    }
}
impl<A, I, T> Sort<T, I> for A where A: Getter<IntoItem=I> + Attribute<Object=T>, I: Ord {}

pub trait Sampler {
    type Item;
    fn sample(&self, vector: &mut Vec<ItemWithData<Self::Item>>);
}


pub mod sort {
    #[derive(Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord, Debug)]
    pub enum Direction { Ascending, Descending }
}

pub trait AttributeIterator<'a, T>: Sized + Iterator<Item=ItemWithData<'a, T>> {
    fn filter_by_attrib<A>(self, attribute: A)
        -> AttributeFilterIter<Self, A>
        where A: Filter<Item=T> {
        AttributeFilterIter { iterator: self, attribute }
    }

    fn select_by_attrib<A, Ta, Tb>(self, attribute: A)
        -> AttributeSelectIter<Self, A, Ta, Tb>
        where A: Select<Ta, Tb> {
        AttributeSelectIter { iterator: self, attribute, function: PhantomData }
    }

    fn sort_by_attrib<A: 'a, I>(self, attribute: A)
        -> std::vec::IntoIter<ItemWithData<'a, T>>
        where A: Sort<T, I>, I: Ord {
        self.sort_by_attrib_with_direction(sort::Direction::Descending, attribute)
    }

    fn sort_by_attrib_with_direction<A: 'a, I>(self, direction: sort::Direction, attribute: A)
                             -> std::vec::IntoIter<ItemWithData<'a, T>>
        where A: Sort<T, I>, I: Ord {
        let mut vector = Vec::from_iter(self);
        attribute.sort(direction, &mut vector);
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
        where A: Group<T, K>, K: Hash + Eq {
        self.map(|item_with_data| {
            let key = attribute.select_key(&item_with_data);
            (key, item_with_data)
        }).into_group_map().into_iter()
    }

    // TODO drop options
}

impl<'a, T, I> AttributeIterator<'a, T> for I
    where I: Sized + Iterator<Item=ItemWithData<'a, T>> {}

pub trait AttributeGroupIterator<'a, K, T>: Sized + Iterator<Item=(K, Vec<ItemWithData<'a, T>>)> {
    fn filter_by_attrib<A>(self, attribute: A)
        -> AttributeGroupFilterIter<Self, A>
        where A: Filter<Item=T> {
        AttributeGroupFilterIter { iterator: self, attribute }
    }
    // TODO filter_key

    fn select_by_attrib<A, Ta, Tb>(self, attribute: A)
        -> AttributeGroupSelectIter<Self, A, Ta, Tb>
        where A: Select<Ta, Tb> {
        AttributeGroupSelectIter { iterator: self, attribute, function: PhantomData }
    }

    fn sort_by_attrib<A: 'a, I>(self, attribute: A)
        -> std::vec::IntoIter<(K, Vec<ItemWithData<'a, T>>)>
        where A: Sort<T, I>, I: Ord {
        self.sort_by_attrib_with_direction(sort::Direction::Descending, attribute)
    }

    fn sort_by_attrib_with_direction<A: 'a, I>(self, direction: sort::Direction, attribute: A)
        -> std::vec::IntoIter<(K, Vec<ItemWithData<'a, T>>)>
        where A: Sort<T, I>, I: Ord {
        let vector: Vec<(K, Vec<ItemWithData<'a, T>>)> =
            self.map(|(key, mut vector)| {
                attribute.sort(direction, &mut vector);
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

impl<'a, K, T, I> AttributeGroupIterator<'a, K, T> for I
    where I: Sized + Iterator<Item=(K, Vec<ItemWithData<'a, T>>)> {}

pub struct AttributeFilterIter<I, A> { iterator: I, attribute: A }
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

pub struct AttributeGroupFilterIter<I, A> { iterator: I, attribute: A }
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

pub struct AttributeSelectIter<I, A, Ta, Tb> { iterator: I, attribute: A, function: PhantomData<(Ta, Tb)> }
impl<'a,I,A,Ta,Tb> Iterator for AttributeSelectIter<I, A, Ta, Tb>
    where I: Iterator<Item=ItemWithData<'a, Ta>>, A: Select<Ta, Tb> {
    type Item = ItemWithData<'a, Tb>;
    fn next(&mut self) -> Option<Self::Item> {
        let attribute = &self.attribute;
        self.iterator.next().map(|item_with_data| {
            ItemWithData::new(item_with_data.data,attribute.select(&item_with_data))
        })
    }
}

pub struct AttributeGroupSelectIter<I, A, Ta, Tb> { iterator: I, attribute: A, function: PhantomData<(Ta, Tb)> }
impl<'a,I,A,K,Ta,Tb> Iterator for AttributeGroupSelectIter<I, A, Ta, Tb>
    where I: Iterator<Item=(K, Vec<ItemWithData<'a, Ta>>)>, A: Select<Ta, Tb> {
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