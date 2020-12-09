use std::iter::FromIterator;
use std::hash::Hash;
use std::marker::PhantomData;

use itertools::Itertools;

use crate::iterators::*;

pub trait Attribute {
    type Object;
}
pub trait Getter<'a>: Attribute {
    type IntoItem;
    fn get(&self, object: &ItemWithData<'a, Self::Object>) -> Self::IntoItem;
    fn _get_with_data(&self, object: &ItemWithData<'a, Self::Object>) -> ItemWithData<'a, Self::IntoItem> {
        ItemWithData::new(object.data, self.get(object))
    }
}
pub trait OptionGetter<'a>: Attribute {
    type IntoItem;
    fn get_opt(&self, object: &ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem>;
    fn _get_opt_with_data(&self, object: &ItemWithData<'a, Self::Object>) -> Option<ItemWithData<'a, Self::IntoItem>> {
        self.get_opt(object).map(|result| {
            ItemWithData::new(object.data, result)
        })
    }
}

pub trait CollectionGetter<'a, T, I>: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<I>> {
    fn _get_opt_each_with_data(&self, object: &ItemWithData<'a, Self::Object>) -> Option<Vec<ItemWithData<'a, I>>> {
        self.get_opt(object).map(|v| {
            v.into_iter().map(|e| {
                ItemWithData::new(object.data, e)
            }).collect()
        })
    }
}
impl<'a, G, T, I> CollectionGetter<'a, T, I> for G where G: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<I>> {}

pub trait Countable<'a>: Attribute { // TODO Option? // FIXME needed?
    fn count(&self, object: &ItemWithData<'a, Self::Object>) -> usize;
}

pub trait OptionCountable<'a>: Attribute { // TODO Option? // FIXME needed?
    fn count(&self, object: &ItemWithData<'a, Self::Object>) -> Option<usize>;
}

pub trait Group<'a, T, I: Hash + Eq>: Attribute<Object=T> + Getter<'a, IntoItem=I> { // XXX
    fn select_key(&self, object: &ItemWithData<'a, T>) -> I { self.get(object) }
}
impl<'a, T, I, A> Group<'a, T, I> for A where A: Attribute<Object=T> + Getter<'a, IntoItem=I>, I: Hash + Eq {}

pub trait Select<'a, T, I>: Attribute<Object=T> + Getter<'a, IntoItem=I> { // XXX
    fn select(&self, object: &ItemWithData<'a, T>) -> I { self.get(object) }
}
impl<'a, T, I, A> Select<'a, T, I> for A where A: Attribute<Object=T> + Getter<'a, IntoItem=I> {}

pub trait Sort<'a, T,I: Ord>: Attribute<Object=T> + Getter<'a, IntoItem=I> {
    fn sort(&self, direction: sort::Direction, vector: &mut Vec<ItemWithData<'a, T>>) {
        vector.sort_by_key(|object| self.get(object));
        if direction == sort::Direction::Descending {
            vector.reverse()
        }
    }
}
impl<'a, A, I, T> Sort<'a, T, I> for A where A: Getter<'a, IntoItem=I> + Attribute<Object=T>, I: Ord {}

pub trait Sampler<'a, T> {
//    type Item;
    fn sample<I>(&self, iter: I) -> Vec<ItemWithData<'a, T>>
        where I: Iterator<Item=ItemWithData<'a, T>>;
    fn sample_from(&self, vector: Vec<ItemWithData<'a, T>>) -> Vec<ItemWithData<'a, T>> {
        self.sample(vector.into_iter())
    }
}

pub trait Filter<'a> {
    type Item;
    fn accept(&self, item_with_data: &ItemWithData<'a, Self::Item>) -> bool;
}

pub mod sort {
    #[derive(Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord, Debug)]
    pub enum Direction { Ascending, Descending }
}

pub trait AttributeIterator<'a, T>: Sized + Iterator<Item=ItemWithData<'a, T>> {
    fn filter_by_attrib<A>(self, attribute: A)
        -> AttributeFilterIter<Self, A>
        where A: Filter<'a, Item=T> {
        AttributeFilterIter { iterator: self, attribute }
    }

    fn map_into_attrib<A, Ta, Tb>(self, attribute: A)
                                  -> AttributeMapIter<Self, A, Ta, Tb>
        where A: Select<'a, Ta, Tb> {
        AttributeMapIter { iterator: self, attribute, function: PhantomData }
    }

    fn sort_by_attrib<A: 'a, I>(self, attribute: A)
        -> std::vec::IntoIter<ItemWithData<'a, T>>
        where A: Sort<'a, T, I>, I: Ord {
        self.sort_by_attrib_with_direction(sort::Direction::Descending, attribute)
    }

    fn sort_by_attrib_with_direction<A: 'a, I>(self, direction: sort::Direction, attribute: A)
                             -> std::vec::IntoIter<ItemWithData<'a, T>>
        where A: Sort<'a, T, I>, I: Ord {
        let mut vector = Vec::from_iter(self);
        attribute.sort(direction, &mut vector);
        vector.into_iter()
    }

    fn sample<S>(self, sampler: S)
        -> std::vec::IntoIter<ItemWithData<'a, T>>
        where S: Sampler<'a, T> {
        sampler.sample(self).into_iter()
    }

    fn group_by_attrib<A, K>(self, attribute: A)
        -> std::collections::hash_map::IntoIter<K, Vec<ItemWithData<'a, T>>>
        where A: Group<'a, T, K>, K: Hash + Eq {
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
        where A: Filter<'a, Item=T> {
        AttributeGroupFilterIter { iterator: self, attribute }
    }
    // TODO filter_key

    fn map_into_attrib<A, Ta, Tb>(self, attribute: A)
                                  -> AttributeGroupMapIter<Self, A, Ta, Tb>
        where A: Select<'a, Ta, Tb> {
        AttributeGroupMapIter { iterator: self, attribute, function: PhantomData }
    }

    fn sort_by_attrib<A: 'a, I>(self, attribute: A)
        -> std::vec::IntoIter<(K, Vec<ItemWithData<'a, T>>)>
        where A: Sort<'a, T, I>, I: Ord {
        self.sort_by_attrib_with_direction(sort::Direction::Descending, attribute)
    }

    fn sort_by_attrib_with_direction<A: 'a, I>(self, direction: sort::Direction, attribute: A)
        -> std::vec::IntoIter<(K, Vec<ItemWithData<'a, T>>)>
        where A: Sort<'a, T, I>, I: Ord {
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
        where S: Sampler<'a, T> {
        let vector: Vec<(K, Vec<ItemWithData<'a, T>>)> =
            self.map(|(key, vector)| {
                (key, sampler.sample_from(vector))
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
    where I: Iterator<Item=ItemWithData<'a, T>>, A: Filter<'a, Item=T> {
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
    where I: Iterator<Item=(K, Vec<ItemWithData<'a, T>>)>, A: Filter<'a, Item=T> {
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

pub struct AttributeMapIter<I, A, Ta, Tb> { iterator: I, attribute: A, function: PhantomData<(Ta, Tb)> }
impl<'a, I, A, Ta, Tb> Iterator for AttributeMapIter<I, A, Ta, Tb>
    where I: Iterator<Item=ItemWithData<'a, Ta>>, A: Select<'a, Ta, Tb> {
    type Item = Tb; //ItemWithData<'a, Tb>;
    fn next(&mut self) -> Option<Self::Item> {
        let attribute = &self.attribute;
        self.iterator.next().map(|item_with_data| {
            attribute.select(&item_with_data)
        })
    }
}

pub struct AttributeGroupMapIter<I, A, Ta, Tb> { iterator: I, attribute: A, function: PhantomData<(Ta, Tb)> }
impl<'a, I, A, K, Ta, Tb> Iterator for AttributeGroupMapIter<I, A, Ta, Tb>
    where I: Iterator<Item=(K, Vec<ItemWithData<'a, Ta>>)>, A: Select<'a, Ta, Tb> {
    type Item = (K, Vec<Tb>);
    fn next(&mut self) -> Option<Self::Item> {
        let attribute = &self.attribute;
        let next_group = self.iterator.next();
        next_group.map(|(key, vector)| {
            let mapped_vector: Vec<Tb> =
                vector.into_iter().map(|item_with_data| {
                    //ItemWithData::new(item_with_data.data,
                    attribute.select(&item_with_data)
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