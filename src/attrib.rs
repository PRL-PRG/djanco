use crate::iterators::ItemWithData;
use std::iter::FromIterator;
use itertools::Itertools;
use std::hash::Hash;

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

// pub trait Integer {}
// pub trait Float   {}