use std::hash::Hash;
use std::marker::PhantomData;

use crate::objects::ItemWithData;

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

pub mod sort {
    #[derive(Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord, Debug)]
    pub enum Direction { Ascending, Descending }
}

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

pub struct AttributeFilterIter<I, A> {
    pub(crate) iterator: I,
    pub(crate) attribute: A
}
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

pub struct AttributeGroupFilterIter<I, A> {
    pub(crate) iterator: I,
    pub(crate) attribute: A
}
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

pub struct AttributeMapIter<I, A, Ta, Tb> {
    pub(crate) iterator: I,
    pub(crate) attribute: A,
    pub(crate) function: PhantomData<(Ta, Tb)>
}
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

pub struct AttributeGroupMapIter<I, A, Ta, Tb> {
    pub(crate) iterator: I,
    pub(crate) attribute: A,
    pub(crate) function: PhantomData<(Ta, Tb)>
}
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