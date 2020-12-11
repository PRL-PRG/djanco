#![type_length_limit="1405002"]

             pub mod fraction;
             pub mod ordf64;
             pub mod commandline;
             pub mod weights_and_measures;
#[macro_use] pub mod log;
#[macro_use] pub mod query;
             pub mod csv;
#[macro_use] pub mod attrib;
             pub mod metadata;
             pub mod persistent;
             pub mod iterators;
             pub mod tuples;
             pub mod data;
             pub mod objects;
             pub mod receipt;
             pub mod spec;
             pub mod time;
             pub mod piracy;
             mod product;

#[macro_use] extern crate mashup;

use std::iter::Sum;
use itertools::Itertools;

use crate::attrib::*;
use crate::iterators::*;
use crate::fraction::*;

pub struct Length<A: Attribute>(pub A);
impl<A, T> Attribute for Length<A> where A: Attribute<Object=T> {
    type Object = T;
}
impl<'a, A, T> Getter<'a> for Length<A> where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=String> {
    type IntoItem = Option<usize>;
    fn get(&self, object: &ItemWithData<'a, Self::Object>) -> Self::IntoItem {
        self.0.get_opt(object).map(|s| s.len())
    }
}
impl<'a, A, T> OptionGetter<'a> for Length<A> where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=String> {
    type IntoItem = usize;
    fn get_opt(&self, object: &ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
        self.0.get_opt(object).map(|s| s.len())
    }
}

pub struct Count<A: Attribute>(pub A);
impl<A, T> Attribute for Count<A> where A: Attribute<Object=T> {
    type Object = T;
}
impl<'a, A, T> Getter<'a> for Count<A> where A: Attribute<Object=T> + OptionCountable<'a> {
    type IntoItem = usize;
    fn get(&self, object: &ItemWithData<'a, Self::Object>) -> Self::IntoItem {
        self.0.count(object).unwrap_or(0)
    }
}
impl<'a, A, T> OptionGetter<'a> for Count<A> where A: Attribute<Object=T> + OptionCountable<'a> {
    type IntoItem = usize;
    fn get_opt(&self, object: &ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
        self.0.count(object)
    }
}

// TODO bucket
pub struct Bin;
pub struct Bucket;

trait CalculateStat<N, T>{ fn calculate(vector: Vec<N>) -> T; }
macro_rules! impl_calculator {
        ($name:ident -> $result:ty where N: $($requirements:path),+; $calculate:item) => {
            pub struct $name<A: Attribute>(pub A);
            impl<A, T> Attribute for $name<A> where A: Attribute<Object=T> {
                type Object = T;
            }
            impl<'a, A, N, T> Getter<'a> for $name<A>
                where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<N>>, N: $($requirements +)+ {
                type IntoItem = Option<$result>;
                fn get(&self, object: &ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                    self.0.get_opt(object).map(|object| Self::calculate(object)).flatten()
                }
            }
            impl<'a, A, N, T> OptionGetter<'a> for $name<A>
                where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<N>>, N: $($requirements +)+  { //$n: $(as_item!($requirements) +)+ {
                type IntoItem = $result;
                fn get_opt(&self, object: &ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                    self.0.get_opt(object).map(|object| Self::calculate(object)).flatten()
                }
            }
            impl<A, N, T> CalculateStat<N, Option<$result>> for $name<A> where A: Attribute<Object=T>, N: $($requirements +)+  {
                $calculate
            }
        }
    }

// trait Unwrap<T,I> { fn unwrap(&self) -> I; }
// impl Unwrap<usize, f64> for std::result::Result<f64, <usize as TryInto<f64>>::Err> {
//     fn unwrap(&self) -> f64 {
//         self.unwrap()
//     }
// }

//TODO min_by/max_by/minmax_by
impl_calculator!(Min -> N where N: Ord, Clone;
        fn calculate(vector: Vec<N>) -> Option<N> { vector.into_iter().min() }
    );
impl_calculator!(Max -> N where N: Ord, Clone;
        fn calculate(vector: Vec<N>) -> Option<N> { vector.into_iter().max() }
    );
impl_calculator!(MinMax -> (N, N) where N: Ord, Clone;
        fn calculate(vector: Vec<N>) -> Option<(N,N)> { vector.into_iter().minmax().into_option() }
    );
impl_calculator!(Mean -> Fraction<N> where N: Sum;
        fn calculate(vector: Vec<N>) -> Option<Fraction<N>> {
            let length = vector.len();
            let sum = vector.into_iter().sum::<N>();
            if length == 0 {
                None
            } else {
                Some(Fraction::new(sum, length))
            }
        }
    );
impl_calculator!(Median -> Fraction<N> where N: Ord, Clone, Sum;
        fn calculate(mut items: Vec<N>) -> Option<Fraction<N>> {
            items.sort();
            let length = items.len();
            if length == 0 {
                None
            } else {
                let value: Fraction<N> =
                    if length == 1 {
                        Fraction::new(items[0].clone(), 1)
                    } else if length % 2 != 0usize {
                        Fraction::new(items[length / 2].clone(), 1)
                    } else {
                        let left: N = items[(length / 2) - 1].clone();
                        let right: N = items[(length / 2)].clone();
                        Fraction::new(vec![left, right].into_iter().sum(), 2)
                    };
                Some(value)
            }
        }
    );

pub struct Ratio<A: Attribute<Object=T>, P: Attribute<Object=T>, T>(pub A, pub P);
impl<A, P, T> Attribute for Ratio<A, P, T>
    where A: Attribute<Object=T>,
          P: Attribute<Object=T> {

    type Object = T;
}
impl<'a, A, P, T> OptionGetter<'a> for Ratio<A, P, T>
    where A: Attribute<Object=T> + OptionCountable<'a>,
          P: Attribute<Object=T> + OptionCountable<'a> {
    type IntoItem = Fraction<usize>;
    fn get_opt(&self, object: &ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
        match (self.0.count(object), self.1.count(object)) {
            (Some(n), Some(m)) => Some(Fraction::new(n, m)),
            _ => None,
        }
    }
}

impl<'a, A, P, T> Getter<'a> for Ratio<A, P, T>
    where A: Attribute<Object=T> + OptionCountable<'a>,
          P: Attribute<Object=T> + OptionCountable<'a> {
    type IntoItem = Option<Fraction<usize>>;
    fn get(&self, object: &ItemWithData<'a, Self::Object>) -> Self::IntoItem {
        match (self.0.count(object), self.1.count(object)) {
            (Some(n), Some(m)) => Some(Fraction::new(n, m)),
            _ => None,
        }
    }
}

/// Get an attribute's attribute.
pub struct From<O: Attribute, A: Attribute> (pub O, pub A);

impl<'a, O, A, T, I> Attribute for From<O, A>
    where O: Attribute<Object=T>, A: Attribute<Object=I> {
    type Object = T;
}

impl<'a, O, A, T, I, E> Getter<'a> for From<O, A>
    where O: Attribute<Object=T> + OptionGetter<'a, IntoItem=ItemWithData<'a, I>>,
          A: Attribute<Object=I> + Getter<'a, IntoItem=E> {
    type IntoItem = Option<E>;
    fn get(&self, object: &ItemWithData<'a, Self::Object>) -> Self::IntoItem {
        self.0.get_opt(object).map(|object| self.1.get(&object))
    }
}

impl<'a, O, A, T, I, E> OptionGetter<'a> for From<O, A>
    where O: Attribute<Object=T> + OptionGetter<'a, IntoItem=ItemWithData<'a, I>>,
          A: Attribute<Object=I> + OptionGetter<'a, IntoItem=E> {
    type IntoItem = E;
    fn get_opt(&self, object: &ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
        self.0.get_opt(object).map(|object| self.1.get_opt(&object)).flatten()
    }
}

/// Get an attribute from each of a sequence of attributes.
pub struct FromEach<O: Attribute, A: Attribute> (pub O, pub A);

impl<'a, O, A, T> Attribute for FromEach<O, A>
    where O: Attribute<Object=T> /*+ OptionGetter<'a, IntoItem=Vec<I>>)*/, A: Attribute {
    //<Object=I>*/ {
    type Object = T;
}

impl<'a, O, A, T, I, E> Getter<'a> for FromEach<O, A>
    where O: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<ItemWithData<'a, I>>>,
          A: Attribute<Object=I> + Getter<'a, IntoItem=E> {
    type IntoItem = Option<Vec<E>>;
    fn get(&self, object: &ItemWithData<'a, Self::Object>) -> Self::IntoItem {
        self.0.get_opt(object).map(|v| {
            v.iter().map(|object| { self.1.get(object) }).collect()
        })
    }
}

impl<'a, O, A, T, I, E> OptionGetter<'a> for FromEach<O, A>
    where O: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<ItemWithData<'a, I>>>,
          A: Attribute<Object=I> + OptionGetter<'a, IntoItem=E> {
    type IntoItem = Vec<E>;
    fn get_opt(&self, object: &ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
        self.0.get_opt(object).map(|v| {
            v.iter().flat_map(|object| { self.1.get_opt(object) }).collect()
        })
    }
}

// Get an attribute from each of a sequence of attributes buy only if a specific condition was met.
pub struct FromEachIf<A: Attribute, P> (pub A, pub P);

impl<'a, A, P, T> Attribute for FromEachIf<A, P>
    where A: Attribute<Object=T> {
    type Object = T;
}

impl<'a, A, P, T, I> OptionGetter<'a> for FromEachIf<A, P>
    where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<ItemWithData<'a, I>>>,
          P: Filter<'a, Item=I> {
    type IntoItem = Vec<ItemWithData<'a, I>>;
    fn get_opt(&self, object: &ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
        self.0.get_opt(object).map(|items| {
            items.into_iter()
                .filter(|item| self.1.accept(item))
                .collect()
        })
    }
}

impl<'a, A, P, T, I> Getter<'a> for FromEachIf<A, P>
    where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<ItemWithData<'a, I>>>,
          P: Filter<'a, Item=I> {
    type IntoItem = Option<Vec<ItemWithData<'a, I>>>;
    fn get(&self, object: &ItemWithData<'a, Self::Object>) -> Self::IntoItem {
        self.0.get_opt(object).map(|items| {
            items.into_iter()
                .filter(|item| self.1.accept(item))
                .collect()
        })
    }
}

impl<'a, A, P, T, I> Countable<'a> for FromEachIf<A, P>
    where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<ItemWithData<'a, I>>>,
          P: Filter<'a, Item=I> {
    fn count(&self, object: &ItemWithData<'a, Self::Object>) -> usize {
        self.get_opt(object).map_or(0, |vector| vector.len())
        // Could potentially count straight from iter, but would have to reimplement all of
        // get_opt. It would save allocating the vector.
    }
}

impl<'a, A, P, T, I> OptionCountable<'a> for FromEachIf<A, P>
    where A: Attribute<Object=T> + OptionGetter<'a, IntoItem=Vec<ItemWithData<'a, I>>>,
          P: Filter<'a, Item=I> {
    fn count(&self, object: &ItemWithData<'a, Self::Object>) -> Option<usize> {
        self.get_opt(object).map(|vector| vector.len())
    }
}

macro_rules! impl_select {
        ($n:ident, $($ti:ident -> $i:tt),+) => {
            pub struct $n<$($ti: Attribute,)+> ($(pub $ti,)+);
            impl<T, $($ti,)+> Attribute for $n<$($ti,)+>
                where $($ti: Attribute<Object=T>,)+ {
                type Object = T;
            }
            impl<'a, T, $($ti,)+> OptionGetter<'a> for $n<$($ti,)+>
                where $($ti: Attribute<Object=T> + OptionGetter<'a>,)+ {
                type IntoItem = ($(Option<$ti::IntoItem>,)+);
                fn get_opt(&self, object: &ItemWithData<'a, Self::Object>) -> Option<Self::IntoItem> {
                    Some(($(self.$i.get_opt(object),)+))
                }
            }
            impl<'a, T, $($ti,)+> Getter<'a> for $n<$($ti,)+>
                where $($ti: Attribute<Object=T> + Getter<'a>,)+ {
                type IntoItem = ($($ti::IntoItem,)+);

                fn get(&self, object: &ItemWithData<'a, Self::Object>) -> Self::IntoItem {
                    ($(self.$i.get(object),)+)
                }
            }
        }
    }

impl_select!(Select1,  Ta -> 0);
impl_select!(Select2,  Ta -> 0, Tb -> 1);
impl_select!(Select3,  Ta -> 0, Tb -> 1, Tc -> 2);
impl_select!(Select4,  Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3);
impl_select!(Select5,  Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4);
impl_select!(Select6,  Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4, Tf -> 5);
impl_select!(Select7,  Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4, Tf -> 5, Tg -> 6);
impl_select!(Select8,  Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4, Tf -> 5, Tg -> 6, Th -> 7);
impl_select!(Select9,  Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4, Tf -> 5, Tg -> 6, Th -> 7, Ti -> 8);
impl_select!(Select10, Ta -> 0, Tb -> 1, Tc -> 2, Td -> 3, Te -> 4, Tf -> 5, Tg -> 6, Th -> 7, Ti -> 8, Tj -> 9);

#[macro_export]
macro_rules! Select {
    ($ta:expr) => {
        Select1($ta)
    };
    ($ta:expr, $tb:expr) => {
        Select2($ta, $tb)
    };
    ($ta:expr, $tb:expr, $tc:expr) => {
        Select3($ta, $tb, $tc)
    };
    ($ta:expr, $tb:expr, $tc:expr, $td:expr) => {
        Select4($ta, $tb, $tc, $td)
    };
    ($ta:expr, $tb:expr, $tc:expr, $td:expr, $te:expr) => {
        Select5($ta, $tb, $tc, $td, $te)
    };
    ($ta:expr, $tb:expr, $tc:expr, $td:expr, $te:expr, $tf:expr) => {
        Select6($ta, $tb, $tc, $td, $te, $tf)
    };
    ($ta:expr, $tb:expr, $tc:expr, $td:expr, $te:expr, $tf:expr, $tg:expr) => {
        Select7($ta, $tb, $tc, $td, $te, $tf, $tg)
    };
    ($ta:expr, $tb:expr, $tc:expr, $td:expr, $te:expr, $tf:expr, $tg:expr, $th:expr) => {
        Select8($ta, $tb, $tc, $td, $te, $tf, $tg, $th)
    };
    ($ta:expr, $tb:expr, $tc:expr, $td:expr, $te:expr, $tf:expr, $tg:expr, $th:expr, $ti:expr) => {
        Select9($ta, $tb, $tc, $td, $te, $tf, $tg, $th, $ti)
    };
    ($ta:expr, $tb:expr, $tc:expr, $td:expr, $te:expr, $tf:expr, $tg:expr, $th:expr, $ti:expr, $tj:expr) => {
        Select10($ta, $tb, $tc, $td, $te, $tf, $tg, $th, $ti, $tj)
    };
}

