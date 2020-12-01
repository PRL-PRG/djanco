use std::collections::{BTreeMap, HashMap};
use crate::iterators::ItemWithData;

pub trait Pick {
    type Ta;
    type Tb;
    fn left(self)  -> Self::Ta;
    fn right(self) -> Self::Tb;
}

// pub trait MapLeftOrRight<Ta, Tb>: IntoIter<(Ta, Tb)> {
//     fn map_left<Fn>(self, F:) -> Iterator<Ta>;
//     fn map_right(self) -> Iterator<Ta>;
// }

impl<Ta, Tb> Pick for (Ta, Tb) {
    type Ta = Ta;
    type Tb = Tb;
    fn left(self)  -> Self::Ta { self.0 }
    fn right(self) -> Self::Tb { self.1 }
}

impl<T, Ta, Tb> Pick for Vec<T> where T: Pick<Ta=Ta, Tb=Tb> {
    type Ta = Vec<T::Ta>;
    type Tb = Vec<T::Tb>;
    fn left(self)  -> Self::Ta { self.into_iter().map(|e| e.left()).collect()  }
    fn right(self) -> Self::Tb { self.into_iter().map(|e| e.right()).collect() }
}

impl<T, Ta, Tb> Pick for Option<T> where T: Pick<Ta=Ta, Tb=Tb> {
    type Ta = Option<T::Ta>;
    type Tb = Option<T::Tb>;
    fn left(self)  -> Self::Ta { self.map(|e| e.left())  }
    fn right(self) -> Self::Tb { self.map(|e| e.right()) }
}

impl<Ta, Tb> Pick for HashMap<Ta, Tb> {
    type Ta = Vec<Ta>;
    type Tb = Vec<Tb>;
    fn left(self)  -> Self::Ta { self.into_iter().map(|e| e.left()).collect()  }
    fn right(self) -> Self::Tb { self.into_iter().map(|e| e.right()).collect() }
}

impl<Ta, Tb> Pick for BTreeMap<Ta, Tb> {
    type Ta = Vec<Ta>;
    type Tb = Vec<Tb>;
    fn left(self)  -> Self::Ta { self.into_iter().map(|e| e.left()).collect()  }
    fn right(self) -> Self::Tb { self.into_iter().map(|e| e.right()).collect() }
}