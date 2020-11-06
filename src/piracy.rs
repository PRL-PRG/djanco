pub trait OptionPiracy<T: Clone> { fn pirate(&self) -> Option<T>; }
impl<T> OptionPiracy<T> for Option<&T> where T: Clone {
    fn pirate(&self) -> Option<T> {
        self.map(|e| e.clone())
    }
}
pub trait VectorPiracy<T: Clone> { fn pirate(&self) -> Vec<T>; }
impl<T> VectorPiracy<T> for Vec<&T> where T: Clone {
    fn pirate(&self) -> Vec<T> { self.iter().map(|e| (*e).clone()).collect() }
}