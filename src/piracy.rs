pub trait Piracy<T: Clone> {
    fn pirate(&self) -> Option<T>;
}

impl<T> Piracy<T> for Option<&T> where T: Clone {
    fn pirate(&self) -> Option<T> {
        self.map(|e| e.clone())
    }
}