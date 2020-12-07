use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::fmt::Display;
use serde::export::Formatter;

pub struct Fraction<N> { numerator: N, denominator: usize }
impl<N> Fraction<N> {
    pub fn new(numerator: N, denominator: usize) -> Self {
        Fraction { numerator, denominator }
    }
    pub fn divide(&self, denominator: usize) -> Self where N: Clone {
        Fraction { numerator: self.numerator.clone(), denominator: denominator * self.denominator }
    }
}
impl<N> Fraction<N> where N: Fractionable {
    pub fn as_fraction_string(&self) -> String {
        self.numerator.fraction_string(self.denominator)
    }
}

pub trait Fractionable: Sized {
    fn fraction_string(&self, denominator: usize) -> String;
}
macro_rules! impl_fractionable_for_a_bigger_type {
    ($type:tt) => {
        impl Fractionable for $type {
            fn fraction_string(&self, denominator: usize) -> String {
                let whole = self / denominator as $type;
                let rest = self % denominator as $type;
                if whole != 0 && rest != 0 {
                    let sign = if rest > 0 { "+" } else { "" };
                    format!("{}{}{}/{}", whole, sign, rest, denominator)
                } else if rest != 0 {
                    format!("{}/{}", rest, denominator)
                } else {
                    whole.to_string()
                }
            }
        }
    }
}
macro_rules! impl_fractionable_for_a_smaller_type {
    ($type:tt) => {
        impl Fractionable for $type {
            fn fraction_string(&self, denominator: usize) -> String {
                let whole = *self as i128 / denominator as i128;
                let rest = *self as i128 % denominator as i128;
                if whole != 0 && rest != 0 {
                    let sign = if rest > 0 { "+" } else { "" };
                    format!("{}{}{}/{}", whole, sign, rest, denominator)
                } else if rest != 0 {
                    format!("{}/{}", rest, denominator)
                } else {
                    whole.to_string()
                }
            }
        }
    }
}

impl_fractionable_for_a_bigger_type!(usize);

impl_fractionable_for_a_bigger_type!(u128);
impl_fractionable_for_a_bigger_type!(u64);
impl_fractionable_for_a_smaller_type!(u32);
impl_fractionable_for_a_smaller_type!(u16);
impl_fractionable_for_a_smaller_type!(u8);

impl_fractionable_for_a_bigger_type!(i128);
impl_fractionable_for_a_smaller_type!(i64);
impl_fractionable_for_a_smaller_type!(i32);
impl_fractionable_for_a_smaller_type!(i16);
impl_fractionable_for_a_smaller_type!(i8);

impl<N> Into<f64> for Fraction<N> where N: Into<f64> {
    fn into(self) -> f64 { self.numerator.into() / self.denominator as f64 }
}

impl<N> Into<f32> for Fraction<N> where N: Into<f32> {
    fn into(self) -> f32 { self.numerator.into() / self.denominator as f32 }
}

impl<N> From<N> for Fraction<N> {
    fn from(n: N) -> Self {
        Fraction::new(n, 1)
    }
}

// impl From<usize> for Fraction<usize> { fn from(n: usize) -> Self { Fraction::new(usize::from(n), 1usize) } }
// impl From<u128>  for Fraction<u128> { fn from(n: u128)  -> Self { Fraction::new(u128::from(n),  1usize) } }
// impl From<u64>   for Fraction<u64> { fn from(n: u64)   -> Self { Fraction::new(u64::from(n),   1usize) } }
// impl From<u32>   for Fraction<u32> { fn from(n: u32)   -> Self { Fraction::new(u32::from(n),   1usize) } }
// impl From<u16>   for Fraction<u16> { fn from(n: u16)   -> Self { Fraction::new(u16::from(n),      1usize) } }
// impl From<u8>    for Fraction<u8>  { fn from(n: u8)    -> Self { Fraction::new(u8::from(n),    1usize) } }
// impl From<i128>  for Fraction<i128> { fn from(n: i128)  -> Self { Fraction::new(i128::from(n),  1usize) } }
// impl From<i64>   for Fraction<i64> { fn from(n: i64)   -> Self { Fraction::new(i64::from(n),   1usize) } }
// impl From<i32>   for Fraction<i32> { fn from(n: i32)   -> Self { Fraction::new(i32::from(n),   1usize) } }
// impl From<i16>   for Fraction<i16> { fn from(n: i16)   -> Self { Fraction::new(i16::from(n),   1usize) } }
// impl From<i8>    for Fraction<i8> { fn from(n: i8)    -> Self { Fraction::new(i8::from(n),    1usize) } }

impl<N> Clone for Fraction<N> where N: Clone {
    fn clone(&self) -> Self {
        Fraction::new(self.numerator.clone(), self.denominator)
    }
}

impl<N> Display for Fraction<N> where N: Display {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
       write!(f, "{}/{}", self.numerator, self.denominator)
    }
}

impl Fraction<usize> { pub fn as_f64(&self) -> f64 { self.numerator as f64 / self.denominator as f64 } }

// FIXME other types
impl Ord for Fraction<usize> {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.numerator * other.denominator).cmp(&(other.numerator * self.denominator))
    }
}

impl PartialOrd for Fraction<usize> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (self.numerator * other.denominator).partial_cmp(&(other.numerator * self.denominator))
    }
}

impl PartialEq for Fraction<usize> {
    fn eq(&self, other: &Self) -> bool {
        (self.numerator * other.denominator).eq(&(other.numerator * self.denominator))
    }
}

impl Eq for Fraction<usize> {}

impl Hash for Fraction<usize> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self.numerator, self.denominator).hash(state)
    }
}