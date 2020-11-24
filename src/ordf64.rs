use std::cmp::Ordering;
use std::f64::*;
use std::hash::{Hash, Hasher};

#[derive(PartialEq, PartialOrd, Copy, Clone)]
pub struct OrdF64(f64);

impl OrdF64 {
    pub fn as_f64(&self) -> f64 { self.0 }
}

impl Into<f64> for OrdF64 { fn into(self)    -> f64  { self.0    } }
impl From<f64> for OrdF64 { fn from(n: f64)  -> Self { OrdF64(n) } }

impl Eq for OrdF64 {}

impl Ord for OrdF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.0 == INFINITY && other.0 == INFINITY { return Ordering::Equal }
        if self.0 == NEG_INFINITY && other.0 == NEG_INFINITY { return Ordering::Equal }
        if self.0 == NAN && other.0 == NAN { return Ordering::Equal }

        if self.0 == INFINITY { return Ordering::Greater }
        if self.0 == NEG_INFINITY { return Ordering::Less }
        if self.0 == NAN { return Ordering::Less }

        return self.0.partial_cmp(&other.0).unwrap()
    }
}

impl OrdF64 {
    fn integer_decode(&self) -> (u64, i16, bool) {
        let bits: u64 = unsafe { std::mem::transmute(self.0) };
        let sign: bool = bits >> 63 == 0;
        let mut exponent: i16 = ((bits >> 52) & 0x7ff) as i16;
        let mantissa = if exponent == 0 {
            (bits & 0xfffffffffffff) << 1
        } else {
            (bits & 0xfffffffffffff) | 0x10000000000000
        };
        exponent -= 1023 + 52;
        return (mantissa, exponent, sign)
    }
    fn integer_decode_as_u64(&self) -> u64 {
        let decoded = self.integer_decode();
        let mantissa = decoded.0;
        let exponent = unsafe { std::mem::transmute::<i16, u16>(decoded.1) } as u64;
        let sign = if decoded.2 { 1u64 } else { 0u64 };
        let bits: u64 = (mantissa & 0x000fffffffffffffu64)
            | ((exponent << 52) & 0x7ff0000000000000u64)
            | ((sign << 63) & 0x8000000000000000u64);
        return bits
    }
}

// I think this is a roughly good enough hash for using here, but it could be improved.
// Here it can potentially be used for keys when grouping by a result of Mean, Median, etc,
// so, one would hope, almost never.
// TODO Note to self, we could probably switch out the need to have hash implemented there by using
// Ord- and iterator-based grouping rather than the current solution using into_group_map.
impl Hash for OrdF64 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if self.0.is_nan() {
            0x7ff8000000000000u64.hash(state);
        } else if self.0 == 0f64 { // FIXME precision?
            0u64.hash(state)
        } else {
            self.integer_decode_as_u64().hash(state)
        }
    }
}