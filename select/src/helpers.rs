use std::f64::{INFINITY,NEG_INFINITY,NAN};
use std::cmp::Ordering;

pub fn opt_cmp<T>(opt1: Option<T>, opt2: Option<T>) -> Ordering where T: Ord {
        match (opt1, opt2) {
            (Some(n1), Some(n2)) =>
                     if n1 < n2 { Ordering::Less    }
                else if n1 > n2 { Ordering::Greater }
                else            { Ordering::Equal   },

                (None, None) =>       Ordering::Equal,
                (None,    _) =>       Ordering::Less,
                (_,    None) =>       Ordering::Greater,
    }
}

pub fn f64_cmp(a: f64, b: f64) -> Ordering {
    // match (*a, *b) {
    //     (INFINITY, INFINITY) => Ordering::Equal,
    //     (NEG_INFINITY, NEG_INFINITY) => Ordering::Equal,
    //     (NAN, NAN) => Ordering::Equal,
    //     (INFINITY, _) => Ordering::Greater,
    //     (NEG_INFINITY, _) => Ordering::Less,
    //     (NAN, _) => Ordering::Less,
    //     (a, b) => a.partial_cmp(&b).unwrap(),
    // }

    if a == INFINITY && b == INFINITY { return Ordering::Equal }
    if a == NEG_INFINITY && b == NEG_INFINITY { return Ordering::Equal }
    if a == NAN && b == NAN { return Ordering::Equal }

    if a == INFINITY { return Ordering::Greater }
    if a == NEG_INFINITY { return Ordering::Less }
    if a == NAN { return Ordering::Less }

    return a.partial_cmp(&b).unwrap()
}

pub fn option_f64_cmp(a: &Option<f64>, b: &Option<f64>) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (Some(_), None) => Ordering::Greater,
        (None, Some(_)) => Ordering::Less,
        (Some(a), Some(b)) => f64_cmp(*a, *b),
    }
}