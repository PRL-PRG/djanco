use std::fmt::{Display, Formatter};

use chrono::{Date, Utc, DateTime, TimeZone};
use serde::{Serialize, Deserialize};

#[derive(Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Seconds(pub u64);

impl Seconds {
    pub fn from_years(years: u64)     -> Seconds { Seconds(years   * 365 * 24 * 60 * 60) }
    pub fn from_months(months: u64)   -> Seconds { Seconds(months  *  30 * 24 * 60 * 60) }
    pub fn from_days(days: u64)       -> Seconds { Seconds(days    *  24 * 60 * 60)      }
    pub fn from_hours(hours: u64)     -> Seconds { Seconds(hours   *  60 * 60)           }
    pub fn from_minutes(minutes: u64) -> Seconds { Seconds(minutes *  60)                }
}

impl From<u64>   for Seconds { fn from(n: u64)   -> Self { Seconds(n)        } }
impl From<usize> for Seconds { fn from(n: usize) -> Self { Seconds(n as u64) } }
impl From<u32>   for Seconds { fn from(n: u32)   -> Self { Seconds(n as u64) } }
impl From<i64>   for Seconds { fn from(n: i64)   -> Self { Seconds(n as u64) } }
impl From<i32>   for Seconds { fn from(n: i32)   -> Self { Seconds(n as u64) } }
impl From<f32>   for Seconds { fn from(n: f32)   -> Self { Seconds(n as u64) } }
impl From<f64>   for Seconds { fn from(n: f64)   -> Self { Seconds(n as u64) } }

impl Into<u64>   for Seconds { fn into(self) -> u64   { self.0          } }
impl Into<usize> for Seconds { fn into(self) -> usize { self.0 as usize } }
impl Into<i64>   for Seconds { fn into(self) -> i64   { self.0 as i64   } }
impl Into<f64>   for Seconds { fn into(self) -> f64   { self.0 as f64   } }

impl Display for Seconds {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// impl ToString for Seconds {
//     fn to_string(&self) -> String { self.0.to_string() }
// }

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Month {
    January(u16), February(u16), March(u16), April(u16), May(u16), June(u16), July(u16),
    August(u16), September(u16), October(u16), November(u16), December(u16),
}

impl Month {
    pub fn month(&self) -> u8 {
        match &self {
            Month::January(_)   => 1,
            Month::February(_)  => 2,
            Month::March(_)     => 3,
            Month::April(_)     => 4,
            Month::May(_)       => 5,
            Month::June(_)      => 6,
            Month::July(_)      => 7,
            Month::August(_)    => 8,
            Month::September(_) => 9,
            Month::October(_)   => 10,
            Month::November(_)  => 11,
            Month::December(_)  => 12,
        }
    }

    pub fn year(&self) -> u16 {
        match &self {
            Month::January(year)   => *year,
            Month::February(year)  => *year,
            Month::March(year)     => *year,
            Month::April(year)     => *year,
            Month::May(year)       => *year,
            Month::June(year)      => *year,
            Month::July(year)      => *year,
            Month::August(year)    => *year,
            Month::September(year) => *year,
            Month::October(year)   => *year,
            Month::November(year)  => *year,
            Month::December(year)  => *year,
        }
    }

    pub fn to_date(&self) -> Date<Utc> {
        Utc.ymd(self.year() as i32, self.month() as u32, 1 as u32)
    }

    pub fn to_datetime(&self) -> DateTime<Utc> {
        Utc.ymd(self.year() as i32, self.month() as u32, 1 as u32)
            .and_hms(0, 0, 0)
    }
}

impl Into<Date<Utc>>     for Month { fn into(self) -> Date<Utc>     { self.to_date()       } }
impl Into<DateTime<Utc>> for Month { fn into(self) -> DateTime<Utc> { self.to_datetime()   } }
impl Into<i64>           for Month { fn into(self) -> i64 { self.to_datetime().timestamp() } }
