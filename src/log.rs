use std::fmt::Display;

use std::cell::RefCell;
use std::time::Instant;
use std::rc::Rc;
use crate::weights_and_measures::{Weighed, Weights};

pub struct Log {
    log: Rc<RefCell<InnerLog>>
}

impl Clone for Log {
    fn clone(&self) -> Self {
        Log { log: self.log.clone() }
    }
}

impl Log {
    pub fn new(level: Verbosity) -> Self {
        Log { log: Rc::new(RefCell::new(InnerLog::new(level))) }
    }
    fn log(&self, event: &Event) {
        self.log.as_ref().borrow().log(event)
    }
    pub fn start<S>(&self, level: Verbosity, event: S) -> Event where S: Display {
        self.log.as_ref().borrow().start(level, event)
    }
    pub fn end(&self, event: Event) {
        self.log.as_ref().borrow().end(event)
    }
}

#[derive(Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord, Debug)] /*Serialize, Deserialize*/
pub enum Verbosity {
    Warning,
    Log,
    Debug,
}

impl Verbosity {
    pub(crate) fn to_level(&self) -> usize {
        match self {
            Verbosity::Warning => 0,
            Verbosity::Log => 1,
            Verbosity::Debug => 2,
        }
    }
    pub(crate) fn should_log(&self, against: &Self) -> bool {
        self.to_level() >= against.to_level()
    }
}

#[derive(Clone, Debug)] /*Serialize, Deserialize*/
struct InnerLog {
    level: Verbosity,
    sequence: RefCell<Vec<Event>>,
}

// macro_rules! log {
//     ($log:expr, $level:ident, $msg:expr) => {{
//         let event = $log.start(Verbosity::$level, $msg);
//
//         $log.end(event);
//     }}
// }

impl InnerLog {
    pub fn new(level: Verbosity) -> Self {
        InnerLog { level, sequence: RefCell::new(vec![]) }
    }
    fn log(&self, event: &Event) {
        if event.should_log(&self.level) {
            eprintln!("{}", event.message());
        }
    }
    pub fn start<S>(&self, level: Verbosity, event: S) -> Event where S: Display {
        let sequence_number = self.sequence.borrow().len() + 1;
        let event = Event::new(sequence_number, level, event);
        self.sequence.borrow_mut().push(event.clone());
        self.log(&event);
        event
    }
    pub fn end(&self, mut event: Event) {
        event.done();
        self.log(&event);
        self.sequence.borrow_mut().push(event);
    }
}

#[derive(Clone, Hash, Eq, PartialEq, PartialOrd, Ord, Debug)] /*Serialize, Deserialize*/
pub struct Event {
    id: usize,
    level: Verbosity,
    event: String,
    start: Instant,
    end: Option<Instant>,
    items: Option<usize>,
    size: Option<usize>,
}

impl Event {
    pub(crate) fn new<S>(id: usize, level: Verbosity, event: S) -> Self where S: Display {
        Event {
            id,
            event: event.to_string(),
            start: Instant::now(),
            end: None,
            items: None,
            size: None,
            level,
        }
    }
    pub(crate) fn done(&mut self) {
        self.end = Some(Instant::now())
    }
    pub(crate) fn counted(&mut self, items: usize) {
        self.items = Some(items)
    }
    pub(crate) fn weighed<T>(&mut self, object: &T) where T: Weighed {
        self.size = Some(object.weigh())
    }
    pub fn message(&self) -> String {
        match (self.elapsed_hr_time(), self.items, self.size) {
            (None, _, _) => {
                format!("Starting {}...", self.event)
            },
            (Some(elapsed), Some(items), Some(bytes)) => {
                let memory = Weights::bytes_as_human_readable_string(bytes);
                format!("Finished {} ({} items in {} and {} in memory)", self.event, items, elapsed, memory)
            }
            (Some(elapsed), Some(items), None) => {
                format!("Finished {} ({} items in {})", self.event, items, elapsed)
            }
            (Some(elapsed), None, Some(bytes)) => {
                let memory = Weights::bytes_as_human_readable_string(bytes);
                format!("Finished {} ({} and {} in memory)", self.event, elapsed, memory)
            }
            (Some(elapsed), None, None) => {
                format!("Finished {} ({}s)", self.event, elapsed)
            }
        }
    }
    pub fn elapsed_secs(&self) -> Option<u64> {
        self.end.map(|end| end.duration_since(self.start).as_secs())
    }
    pub fn elapsed_hr_time(&self) -> Option<String> {
        match self.elapsed_secs() {
            Some(seconds) if seconds < 60 => {
                Some(format!("{}s", seconds))
            }
            Some(seconds) if seconds < 60 * 60 => {
                let minutes = seconds / 60;
                let seconds = seconds % 60;
                Some(format!("{}m {}s", minutes, seconds))
            }
            Some(seconds) => {
                let hours = seconds / (60 * 60);
                let minutes = (seconds % (60 * 60)) / 60;
                let seconds = (seconds % (60 * 60)) % 60;
                Some(format!("{}h {}m {}s", hours, minutes, seconds))
            }
            None => {
                None
            }
        }
    }
    pub(crate) fn should_log(&self, system_log_level: &Verbosity) -> bool {
        system_log_level.should_log(&self.level)
    }
}

pub trait Warning {
    fn warn<S>(self, warning: S) -> Self where S: Into<String>;
}

impl<T> Warning for Option<T> {
    fn warn<S>(self, warning: S) -> Self where S: Into<String> {
        if self.is_none() {
            eprintln!("WARNING! {}", warning.into());
        }
        self
    }
}

impl<T, E> Warning for Result<T, E> where E: std::fmt::Debug {
    fn warn<S>(self, warning: S) -> Self where S: Into<String> {
        if let Some(error) = self.as_ref().err() {
            eprintln!("WARNING! {}", warning.into());
            eprintln!("associated error: {:?}", error);
        }
        self
    }
}

#[macro_export]
macro_rules! with_elapsed_secs {
    ($name:expr,$thing:expr) => {{
        eprintln!("Starting task {}...", $name);
        let start = std::time::Instant::now();
        let result = { $thing };
        let secs = start.elapsed().as_secs();
        eprintln!("Finished task {} in {}s.", $name, secs);
        (result, secs)
    }}
}

#[macro_export]
macro_rules! elapsed_secs {
    ($name:expr,$thing:expr) => {{
        eprintln!("Starting task {}...", $name);
        let start = std::time::Instant::now();
        { $thing };
        let secs = start.elapsed().as_secs();
        eprintln!("Finished task {} in {}s.", $name, secs);
        secs
    }}
}

pub(crate) struct LogIter<I: Iterator> {
    iter: I,
    log: Log,
    event: Option<Event>,
    items: usize,
    level: Verbosity,
    description: String
}
impl<I> LogIter<I> where I: Iterator {
    pub fn new<S>(description: S, log: &Log, level: Verbosity, iter: I) -> Self
        where S: Into<String> {

        LogIter {
            description: description.into(),
            log: log.clone(),
            level,
            iter,
            event: None,
            items: 0,
        }
    }
}
impl<I,T> Iterator for LogIter<I> where I: Iterator<Item=T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        if self.event.is_none() {
            self.event = Some(self.log.start(self.level, &self.description))
        }

        let item = self.iter.next();

        if item.is_some() {
            self.items += 1;

        } else {
            let mut event = self.event.as_ref().unwrap().clone();
            event.counted(self.items); // TODO factory pattern here
            self.log.end(event);
        }

        return item
    }
}