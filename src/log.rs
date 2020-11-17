use std::fmt::Display;

use std::cell::RefCell;
use std::time::Instant;
use std::rc::Rc;

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
    pub(crate) fn weighed<T>(&mut self, object: &T) {
        self.size = Some(std::mem::size_of_val(object))
    }
    pub fn message(&self) -> String {
        match (self.elapsed_hr_time(), self.items, self.size) {
            (None, _, _) => {
                format!("Starting {}...", self.event)
            },
            (Some(elapsed), Some(items), Some(bytes)) => {
                format!("Finished {} ({} items in {} and {}B in memory)", self.event, items, elapsed, bytes)
            }
            (Some(elapsed), Some(items), None) => {
                format!("Finished {} ({} items in {})", self.event, items, elapsed)
            }
            (Some(elapsed), None, Some(bytes)) => {
                format!("Finished {} ({} and {}B in memory)", self.event, elapsed, bytes)
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