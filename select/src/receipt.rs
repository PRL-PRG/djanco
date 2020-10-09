use crate::djanco::Spec;
use std::time::{Duration, Instant};

pub fn name_of<T>() -> String {
    let name = std::any::type_name::<T>();
    name.to_string()
}

pub trait ReceiptHolder {
    fn get_receipt(&self) -> &Receipt;
    // fn relinquish_receipt(&self) -> Receipt {
    //     let mut receipt = self.get_receipt().clone();
    //     //receipt.complete();
    //     receipt
    // }
    // fn pass_receipt(&self) -> Receipt {
    //     let mut receipt = self.relinquish_receipt();
    //     receipt.start();
    //     receipt
    // }
}

#[derive(Debug, Clone)]
pub enum Event {
    Initial(Spec),
    Prefiltering(),
    Sorting(),
    Sampling(),
    Filtering(),
    Grouping(),
    Squashing(),
    Mapping(),
    FlatMapping(),
}

impl Event {
    fn get_message(&self) -> String {
        match &self {
            Event::Initial(spec) => format!("specification {:?}", spec), // TODO
            Event::Prefiltering() => format!("prefiltering"),
            Event::Sorting() => format!("sorting"),
            Event::Sampling() => format!("sampling"),
            Event::Filtering() => format!("filtering"),
            Event::Grouping() => format!("grouping"),
            Event::Mapping() => format!("mapping"),
            Event::Squashing() => format!("squashing"),
            Event::FlatMapping() => format!("mapping and flattening"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Task {
    event: Event,
    mark: Instant,
    touched: Option<String>,
    // FIXME remember what was done
}

impl Task {
    pub fn new(event: Event) -> Self{
        Task { event, mark: Instant::now(), touched: None }
    }
    pub fn initial(spec: &Spec) -> Self {
        Task { event: Event::Initial(spec.clone()), mark: Instant::now(), touched: None }
    }
    pub fn prefiltering() -> Self {
        Task {event: Event::Prefiltering(), mark: Instant::now(), touched: Some("projects".to_owned()) }
    }
    pub fn sorting<T>() -> Self {
        Task { event: Event::Sorting(), mark: Instant::now(), touched: Some(name_of::<T>()) }
    }
    pub fn sampling<T>() -> Self {
        Task { event: Event::Sampling(), mark: Instant::now(), touched: Some(name_of::<T>()) }
    }
    pub fn squashing<K,T>() -> Self {
        Task { event: Event::Squashing(), mark: Instant::now(), touched: Some(format!("{} from {} groups", name_of::<K>(), name_of::<T>())) }
    }
    pub fn filtering<T>() -> Self {
        Task { event: Event::Filtering(), mark: Instant::now(), touched: Some(name_of::<T>()) }
    }
    pub fn grouping<K,T>() -> Self {
        Task { event: Event::Grouping(), mark: Instant::now(), touched: Some(format!("{} by {}", name_of::<T>(), name_of::<K>())) }
    }
    pub fn mapping<T, R>() -> Self {
        Task { event: Event::Mapping(), mark: Instant::now(), touched: Some(format!("{} to {}", name_of::<T>(), name_of::<R>())) }
    }
    pub fn flat_mapping<T, R>() -> Self {
        Task { event: Event::FlatMapping(), mark: Instant::now(), touched: Some(format!("{} to {}", name_of::<T>(), name_of::<R>())) }
    }
}

impl Task {
    pub fn get_message(&self) -> String {
        let event = self.event.get_message();
        let touched =
            self.touched.as_ref().map_or(String::new(), |s| format!(" {} ", s));
        //let elapsed_time = self.elapsed.as_secs();
        format!("  Started {}{}...", event, touched)
    }
}

#[derive(Debug, Clone)]
pub struct CompletedTask {
    event: Event,
    mark: Instant,
    elapsed: Duration,
    //comment: String,
    touched: Option<(usize, String)>,
}

impl CompletedTask {
    pub fn get_message(&self) -> String {
        let event = self.event.get_message();
        let touched = self.touched.as_ref().
            map_or(String::new(), |(n, items)| format!(" {} {} ", n, items));
        let elapsed_time = self.elapsed.as_secs();
        format!("  Finished {}{} in {}s", event, touched, elapsed_time)
    }
}

impl CompletedTask {
    fn new(item: &Task, touched: usize) -> Self {
        CompletedTask {
            event: item.event.clone(),
            mark: item.mark,
            elapsed: item.mark.elapsed(),
            //comment: comment.into(),
            touched: Some((touched, item.touched.as_ref().map_or(String::new(), |e| e.clone()))),
        }
    }
}

impl From<&Task> for CompletedTask {
    fn from(task: &Task) -> Self {
        CompletedTask {
            event: task.event.clone(), mark: task.mark, elapsed: task.mark.elapsed(), touched: None,
        }
    }
}

impl From<Task> for CompletedTask {
    fn from(task: Task) -> Self {
        CompletedTask {
            event: task.event, mark: task.mark, elapsed: task.mark.elapsed(), touched: None,
        }
    }
}

#[derive(Clone,Debug)]
pub struct Receipt {
    log: Vec<CompletedTask>,
    ongoing: Option<Task>,
    quiet: bool,
}

impl Receipt {
    fn log<S>(&self, string: S) where S: Into<String> {
        if !self.quiet { eprintln!("{}", string.into()) }
    }
    pub fn new() -> Self { Receipt { quiet: false, log: vec![], ongoing: None } } // TODO quiet
    pub fn instantaneous(&mut self, task: Task) {
        if let Some(ongoing) = &self.ongoing {
            panic!("Trying to log a start of new task, but current task is still ongoing: {:?}", ongoing);
        }
        self.log(task.get_message());
        let instantaneous_task = CompletedTask::from(task);
        self.log(instantaneous_task.get_message());
        self.log.push(instantaneous_task)
    }
    pub fn start(&mut self, task: Task) {
        self.log(task.get_message());
        if let Some(ongoing) = &self.ongoing {
            panic!("Trying to log a start of new task, but current task is still ongoing: {:?}", ongoing);
        }
        self.ongoing = Some(task)
    }
    pub fn complete_processing(&mut self, items: usize) {
        match &self.ongoing {
            Some(task) => {
                self.log.push(CompletedTask::new(task, items));
                self.ongoing = None;
            }
            None => panic!("Trying to log a completion of a task, but there is no ongoing task"),
        }
        self.log(self.log.last().unwrap().get_message());
    }
    pub fn complete(&mut self) {
        match &self.ongoing {
            Some(task) => {
                self.log.push(CompletedTask::from(task));
                self.ongoing = None;
            }
            None => panic!("Trying to log a completion of a task, but there is no ongoing task"),
        }
        self.log(self.log.last().unwrap().get_message());
    }
}

// #[derive(Copy,Clone,Debug)]
// enum Step {
//     DCD,
//     Sample,
//     Sort,
//     Filter,
//     Group,
// }

// #[derive(Copy,Clone,Debug)]
// struct CompleteStep {
//     step: Step,
//     elapsed_time: Duration,
//     items: usize,
// }



