pub mod canned_queries;

use std::path::{PathBuf, Path};
use structopt::StructOpt;

#[macro_export]
macro_rules! with_elapsed_seconds {
    ($thing:expr) => {{
        let start = std::time::Instant::now();
        let result = { $thing };
        (result, start.elapsed().as_secs())
    }}
}

#[macro_export]
macro_rules! top {
    ($n:expr) => {{
        move |projects: Vec<Project>| {
            projects.into_iter().take($n).collect::<Vec<Project>>()
        }
    }}
}

#[macro_export]
macro_rules! top_distinct {
    ($dedup:expr, $n:expr) => {{
        move |projects: Vec<Project>| {
            projects
                .into_iter()
                .unique_by($dedup)
            .take($n)
            .collect::<Vec<Project>>()
        }
    }}
}

#[derive(Copy,Debug,Clone)]
pub enum Direction {
    Descending,
    Ascending,
}

#[macro_export]
macro_rules! sort_by_numbers_opt {
    ($direction:expr, $accessor:expr) => {{
        move |p1: &Project, p2: &Project| {
            let ascending_ordering =
                match ($accessor(p1), $accessor(p2)) {
                    (Some(n1), Some(n2)) =>
                             if n1 < n2 { Ordering::Less    }
                        else if n1 > n2 { Ordering::Greater }
                        else            { Ordering::Equal   },

                    (None, None) =>       Ordering::Equal,
                    (None,    _) =>       Ordering::Less,
                    (_,    None) =>       Ordering::Greater,
                };

            match $direction {
                Direction::Ascending  => ascending_ordering,
                Direction::Descending => ascending_ordering.reverse(),
            }
        }
    }}
}

#[macro_export]
macro_rules! sort_by_numbers {
    ($direction:expr, $accessor:expr) => {{
        |p1: &Project, p2: &Project| {
            let (n1, n2) = ($accessor(p1), $accessor(p2));
            let ascending_ordering =
                     if n1 < n2 { Ordering::Less    }
                else if n1 > n2 { Ordering::Greater }
                else            { Ordering::Equal   };

            match $direction {
                Direction::Ascending  => ascending_ordering,
                Direction::Descending => ascending_ordering.reverse(),
            }
        }
    }}
}

#[derive(StructOpt,Debug)]
pub struct Configuration {
    #[structopt(parse(from_os_str), short = "o", long = "output", name = "OUTPUT_PATH")]
    pub output_path: PathBuf,

    #[structopt(parse(from_os_str), short = "d", long = "dataset", name = "DATASET_PATH")]
    pub dataset_path: PathBuf,

    #[structopt(parse(from_os_str), short = "l", long = "timing-log", name = "TIMING_LOG_PATH", default_value = "timing.log")]
    pub timing_log: PathBuf,

    #[structopt(long = "show-details")]
    pub show_details: bool,

    #[structopt(long = "do-not-cache")]
    pub skip_cache: bool,

    #[structopt(long = "experiment-group", short = "g", name = "EXPERIMENT_NAME", default_value = "")]
    pub group: String,

    #[structopt(name = "QUERIES")]
    pub queries: Vec<String>,

    #[structopt(parse(from_os_str), short = "p", long = "persistent-cache", name = "PERSISTENT_CACHE_PATH")]
    pub persistent_cache_path: Option<PathBuf>,
}

impl Configuration {
    pub fn dataset_path_as_string(&self) -> String {
        self.dataset_path.as_os_str().to_str().unwrap().to_string()
    }

    pub fn output_path_as_string(&self) -> String {
        self.output_path.as_os_str().to_str().unwrap().to_string()
    }

    pub fn output_path_as_path(&self) -> &Path {
        self.output_path.as_path()
    }

    pub fn output_path_for(&self, query: String) -> PathBuf {
        let mut path = self.output_path.clone();
        path.push(query);
        path.set_extension("csv");
        path
    }

    pub fn output_path_for_as_string(&self, query: String) -> String {
        let mut path = self.output_path.clone();
        path.push(query);
        path.set_extension("csv");
        path.as_os_str().to_str().unwrap().to_string()
    }

    pub fn timing_log_as_string(&self) -> String {
        self.timing_log.as_os_str().to_str().unwrap().to_string()
    }

    pub fn persistent_cache_path_as_string(&self) -> Option<String> {
        match &self.persistent_cache_path {
            None => None,
            Some(p) => Some(p.as_os_str().to_str().unwrap().to_string()),
        }
    }

    pub fn use_persistent_cache(&self) -> bool {
        self.persistent_cache_path.is_some()
    }
}

macro_rules! to_string_or_empty {
        ($object: expr) => {{
            match $object {
                Some(s) => format!("{}", s),
                None    => String::new(),
            }
        }}
    }


pub mod io {
    use crate::Configuration;
    use std::error::Error;
    use dcd::Project;
    use std::fs::{File, OpenOptions, create_dir_all};
    use std::io::Write;
    use select::meta::ProjectMeta;
    use itertools::Itertools;

    fn error_on_write_to_output(configuration: &Configuration, error: &impl Error) {
        panic!("cannot write to {} ({})", configuration.output_path_as_string(), error);
    }

    fn write_anything_to_output<Formatter>(configuration: &Configuration,
                                           query: String,
                                           projects: &Vec<Project>,
                                           printer: Formatter)
        where Formatter: Fn(&Project) -> String {
        create_dir_all(&configuration.output_path).unwrap();
        match File::create(&configuration.output_path_for(query)) {
            Ok(mut file) => {
                for project in projects.iter().sorted_by_key(|p| p.id) {
                    let line = printer(project);
                    if let Err(e) = writeln!(file, "{}", line) {
                        error_on_write_to_output(configuration, &e)
                    }
                }
            },
            Err(e) => error_on_write_to_output(configuration, &e),
        }
    }

    pub fn write_to_output_with_details(configuration: &Configuration, query: String, projects: &Vec<Project>) {
        write_anything_to_output(configuration, query, projects, |project| {
            let language = to_string_or_empty!(project.get_language());
            let stars = to_string_or_empty!(project.get_stars());
            format!("{}, {}, {}, {}", project.id, project.url, language, stars).to_string()
        })
    }

    pub fn write_to_output_without_details(configuration: &Configuration, query: String, projects: &Vec<Project>) {
        write_anything_to_output(configuration, query, projects, |project| {
            project.id.to_string()
        })
    }

    pub fn write_to_output(configuration: &Configuration, query: String, projects: &Vec<Project>) {
        if configuration.show_details {
            write_to_output_with_details(configuration, query, projects)
        } else {
            write_to_output_without_details(configuration, query, projects)
        }
    }

    pub fn log_timing(configuration: &Configuration, task: &str, loading_time: u64, query_time: u64, output_time: u64) {
        let mut file = if !configuration.timing_log.is_file() {
            let mut file = File::create(configuration.timing_log.clone()).unwrap();
            writeln!(file, "{:16} {:32} {:36} {:12} {:10} {:11}",
                     "experiment", "task", "dataset",
                     "loading_time", "query_time", "output_time").unwrap();
            file
        } else {
            OpenOptions::new()
                .write(true)
                .append(true)
                .open(configuration.timing_log.clone())
                .unwrap()
        };

        writeln!(file, "{:16} {:32} {:36} {:12} {:10} {:11}",
                 configuration.group, task, configuration.dataset_path_as_string(),
                 loading_time, query_time, output_time).unwrap()
    }
}