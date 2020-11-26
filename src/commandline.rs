use std::path::PathBuf;

use structopt::StructOpt;

#[derive(StructOpt,Debug)]
pub struct Configuration {
    #[structopt(parse(from_os_str), short = "o", long = "output", name = "OUTPUT_PATH")]
    pub output_path: PathBuf,

    #[structopt(parse(from_os_str), short = "d", long = "dataset", name = "DCD_PATH")]
    pub dataset_path: PathBuf,

    // #[structopt(parse(from_os_str), short = "l", long = "timing-log", name = "TIMING_LOG_PATH", default_value = "timing.log")]
    // pub timing_log: PathBuf,

    // #[structopt(long = "experiment-group", short = "g", name = "EXPERIMENT_NAME", default_value = "")]
    // pub group: String,

    #[structopt(parse(from_os_str), short = "c", long = "cache", name = "PERSISTENT_CACHE_PATH")]
    pub cache_path: PathBuf,

    #[structopt(parse(from_os_str), long = "data-dump", name = "DATA_DUMP_PATH")]
    pub dump_path: Option<PathBuf>,

    #[structopt(long)]
    pub grep_snapshots: bool,
}

impl Configuration {
    fn path_to_string(p: &PathBuf) -> String { p.to_str().unwrap().to_owned() }

    pub fn dataset_path(&self) -> &str           { self.dataset_path.to_str().unwrap() }
    pub fn output_path(&self)  -> &str           { self.output_path.to_str().unwrap()  }
    pub fn cache_path(&self)   -> &str           { self.cache_path.to_str().unwrap()   }
    pub fn dump_path(&self)    -> Option<String> { self.dump_path.as_ref().map(Configuration::path_to_string) }

    pub fn output_csv_path<S>(&self, file: S) -> String where S: Into<String> {
        let mut path: PathBuf = self.output_path.clone();
        path.push(file.into());
        path.set_extension("csv");
        path.to_str().unwrap().to_owned()
    }
}