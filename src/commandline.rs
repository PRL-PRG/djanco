use anyhow::{Context, bail};
use std::{path::PathBuf, str::FromStr};
use clap::{Clap, crate_version, crate_authors};

use crate::log::Verbosity;

pub type CommandLineOptions = Configuration;

#[derive(Clap)]
#[clap(version = crate_version!(), author = crate_authors!(), name = "Djanco query execution helper")]
pub struct Configuration {
    #[clap(short = 'o', long = "output-path", alias = "output-dir", parse(from_os_str))]
    pub output_path: PathBuf,

    #[clap(short = 'c', long = "cache-path", parse(from_os_str))]
    pub cache_path: Option<PathBuf>,

    #[clap(short = 'd', long = "dataset-path", parse(from_os_str))]
    pub dataset_path: PathBuf,

    #[clap(long = "skip-results")]
    pub do_not_archive_results: bool,

    #[clap(long = "size-limit-mb")]
    pub size_limit: Option<u32>,

    #[clap(long = "repository", alias = "repo")]
    pub repository: Option<String>,

    #[clap(long = "verbosity", short = 'v', default_value = "log")]
    pub verbosity: Verbosity,

    #[clap(long = "preclean-cache", alias = "preclean")]
    pub preclean_cache: bool,

    #[clap(long = "preclean-merged-substores", alias = "preclean-merged")]
    pub preclean_merged_substores: bool,   
}

impl FromStr for Verbosity {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s.to_lowercase().as_str() {
            "warn" | "warning" => Ok(Verbosity::Warning),
            "log" => Ok(Verbosity::Log),
            "debug" => Ok(Verbosity::Debug),
            level => bail!(format!("Invalid log level: {}", level)),
        }
    }
}

impl Configuration {
    pub fn dataset_path_as_str(&self) -> &str {
        self.dataset_path.as_os_str().to_str().unwrap()
    }
    pub fn cache_path_as_str(&self) -> &str {
        self.cache_path.as_ref().map_or(".cache", |p| p.as_os_str().to_str().unwrap())
    }
    pub fn output_path_as_str(&self) -> &str {
        self.output_path.as_os_str().to_str().unwrap()
    }
    pub fn path_in_output_dir(&self, file: impl Into<String>, extension: impl Into<String>) -> anyhow::Result<PathBuf> {
        let mut path: PathBuf = self.output_path.clone();
        std::fs::create_dir_all(path.clone())?;
        path.push(file.into());
        path.set_extension(extension.into());
        Ok(path)
    }
    pub fn path_in_output_dir_as_str(&self, file: impl Into<String>, extension: impl Into<String>) -> anyhow::Result<String> {
        let path = self.path_in_output_dir(file, extension)?;
        Ok(path
            .as_os_str()
            .to_str()
            .with_context(|| format!("Cannot convert path {:?} to UTF-8 string", path))?
            .to_owned())        
    }
}