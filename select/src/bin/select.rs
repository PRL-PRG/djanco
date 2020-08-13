use structopt::StructOpt;

use dcd::DCD;

use select::selectors;
use std::path::{Path, PathBuf};

macro_rules! with_elapsed_seconds {
    ($thing:expr) => {{
        let start = std::time::Instant::now();
        let result = { $thing };
        (result, start.elapsed().as_secs())
    }}
}

#[derive(StructOpt)]
struct Configuration {
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: PathBuf,

    #[structopt(parse(from_os_str), short = "d", long = "dataset")]
    dataset_path: PathBuf,

    #[structopt(long = "show-details")]
    show_details: Option<bool>,

    #[structopt(short = "n", long = "top-n")]
    top_n: Option<usize>,
}

impl Configuration {
    fn dataset_path_as_string(&self) -> String {
        self.dataset_path.as_os_str().to_str().unwrap().to_string()
    }

    fn output_path_as_string(&self) -> String {
        self.output_path.as_os_str().to_str().unwrap().to_string()
    }

    fn output_path_as_path(&self) -> &Path {
        self.output_path.as_path()
    }

    fn show_details_as_bool(&self) -> bool {
        match self.show_details  {
            Some(v) => v,
            None => false,
        }
    }

    fn top_n_as_usize(&self) -> usize {
        match self.top_n  {
            Some(v) => v,
            None => 100, // FIXME
        }
    }
}

mod io {
    use crate::Configuration;
    use dcd::{ProjectId, Database};
    use std::fs::File;
    use std::error::Error;
    use select::meta::ProjectMeta;
    use std::io::Write;

    macro_rules! to_string_or_empty {
        ($object: expr) => {{
            match $object {
                Some(s) => format!("{}", s),
                None    => String::new(),
            }
        }}
    }

    fn error_on_write_to_output(configuration: &Configuration, error: &impl Error) {
        panic!("cannot write to {} ({})", configuration.output_path_as_string(), error);
    }

    fn write_anything_to_output<Formatter> (configuration: &Configuration,
                                   project_ids: &Vec<ProjectId>,
                                   printer: Formatter)

        where Formatter: Fn(ProjectId) -> String {

        match File::create(configuration.output_path_as_path()) {
            Ok(mut file) => {
                for project_id in project_ids.iter() {
                    let line = printer(*project_id);
                    if let Err(e) = writeln!(file, "{}", line) {
                        error_on_write_to_output(configuration, &e)
                    }
                }
            },
            Err(e) => error_on_write_to_output(configuration, &e),
        }
    }

    pub(crate) fn write_to_output_with_details(configuration: &Configuration,
                                               database: &impl Database,
                                               project_ids: &Vec<ProjectId>) {

        write_anything_to_output(configuration, project_ids, |project_id| {
            let project = database.get_project(project_id).unwrap();
            let language = to_string_or_empty!(project.get_language());
            let stars    = to_string_or_empty!(project.get_stars());
            format!("{}, {}, {}", project.id, language, stars).to_string()
        })
    }

    pub(crate) fn write_to_output_without_details(configuration: &Configuration,
                                                  project_ids: &Vec<ProjectId>) {

        write_anything_to_output(configuration, project_ids, |project_id| {
            project_id.to_string()
        })
    }

    pub(crate) fn write_to_output(configuration: &Configuration,
                                  database: &impl Database,
                                  project_ids: &Vec<ProjectId>) {

        if configuration.show_details_as_bool() {
            write_to_output_with_details(configuration, database, project_ids)
        } else {
            write_to_output_without_details(configuration, project_ids)
        }
    }
}



fn main() {
    let configuration = Configuration::from_args();

    eprintln!("Loading dataset at `{}`", configuration.dataset_path_as_string());
    let (database, loading_time) = with_elapsed_seconds!(
        DCD::new(configuration.dataset_path_as_string())
    );

    eprintln!("Executing query, selecting top {} project for each language", configuration.top_n_as_usize());
    let (projects, query_execution_time) = with_elapsed_seconds!(
        selectors::group_by_language_order_by_stars_top_n(&database, configuration.top_n_as_usize())
    );

    eprintln!("Writing results to `{}`", configuration.output_path_as_string());
    let (_, writing_to_output_time) = with_elapsed_seconds!(
        io::write_to_output(&configuration, &database, &projects)
    );

    eprintln!("Elapsed time...");
    eprintln!("    {}s loading",           loading_time);
    eprintln!("    {}s query execution",   query_execution_time);
    eprintln!("    {}s writing to output", writing_to_output_time);
}