use structopt::StructOpt;
use dcd::DCD;
use select::selectors;
use std::path::{Path, PathBuf};
use regex::Regex;
use select::selectors::{Filter, Sorter, Sampler};

macro_rules! with_elapsed_seconds {
    ($thing:expr) => {{
        let start = std::time::Instant::now();
        let result = { $thing };
        (result, start.elapsed().as_secs())
    }}
}

fn parse_filter_string (input: &str) -> Result<Filter, String> {
    let possible_filters = "(commits|users|stars)";
    let possible_operators = "(>|<|>=|=|==|=<)";
    let possible_values = "([0-9]+)([KM]?)";
    let expression =
        format!(r"^{}{}{}$", possible_filters, possible_operators, possible_values);
    let regex = Regex::new(expression.as_str()).unwrap();

    if !regex.is_match(input) {
        return Err(format!("Cannot parse filter specification. \
                            Input string `{}` does not match regex `{}`", input, expression));
    }

    for capture in regex.captures(input).iter() {
        let multiplier = match &capture[4] {
            "M" => 1000000usize,
            "K" =>    1000usize,
            ""  =>       1usize,
            _   => unreachable!(),
        };
        let number = (&capture[3]).parse::<usize>().unwrap() * multiplier;
        let relation: selectors::Relation = match &capture[2] {
            "="  => selectors::Relation::Equal(number),
            "==" => selectors::Relation::Equal(number),
            ">=" => selectors::Relation::EqualOrMoreThan(number),
            "=<" => selectors::Relation::EqualOrLessThan(number),
            "<"  => selectors::Relation::LessThan(number),
            ">"  => selectors::Relation::MoreThan(number),
            _    => unreachable!(),
        };
        let filter = match &capture[1] {
            "commits" => selectors::Filter::ByCommits(relation),
            "users"   => selectors::Filter::ByUsers(relation),
            "stars"   => selectors::Filter::ByStars(relation),
            _         => unreachable!(),
        };
        return Ok(filter);
    }
    unreachable!()
}

fn parse_sorter_string (input: &str) -> Result<Sorter, String> {
    let possible_sorters = "(commits|users|stars)";
    let possible_directions = r"(\(ascending\)|\(descending\)|\(\)|)";
    let expression =
        format!(r"^{}{}$", possible_sorters, possible_directions);
    let regex = Regex::new(expression.as_str()).unwrap();

    if !regex.is_match(input) {
        return Err(format!("Cannot parse sorter specification. \
                            Input string `{}` does not match regex `{}`", input, expression));
    }

    for capture in regex.captures(input).iter() {
        let direction= match &capture[2] {
            "(ascending)"  => selectors::Direction::Ascending,
            "(descending)" => selectors::Direction::Descending,
            "()"           => selectors::Direction::Descending,
            ""             => selectors::Direction::Descending,
            _              => unreachable!(),
        };
        let sorter = match &capture[1] {
            "commits"  => selectors::Sorter::ByCommits(direction),
            "users"    => selectors::Sorter::ByUser(direction),
            "stars"    => selectors::Sorter::ByStars(direction),
            _          => unreachable!(),
        };
        return Ok(sorter);
    }
    unreachable!()
}

fn parse_sampler_string (input: &str) -> Result<Sampler, String> {
    let possible_samplers = "(top|random)";
    let possible_values = "([0-9]+)([KM]?)";
    let expression =
        format!(r"^{}\({}\)$", possible_samplers, possible_values);
    let regex = Regex::new(expression.as_str()).unwrap();

    if !regex.is_match(input) {
        return Err(format!("Cannot parse sampler specification. \
                            Input string `{}` does not match regex `{}`", input, expression));
    }

    for capture in regex.captures(input).iter() {
        let multiplier = match &capture[3] {
            "M" => 1000000usize,
            "K" =>    1000usize,
            ""  =>       1usize,
            _   => unreachable!(),
        };
        let number = (&capture[2]).parse::<usize>().unwrap() * multiplier;
        let sampler = match &capture[1] {
            "top"      => selectors::Sampler::Head(number),
            "random"   => unimplemented!(),
            _          => unreachable!(),
        };
        return Ok(sampler);
    }
    unreachable!()
}

#[derive(StructOpt,Debug)]
struct Configuration {
    #[structopt(parse(from_os_str), short = "o", long = "output", name = "OUTPUT_PATH")]
    output_path: PathBuf,

    #[structopt(parse(from_os_str), short = "d", long = "dataset", name = "DATASET_PATH")]
    dataset_path: PathBuf,

    #[structopt(long = "show-details")]
    show_details: bool,

    #[structopt(long="order-by", parse(try_from_str = parse_sorter_string))]
    order_by: Option<selectors::Sorter>,

    #[structopt(long="filter", parse(try_from_str = parse_filter_string))]
    filter: Option<selectors::Filter>,

    #[structopt(long="take", parse(try_from_str = parse_sampler_string))]
    sampler: Option<selectors::Sampler>,
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
}

mod query_weaver {
    use select::selectors::{Sorter, Sampler, Filter, Query};
    use crate::Configuration;

    macro_rules! value_or_default {
        ($obj:expr,$default:expr) => {
            match $obj {
                Some(actual) => actual,
                None         => $default,
            }
        }
    }

    pub(crate) fn weave_query_from(configuration: &Configuration) -> Query {
        Query::from(value_or_default!(configuration.filter, Filter::Everything),
                    value_or_default!(configuration.order_by, Sorter::AsIs),
                    value_or_default!(configuration.sampler, Sampler::Everything))
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

        if configuration.show_details {
            write_to_output_with_details(configuration, database, project_ids)
        } else {
            write_to_output_without_details(configuration, project_ids)
        }
    }
}

fn main() {
    let configuration = Configuration::from_args();
    println!("{:?}", configuration);

    eprintln!("Loading dataset at `{}`", configuration.dataset_path_as_string());
    let (database, loading_time) = with_elapsed_seconds!(
        DCD::new(configuration.dataset_path_as_string())
    );

    let query = query_weaver::weave_query_from(&configuration);
    eprintln!("Executing query");
    let (projects, query_execution_time) = with_elapsed_seconds!(
        //selectors::group_by_language_order_by_stars_top_n(&database, configuration.top_n_as_usize())
        query.execute(&database)
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