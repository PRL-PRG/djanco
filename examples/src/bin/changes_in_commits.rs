use std::cmp::Ordering;

use structopt::StructOpt;
//use itertools::Itertools;

use select::selectors::sort_and_sample;
//use select::meta::ProjectMeta;

use dcd::{DCD, Database};
use dcd::Project;
use dcd::Commit;
//use dcd::Database;

use examples::sort_by_numbers;
use examples::with_elapsed_seconds;
use examples::top;
use examples::Configuration;
use examples::Direction;
use examples::io::*;


fn main() {
    let configuration = Configuration::from_args();

    eprintln!("Loading dataset at `{}`", configuration.dataset_path_as_string());
    let (database, loading_time) = with_elapsed_seconds!(
        DCD::new(configuration.dataset_path_as_string())
    );

    eprintln!("Executing query");
    let (projects, query_execution_time) = with_elapsed_seconds!({

        let how_sort = sort_by_numbers!(Direction::Descending, |p: &Project| {
            let changes_per_commit: Vec<usize> =
                database.commits_from(p).map(|c: Commit| {
                    c.changes.map_or(0, |m| m.len())
                }).collect();

            let average_changes_per_commit = if changes_per_commit.len() == 0 {
                0u64
            } else {
                changes_per_commit.iter().fold(0u64, |s: u64, c: &usize| (*c as u64) + s)
                / changes_per_commit.len() as u64
            };

            average_changes_per_commit
        });
        let how_sample = top!(50);

        sort_and_sample(&database, how_sort, how_sample)
    });

    eprintln!("Writing results to `{}`", configuration.output_path_as_string());
    let (_, writing_to_output_time) = with_elapsed_seconds!(
        write_to_output(&configuration, &projects)
    );

    eprintln!("Elapsed time...");
    eprintln!("    {}s loading",           loading_time);
    eprintln!("    {}s query execution",   query_execution_time);
    eprintln!("    {}s writing to output", writing_to_output_time);
}