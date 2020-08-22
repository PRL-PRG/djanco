use std::cmp::Ordering;

use structopt::StructOpt;
//use itertools::Itertools;

use select::selectors::sort_and_sample;
//use select::meta::ProjectMeta;
use select::cachedb::CachedDatabase;

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
    let (dcd, loading_time) = with_elapsed_seconds!(
        DCD::new(configuration.dataset_path_as_string())
    );

    let database = CachedDatabase::from(&dcd, configuration.skip_cache);

    eprintln!("Executing query");
    let (projects, query_execution_time) = with_elapsed_seconds!({

        let how_sort = sort_by_numbers!(Direction::Descending, |p: &Project| {
            let message_sizes: Vec<usize> = database
                .commits_from(p)
                .map(|c: Commit| c.message.map_or(0usize, |s| s.len()))
                .collect();

            let avg_message_size = if message_sizes.len() == 0usize {
                0usize
            } else {
                message_sizes.iter().fold(0usize, |c, e| c + e) / message_sizes.len()
            };

            avg_message_size
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

    eprintln!("Logging elapsed time to `{}`", configuration.timing_log_as_string());
    log_timing(&configuration, "commit_message_sizes",
               loading_time, query_execution_time, writing_to_output_time);
}
