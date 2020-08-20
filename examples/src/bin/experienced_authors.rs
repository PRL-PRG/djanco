use std::cmp::Ordering;
use std::collections::HashMap;

use structopt::StructOpt;
use itertools::Itertools;
use itertools::MinMaxResult;

use select::selectors::filter_sort_and_sample;
use select::meta::ProjectMeta;

use dcd::{DCD, Database};
use dcd::Project;
use dcd::Commit;
use dcd::UserId;
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

        let required_experience: u64 = 2/*yrs*/ * 365/*days*/ * 24/*hrs*/ * 60/*mins*/ * 60/*secs*/;
        let required_number_of_commits_by_experienced_authors: u64 = 25;

        let author_experience: HashMap<UserId, u64> =
            database.commits()
                .map(|c| (c.author_id, c.author_time))
                .into_group_map()
                .into_iter()
                .map(|(author_id, author_times)| {(
                    author_id,
                    match author_times.iter().minmax() {
                        MinMaxResult::NoElements       => 0u64,
                        MinMaxResult::OneElement(_)    => 0u64,
                        MinMaxResult::MinMax(min, max) => (max - min) as u64,
                    }
                )})
                .collect();

        println!("Experienced authors: {} out of {}",
                 author_experience.iter().filter(|(_,t)| **t > required_experience).count(),
                 author_experience.len());

        let how_filter = |p: &Project| {
            let commits_with_experienced_authors: u64 =
                database
                    .commits_from(p)
                    .map(|c| { author_experience.get(&c.author_id).map_or(0u64, |e| *e) })
                    .filter(|experience_in_seconds| *experience_in_seconds > required_experience)
                    .count() as u64;

            commits_with_experienced_authors > required_number_of_commits_by_experienced_authors
        };

        let how_sort = sort_by_numbers!(Direction::Descending,
                                        |p: &Project| p.get_commit_count_in(&database));
        let how_sample = top!(50);

        filter_sort_and_sample(&database, how_filter, how_sort, how_sample)
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