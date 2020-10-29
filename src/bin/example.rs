use structopt::StructOpt;
use std::path::PathBuf;

use std::collections::BTreeMap;
// TODO
// * snapshots aka file contents
// * keep and produce receipt snippets
// * fix load filters, maybe base on git commit hash of query
// * logging everywhere

#[derive(StructOpt,Debug)]
pub struct Configuration {
    #[structopt(parse(from_os_str), short = "o", long = "output", name = "OUTPUT_PATH")]
    pub output_path: PathBuf,

    #[structopt(parse(from_os_str), short = "d", long = "dataset", name = "DATASET_PATH")]
    pub dataset_path: PathBuf,

    // #[structopt(parse(from_os_str), short = "l", long = "timing-log", name = "TIMING_LOG_PATH", default_value = "timing.log")]
    // pub timing_log: PathBuf,

    // #[structopt(long = "experiment-group", short = "g", name = "EXPERIMENT_NAME", default_value = "")]
    // pub group: String,

    #[structopt(parse(from_os_str), short = "c", long = "cache", name = "PERSISTENT_CACHE_PATH")]
    pub cache_path: Option<PathBuf>,

    #[structopt(parse(from_os_str), long = "data-dump", name = "DATA_DUMP_PATH")]
    pub dump_path: Option<PathBuf>
}

// macro_rules! with_elapsed_secs {
//     ($name:expr,$thing:expr) => {{
//         eprintln!("Starting task {}...", $name);
//         let start = std::time::Instant::now();
//         let result = { $thing };
//         let secs = start.elapsed().as_secs();
//         eprintln!("Finished task {} in {}s.", $name, secs);
//         (result, secs)
//     }}
// }

// macro_rules! elapsed_secs {
//     ($name:expr,$thing:expr) => {{
//         eprintln!("Starting task {}...", $name);
//         let start = std::time::Instant::now();
//         { $thing };
//         let secs = start.elapsed().as_secs();
//         eprintln!("Finished task {} in {}s.", $name, secs);
//         secs
//     }}
// }


// works with downloader from commit  146e55e34ca1f4cc5b826e0c909deac96afafc17
// cargo run --bin example --release -- -o /dejacode/query_results_old/artifact_testing/output -d /dejacode/dataset -c /dejacode/query_results_old/artifact_testing/cache --data-dump=/dejacode/query_results_old/artifact_testing/dump
fn main() {
    let m: BTreeMap<u32,u32> = vec![(1,1), (2,2), (1,2), (1,3)].into_iter().collect();
    println!("{:?}", m)
}
