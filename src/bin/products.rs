use clap::Clap;

use djanco::*;
use djanco::csv::*;
use djanco::log::*;
use djanco::objects::*;

fn main() {
    let config = Configuration::parse();
    let database =
        Djanco::from_config(&config, timestamp!(December 2020), store!(JavaScript, TypeScript, Python), Log::new(Verbosity::Log)).unwrap();

    //database.projects()
    //    .into_csv_in_dir(&config.output_path, "projects.csv").unwrap();

    database.projects()
        //.map_into(Select!(project::URL, project::License, project::Homepage))
        .map(|project| {
            let t: (String, Option<String>, Option<String>, ProjectId) = (
                project.url(),
                project.license(),
                None,                
                project.id()
            );
            t
        })
        .into_csv_with_headers_in_dir(vec!["URL", "License", "Homepage", "ID"], &config.output_path, "project_licenses.csv").unwrap();
}
