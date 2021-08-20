use clap::Clap;

use djanco::*;
use djanco::csv::*;
use djanco::log::*;
use djanco::objects::*;


fn main() {
    let config = Configuration::parse();
    let database =
        Djanco::from_config(&config, timestamp!(December 2020), store!(JavaScript, TypeScript, Python), Log::new(Verbosity::Log)).unwrap();

    for project in database.projects() {
        let mut commits: Vec<ItemWithData<Commit>> = project.commits_with_data().unwrap();
        let just_the_one_commit: ItemWithData<Commit> = commits.pop().unwrap();

        let mut project_dir = std::path::PathBuf::from(&config.output_path);
        project_dir.push(project.id().to_string());
        std::fs::create_dir_all(&project_dir).unwrap();

        let project_tree: ItemWithData<Tree> = just_the_one_commit.tree_with_data();
        project_tree.into_files_in_dir(&project_dir).unwrap();
    }
}