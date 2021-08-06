use std::path::PathBuf;

use git2::BranchType::{Remote, Local};
use fs_extra;

#[macro_export]
macro_rules! init_timing_log {
    () => {{
        use std::io::Write;
        let mut timing_log = std::fs::File::create("timing.csv")
            .expect("Cannot create a timing log");
       writeln!(timing_log, "query, elapsed seconds, error")
            .expect("Cannot write to timing log.");
    }}
}

#[macro_export]
macro_rules! timed_query {
    ($method:path[$database:expr, $log:expr, $output:expr]) => {{
        use std::io::Write;

        let name: &str = std::stringify!($method);

        eprintln!("Starting query {}...", name);
        let start = std::time::Instant::now();
        let result = $method($database, $log, $output);
        let elapsed_secs = start.elapsed().as_secs();
        eprintln!("Finished query {} in {}s", name, elapsed_secs);
        if let Err(error) = result.as_ref() {
            eprintln!("ERROR: {}", error);
        }

        let error = result.map_or_else(
            |error| { format!("\"{}\"", error) },
            |_    | { String::new()            },
        );

        let mut timing_log_path = $output.clone();
        timing_log_path.push("timing.csv");
        std::fs::create_dir_all(&$output)
            .expect(&format!("Cannot create directory {:?}.", &$output));

        let mut timing_log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&timing_log_path)
            .expect(&format!("Cannot open timing log for appending {:?}.", timing_log_path));

        writeln!(timing_log, "{}, {}, {}", name, elapsed_secs, error)
            .expect(&format!("Cannot write to timing log {:?}.", timing_log_path));

        timing_log.flush()
            .expect(&format!("Cannot flush timing log {:?}.", timing_log_path));
    }}
}

fn clone_repository(url: &str) -> (git2::Repository, PathBuf) {
    let repository_path = tempfile::tempdir()
        .expect("Cannot create a directory for repository").into_path();

    println!("Reproduction repository cloned into {:?} from {}", &repository_path, url);

    let git_config = git2::Config::open_default().unwrap();
    let mut credential_handler = git2_credentials::CredentialHandler::new(git_config);

    let mut callbacks = git2::RemoteCallbacks::new();
    callbacks.credentials(move |url, username, allowed| {
        credential_handler.try_next_credential(url, username, allowed)
    });

    let mut fetch_options = git2::FetchOptions::new();
    fetch_options.remote_callbacks(callbacks);

    let mut builder = git2::build::RepoBuilder::new();
    builder.fetch_options(fetch_options);

    let repository = builder.clone(url, &repository_path)
        .expect(&format!("Cannot clone repository {} into directory {:?}", url, repository_path));

    (repository, repository_path)
}

fn find_or_create_branch<'a>(repository: &'a git2::Repository, url: &str, branch_name: &str) {
    if let Ok(branch) = repository.find_branch(branch_name, Local) {
        println!("Local branch {} exists in repository {}, checking out.", branch_name, url);

        let branch_name = branch.name().unwrap().unwrap();
        let branch_spec = format!("refs/heads/{}", branch_name);

        repository.checkout_tree(&repository.revparse_single(&branch_spec).unwrap(), None).unwrap();
        repository.set_head(&branch_spec).unwrap();

    } else if let Ok(_branch) = repository.find_branch(&format!("origin/{}", branch_name), Remote) {
        println!("Remote branch {} exists in repository {}, checking out.", branch_name, url);
        println!("Creating local branch {} in repository {}", branch_name, url);

        let git_config = git2::Config::open_default().unwrap();
        let mut credential_handler = git2_credentials::CredentialHandler::new(git_config);

        let mut callbacks = git2::RemoteCallbacks::new();
        callbacks.credentials(move |url, username, allowed| {
            credential_handler.try_next_credential(url, username, allowed)
        });

        let mut fetch_options = git2::FetchOptions::new();
        fetch_options.remote_callbacks(callbacks);

        repository.find_remote("origin").unwrap()
            .fetch(&[branch_name], Some(&mut fetch_options), None).unwrap();

        let fetch_head = repository.find_reference("FETCH_HEAD").unwrap();
        let fetch_commit = repository.reference_to_annotated_commit(&fetch_head).unwrap();
        let (analysis, _preference) = repository.merge_analysis(&[&fetch_commit]).unwrap();

        if analysis.is_up_to_date() {
            println!("Up to date with remote for branch {} at repository {}", branch_name, url);
        } else if analysis.is_fast_forward() {
            println!("Fast forwarding to remote for branch {} at repository {}", branch_name, url);
            let refname = format!("refs/heads/{}", branch_name);
            if let Ok(mut reference) = repository.find_reference(&refname) {
                reference.set_target(fetch_commit.id(), "Fast-Forward").unwrap();
                repository.set_head(&refname).unwrap();
                repository.checkout_head(Some(git2::build::CheckoutBuilder::default().force())).unwrap();
            } else {
                repository.reference(&refname, fetch_commit.id(), true,
                                     &format!("Setting {} to {}", branch_name, fetch_commit.id())).unwrap();
                repository.set_head(&refname).unwrap();
                repository.checkout_head(Some(
                    git2::build::CheckoutBuilder::default()
                        .allow_conflicts(true)
                        .conflict_style_merge(true)
                        .force(),
                )).unwrap();
            }
        } else {
            panic!("Cannot fast forward to remote for branch {} at repository {} [{:?}]", branch_name, url, analysis);
        }

    } else {
        println!("Creating new branch {} in repository {}", branch_name, url);

        let head = repository.head().unwrap();
        let head_oid = head.target().unwrap();
        let head_commit = repository.find_commit(head_oid).unwrap();

        let branch = repository.branch(branch_name, &head_commit, false)
            .expect(&format!("Cannot create a new branch {} in repository {}",
                             branch_name, url));

        let branch_name = branch.name().unwrap().unwrap();
        let branch_spec = format!("refs/heads/{}", branch_name);

        repository.checkout_tree(&repository.revparse_single(&branch_spec).unwrap(), None).unwrap();
        repository.set_head(&branch_spec).unwrap();
    }
}

fn wipe_repository_contents(repository_path: &PathBuf) {
    println!("Removing current contents of repository at {:?}", repository_path);
    std::fs::read_dir(&repository_path)
        .expect(&format!("Cannot read directory {:?}", repository_path))
        .map(|entry| {
            entry.expect(&format!("Cannot read entry from directory {:?}", repository_path))
        })
        .filter(|entry| entry.file_name() != ".git")
        .map(|entry| entry.path())
        .for_each(|path| {
            println!("  - {:?}", path);
            if path.is_dir() {
                std::fs::remove_dir_all(&path).expect(&format!("Cannot remove directory {:?}", path))
            } else {
                std::fs::remove_file(&path).expect(&format!("Cannot remove file {:?}", path))
            }
        });
}

fn populate_directory_from(repository_path: &PathBuf, project_path: &PathBuf) {
    println!("Populating directory {:?} from {:?}", repository_path, project_path);

    let copy_options = fs_extra::dir::CopyOptions::new();
    std::fs::read_dir(&project_path)
        .expect(&format!("Cannot read directory {:?}", repository_path))
        .map(|entry| {
            entry.expect(&format!("Cannot read entry from directory {:?}", repository_path))
        })
        .filter(|entry| entry.file_name() != ".git")
        .map(|entry| (entry.file_name(), entry.path()))
        .for_each(|(filename, source_path)| {
            let mut target_path = PathBuf::new();
            target_path.push(repository_path.clone());
            target_path.push(filename.to_str().unwrap().to_owned());

            println!("  - {:?} -> {:?}", source_path, target_path);
            if source_path.is_dir() {
                fs_extra::dir::copy(source_path, repository_path, &copy_options)
                    .expect("Failed to copy directory");
            } else {
                std::fs::copy(source_path, target_path)
                    .expect("Failed to copy file.");
            }
        });
}

fn commit_all<S>(repository: &git2::Repository, message: S) where S: Into<String> {
    let message = message.into();
    println!("Preparing a commit with message \"{}\"", message);

    let signature = repository.signature().unwrap();
    let mut index = repository.index().unwrap();

    let mut status_options = git2::StatusOptions::new();
    status_options.include_ignored(false);
    status_options.include_untracked(true);
    status_options.recurse_untracked_dirs(true);
    let statuses = repository.statuses(Some(&mut status_options)).unwrap();

    let filenames = statuses.iter().map(|e| e.path().unwrap().to_owned());
    index.add_all(filenames, git2::IndexAddOption::DEFAULT, None).unwrap();
    index.write().unwrap();

    let tree_id = index.write_tree().unwrap();
    let tree = repository.find_tree(tree_id).unwrap();

    let head = repository.head().unwrap();
    let head_oid = head.target().unwrap();
    let parent = repository.find_commit(head_oid).unwrap();

    repository.commit(Some("HEAD"), &signature, &signature, &message, &tree, &[&parent]).unwrap();
}

fn push<S>(repository: &git2::Repository, branch: S) where S: Into<String> {
    let branch = branch.into();
    println!("Attempting to push commit to branch {}", branch);

    let mut remote = repository.find_remote("origin").expect("No `origin` remote in repository");

    let git_config = git2::Config::open_default().unwrap();
    let mut credential_handler = git2_credentials::CredentialHandler::new(git_config);

    let mut callbacks = git2::RemoteCallbacks::new();
    callbacks.credentials(move |url, username, allowed| {
        credential_handler.try_next_credential(url, username, allowed)
    });

    let mut push_options = git2::PushOptions::new();
    push_options.remote_callbacks(callbacks);

    //remote.refspecs().for_each(|e| println!("{:?}", e.str()));

    remote.push(&[&format!("refs/heads/{}", branch)], Some(&mut push_options))
        .expect(&format!("Error pushing to {}", remote.url().unwrap()));
}

pub fn create_project_archive(project_name: &str, repository_url: &str) -> PathBuf {
    let (repository, repository_path) = clone_repository(repository_url);
    find_or_create_branch(&repository, repository_url, project_name);
    //checkout_branch(&repository, &branch);
    wipe_repository_contents(&repository_path);
    populate_directory_from(&repository_path, &std::env::current_dir().unwrap());
    commit_all(&repository, project_name);
    push(&repository, project_name); // FIXME
    repository_path
}

pub fn add_results(project_name: &str, repository_path: &PathBuf, results_dir: &PathBuf, size_limit: Option<u32>) {
    let repository = git2::Repository::open(repository_path)
        .expect(&format!("Cannot re-open repository {:?}", repository_path));

    let size = fs_extra::dir::get_size(results_dir)
        .expect(&format!("Cannot measure size of directory {:?}", results_dir));

    if let Some(size_limit) = size_limit {
        if (size_limit as u64) * 1024 * 1024 < size {
            panic!("Size of {:?} [~{}MB] exceeds the output size limit of {}MB.",
                   results_dir, size / 1024 / 1024, size_limit);
        }
    }

    let mut output_in_repository = repository_path.clone();
    output_in_repository.push("output");

    std::fs::create_dir_all(&output_in_repository)
        .expect(&format!("Cannot create directory {:?}", output_in_repository));

    let copy_options = fs_extra::dir::CopyOptions::new();

    fs_extra::dir::copy(&results_dir, &output_in_repository, &copy_options)
        .expect(&format!("Failed to copy directory {:?} to {:?}",
                         results_dir, output_in_repository));

    commit_all(&repository, &format!("{}: output", project_name));
    push(&repository, project_name); // FIXME
}