use std::collections::{HashMap, HashSet};

use dcd::{Database, FilePath};
use dcd::{Commit,   Project,   User, };
use dcd::{CommitId, ProjectId, UserId};
use std::iter::FromIterator;

#[derive(Debug)]
pub struct MockDatabase {
    projects:  Vec<Project>,
    commits:   Vec<Commit>,
    users:     Vec<User>,
    paths:     Vec<FilePath>,
    // snapshots: HashMap<BlobId, Snapshot>,
}

impl Database for MockDatabase {
    fn num_projects(&self) -> u64 {
        self.projects.len() as u64
    }

    fn num_commits(&self) -> u64 {
        self.commits.len() as u64
    }

    fn num_users(&self) -> u64 {
        self.users.len() as u64
    }

    fn num_file_paths(&self) -> u64 {
        self.paths.len() as u64
    }

    fn get_project(&self, id: ProjectId) -> Option<Project> {
        self.projects.get(id as usize).map(|project| project.clone())
    }

    fn get_commit(&self, id: CommitId) -> Option<Commit> {
        self.commits.get(id as usize).map(|commit| commit.clone())
    }

    fn get_user(&self, id: UserId) -> Option<&User> {
        self.users.get(id as usize)
    }

    fn get_file_path(&self, id: u64) -> Option<FilePath> {
        self.paths.get(id as usize).map(|path| path.clone())
    }

    // fn get_snapshot(&self, id: BlobId) -> Option<Snapshot> {
    //     self.snapshots.get(&id).map(|snapshot| snapshot.clone())
    // }
}

macro_rules! get_mod {
    ($map: expr, $index: expr) => { $map[($index) % $map.len()] }
}

macro_rules! copy_sorted {
    ($collection: expr) => {{ let mut v = Vec::from_iter($collection.into_iter()); v.sort(); v }}
}

impl MockDatabase {
    pub fn example(how_many_projects:            usize,
                   how_many_commits_per_head:    Vec<usize>,
                   how_many_heads_per_project:   Vec<usize>,
                   how_many_stars:               Vec<usize>,
                   project_languages:            Vec<&str>,
                   how_many_users:               usize,
                   how_many_project_owners:      usize,
                   how_many_updates:             usize)
                   -> MockDatabase {

        let mut last_commit_id = 0usize;
        let mut last_head_id = 0usize;

        let mut commits: Vec<Commit> = vec![];
        let mut projects: Vec<Project> = vec![];
        let mut users: Vec<User> = vec![];

        println!("Creating {} users", how_many_users);
        for user_id in 0..how_many_users {
            let user = User {
                id: user_id as u64,
                email: format!("{}@email.com", MockDatabase::string_from_number(user_id)).to_string(),
                name: MockDatabase::string_from_number(user_id),
            };
            users.push(user);
        }

        println!("Creating {} projects", how_many_projects);
        for project_id in 0..how_many_projects {

            let mut metadata = HashMap::new();
            metadata.insert("stars".to_string(),
                            get_mod!(how_many_stars, project_id).to_string());
            metadata.insert("ght_language".to_string(),
                            get_mod!(project_languages, project_id).to_string());

            let owner = MockDatabase::string_from_number(project_id % how_many_project_owners);
            let project_name = MockDatabase::string_from_number(project_id);
            let mut users_in_project: HashSet<UserId> = HashSet::new();

            println!("   * creating {} heads for project {}", get_mod!(how_many_heads_per_project, project_id), project_id);
            let mut heads: Vec<(String, CommitId)> = vec![];
            for head_id in last_head_id..(last_head_id+get_mod!(how_many_heads_per_project, project_id)) {
                //if last_commit_id != 0 { last_commit_id += 1 };
                let head_name = MockDatabase::string_from_number(last_commit_id);
                heads.push((head_name, last_commit_id as CommitId));
                let next_head_first_commit_id = last_commit_id + get_mod!(how_many_commits_per_head, head_id);

                println!("       - creating {} commits for project {} for head {}",
                        next_head_first_commit_id - last_commit_id, project_id, head_id);
                print!("         ");
                for commit_id in last_commit_id..next_head_first_commit_id {
                    let parents: Vec<u64> = if next_head_first_commit_id - 1 == commit_id { vec![] } else { vec![commit_id as u64 + 1] };

                    let committer_id = get_mod!(users, commit_id).id;
                    let author_id = get_mod!(users, commit_id).id;

                    users_in_project.insert(committer_id);
                    users_in_project.insert(author_id);

                    let commit = Commit {
                        id: commit_id as u64,
                        parents,
                        committer_id,
                        committer_time: 0,
                        author_id,
                        author_time: 0,
                        message: None,      // TODO currently ignored
                        changes: None,      // TODO currently ignored
                        additions: None,    // TODO currently ignored
                        deletions: None     // TODO currently ignored
                    };
                    if commit.parents.len() == 0 {
                        print!("{}", commit.id);
                    }
                    if commit.parents.len() == 1 {
                        print!("{}->", commit.id);
                    }
                    if commit.parents.len() > 1 {
                        print!("{}->{:?} ", commit.id, commit.parents);
                    }
                    commits.push(commit)
                }
                println!();
                last_commit_id = next_head_first_commit_id;
            }
            last_head_id += heads.len();

            println!("   * project {} has users: {:?}", project_id, {
                let mut users = Vec::from_iter(users_in_project.iter());
                users.sort();
                users
            });

            let project = Project {
                id: project_id as u64,
                url: format!("https://something.com/_{}/_{}.git", owner, project_name),
                last_update: (project_id % how_many_updates) as i64,
                metadata,
                heads,
            };
            projects.push(project);
        }

        return MockDatabase {
            projects,
            commits,
            users,
            paths: vec![] // TODO currently ignored
        }
    }

    fn string_from_number(n: usize) -> String {
        n.to_string() // TODO
    }
}

#[cfg(test)]
mod tests {
    use crate::mockdb::MockDatabase;
    use dcd::{Database, ProjectId, CommitId, UserId};
    use std::collections::HashSet;
    use itertools::__std_iter::FromIterator;

    fn create_db() -> MockDatabase {
        MockDatabase::example(
            /*how_many_projects:          */ 10,
            /*how_many_commits_per_head:  */ vec![10,50,100],
            /*how_many_heads_per_project: */ vec![1,2,3,4,5],
            /*how_many_stars:             */ vec![10,20,50,100,200,500],
            /*project_languages:          */ vec!["Java","JavaScript","Rust","Scala","Shell","R"],
            /*how_many_users:             */ 115,
            /*how_many_project_owners:    */ 7,
            /*how_many_updates:           */ 1,
        )
    }

    #[test] fn example_self_test() {
        let db = create_db();

        assert_eq!(10, db.num_projects());
        assert_eq!(115, db.num_users());
        //   10
        // + 50 + 100
        // + 10 + 50 + 100
        // + 10 + 50 + 100 + 10
        // + 50 + 100 + 10 + 50 + 100
        // + 10
        // + 50 + 100
        // + 10 + 50 + 100
        // + 10 + 50 + 100 + 10
        // + 50 + 100 + 10 + 50 + 100 = 1600
        assert_eq!(1600, db.num_commits());

        for i in 0..db.num_projects() {
            assert_eq!(Some(i), db.get_project(i).map(|p| p.id))
        }
        for i in 0..db.num_users() {
            assert_eq!(Some(i), db.get_user(i).map(|u| u.id))
        }
        for i in 0..db.num_commits() {
            assert_eq!(Some(i), db.get_commit(i).map(|c| c.id))
        }
    }

    #[test] fn project_iterator_test() {
        let db = create_db();
        let mut visited_projects: Vec<ProjectId> = Vec::from_iter(db.projects().map(|p| p.id));
        visited_projects.sort();
        assert_eq!(visited_projects, Vec::from_iter(0..db.num_projects()))
    }

    #[test] fn commit_iterator_test() {
        let db = create_db();
        let mut visited_commits: Vec<CommitId> = Vec::from_iter(db.commits().map(|c| c.id));
        visited_commits.sort();
        assert_eq!(visited_commits, Vec::from_iter(0..db.num_commits()))
    }

    #[test] fn user_iterator_test() {
        let db = create_db();
        let mut visited_users: Vec<UserId> = Vec::from_iter(db.users().map(|u| u.id));
        visited_users.sort();
        assert_eq!(visited_users, Vec::from_iter(0..db.num_users()))
    }

    #[test] fn project_commit_iterator_test() {
        let mut all_visited_commits: HashSet<CommitId> = HashSet::new();
        let db = create_db();

        let expected_commits: Vec<Vec<CommitId>> = vec![
            /* project 0: */ Vec::from_iter(0    .. 9    + 1),
            /* project 1: */ Vec::from_iter(10   .. 159  + 1),
            /* project 2: */ Vec::from_iter(160  .. 319  + 1),
            /* project 3: */ Vec::from_iter(320  .. 489  + 1),
            /* project 4: */ Vec::from_iter(490  .. 799  + 1),
            /* project 5: */ Vec::from_iter(800  .. 809  + 1),
            /* project 6: */ Vec::from_iter(810  .. 959  + 1),
            /* project 7: */ Vec::from_iter(960  .. 1119 + 1),
            /* project 8: */ Vec::from_iter(1120 .. 1289 + 1),
            /* project 9: */ Vec::from_iter(1290 .. 1599 + 1),
        ];

        for project in db.projects() {
             let mut project_visited_commits: Vec<CommitId> =
                 db.commits_from(&project).map(|p| p.id).collect();
             project_visited_commits.sort();
             assert_eq!(expected_commits[project.id as usize], project_visited_commits);
             all_visited_commits.extend(project_visited_commits);
        }

        assert_eq!(all_visited_commits, HashSet::from_iter(0..db.num_commits()))
    }

    #[test] fn project_users_iterator_test() {
        let mut all_visited_users: HashSet<UserId> = HashSet::new();
        let db = create_db();

        let expected_users: Vec<Vec<UserId>> = vec![
            /* project 0: */ Vec::from_iter(0    .. 9    + 1),
            /* project 1: */ Vec::from_iter(0    .. 114  + 1),
            /* project 2: */ Vec::from_iter(0    .. 114  + 1),
            /* project 3: */ Vec::from_iter(0    .. 114  + 1),
            /* project 4: */ Vec::from_iter(0    .. 114  + 1),
            /* project 5: */ vec![0, 1, 2, 3, 4, 110, 111, 112, 113, 114],
            /* project 6: */ Vec::from_iter(0    .. 114  + 1),
            /* project 7: */ Vec::from_iter(0    .. 114  + 1),
            /* project 8: */ Vec::from_iter(0    .. 114  + 1),
            /* project 9: */ Vec::from_iter(0    .. 114  + 1),
        ];

        for project in db.projects() {
            let mut project_visited_users: Vec<UserId> =
                db.user_ids_from(&project).collect();
            project_visited_users.sort();
            assert_eq!(expected_users[project.id as usize], project_visited_users);
            all_visited_users.extend(project_visited_users);
        }

        assert_eq!(copy_sorted!(all_visited_users), Vec::from_iter(0..db.num_users()))
    }
}