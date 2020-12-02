use structopt::StructOpt;

use dcd::DatastoreView;

use djanco::data::*;
use djanco::time;
use djanco::csv::*;
use djanco::log::*;
use djanco::commandline::*;
use djanco::objects::*;
use std::fs::File;
use std::io::Write;
use bstr::ByteSlice;
use std::collections::BTreeMap;

fn main() {
    let now = time::now();
    let config = Configuration::from_args();
    let log = Log::new(Verbosity::Debug);

    let wanted_commit_id = 46349u64;

    let store = DatastoreView::new(config.dataset_path(), now);

    let database = Database::from_store(store, config.cache_path(), log);

    //println!("{}", database.snapshot(&SnapshotId::from(91851u64)).unwrap().contents());

    database.commits().map(|commit| commit.message()).count();

    // store.commit_hashes()
    //     .filter(|(commit_id, hash)| {
    //         hash.to_string() == "f42ad929576723db1af0f416886f57e4cb057d48".to_owned()
    //     })
    //     .for_each(|(commit_id, hash)| {
    //         println!("commit_id {} -> {}", commit_id, hash.to_string())
    //     });
    //
    //
    // let hash_id_to_content_id_map: BTreeMap<u64, u64> = store.contents().map(|(a, b)| (b, a)).collect();
    //
    // let content_ids: Vec<(u64, Option<u64>)> =
    //     store.commits().flat_map(|(id, commit)| {
    //         commit.changes.iter().map(|(path_id, hash_id)| {
    //             let content_id: Option<&u64> = hash_id_to_content_id_map.get(hash_id);
    //             (id, content_id.map(|e| *e))
    //         }).collect::<Vec<(u64, Option<u64>)>>()
    //     }).collect();
    //
    // content_ids.into_iter()
    //     .filter(|(id, content_id)| *id == wanted_commit_id)
    //     .for_each(|(id, content_id)| {
    //         println!("{} {:?}", id, content_id);
    //         if content_id.is_some() {
    //             let content = store.content_data(content_id.unwrap());
    //             //println!("{}", content.unwrap());
    //             let mut file = File::create(format!("{}.txt", content_id.unwrap()).unwrap();
    //             if content.is_some() {
    //                 file.write(content.unwrap().as_slice()).unwrap();
    //             }
    //         }
    //     });

    // // FIXME i think path retrieval is screwed up: needs testing
    // database.snapshots().filter(|snapshot|{
    //     snapshot.contains("ruote-kit")
    // }).for_each(|snapshot| {
    //     let path = config.output_path_in_subdir("zzz", snapshot.id().to_string(), "txt").unwrap();
    //     let mut file = File::create(path).unwrap();
    //     file.write(snapshot.raw_contents()).unwrap();
    //
    //     println!("snapshot with thng: {}", snapshot.id());
    //     //println!("{}", snapshot.contents());
    // });

    //println!("{}", store.content(132227u64).unwrap().to_str_lossy());

    // DatastoreView::new(config.dataset_path(), now).commits().filter(|(id, commit)| {
    //       *id == 46344u64
    // }).map(|(id, commit)| {
    //     println!("-->commit id: {}", id);
    //     for (path, snapshot) in commit.changes {
    //         println!("{} {}", path, snapshot);
    //     }
    // }).count();

    // //let x: Vec<(String, String, String, /*String, SnapshotId*/)> =
    // for project in database.projects().filter(|p| p.id() == ProjectId::from(1u64)) {
    //     let project_id = project.id().clone();
    //     let project_url = project.url().clone();
    //     println!("{} {} ", project_id, project_url);
    //
    //     for head in project.heads().unwrap() /*.iter().filter(|head| head.name() == "master")*/ {
    //         let commit = head.commit(project.data);
    //         println!("commit id: {:?}", commit.id());
    //         println!("head: {:?}", head);
    //         println!("hash: {:?}", commit.hash(project.data));
    //         let snapshots = commit.changed_snapshots(project.data).unwrap_or(vec![]);
    //         for snapshot in snapshots {
    //             println!("snapshot_id: {}", snapshot.id());
    //             let path = config.output_path_in_subdir("butts", snapshot.id().to_string(), "txt").unwrap();
    //             let mut file = File::create(path).unwrap();
    //             file.write(snapshot.raw_contents()).unwrap();
    //         }
    //         let paths = commit.changed_path_ids(project.data).unwrap_or(vec![]);
    //         // for path in paths {
    //         //     println!("path_id: {} ", path);
    //         //     let u: u64 = path.into();
    //         //     let x: SnapshotId = SnapshotId::from(u);
    //         //     let snapshot = database.snapshot(&x);
    //         //     if snapshot.is_none() {
    //         //         println!("dud");
    //         //         continue
    //         //     }
    //         //     let snapshot = snapshot.unwrap();
    //         //     let path = config.output_path_in_subdir("yyy", x.to_string(), "txt").unwrap();
    //         //     let mut file = File::create(path).unwrap();
    //         //     file.write(snapshot.raw_contents()).unwrap();
    //         // }
    //     }
    // }

        // for commit in project.commits().unwrap_or(vec![]) {
        //     let hash = commit.hash(project.data).unwrap_or(String::new());
        //     let changes = commit.change_ids(project.data).unwrap_or(vec!());
        //     for (path_id, snapshot_id) in changes {
        //         println!("{} {} {} {:?} {}", project_id, project_url, hash, database.path(&path_id), snapshot_id);
        //     }
        // }
    // }

    //     let project_id = project.url().clone();
    //     project.commits().map_or(vec![], |v| {
    //         v.iter().map(|c| {
    //             let hash = c.hash(project.data).unwrap_or(String::new());
    //             let changes = c.change_ids(project.data).unwrap_or(vec!());
    //             changes.iter().map(|(path_id, snapshot_id)|{
    //                 (project_id.clone(), hash.clone(),
    //                 //database.path(path_id).map_or(String::new(), |p| p.location()),
    //                  snapshot_id.clone())
    //             }).collect::<Vec<(String, String, /*String,*/ SnapshotId)>>()
    //         }).collect::<Vec<(String, String, String, /*String, SnapshotId*/)>>()
    //     })
    // })
    // .collect();

    // for (id, project, commit, /*path, snapshot*/) in x {
    //     println!("{}", commit, /*path, snapshot*/);
    // }

    // let snapshot1 =
    //     database.snapshot(&SnapshotId(375603357u64))
    //         .map(|s| s.contents().to_string());
    // eprintln!("snapshot1\n{:?}\n------------------------------------------------------", snapshot1);
    //
    // let snapshot2: Vec<String> =
    //     database.snapshots()
    //         .filter(|s| s.id() == SnapshotId(375603357u64))
    //         .map(|s| s.contents().to_string())
    //         .collect();
    // for string in snapshot2 {
    //     eprintln!("snapshot2\n{:?}\n------------------------------------------------------", string);
    // }
}
