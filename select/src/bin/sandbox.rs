use dcd::{CommitId, ProjectId};
use select::cachedb::PersistentProjectCommitIndex;
use std::path::Path;


fn main() {
    let stuff:Vec<(ProjectId, Vec<CommitId>)> = vec![(0, vec![1,2,3,4]), (1, vec![5,6,7,8,9])];

    PersistentProjectCommitIndex::write_to(Path::new("hello.bin"), &mut stuff.into_iter()).unwrap();
    let pci = PersistentProjectCommitIndex::read_from(Path::new("hello.bin")).unwrap();

    println!("{:?}", pci);
}