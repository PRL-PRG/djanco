use dcd::{CommitId, ProjectId};


fn main() {
    let stuff:Vec<(ProjectId, Vec<CommitId>)> = vec![(0, vec![1,2,3,4]), (1, vec![5,6,7,8,9])];

    println!("{:?}", stuff);
}