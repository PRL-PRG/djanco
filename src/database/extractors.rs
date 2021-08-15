use std::collections::{BTreeMap, BTreeSet};
use std::collections::btree_map::Entry;
use std::marker::PhantomData;
use std::iter::FromIterator;

use itertools::{Itertools, MinMaxResult};

use crate::objects::*;
use crate::piracy::*;
use crate::weights_and_measures::{Weighed};
use crate::{Store, Percentage, Timestamp};

use super::lazy::{DoubleItemExtractor, ItemExtractor};
use super::source::Source;
use super::persistent::*;

pub(crate) struct IdExtractor<Id: Identity + Persistent> { _type: PhantomData<Id> }
impl<Id> IdExtractor<Id> where Id: Identity + Persistent {
    pub fn _new() -> IdExtractor<Id> {
        IdExtractor { _type: PhantomData }
    }
}
impl<Id> VectorExtractor for IdExtractor<Id> where Id: Identity + Persistent {
    type Value = Id;
}
impl<Id> SingleVectorExtractor for IdExtractor<Id> where Id: Identity + Persistent  {
    type A = BTreeMap<Id, String>;
    fn extract(_: &Source, whatever: &Self::A) -> Vec<Self::Value> {
        whatever.keys().collect::<Vec<&Id>>().pirate()
    }
}

pub(crate) struct ProjectUrlExtractor;
impl MapExtractor for ProjectUrlExtractor {
    type Key = ProjectId;
    type Value = String;
}
impl SourceMapExtractor for ProjectUrlExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.project_urls().collect()
    }
}

pub(crate) struct MaxCommitDeltaExtractor {}
impl MapExtractor for MaxCommitDeltaExtractor {
    type Key = ProjectId;
    type Value = i64;
}
impl DoubleMapExtractor for MaxCommitDeltaExtractor  {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Timestamp>;
    fn extract(_: &Source, project_commits: &Self::A, committed_timestamps: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        project_commits.iter().flat_map(|(project_id, commit_ids)| {
            let mut timestamps: Vec<i64> = Vec::new();
            for commit_id in commit_ids {
                let committer_timestamp = committed_timestamps.get(&commit_id);
                if let Some(timestamp) = committer_timestamp { timestamps.push(*timestamp) };
            }
            if timestamps.clone().len() == 0 {
                Some((project_id.clone(), 0))
            }else{
                timestamps.sort();
                let mut ans: i64 = 0;
                let mut previous: i64 = timestamps[0];
                for i in 1..timestamps.len() {
                    if (timestamps[i] - previous) > ans {
                        ans = timestamps[i] - previous;
                    }
                    previous = timestamps[i];   
                }
                Some((project_id.clone(), ans))
            }
        }).collect()
    }
}

pub(crate) struct ProjectMaxUserLifetimeExtractor { }
impl MapExtractor for ProjectMaxUserLifetimeExtractor {
    type Key = ProjectId;
    type Value = i64;
}

impl DoubleMapExtractor for ProjectMaxUserLifetimeExtractor  {
    type A = BTreeMap<ProjectId, Vec<UserId>>;
    type B = BTreeMap<UserId, (i64, i64)>;

    fn extract(_: &Source, project_authors: &Self::A, user_lifetime: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        project_authors.iter().filter_map(|(project_id, author_ids)| {
            match author_ids.iter().filter_map(|id| user_lifetime.get(id)).map(|(min, max)| max - min).max() {
                Some(x) => Some((*project_id, x)),
                None => None
            }
        }).collect()
    }
}

pub(crate) struct ProjectMaxHIndex1 { }
impl MapExtractor for ProjectMaxHIndex1 {
    type Key = ProjectId;
    type Value = u64;
}

impl DoubleMapExtractor for ProjectMaxHIndex1  {
    type A = BTreeMap<ProjectId, Vec<UserId>>;
    type B = BTreeMap<UserId, u64>;

    fn extract(_: &Source, project_authors: &Self::A, user_hindex1: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        project_authors.iter().filter_map(|(project_id, author_ids)| {
            match author_ids.iter().filter_map(|id| user_hindex1.get(id)).max() {
                Some(x) => Some((*project_id, *x)),
                None => None
            }
        }).collect()
    }
}

pub(crate) struct ProjectMaxHIndex2 { }
impl MapExtractor for ProjectMaxHIndex2 {
    type Key = ProjectId;
    type Value = u64;
}

impl DoubleMapExtractor for ProjectMaxHIndex2  {
    type A = BTreeMap<ProjectId, Vec<UserId>>;
    type B = BTreeMap<UserId, u64>;

    fn extract(_: &Source, project_authors: &Self::A, user_hindex2: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        project_authors.iter().filter_map(|(project_id, author_ids)| {
            match author_ids.iter().filter_map(|id| user_hindex2.get(id)).max() {
                Some(x) => Some((*project_id, *x)),
                None => None
            }
        }).collect()
    }
}


pub(crate) struct ProjectMaxExperienceExtractor {}
impl MapExtractor for ProjectMaxExperienceExtractor {
    type Key = ProjectId;
    type Value = i32;
}
impl DoubleMapExtractor for ProjectMaxExperienceExtractor  {
    type A = BTreeMap<ProjectId, Vec<UserId>>;
    type B = BTreeMap<UserId, i32>;
    fn extract(_: &Source, project_authors: &Self::A, developer_experience: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        project_authors.iter().map(|(project_id, author_ids)| {
            let mut experiences: Vec<i32> = Vec::new();
            for author_id in author_ids {
                if let Some(author_experience ) = developer_experience.get(&author_id){ 
                    experiences.push(*author_experience) 
                };
            }
            if let Some(max_value) = experiences.iter().max(){
                return (project_id.clone(), *max_value);
            }
            (project_id.clone(), 0)
            
        }).collect()
    }
}

pub(crate) struct AvgCommitDeltaExtractor {}
impl MapExtractor for AvgCommitDeltaExtractor {
    type Key = ProjectId;
    type Value = i64;
}
impl DoubleMapExtractor for AvgCommitDeltaExtractor  {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Timestamp>;
    fn extract(_: &Source, project_commits: &Self::A, committed_timestamps: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        project_commits.iter().flat_map(|(project_id, commit_ids)| {
            let mut timestamps: Vec<i64> = Vec::new();
            for i in 0..commit_ids.len(){
                let committer_timestamp = committed_timestamps.get(&commit_ids[i]);
                if let Some(timestamp) = committer_timestamp { timestamps.push(*timestamp) };
            }
            if timestamps.clone().len() == 0 {
                Some((project_id.clone(), 0))
            }else{
                timestamps.sort();
                let mut ans: f64 = timestamps[0] as f64;
                let mut previous: Timestamp = timestamps[0];
                for i in 1..timestamps.len() {
                    ans += (timestamps[i] - previous) as f64;
                    previous = timestamps[i];
                }
                if timestamps.len() > 2 {
                    ans /= (timestamps.len()-1) as f64;
                }
                Some((project_id.clone(), ans.round() as i64))
            }
        }).collect()
    }
}

pub(crate) struct TimeSinceLastCommitExtractor {}
impl MapExtractor for TimeSinceLastCommitExtractor {
    type Key = ProjectId;
    type Value = i64;
}
impl TripleMapExtractor for TimeSinceLastCommitExtractor  {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Timestamp>;
    type C = BTreeMap<ProjectId, Timestamp>;
    fn extract(_source: &Source, project_commits: &Self::A, committed_timestamps: &Self::B, last_checkpoint: &Self::C) -> BTreeMap<Self::Key, Self::Value> {
        
        project_commits.iter().map(|(project_id, commit_ids)| {

            if let Some(time_checked) = last_checkpoint.get(&project_id) {
                if let Some(ts) = commit_ids.iter().filter_map(|cid| committed_timestamps.get(&cid)).max() {
                    //println!("project {}, update: {}, last commit: {}", project_id, time_checked, ts);
                    return (*project_id, time_checked - ts);
                } 
            }
            return (*project_id, 0);

        }).collect()
    }
}

pub(crate) struct OldestNewestCommitExtractor {}
impl MapExtractor for OldestNewestCommitExtractor {
    type Key = ProjectId;
    type Value = (CommitId, CommitId);
}

impl DoubleMapExtractor for OldestNewestCommitExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, i64>;

    fn extract(_source: &Source, project_commits : & Self::A, commit_committer_timestamps : &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        project_commits.iter().filter_map(|(project_id, commit_ids)| {
            match commit_ids.iter().minmax_by_key(|x| commit_committer_timestamps.get(x)) {
                MinMaxResult::NoElements => None,
                MinMaxResult::OneElement(x) => Some((*project_id, (*x, *x))),
                MinMaxResult::MinMax(min,max) => Some((*project_id, (*min, *max))),
            }
        }).collect()
    }
}

/*
pub(crate) struct LastUpdateTimeExtractor {}
impl MapExtractor for LastUpdateTimeExtractor {
    type Key = ProjectId;
    type Value = i64;
}

impl SingleMapExtractor for TimeSinceLastCommitExtractor  {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    fn extract(_source: &Source, last_checkpoint: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        
        project_commits.iter().filter_map(|(project_id, commit_ids)| last_checkpoint.get(&project_id)).collect()

        /*
            if let Some(time_checked) = last_checkpoint.get(&project_id) {
                if let Some(ts) = commit_ids.iter().filter_map(|cid| committed_timestamps.get(&cid)).max() {
                    //println!("project {}, update: {}, last commit: {}", project_id, time_checked, ts);
                    return (*project_id, time_checked - ts);
                } 
            }
            return (*project_id, 0);

        }).collect()
        */
    }
}
*/


pub(crate) struct TimeSinceFirstCommitExtractor {}
impl MapExtractor for TimeSinceFirstCommitExtractor {
    type Key = ProjectId;
    type Value = i64;
}
impl TripleMapExtractor for TimeSinceFirstCommitExtractor  {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Timestamp>;
    type C = BTreeMap<ProjectId, Timestamp>;
    fn extract(_source: &Source, project_commits: &Self::A, committed_timestamps: &Self::B, last_checkpoint: &Self::C) -> BTreeMap<Self::Key, Self::Value> {
        
        project_commits.iter().flat_map(|(project_id, commit_ids)| {
            let mut timestamps: Vec<Timestamp> = Vec::new();
            for commit_id in commit_ids {
                let committer_timestamp = committed_timestamps.get( &commit_id );
                if let Some(timestamp) = committer_timestamp { timestamps.push(*timestamp) };
            }
            if timestamps.clone().len() == 0 {
                Some((project_id.clone(), 0))
            }else{
                timestamps.sort();
                if let Some(now) = last_checkpoint.get( &project_id ) {
                    if *now > 0 {
                        return Some((project_id.clone(), (*now) - timestamps[0]));
                    }   
                }
                Some((project_id.clone(), 0))   
            }
        }).collect()
    }
}

pub(crate) struct IsAbandonedExtractor {}
impl MapExtractor for IsAbandonedExtractor {
    type Key = ProjectId;
    type Value = bool;
}
impl DoubleMapExtractor for IsAbandonedExtractor  {
    type A = BTreeMap<ProjectId, i64>;
    type B = BTreeMap<ProjectId, i64>;
    fn extract(_: &Source, max_commit_delta: &Self::A, time_since_last_commit: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        max_commit_delta.iter().flat_map(|(project_id, delta)| {
            let option_last_commit = time_since_last_commit.get(&project_id);
            if let Some(last_commit) = option_last_commit { 
                return Some((project_id.clone(), *last_commit > *delta));
            }
            Some((project_id.clone(), false))
        }).collect()
    }
}

pub(crate) struct ProjectSubstoreExtractor;
impl MapExtractor for ProjectSubstoreExtractor {
    type Key = ProjectId;
    type Value = Store;
}
impl SourceMapExtractor for ProjectSubstoreExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.project_substores().collect()
    }
}

pub(crate) struct ProjectCredentialsExtractor; // TODO plug in
impl MapExtractor for ProjectCredentialsExtractor {
    type Key = ProjectId;
    type Value = String;
}
impl SourceMapExtractor for ProjectCredentialsExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.project_credentials().collect()
    }
}

pub(crate) struct ProjectHeadsExtractor;
impl MapExtractor for ProjectHeadsExtractor {
    type Key = ProjectId;
    type Value = Vec<Head>;
}
impl SourceMapExtractor for ProjectHeadsExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.project_heads().map(|(project_id, map)| {
            let heads = map.into_iter()
                .map(|(branch_name, (commit_id, _hash))| {
                    Head::new(branch_name, commit_id)
                }).collect::<Vec<Head>>();

            (project_id, heads)
        }).collect()
    }
}

pub(crate) struct ProjectSnapshotsExtractor {}
impl MapExtractor for ProjectSnapshotsExtractor {
    type Key = ProjectId;
    type Value = Vec<SnapshotId>;
}
impl TripleMapExtractor for ProjectSnapshotsExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Vec<ChangeTuple>>;
    type C = BTreeMap<SnapshotId, bool>;

    fn extract(_: &Source, project_commit_ids: &Self::A, commit_change_ids: &Self::B, snapshots_with_contents : &Self::C) -> BTreeMap<Self::Key, Self::Value> {
        project_commit_ids.iter().map(|(project_id, commit_ids)| {
            let path_ids /* Iterator equivalent of Vec<Vec<PathId>>*/ =
                commit_ids.iter().flat_map(|commit_id| {
                    let path_ids_option =
                        commit_change_ids.get(commit_id).map(|changes| {
                            let vector: Vec<SnapshotId> =
                                changes.iter().flat_map(|change| {
                                    change.1/*snapshot_id()*/
                                }).collect();
                            vector
                        });
                    path_ids_option
                });
            // filter unique snapshots only and only those for which we have contents actually
            (project_id.clone(), path_ids.flatten().unique().filter(|x| {
                snapshots_with_contents.get(x).map_or(false, |x| *x)
            }).collect())
        }).collect()
    }
}

pub(crate) struct ProjectPathsExtractor {}
impl MapExtractor for ProjectPathsExtractor {
    type Key = ProjectId;
    type Value = Vec<PathId>;
}
impl DoubleMapExtractor for ProjectPathsExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Vec<ChangeTuple>>;

    fn extract(_: &Source, project_commit_ids: &Self::A, commit_change_ids: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        project_commit_ids.iter().map(|(project_id, commit_ids)| {
            let path_ids /* Iterator equivalent of Vec<Vec<PathId>>*/ =
                commit_ids.iter().flat_map(|commit_id| {
                    let path_ids_option =
                        commit_change_ids.get(commit_id).map(|changes| {
                            let vector: Vec<PathId> =
                                changes.iter().map(|change| {
                                    change.0//path_id()
                                }).collect();
                            vector
                        });
                    path_ids_option
                });
            (project_id.clone(), path_ids.flatten().unique().collect())
        }).collect()
    }
}

pub(crate) struct ProjectUsersExtractor {}
impl MapExtractor for ProjectUsersExtractor {
    type Key = ProjectId;
    type Value = Vec<UserId>;
}
impl DoubleMapExtractor for ProjectUsersExtractor {
    type A = BTreeMap<ProjectId, Vec<UserId>>;
    type B = BTreeMap<ProjectId, Vec<UserId>>;
    fn extract(_: &Source, project_authors: &Self::A, project_committers: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        project_authors.iter().map(|(project_id, authors)| {
            let mut users: Vec<UserId> = vec![];
            let committers = project_committers.get(project_id);
            if let Some(committers) = committers {
                users.extend(committers.iter().map(|user_id| user_id.clone()));
            }
            users.extend(authors.iter().map(|user_id| user_id.clone()));
            (project_id.clone(), users.into_iter().unique().collect())
        }).collect()
    }
}

pub(crate) struct ProjectAuthorsExtractor {}
impl MapExtractor for ProjectAuthorsExtractor {
    type Key = ProjectId;
    type Value = Vec<UserId>;
}
impl DoubleMapExtractor for ProjectAuthorsExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Commit>;
    fn extract(_: &Source, project_commits: &Self::A, commits: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        project_commits.iter().map(|(project_id, commit_ids)| {
            (project_id.clone(), commit_ids.iter().flat_map(|commit_id| {
                commits.get(commit_id).map(|c| c.author_id())
            }).unique().collect())
        }).collect()
    }
}

pub(crate) struct ProjectCommittersExtractor {}
impl MapExtractor for ProjectCommittersExtractor {
    type Key = ProjectId;
    type Value = Vec<UserId>;
}
impl DoubleMapExtractor for ProjectCommittersExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Commit>;
    fn extract(_: &Source, project_commits: &Self::A, commits: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        project_commits.iter().map(|(project_id, commit_ids)| {
            (project_id.clone(), commit_ids.iter().flat_map(|commit_id| {
                commits.get(commit_id).map(|c| c.committer_id())
            }).unique().collect())
        }).collect()
    }
}

pub(crate) struct CountPerKeyExtractor<K: Clone + Ord + Persistent, V>(PhantomData<(K, V)>);
impl<K, V> MapExtractor for CountPerKeyExtractor<K, V> where K: Clone + Ord + Persistent + Weighed {
    type Key = K;
    type Value = usize;
}
impl<K, V> SingleMapExtractor for CountPerKeyExtractor<K, V> where K: Clone + Ord + Persistent + Weighed {
    type A = BTreeMap<K, Vec<V>>;

    fn extract(_: &Source, primary: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        primary.iter().map(|(key, value)| (key.clone(), value.len())).collect()
    }
}

pub(crate) struct ProjectCommitsExtractor {}
impl ProjectCommitsExtractor {
    fn commits_from_head(commits: &BTreeMap<CommitId, Commit>, head: &CommitId) -> BTreeSet<CommitId> {
        let mut commits_in_head: BTreeSet<CommitId> = BTreeSet::new();
        let mut stack = vec![head.clone()];
        let mut visited: BTreeSet<CommitId> = BTreeSet::new();
        while !stack.is_empty() {
            let commit_id = stack.pop().unwrap();
            if !visited.insert(commit_id) { continue } // If the set **did have** this value present, `false` is returned.
            commits_in_head.insert(commit_id);
            if let Some(commit) = commits.get(&commit_id) {// Potentially explosive?
                let parents = commit.parent_ids();
                stack.extend(parents)
            } else {
                eprintln!("WARNING: commit id {} was found as a parent of another commit, but it \
                           does not have a commit associated with it", commit_id)
            }
        }
        commits_in_head
    }
}
impl MapExtractor for ProjectCommitsExtractor {
    type Key = ProjectId;
    type Value = Vec<CommitId>;
}
impl DoubleMapExtractor for ProjectCommitsExtractor {
    type A = BTreeMap<ProjectId, Vec<Head>>;
    type B = BTreeMap<CommitId, Commit>;
    fn extract(_: &Source, heads: &Self::A, commits: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        heads.iter().map(|(project_id, heads)| {
            (project_id.clone(),
             heads.iter().flat_map(|head| {
                 Self::commits_from_head(commits, &head.commit_id())
             }).collect::<BTreeSet<CommitId>>())
        }).map(|(project_id, commits)| {
            (project_id, Vec::from_iter(commits.into_iter()))
        }).collect()
    }
}

pub(crate) struct ProjectMainBranchCommitsExtractor {}
impl MapExtractor for ProjectMainBranchCommitsExtractor {
    type Key = ProjectId;
    type Value = Vec<CommitId>;
}
impl TripleMapExtractor for ProjectMainBranchCommitsExtractor {
    type A = BTreeMap<ProjectId, Vec<Head>>;
    type B = BTreeMap<CommitId, Commit>;
    type C = BTreeMap<ProjectId, String>;
    fn extract(_: &Source, heads: &Self::A, commits: &Self::B, main_branches : &Self::C) -> BTreeMap<Self::Key, Self::Value> {
        heads.iter().filter_map(|(project_id, heads)| {
            //println!("Analyzing project {}", project_id);
            if let Some(main_branch) = main_branches.get(project_id) {
                let main_ref = format!("refs/heads/{}", main_branch);
                //println!("    main branch: {}", main_branch);
                if let Some(head) = heads.iter().filter(|head| {
                    if head.name() != main_ref {
                        //println!("    skipping branch {}", head.name());
                        return false;
                    } else {
                        //println!("    FOUND {}", head.name());
                        return true;
                    }
                }).next() {
                    return Some((* project_id, ProjectCommitsExtractor::commits_from_head(commits, & head.commit_id()).iter().map(|x| *x).collect::<Vec<_>>()));
                }                
            }
            return None;
        }).collect()
    }
}

pub(crate) struct ProjectLifetimesExtractor {}
impl MapExtractor for ProjectLifetimesExtractor {
    type Key = ProjectId;
    type Value = u64;
}
impl TripleMapExtractor for ProjectLifetimesExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Timestamp>;
    type C = BTreeMap<CommitId, Timestamp>;
    fn extract(_: &Source, 
               project_commits: &Self::A,
               authored_timestamps: &Self::B,
               committed_timestamps: &Self::B) -> BTreeMap<Self::Key, Self::Value> {

       project_commits.iter().flat_map(|(project_id, commit_ids)| {
           let min_max =
               commit_ids.iter()
                   .flat_map(|commit_id: &CommitId| {
                       let mut timestamps: Vec<Timestamp> = Vec::new();
                       let author_timestamp = authored_timestamps.get(commit_id);
                       let committer_timestamp = committed_timestamps.get(commit_id);
                       if let Some(timestamp) = author_timestamp { timestamps.push(*timestamp) }
                       if let Some(timestamp) = committer_timestamp { timestamps.push(*timestamp) }
                       timestamps
                   })
                   .minmax();

           match min_max {
               MinMaxResult::NoElements => { None }
               MinMaxResult::OneElement(_) => { Some((project_id.clone(), 0)) }
               MinMaxResult::MinMax(min, max) => { Some((project_id.clone(), (max - min) as u64)) }
           }
       }).collect()
    }
}

pub(crate) struct UserExtractor {}
impl MapExtractor for UserExtractor {
    type Key = UserId;
    type Value = User;
}
impl SourceMapExtractor for UserExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.user_emails().map(|(id, email)| {
            (UserId::from(id), User::new(UserId::from(id), email))
        }).collect()
    }
}

pub(crate) struct UserAuthoredCommitsExtractor {}
impl MapExtractor for UserAuthoredCommitsExtractor {
    type Key = UserId;
    type Value = Vec<CommitId>;
}
impl SingleMapExtractor for UserAuthoredCommitsExtractor {
    type A = BTreeMap<CommitId, Commit>;
    fn extract(_: &Source, commits: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        commits.iter()
            .map(|(commit_id, commit)| {
                (commit.author_id().clone(), commit_id.clone(), )
            }).into_group_map()
            .into_iter()
            .collect()
    }
}



pub(crate) struct UserExperienceExtractor {}
impl MapExtractor for UserExperienceExtractor {
    type Key = UserId;
    type Value = u64;
}
impl DoubleMapExtractor for UserExperienceExtractor  {
    type A = BTreeMap<UserId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Timestamp>;
    fn extract(_: &Source, user_commits: &Self::A, timestamps: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        user_commits.iter()
        .flat_map(|(user_id, commit_ids)| {
            let min_max = commit_ids.iter()
                .flat_map(|commit_id| {
                    timestamps.get(commit_id).pirate()
                })
                .minmax();
            match min_max {
                MinMaxResult::NoElements => None,
                MinMaxResult::OneElement(_) => Some((user_id.clone(), 0)),
                MinMaxResult::MinMax(min, max) => Some((user_id.clone(), (max - min) as u64)),
            }
        }).collect()
    }
}

pub(crate) struct UserHIndex1Extractor {}
impl MapExtractor for UserHIndex1Extractor {
    type Key = UserId;
    type Value = u64;
}

impl TripleMapExtractor for UserHIndex1Extractor {
    type A = BTreeMap<UserId, Vec<ProjectId>>;
    type B = BTreeMap<ProjectId, Vec<CommitId>>;
    type C = BTreeMap<UserId, Vec<CommitId>>;
    fn extract(_: &Source, user_projects: &Self::A, project_commits: &Self::B, user_commits : &Self::C) -> BTreeMap<Self::Key, Self::Value> {
        user_projects.iter().filter_map(|(user_id, projects)| {
            // get the user commits and convert them to a set for faster searching
            if let Some(this_user_commits) = user_commits.get(user_id).map(|x| x.iter().collect::<BTreeSet<_>>()) {
                let mut own_commits:Vec<u64> = projects.iter().filter_map(|project_id| 
                    project_commits.get(project_id)
                ).map(
                    |x| x.iter().filter(|x| this_user_commits.contains(x)).count() as u64
                ).collect();
                // we have list of own commits for the project, sort it by descending order
                own_commits.sort_by(|a, b| b.cmp(a));
                // and determine the appropriate N
                let mut result:u64 = 0;
                for n in own_commits {
                    if n >= result {
                        result += 1;
                    } else {
                        break;
                    }
                }
                return Some((*user_id, result));
            } 
            return None;
        }).collect()
    }
}

pub(crate) struct UserHIndex2Extractor {}
impl MapExtractor for UserHIndex2Extractor {
    type Key = UserId;
    type Value = u64;
}
impl QuadrupleMapExtractor for UserHIndex2Extractor {
    type A = BTreeMap<UserId, Vec<ProjectId>>;
    type B = BTreeMap<ProjectId, Vec<CommitId>>;
    type C = BTreeMap<UserId, Vec<CommitId>>;
    type D = BTreeMap<ProjectId, usize>;
    fn extract(_: &Source, user_projects: &Self::A, project_commits: &Self::B, user_commits : &Self::C, project_user_count: &Self::D) -> BTreeMap<Self::Key, Self::Value> {
        user_projects.iter().filter_map(|(user_id, projects)| {
            // get the user commits and convert them to a set for faster searching
            if let Some(this_user_commits) = user_commits.get(user_id).map(|x| x.iter().collect::<BTreeSet<_>>()) {
                let mut own_commits:Vec<u64> = projects.iter().filter_map(|project_id| {
                    // for each project get (commits in the projects, number of users in the project)
                    if let Some(users) = project_user_count.get(project_id) {
                        if let Some(commits) = project_commits.get(project_id) {
                            return Some((commits, users));
                        }
                    }
                    return None;
                }).map(
                    |(x, users)| {
                        // filter commits of the projects to keep only those authored by current user
                        let commits = x.iter().filter(|x| this_user_commits.contains(x)).count() as u64;
                        // and return either the number of commits, or the number of users in the project, whichever is smaller
                        std::cmp::min(*users as u64, commits)
                    }
                ).collect();
                // we have list of own commits for the project, sort it by descending order
                own_commits.sort_by(|a, b| b.cmp(a));
                // and determine the appropriate N
                let mut result:u64 = 0;
                for n in own_commits {
                    if n >= result {
                        result += 1;
                    } else {
                        break;
                    }
                }
                return Some((*user_id, result));
            } 
            return None;
        }).collect()
    }
}

pub(crate) struct UserProjectIdsExtractor {} 
impl MapExtractor for UserProjectIdsExtractor {
    type Key = UserId;
    type Value = Vec<ProjectId>;
}

impl DoubleMapExtractor for UserProjectIdsExtractor {
    type A = BTreeMap<UserId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Vec<ProjectId>>;
    fn extract(_: &Source, user_commits: &Self::A, commit_projects: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        user_commits.iter().filter_map(|(user_id, commits)| {
            let mut projects = BTreeSet::<ProjectId>::new();
            for commit_id in commits {
                if let Some(cprojs) = commit_projects.get(commit_id) {
                    projects.extend(cprojs.iter())
                }
            }
            Some((*user_id, projects.into_iter().collect::<Vec<_>>()))
        }).collect()
    }
}

pub(crate) struct UserLifetimeExtractor {}
impl MapExtractor for UserLifetimeExtractor {
    type Key = UserId;
    type Value = (i64, i64);
}

impl DoubleMapExtractor for UserLifetimeExtractor {
    type A = BTreeMap<UserId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Timestamp>;
    fn extract(_: &Source, user_commits: &Self::A, timestamps: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        user_commits.iter().filter_map(|(user_id, commits)| {
            match commits.iter().filter_map(|x| timestamps.get(x)).minmax() {
                MinMaxResult::NoElements => None,
                MinMaxResult::OneElement(x) => Some((*user_id, (*x, *x))),
                MinMaxResult::MinMax(min, max) => Some((*user_id, (*min, *max))),
            }
        }).collect()
    }
}


pub(crate) struct DeveloperExperienceExtractor {}
impl MapExtractor for DeveloperExperienceExtractor {
    type Key = UserId;
    type Value = i32;
}
impl DoubleMapExtractor for DeveloperExperienceExtractor  {
    type A = BTreeMap<UserId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Timestamp>;
    fn extract(_: &Source, user_commits: &Self::A, timestamps: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        user_commits.iter().map(|(user_id, commit_ids)| {
            let mut user_timestamps : Vec<Timestamp> = Vec::new();
            for commit_id in commit_ids {
                if let Some(timestamp) = timestamps.get(&commit_id) {
                    user_timestamps.push(*timestamp);
                }                
            }
            user_timestamps.sort();
            if user_timestamps.len() > 0 {
                let first_time = user_timestamps[0];
                let delta_month = 2592001; // total seconds in a month (+1)
                let mut month_commits : BTreeMap< i64, i64> = BTreeMap::new();
                month_commits.insert(0, 1);
                let mut index_month : i64;
                for i in 1 .. user_timestamps.len() {
                    index_month = (user_timestamps[i]-first_time)/delta_month;
                    if !month_commits.contains_key(&index_month) {
                        month_commits.insert(index_month, 0);
                    }
                    month_commits.insert(index_month, month_commits.get(&index_month).unwrap() + 1 );
                }
                let mut values: Vec<i64> = month_commits.values().cloned().collect();
                values.sort();
                values.reverse();
                for i in 0..values.len() {
                    if values[i] < (i+1) as i64  {
                        return (user_id.clone(), (i+1) as i32);
                    }
                }
                (user_id.clone(), values.len() as i32)
            }else{
                (user_id.clone(), 0 as i32)
            }
        }).collect()
    }
}

pub(crate) struct CombinedUserExperienceExtractor {}
impl MapExtractor for CombinedUserExperienceExtractor {
    type Key = UserId;
    type Value = u64;
}
impl TripleMapExtractor for CombinedUserExperienceExtractor  {
    type A = BTreeMap<UserId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Timestamp>;
    type C = BTreeMap<CommitId, Timestamp>;
    fn extract(_: &Source, user_commits: &Self::A, authored_timestamps: &Self::B, committed_timestamps: &Self::C) -> BTreeMap<Self::Key, Self::Value> {
        user_commits.iter()
            .flat_map(|(user_id, commit_ids)| {
                let min_max = commit_ids.iter()
                    .flat_map(|commit_id| {
                        let mut timestamps: Vec<Timestamp> = Vec::new();
                        let authored_timestamp = authored_timestamps.get(commit_id).pirate();
                        let committed_timestamp = committed_timestamps.get(commit_id).pirate();
                        if let Some(timestamp) = authored_timestamp { timestamps.push(timestamp) }
                        if let Some(timestamp) = committed_timestamp { timestamps.push(timestamp) }
                        timestamps
                    })
                    .minmax();

                match min_max {
                    MinMaxResult::NoElements => None,
                    MinMaxResult::OneElement(_) => Some((user_id.clone(), 0)),
                    MinMaxResult::MinMax(min, max) => Some((user_id.clone(), (max - min) as u64)),
                }
            }).collect()
    }
}

pub(crate) struct PathExtractor {}
impl MapExtractor for PathExtractor {
    type Key = PathId;
    type Value = Path;
}
impl SourceMapExtractor for PathExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.paths().map(|(id, path)| {
            (id.clone(), Path::new(id, path))
        }).collect()
    }
}

// FIXME impl path_shas

pub(crate) struct SnapshotExtractor {}
impl MapExtractor for SnapshotExtractor {
    type Key = SnapshotId;
    type Value = Snapshot;
}
impl SourceMapExtractor for SnapshotExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.snapshot_bytes().map(|(id, contents)| {
             (id.clone(), Snapshot::new(id, contents))
        }).collect()
        // FIXME snapshots this shouldn't exist, right?
    }
}

pub(crate) struct CommitExtractor {}
impl MapExtractor for CommitExtractor {
    type Key = CommitId;
    type Value = Commit;
}
impl SourceMapExtractor for CommitExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.commit_info().map(|(id, basics)| {
            (id, Commit::new(id, basics.committer, basics.author, basics.parents))
        }).collect()
    }
}

pub(crate) struct CommitHashExtractor {}
impl MapExtractor for CommitHashExtractor {
    type Key = CommitId;
    type Value = String;
}
impl SourceMapExtractor for CommitHashExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.commit_hashes().collect()
    }
}

pub(crate) struct CommitMessageExtractor {}
impl MapExtractor for CommitMessageExtractor {
    type Key = CommitId;
    type Value = String;
}
impl SourceMapExtractor for CommitMessageExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.commit_info()
            .map(|(id, basics)| (id, basics.message))
            .collect()
    }
}

pub(crate) struct CommitterTimestampExtractor {}
impl MapExtractor for CommitterTimestampExtractor {
    type Key = CommitId;
    type Value = Timestamp;
}
impl SourceMapExtractor for CommitterTimestampExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.commit_info().map(|(id, commit)| {
            (id, commit.committer_time)
        }).collect()
    }
}

pub type ChangeTuple = (PathId, Option<SnapshotId>); // This is a tuple and not a pub(crate) struct for performance reasons.
pub(crate) struct CommitChangesExtractor {}
impl MapExtractor for CommitChangesExtractor {
    type Key = CommitId;
    type Value = Vec<ChangeTuple>;
}
impl SourceMapExtractor for CommitChangesExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.commit_info()
            .map(|(commit_id,info)| (commit_id, info.changes))
            .collect()
    }
}

pub(crate) struct CommitChangesWithContentsExtractor { }
impl MapExtractor for CommitChangesWithContentsExtractor {
    type Key = CommitId;
    type Value = Vec<ChangeTuple>;
}
impl DoubleMapExtractor for CommitChangesWithContentsExtractor {
    type A = BTreeMap<CommitId, Vec<ChangeTuple>>;
    type B = BTreeMap<SnapshotId, bool>;
    fn extract(_: &Source, commit_changes: &Self::A, snapshot_has_contents: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        commit_changes.iter().map(|(pid, changes)| {
            (*pid, changes.iter().filter_map(|(path_id, snapshot_id)| {
                match snapshot_id {
                    None => Some((*path_id, None)), 
                    Some(snapshot_id) => snapshot_has_contents.get(&snapshot_id).map_or(None, |_| Some((*path_id, Some(*snapshot_id))))
                }
            }).collect())
        }).collect()
    }
}


pub(crate) struct AuthorTimestampExtractor {}
impl MapExtractor for AuthorTimestampExtractor {
    type Key = CommitId;
    type Value = Timestamp; // TODO wrap
}
impl SourceMapExtractor for AuthorTimestampExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.commit_info().map(|(id, commit)| {
            (id, commit.author_time)
        }).collect()
    }
}

pub(crate) struct SnapshotLocsExtractor{}
impl MapExtractor for SnapshotLocsExtractor {
    type Key = SnapshotId;
    type Value = usize;
}
impl SourceMapExtractor for SnapshotLocsExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.snapshot_bytes().map(|(id, contents)| {
            let snapshot = Snapshot::new(id, contents);
            let contents = snapshot.contents_owned();
            (id.clone(), contents.matches("\n").count())
       }).collect()
    }
}

pub(crate) struct SnapshotHasContentsExtractor{}
impl MapExtractor for SnapshotHasContentsExtractor {
    type Key = SnapshotId;
    type Value = bool;
}
impl SourceMapExtractor for SnapshotHasContentsExtractor {
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.snapshot_has_contents().map(|x| (x, true)).collect()
    }
}


pub(crate) struct ProjectLocsExtractor{} 
impl MapExtractor for ProjectLocsExtractor{
    type Key = ProjectId;
    type Value = usize;
}

// project_default_branch, project_head_trees, snapshot_locs

impl TripleMapExtractor for ProjectLocsExtractor {
    type A = BTreeMap<ProjectId, Vec<(String, Vec<(PathId, SnapshotId)>)>>;
    type B = BTreeMap<ProjectId, String>;
    type C = BTreeMap<SnapshotId, usize>;
    fn extract(_: &Source, project_head_trees: &Self::A, project_default_branch: &Self::B, snapshot_locs: &Self::C) -> BTreeMap<Self::Key, Self::Value> {
        project_head_trees.iter().filter_map(|(pid, heads)| {
            if let Some(default_branch_name) = project_default_branch.get(pid) {
                let ref_name = format!("refs/heads/{}", default_branch_name);
                if let Some((_, tree)) = heads.iter().filter(|(name, _)| *name == ref_name ).next() {
                    let snapshot_locs = tree.iter().filter_map(|(_, snapshot_id)| snapshot_locs.get(snapshot_id)).sum(); 
                    return Some((*pid, snapshot_locs));
                }
            } 
            return None;
        }).collect()


        // TODO: We should look after parent commits rather than timestamps. 
        /*
        project_commits.iter().map(|(project_id, commit_ids)| {
            let mut last_state_files : BTreeMap<PathId, usize> = BTreeMap::new(); // store locs of a file from the latest seen snapshot
            let mut last_timestamp : BTreeMap<PathId, Timestamp> = BTreeMap::new();
            for commit_id in commit_ids {
                if let Some(changes) = commit_changes.get(&commit_id){
                    for change in changes {
                        let path = &(change.0);
                        let current_timestamp = commit_timestamps.get(&commit_id).unwrap();
                        if !last_state_files.contains_key(path) ||  *current_timestamp > *last_timestamp.get(path).unwrap() {
                            if let Some(snapshot_id) = change.1 {
                                if let Some(count_locs) =  snapshot_locs.get(&(snapshot_id)) {
                                    last_timestamp.insert(*path, *current_timestamp);
                                    last_state_files.insert(*path, *count_locs);
                                }
                            }
                        }
                    }  
                }
            }
            let vec_locs : Vec<usize> = last_state_files.values().cloned().collect();
            (project_id.clone(), vec_locs.iter().sum())
        }).collect()
        */
    }
}

// TODO change this to usize so that we are the same as the other duplication related attributes
pub(crate) struct DuplicatedCodeExtractor {}
impl MapExtractor for DuplicatedCodeExtractor {
    type Key = ProjectId;
    type Value = f64;
}

impl TripleMapExtractor for DuplicatedCodeExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Vec<ChangeTuple>>;
    type C = BTreeMap<SnapshotId, (usize, ProjectId)>;

    fn extract (_: &Source, project_commits : &Self::A, commit_changes_with_contents : &Self::B, snapshot_projects : &Self::C) -> BTreeMap<ProjectId, f64> {
        // visited snapshots so that we only add each snapshot once (original & unique snapshots can be cloned within project too)
        let mut visited = BTreeSet::<SnapshotId>::new();
        return project_commits.iter().map(|(pid, commits)| {
            // for all commits
            let unique_files : usize = commits.iter().map(|cid| {
                // for all changes with snapshots
                if let Some(changes) = commit_changes_with_contents.get(cid) {
                    changes.iter().map(|(_path_id, snapshot)| {
                        if let Some(snapshot_id) = snapshot {
                            if visited.insert(*snapshot_id) {
                                return match snapshot_projects.get(snapshot_id) {
                                    Some((_, original)) => if original == pid { 0 } else { 1 },
                                    _ => 0, // TODO or maybe panic really?
                                }
                            }
                        }
                        0
                    }).sum::<usize>()
                } else {
                    0
                }
            }).sum();
            return (*pid, f64::trunc(unique_files as f64 / visited.len() as f64 * 100.0));
        }).collect();        
    }
}



/*
pub(crate) struct DuplicatedCodeExtractor {}
impl MapExtractor for DuplicatedCodeExtractor {
    type Key = ProjectId;
    type Value = f64;
}
impl TripleMapExtractor for DuplicatedCodeExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Vec<ChangeTuple>>;
    type C = BTreeMap<SnapshotId, (usize, ProjectId)>;
    fn extract (_: &Source, project_commits : &Self::A, commit_changes : &Self::B, snapshot_projects : &Self::C) -> BTreeMap<Self::Key, Self::Value> {
        let mut total_snapshots : f64 = 0.0;
        let mut num_clones : f64 = 0.0;
        project_commits.iter().map(|(project_id, commit_ids)| {
            total_snapshots = 0.0;
            num_clones= 0.0;
            for commitid in commit_ids {
                if let Some(changes) = commit_changes.get(&commitid) {
                    for change in changes {
                        if let Some(snapshot_id) = change.1 {
                            total_snapshots+=1.0;
                            if let Some(snapshot) = snapshot_projects.get(&snapshot_id) {
                                if (*snapshot).1 != *project_id {
                                    num_clones += 1.0;
                                }       
                            }   
                        }
                    } 
                }
            }
            if total_snapshots == 0.0 {
                (project_id.clone(), -1.0)
            }else{
                (project_id.clone(), f64::trunc(num_clones/total_snapshots*100.0)/100.0)
            }

        }).collect()
    }
}
*/
            
pub(crate) struct CommitProjectsExtractor {}
impl MapExtractor for CommitProjectsExtractor {
    type Key = CommitId;
    type Value = Vec<ProjectId>;
}

impl SingleMapExtractor for CommitProjectsExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    fn extract(_: &Source, project_commits: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        let mut result = BTreeMap::<CommitId, Vec<ProjectId>>::new();
        project_commits.iter().for_each(|(pid, commits)| {
            commits.iter().for_each(|cid|{
                match result.entry(*cid) {
                    Entry::Vacant(e) => { e.insert(vec!{*pid}); },
                    Entry::Occupied(mut e) => { e.get_mut().push(*pid); },
                }
            });
        });
        return result;
    }
}


pub(crate) struct CommitLanguagesExtractor {}
impl MapExtractor for CommitLanguagesExtractor {
    type Key = CommitId;
    type Value = Vec<Language>;
}

impl DoubleMapExtractor for CommitLanguagesExtractor {
    type A = BTreeMap<CommitId, Vec<ChangeTuple>>;
    type B = BTreeMap<PathId, Path>;
    fn extract(_source: &Source, changes: &Self::A, paths: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        changes.iter().map(|(commit_id, commit_changes)| {
            (commit_id.clone(), 
             commit_changes.iter().flat_map(|(path_id, _snapshot_id)| {
                paths.get(path_id).map(|path| path.language()).flatten()
             }).unique().collect::<Vec<Language>>())
        }).collect()
    }
}

pub(crate) struct SnapshotCloneInfo {
    original : ProjectId,
    oldest_commit_time : Timestamp,
    oldest_project_time : Timestamp,
    projects : BTreeSet<ProjectId>,
}

impl SnapshotCloneInfo {
    pub fn new() -> SnapshotCloneInfo {
        return SnapshotCloneInfo{
            original : ProjectId(0),
            oldest_commit_time: Timestamp::MAX,
            oldest_project_time : Timestamp::MAX,
            projects : BTreeSet::new(),
        };
    }

    pub fn possibly_update_original(& mut self, p : ProjectId, p_time : Option<Timestamp>) {
        match p_time {
            Some(t) => {
                if self.oldest_project_time > t {
                    self.oldest_project_time = t;
                    self.original = p;
                }
            },
            _ => {}, 
        }
    }
}

pub(crate) struct SnapshotProjectsExtractor {}
impl MapExtractor for SnapshotProjectsExtractor {
    type Key = SnapshotId;
    type Value = (usize, ProjectId);
}
impl QuadrupleMapExtractor for SnapshotProjectsExtractor {
    type A = BTreeMap<CommitId, Vec<ChangeTuple>>;
    type B = BTreeMap<CommitId, Vec<ProjectId>>;
    type C = BTreeMap<CommitId, Timestamp>;
    type D = BTreeMap<ProjectId, Timestamp>;

    fn extract (_: &Source, commit_changes : &Self::A, commit_projects : &Self::B, commit_author_timestamps : &Self::C, projects_created : &Self::D) -> BTreeMap<SnapshotId, (usize, ProjectId)> {
        // first for each snapshot get projects and 
        let mut snapshot_projects = BTreeMap::<SnapshotId, SnapshotCloneInfo>::new();
        // for each commit
        commit_changes.iter().for_each(|(cid, changes)| {
            let commit_time = *commit_author_timestamps.get(cid).unwrap();
            // for each snapshot
            changes.iter().for_each(|(_path_id, sid_option)| {
                // if it is actually a snapshot (i.e. not a deleted file)
                if let Some(sid) = sid_option {
                    let ref mut sinfo = snapshot_projects.entry(*sid).or_insert_with(|| { SnapshotCloneInfo::new() });
                    // add the projects of the commit to the projects of the snapshot 
                    if let Some(pids) = commit_projects.get(cid) {
                        pids.iter().for_each(|pid| { sinfo.projects.insert(*pid); });
                        // if the commit time is older than the current oldest commit time, then the snapshots original will be amongst the projects of the current commit, find the oldest project amongst them and make it the original project
                        if sinfo.oldest_commit_time >= commit_time {
                            for pid in pids {
                                sinfo.possibly_update_original(*pid, projects_created.get(pid).map(|x| *x));
                            }
                        }
                    }
                }
            });
        });
        // once we have a vector of projects for each snapshot, we want to collapse these to the number of unique projects and the original project. To calculate the original project, we get projects and select the one with smallest creation time
        return snapshot_projects.iter().map(|(sid, sinfo)| {
            return (*sid, (sinfo.projects.len(), sinfo.original));
        }).collect();
    }
}

pub(crate) struct ProjectCommitContributionsExtractor {}
impl MapExtractor for ProjectCommitContributionsExtractor {
    type Key = ProjectId;
    type Value = Vec<(UserId, usize)>;
}
impl DoubleMapExtractor for ProjectCommitContributionsExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Commit>;
    fn extract(_: &Source, project_commits: &Self::A, commits: &Self::B) -> BTreeMap<Self::Key, Self::Value> {
        project_commits.iter().map(|(project_id, commit_ids)| {
            (project_id.clone(), commit_ids.iter()
                .flat_map(|commit_id| commits.get(commit_id))
                .map(|commit| (commit.author.clone(), 1usize))
                .into_group_map()
                .into_iter()
                .map(|(author_id, commits)| {
                    (author_id, commits.len())
                })
                .sorted_by_key(|(_, contributed_commits)| *contributed_commits)
                .rev()
                .collect::<Vec<(UserId, usize)>>())
        }).collect()
    }
}

pub(crate) struct ProjectChangeContributionsExtractor {}
impl MapExtractor for ProjectChangeContributionsExtractor {
    type Key = ProjectId;
    type Value = Vec<(UserId, usize)>;
}
impl TripleMapExtractor for ProjectChangeContributionsExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Commit>;
    type C = BTreeMap<CommitId, Vec<ChangeTuple>>;
    fn extract(_: &Source, project_commits: &Self::A, commits: &Self::B, commit_changes: &Self::C) -> BTreeMap<Self::Key, Self::Value> {
        project_commits.iter().map(|(project_id, commit_ids)| {
            (project_id.clone(), commit_ids.iter()
                .flat_map(|commit_id| {
                    commits.get(commit_id).map(|commit| {
                        commit_changes.get(commit_id).map(|changes| {
                            (commit.author.clone(), changes.len())
                        })
                    }).flatten()
                })
                .into_group_map()
                .into_iter()
                .map(|(author_id, changes)| {
                    (author_id, changes.into_iter().sum()) 
                })
                .sorted_by_key(|(_, contributed_commits)| *contributed_commits)
                .rev()
                .collect::<Vec<(UserId, usize)>>())
        }).collect()
    }
}

pub(crate) struct ProjectCumulativeContributionsExtractor {}
impl MapExtractor for ProjectCumulativeContributionsExtractor {
    type Key = ProjectId;
    type Value = Vec<Percentage>;
}
impl SingleMapExtractor for ProjectCumulativeContributionsExtractor {
    type A = BTreeMap<ProjectId, Vec<(UserId, usize)>>;
    fn extract(_: &Source, project_change_contributions: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        project_change_contributions.iter().map(|(project_id, contributions)| {
            let mut total_contributions = 0usize;
            let mut cumulative_contributions: Vec<usize> = Vec::new();
            for &(_user_id, contribution) in contributions {
                cumulative_contributions.push(0usize);
                for i in 0..cumulative_contributions.len() {
                    cumulative_contributions[i] = cumulative_contributions[i] + contribution;                    
                }
                total_contributions = total_contributions + contribution;
            }                    
            let cumulative_contribution_percentages = cumulative_contributions.into_iter()
                .map(|contributions| (contributions / total_contributions) as Percentage)
                .collect();
            (project_id.clone(), cumulative_contribution_percentages)
        }).collect()
    }
} 

pub(crate) struct ProjectUniqueFilesExtractor {}
impl MapExtractor for ProjectUniqueFilesExtractor {
    type Key = ProjectId;
    type Value = usize;
}

impl TripleMapExtractor for ProjectUniqueFilesExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Vec<ChangeTuple>>;
    type C = BTreeMap<SnapshotId, (usize, ProjectId)>;

    fn extract (_: &Source, project_commits : &Self::A, commit_changes_with_contents : &Self::B, snapshot_projects : &Self::C) -> BTreeMap<ProjectId, usize> {
        // visited snapshots so that we only add each snapshot once (original & unique snapshots can be cloned within project too)
        let mut visited = BTreeSet::<SnapshotId>::new();
        return project_commits.iter().map(|(pid, commits)| {
            // for all commits
            let unique_files = commits.iter().map(|cid| {
                // for all changes with snapshots
                if let Some(changes) = commit_changes_with_contents.get(cid) {
                    changes.iter().map(|(_path_id, snapshot)| {
                        if let Some(snapshot_id) = snapshot {
                            if visited.insert(*snapshot_id) {
                                return match snapshot_projects.get(snapshot_id) {
                                    Some((1, _)) => 1,
                                    _ => 0,
                                }
                            }
                        }
                        0
                    }).sum::<usize>()
                } else {
                    0
                }
            }).sum();
            return (*pid, unique_files);
        }).collect();        
    }
}

pub(crate) struct ProjectOriginalFilesExtractor {}
impl MapExtractor for ProjectOriginalFilesExtractor {
    type Key = ProjectId;
    type Value = usize;
}

impl TripleMapExtractor for ProjectOriginalFilesExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Vec<ChangeTuple>>;
    type C = BTreeMap<SnapshotId, (usize, ProjectId)>;


    fn extract (_: &Source, project_commits : &Self::A, commit_changes_with_contents : &Self::B, snapshot_projects : &Self::C) -> BTreeMap<ProjectId, usize> {
        // visited snapshots so that we only add each snapshot once (original & unique snapshots can be cloned within project too)
        let mut visited = BTreeSet::<SnapshotId>::new();
        return project_commits.iter().map(|(pid, commits)| {
            // for all commits
            let unique_files = commits.iter().map(|cid| {
                // for all changes with snapshots
                if let Some(changes) = commit_changes_with_contents.get(cid) {
                    changes.iter().map(|(_path_id, snapshot)| {
                        if let Some(snapshot_id) = snapshot {
                            if visited.insert(*snapshot_id) {
                                if let Some((copies, original)) = snapshot_projects.get(snapshot_id) {
                                    if original == pid && *copies > 1 {
                                        return 1;
                                    }
                                }
                            }
                        } 
                        0
                    }).sum::<usize>()
                } else {
                    0
                }
            }).sum();
            return (*pid, unique_files);
        }).collect();        
    }
}

pub(crate) struct ProjectImpactExtractor {}
impl MapExtractor for ProjectImpactExtractor {
    type Key = ProjectId;
    type Value = usize;
}

impl TripleMapExtractor for ProjectImpactExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Vec<ChangeTuple>>;
    type C = BTreeMap<SnapshotId, (usize, ProjectId)>;

    fn extract (_: &Source, project_commits : &Self::A, commit_changes_with_contents : &Self::B, snapshot_projects : &Self::C) -> BTreeMap<ProjectId, usize> {
        // visited snapshots so that we only add each snapshot once (original & unique snapshots can be cloned within project too)
        let mut visited = BTreeSet::<SnapshotId>::new();
        return project_commits.iter().map(|(pid, commits)| {
            // for all commits
            let unique_files = commits.iter().map(|cid| {
                // for all changes with snapshots
                if let Some(changes) = commit_changes_with_contents.get(cid) {
                    changes.iter().map(|(_path_id, snapshot)| {
                        if let Some(snapshot_id) = snapshot {
                            if visited.insert(*snapshot_id) {
                                // so if we are unique, then by definition we are original as well
                                if let Some((copies, original)) = snapshot_projects.get(snapshot_id) {
                                    if original == pid {
                                        return *copies;
                                    }
                                }
                            }
                        } 
                        0
                    }).sum::<usize>()
                } else {
                    0
                }
            }).sum();
            return (*pid, unique_files);
        }).collect();        
    }
}

pub(crate) struct ProjectFilesExtractor {}
impl MapExtractor for ProjectFilesExtractor {
    type Key = ProjectId;
    type Value = usize;
}
impl DoubleMapExtractor for ProjectFilesExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Vec<ChangeTuple>>;

    fn extract (_: &Source, project_commits : &Self::A, commit_changes : &Self::B) -> BTreeMap<ProjectId, usize> {
        project_commits.iter().map(|(pid, commits)| {
            let mut paths = BTreeSet::<PathId>::new();
            for cid in commits {
                if let Some(commits) = commit_changes.get(cid) {
                    for (path_id, _hash) in commits {
                        paths.insert(*path_id);
                    }
                }
            }
            (*pid, paths.len())            
        }).collect()
    }
}

pub(crate) struct ProjectLanguagesExtractor {}
impl MapExtractor for ProjectLanguagesExtractor {
    type Key = ProjectId;
    type Value = Vec<(Language,usize)>;
}
impl TripleMapExtractor for ProjectLanguagesExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Vec<ChangeTuple>>;
    type C = BTreeMap<PathId, Path>;

    fn extract (_: &Source, project_commits : &Self::A, commit_changes : &Self::B, paths : &Self::C) -> BTreeMap<ProjectId, Vec<(Language,usize)>> {
        let mut cached_paths = BTreeMap::<PathId, Language>::new();
        project_commits.iter().map(|(pid, commits)| {
            let mut languages = BTreeMap::<Language, usize>::new();
            for cid in commits {
                if let Some(commits) = commit_changes.get(cid) {
                    for (path_id, hash) in commits {
                        if let Some(_) = hash {
                            let lang = cached_paths.entry(*path_id).or_insert_with(|| {
                                if let Some(language) = paths.get(path_id).unwrap().language() {
                                    language
                                } else {
                                    Language::Other
                                }
                            });
                            match languages.entry(*lang) {
                                Entry::Occupied(mut e) => { *e.get_mut() += 1; },
                                Entry::Vacant(e) => { e.insert(1); },
                            }
                        }
                    }
                }
            }
            let mut langs : Vec<(Language, usize)> = languages.into_iter().collect();
            langs.sort_by(|a, b| b.1.cmp(&a.1) ); // reversed
            (*pid, langs)            
        }).collect()
    }
}

pub(crate) struct ProjectMajorLanguageExtractor {}
impl MapExtractor for ProjectMajorLanguageExtractor {
    type Key = ProjectId;
    type Value = Language;
}
impl SingleMapExtractor for ProjectMajorLanguageExtractor {
    type A = BTreeMap<ProjectId, Vec<(Language, usize)>>;

    fn extract (_: &Source, project_languages : &Self::A) -> BTreeMap<ProjectId, Language> {
        project_languages.iter()
            .filter(|(_pid, langs)| langs.len() > 0)
            .map(|(pid, langs)| (*pid, langs.get(0).unwrap().0))
            .collect()
    }
}

pub(crate) struct ProjectMajorLanguageRatioExtractor {}
impl MapExtractor for ProjectMajorLanguageRatioExtractor {
    type Key = ProjectId;
    type Value = f64;
}
impl SingleMapExtractor for ProjectMajorLanguageRatioExtractor {
    type A = BTreeMap<ProjectId, Vec<(Language, usize)>>;

    fn extract (_: &Source, project_languages : &Self::A) -> BTreeMap<ProjectId, f64> {
        project_languages.iter()
            .filter(|(_pid, langs)| langs.len() > 0)
            .map(|(pid, langs)| (*pid, langs.get(0).unwrap().1 as f64 / langs.iter().map(|(_, count)| *count).sum::<usize>() as f64))
            .collect()
    }
}

pub(crate) struct ProjectMajorLanguageChangesExtractor {}
impl MapExtractor for ProjectMajorLanguageChangesExtractor {
    type Key = ProjectId;
    type Value = usize;
}
impl SingleMapExtractor for ProjectMajorLanguageChangesExtractor {
    type A = BTreeMap<ProjectId, Vec<(Language, usize)>>;

    fn extract (_: &Source, project_languages : &Self::A) -> BTreeMap<ProjectId, usize> {
        project_languages.iter()
            .filter(|(_pid, langs)| langs.len() > 0)
            .map(|(pid, langs)| (*pid, langs.get(0).unwrap().1))
            .collect()
    }
}

pub(crate) struct ProjectLatestUpdateTimeExtractor;
impl MapExtractor for ProjectLatestUpdateTimeExtractor {
    type Key = ProjectId;
    type Value = i64;
}
impl SingleMapExtractor for ProjectLatestUpdateTimeExtractor {
    type A = BTreeMap<ProjectId, bool>;
    fn extract(source: &Source, project_is_valid: &Self::A) -> BTreeMap<Self::Key, Self::Value> {
        source.project_logs().map(|(project_id, logs)|{
            if let Some(is_valid) = project_is_valid.get(&project_id) {
                if *is_valid  && logs.len() > 0 {
                    return (project_id.clone(), logs[0].time());
                }
            }
            (project_id.clone(), -1)
        }).collect()
    }
}

pub(crate) struct ProjectIsValidExtractor;
impl MapExtractor for ProjectIsValidExtractor {
    type Key = ProjectId;
    type Value = bool;
}
impl SourceMapExtractor for ProjectIsValidExtractor{
    fn extract(source: &Source) -> BTreeMap<Self::Key, Self::Value> {
        source.project_logs().map(|(project_id, logs)|{
            if logs.len() > 0 {
                return (project_id.clone(), !logs[0].is_error());
            }
            (project_id.clone(), false)
        }).collect()
    }
}

pub(crate) struct ProjectAllForksExtractor {}
impl MapExtractor for ProjectAllForksExtractor {
    type Key = ProjectId;
    type Value = Vec<ProjectId>;
}

impl TripleMapExtractor for ProjectAllForksExtractor {
    type A = BTreeMap<ProjectId, Vec<CommitId>>;
    type B = BTreeMap<CommitId, Vec<ProjectId>>;
    type C = BTreeMap<ProjectId, Timestamp>;

    fn extract (_: &Source, project_commits: &Self::A, commit_projects: &Self::B, project_created: & Self::C) -> BTreeMap<ProjectId, Vec<ProjectId>> {
        project_commits.iter()
            .map(|(pid, commits)| {
                let mut projects = BTreeSet::<ProjectId>::new();
                for cid in commits {
                    if let Some(cprojects) = commit_projects.get(cid) {
                        for p in cprojects {
                            if p != pid {
                                projects.insert(*p);
                            }
                        }    
                    }
                }
                if let Some(created) = project_created.get(pid) {
                    if let Some((oldest_pid, oldest_time)) = projects.iter()
                        .filter_map(|p| if let Some(ctime) = project_created.get(p) { Some((p, ctime)) } else { None } )
                        .min_by(|a, b| if a.1 != b.1 { b.1.cmp(a.1) } else { b.0.cmp(a.0) }) {
                        if oldest_time < created || (oldest_time == created && oldest_pid < pid) {
                            projects.clear();
                        }
                    }
                }
                (*pid, projects.iter().map(|x| *x).collect())
             })
            .collect()
    }
}

pub(crate) struct ProjectHeadTreesExtractor {}

impl MapExtractor for ProjectHeadTreesExtractor {
    type Key = ProjectId;
    type Value = Vec<(String, Vec<(PathId, SnapshotId)>)>;
}

impl TripleMapExtractor for ProjectHeadTreesExtractor {
    type A = BTreeMap<ProjectId, Vec<Head>>;
    type B = BTreeMap<CommitId, Commit>;
    type C = BTreeMap<CommitId, Vec<ChangeTuple>>;

    fn extract (_: &Source, project_heads: &Self::A, commits: &Self::B, commit_changes: & Self::C) -> BTreeMap<ProjectId, Vec<(String, Vec<(PathId, SnapshotId)>)>> {
        project_heads.iter().map(|(pid, heads)| {
            let heads = heads.iter().map(|Head{name, commit}| {
                let mut contents = BTreeMap::<PathId, Option<SnapshotId>>::new();
                let mut visited = BTreeSet::<CommitId>::new();
                let mut q = Vec::<CommitId>::new();
                q.push(*commit);
                while let Some(cid) = q.pop() {
                    if visited.insert(cid) {
                        if let Some(changes) = commit_changes.get(&cid) {
                            for (path_id, snapshot_id) in changes {
                                if let Entry::Vacant(e) = contents.entry(*path_id) {
                                    e.insert(*snapshot_id);
                                }
                            }
                        }
                        if let Some(cinfo) = commits.get(&cid) {
                            q.extend(cinfo.parents.iter());
                        }
                    }
                }
                (name.clone(), contents.iter().filter(|(_path_id, snapshot_id)| snapshot_id.is_some()).map(|(path_id, snapshot_id)| (*path_id, snapshot_id.unwrap())).collect())
            }).collect();
            (*pid, heads)
        }).collect()
    }
}

pub(crate) struct ProjectExperienceExtractor {}
impl MapExtractor for ProjectExperienceExtractor {
    type Key = ProjectId;
    type Value = f64;
}
impl TripleMapExtractor for ProjectExperienceExtractor {
    type A = BTreeMap<UserId, i32>;
    type B = BTreeMap<ProjectId, Vec<CommitId>>;
    type C = BTreeMap<CommitId, Commit>;

    fn extract (_: &Source, developers_experience : &Self::A, project_commits : &Self::B, commits : &Self::C) -> BTreeMap<Self::Key, Self::Value> {
        
        project_commits.iter().map(|(project_id, commit_ids)| {
            let mut result : f64 = 0.0;
            let mut total_commits : f64 = 0.0;

            for commit_id in commit_ids {
                if let Some(commit) = commits.get(&commit_id) {
                    if let Some(dev_exp) = developers_experience.get(&(*commit).author) {
                        total_commits += 1.0;
                        result += *dev_exp as f64;
                    }
                }
            }
            (project_id.clone(), result/total_commits)
        }).collect()
    }
}

pub(crate) struct CommitTreeExtractor {}

impl ItemExtractor for CommitTreeExtractor {
    type Key = CommitId;
    type Value = Tree;
}

impl DoubleItemExtractor for CommitTreeExtractor {
    type A = BTreeMap<CommitId, Vec<ChangeTuple>>;
    type B = BTreeMap<CommitId, Commit>;

    fn extract(commit_id: Self::Key, _source: &Source, 
               commit_changes: &BTreeMap<CommitId, Vec<ChangeTuple>>, 
               commits: &BTreeMap<CommitId, Commit>) -> Tree {

        let mut contents: BTreeMap<PathId, Option<SnapshotId>> = BTreeMap::new();
        
        let mut visited:  BTreeSet<CommitId> = BTreeSet::new();
        let mut work_list: Vec<CommitId> = vec![commit_id];

        let empty_vector: Vec<ChangeTuple> = Vec::new(); // This is dumb, just go with it xD

        while let Some(commit_id) = work_list.pop() {
            if visited.insert(commit_id) {

                let changes = 
                    commit_changes.get(&commit_id).unwrap_or(&empty_vector);
                for (path_id, snapshot_id) in changes {
                    if let Entry::Vacant(change) = contents.entry(*path_id) {
                        change.insert(*snapshot_id);
                    }
                }

                if let Some(commit) = commits.get(&commit_id) {
                    work_list.extend(commit.parents.iter());
                }
            }
        }

        Tree::new(commit_id, contents)
    }

    // fn extract (_: &Source, project_heads: &Self::A, commits: &Self::B, commit_changes: & Self::C) -> BTreeMap<ProjectId, Vec<(String, Vec<(PathId, SnapshotId)>)>> {
    //     project_heads.iter().map(|(pid, heads)| {
    //         let heads = heads.iter().map(|Head{name, commit}| {
    //             let mut contents = BTreeMap::<PathId, Option<SnapshotId>>::new();
    //             let mut visited = BTreeSet::<CommitId>::new();
    //             let mut q = Vec::<CommitId>::new();
    //             q.push(*commit);
    //             while let Some(cid) = q.pop() {
    //                 if visited.insert(cid) {
    //                     if let Some(changes) = commit_changes.get(&cid) {
    //                         for (path_id, snapshot_id) in changes {
    //                             if let Entry::Vacant(e) = contents.entry(*path_id) {
    //                                 e.insert(*snapshot_id);
    //                             }
    //                         }
    //                     }
    //                     if let Some(cinfo) = commits.get(&cid) {
    //                         q.extend(cinfo.parents.iter());
    //                     }
    //                 }
    //             }
    //             (name.clone(), contents.iter().filter(|(_path_id, snapshot_id)| snapshot_id.is_some()).map(|(path_id, snapshot_id)| (*path_id, snapshot_id.unwrap())).collect())
    //         }).collect();
    //         (*pid, heads)
    //     }).collect()
    // }
}