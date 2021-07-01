use std::collections::BTreeMap;

use anyhow::*;

use crate::objects::*;
use crate::piracy::*;
use crate::log::*;
use crate::time::Duration;
use crate::{CacheDir, Store, Percentage, Timestamp};

use super::cache::*;
use super::lazy::LazyMap;
use super::persistent::*;
use super::metadata::*;
use super::extractors::*;
use super::source::Source;

pub(crate) struct Data {
    project_metadata:            ProjectMetadataSource,
    
    project_substores:           PersistentMap<ProjectSubstoreExtractor>,
    project_urls:                PersistentMap<ProjectUrlExtractor>,
    project_heads:               PersistentMap<ProjectHeadsExtractor>,
    project_paths:               PersistentMap<ProjectPathsExtractor>,
    project_snapshots:           PersistentMap<ProjectSnapshotsExtractor>,
    project_users:               PersistentMap<ProjectUsersExtractor>,
    project_authors:             PersistentMap<ProjectAuthorsExtractor>,
    project_committers:          PersistentMap<ProjectCommittersExtractor>,
    project_commits:             PersistentMap<ProjectCommitsExtractor>,
    project_lifetimes:           PersistentMap<ProjectLifetimesExtractor>,

    project_path_count:          PersistentMap<CountPerKeyExtractor<ProjectId, PathId>>,
    project_snapshot_count:      PersistentMap<CountPerKeyExtractor<ProjectId, SnapshotId>>,
    project_user_count:          PersistentMap<CountPerKeyExtractor<ProjectId, UserId>>,
    project_author_count:        PersistentMap<CountPerKeyExtractor<ProjectId, UserId>>,
    project_committer_count:     PersistentMap<CountPerKeyExtractor<ProjectId, UserId>>,
    project_commit_count:        PersistentMap<CountPerKeyExtractor<ProjectId, CommitId>>,

    project_change_contributions:            PersistentMap<ProjectChangeContributionsExtractor>,
    project_commit_contributions:            PersistentMap<ProjectCommitContributionsExtractor>,
    project_cumulative_change_contributions: PersistentMap<ProjectCumulativeContributionsExtractor>,
    project_cumulative_commit_contributions: PersistentMap<ProjectCumulativeContributionsExtractor>,

    project_unique_files:           PersistentMap<ProjectUniqueFilesExtractor>,
    project_original_files:         PersistentMap<ProjectOriginalFilesExtractor>,
    project_impact:                 PersistentMap<ProjectImpactExtractor>,
    project_files:                  PersistentMap<ProjectFilesExtractor>,
    project_languages:              PersistentMap<ProjectLanguagesExtractor>,
    project_languages_count:        PersistentMap<CountPerKeyExtractor<ProjectId, (Language,usize)>>,
    project_major_language:         PersistentMap<ProjectMajorLanguageExtractor>,
    project_major_language_ratio:   PersistentMap<ProjectMajorLanguageRatioExtractor>,
    project_major_language_changes: PersistentMap<ProjectMajorLanguageChangesExtractor>,
    project_all_forks:              PersistentMap<ProjectAllForksExtractor>,
    project_all_forks_count:        PersistentMap<CountPerKeyExtractor<ProjectId, ProjectId>>,
    project_head_trees:             PersistentMap<ProjectHeadTreesExtractor>,
    project_head_trees_count:       PersistentMap<CountPerKeyExtractor<ProjectId, (String, Vec<(PathId, SnapshotId)>)>>,

    project_buggy_issue_count:   PersistentMap<ProjectBuggyIssuesExtractor>,
    project_issue_count:         PersistentMap<ProjectBuggyIssuesExtractor>,
    project_is_fork:             PersistentMap<ProjectIsForkExtractor>,
    project_is_archived:         PersistentMap<ProjectIsArchivedExtractor>,
    project_is_disabled:         PersistentMap<ProjectIsDisabledExtractor>,
    project_star_gazer_count:    PersistentMap<ProjectStargazersExtractor>,
    project_watcher_count:       PersistentMap<ProjectWatchersExtractor>,
    project_project_size:        PersistentMap<ProjectSizeExtractor>,
    project_open_issue_count:    PersistentMap<ProjectIssuesExtractor>,
    project_fork_count:          PersistentMap<ProjectForksExtractor>,
    project_subscriber_count:    PersistentMap<ProjectSubscribersExtractor>,
    project_license:             PersistentMap<ProjectLicenseExtractor>,
    project_language:            PersistentMap<ProjectLanguageExtractor>,
    project_description:         PersistentMap<ProjectDescriptionExtractor>,
    project_homepage:            PersistentMap<ProjectHomepageExtractor>,
    project_has_issues:          PersistentMap<ProjectHasIssuesExtractor>,
    project_has_downloads:       PersistentMap<ProjectHasDownloadsExtractor>,
    project_has_wiki:            PersistentMap<ProjectHasWikiExtractor>,
    project_has_pages:           PersistentMap<ProjectHasPagesExtractor>,
    project_created:             PersistentMap<ProjectCreatedExtractor>,
    project_updated:             PersistentMap<ProjectUpdatedExtractor>,
    project_pushed:              PersistentMap<ProjectPushedExtractor>,
    project_default_branch:      PersistentMap<ProjectDefaultBranchExtractor>,

    users:                       PersistentMap<UserExtractor>,
    user_authored_commits:       PersistentMap<UserAuthoredCommitsExtractor>,
    user_committed_commits:      PersistentMap<UserAuthoredCommitsExtractor>,
    user_author_experience:      PersistentMap<UserExperienceExtractor>,
    user_committer_experience:   PersistentMap<UserExperienceExtractor>,
    user_experience:             PersistentMap<CombinedUserExperienceExtractor>,
    developer_experience:        PersistentMap<DeveloperExperienceExtractor>,

    user_authored_commit_count:  PersistentMap<CountPerKeyExtractor<UserId, CommitId>>,
    user_committed_commit_count: PersistentMap<CountPerKeyExtractor<UserId, CommitId>>,

    paths:                       PersistentMap<PathExtractor>,
    //snapshots:                   PersistentMap<SnapshotExtractor>,

    commits:                     PersistentMap<CommitExtractor>,
    commit_hashes:               PersistentMap<CommitHashExtractor>,
    commit_messages:             PersistentMap<CommitMessageExtractor>,
    commit_author_timestamps:    PersistentMap<AuthorTimestampExtractor>,
    commit_committer_timestamps: PersistentMap<CommitterTimestampExtractor>,
    commit_changes:              PersistentMap<CommitChangesExtractor>,

    commit_change_count:         PersistentMap<CountPerKeyExtractor<CommitId, ChangeTuple>>,

    commit_projects:             PersistentMap<CommitProjectsExtractor>,
    commit_projects_count:       PersistentMap<CountPerKeyExtractor<CommitId, ProjectId>>,
    commit_languages:            PersistentMap<CommitLanguagesExtractor>,
    commit_languages_count:      PersistentMap<CountPerKeyExtractor<CommitId, Language>>,

    commit_trees:                LazyMap<CommitTreeExtractor>,

    snapshot_projects :          PersistentMap<SnapshotProjectsExtractor>,

    // TODO frequency of commits/regularity of commits
    // TODO maybe some of these could be pre-cached all at once (eg all commit properties)

    project_max_commit_delta:    PersistentMap<MaxCommitDeltaExtractor>,
    avg_commit_delta:              PersistentMap<AvgCommitDeltaExtractor>,
    project_time_since_last_commit:       PersistentMap<TimeSinceLastCommitExtractor>,
    project_time_since_first_commit:       PersistentMap<TimeSinceFirstCommitExtractor>,
    is_abandoned:                 PersistentMap<IsAbandonedExtractor>,
    snapshot_locs:                PersistentMap<SnapshotLocsExtractor>,
    project_locs:                 PersistentMap<ProjectLocsExtractor>,
    duplicated_code:              PersistentMap<DuplicatedCodeExtractor>,
    project_is_valid:             PersistentMap<ProjectIsValidExtractor>,
    project_logs:                 PersistentMap<ProjectLogsExtractor>,
    project_max_experience:       PersistentMap<ProjectMaxExperienceExtractor>,
    project_experience:           PersistentMap<ProjectExperienceExtractor>
}

impl Data {
    pub fn new(/*source: DataSource,*/ cache_dir: CacheDir, log: Log) -> Data {
        let dir = cache_dir.as_string();
        Data {
            project_metadata:               ProjectMetadataSource::new(log.clone(),dir.clone()),
   
            project_urls:                   PersistentMap::new(CACHE_FILE_PROJECT_URL,                    log.clone(),dir.clone()).without_cache(),
            project_substores:              PersistentMap::new(CACHE_FILE_PROJECT_SUBSTORE,               log.clone(),dir.clone()).without_cache(),
            project_heads:                  PersistentMap::new(CACHE_FILE_PROJECT_HEADS,                  log.clone(),dir.clone()),
            project_paths:                  PersistentMap::new(CACHE_FILE_PROJECT_PATHS,                  log.clone(),dir.clone()),
            project_path_count:             PersistentMap::new(CACHE_FILE_PROJECT_PATH_COUNT,             log.clone(),dir.clone()),
            project_snapshots:              PersistentMap::new(CACHE_FILE_PROJECT_SNAPSHOTS,              log.clone(),dir.clone()),
            project_snapshot_count:         PersistentMap::new(CACHE_FILE_PROJECT_SNAPSHOT_COUNT,         log.clone(),dir.clone()),
            project_users:                  PersistentMap::new(CACHE_FILE_PROJECT_USERS,                  log.clone(),dir.clone()),
            project_user_count:             PersistentMap::new(CACHE_FILE_PROJECT_USER_COUNT,             log.clone(),dir.clone()),
            project_authors:                PersistentMap::new(CACHE_FILE_PROJECT_AUTHORS,                log.clone(),dir.clone()),
            project_author_count:           PersistentMap::new(CACHE_FILE_PROJECT_AUTHOR_COUNT,           log.clone(),dir.clone()),
            project_committers:             PersistentMap::new(CACHE_FILE_PROJECT_COMMITTERS,             log.clone(),dir.clone()),
            project_committer_count:        PersistentMap::new(CACHE_FILE_PROJECT_COMMITTER_COUNT,        log.clone(),dir.clone()),
            project_commits:                PersistentMap::new(CACHE_FILE_PROJECT_COMMITS,                log.clone(),dir.clone()),
            project_commit_count:           PersistentMap::new(CACHE_FILE_PROJECT_COMMIT_COUNT,           log.clone(),dir.clone()),
            project_lifetimes:              PersistentMap::new(CACHE_FILE_PROJECT_LIFETIME,               log.clone(),dir.clone()),
            project_issue_count:            PersistentMap::new(CACHE_FILE_PROJECT_ISSUE_COUNT,            log.clone(),dir.clone()),
            project_buggy_issue_count:      PersistentMap::new(CACHE_FILE_PROJECT_BUGGY_ISSUE_COUNT,      log.clone(),dir.clone()),
            project_open_issue_count:       PersistentMap::new(CACHE_FILE_PROJECT_OPEN_ISSUE_COUNT,       log.clone(),dir.clone()),
            project_is_fork:                PersistentMap::new(CACHE_FILE_PROJECT_IS_FORK,                log.clone(),dir.clone()),
            project_is_archived:            PersistentMap::new(CACHE_FILE_PROJECT_IS_ARCHIVED,            log.clone(),dir.clone()),
            project_is_disabled:            PersistentMap::new(CACHE_FILE_PROJECT_IS_DISABLED,            log.clone(),dir.clone()),
            project_star_gazer_count:       PersistentMap::new(CACHE_FILE_PROJECT_STARGAZER_COUNT,        log.clone(),dir.clone()),
            project_watcher_count:          PersistentMap::new(CACHE_FILE_PROJECT_WATCHER_COUNT,          log.clone(),dir.clone()),
            project_project_size:           PersistentMap::new(CACHE_FILE_PROJECT_SIZE,                   log.clone(),dir.clone()),
            project_fork_count:             PersistentMap::new(CACHE_FILE_PROJECT_FORK_COUNT,             log.clone(),dir.clone()),
            project_subscriber_count:       PersistentMap::new(CACHE_FILE_PROJECT_SUBSCRIBER_COUNT,       log.clone(),dir.clone()),
            project_license:                PersistentMap::new(CACHE_FILE_PROJECT_LICENSE,                log.clone(),dir.clone()),
            project_language:               PersistentMap::new(CACHE_FILE_PROJECT_LANGUAGE,               log.clone(),dir.clone()),
            project_description:            PersistentMap::new(CACHE_FILE_PROJECT_DESCRIPTION,            log.clone(),dir.clone()),
            project_homepage:               PersistentMap::new(CACHE_FILE_PROJECT_HOMEPAGE,               log.clone(),dir.clone()),
            project_has_issues:             PersistentMap::new(CACHE_FILE_PROJECT_HAS_ISSUES,             log.clone(),dir.clone()),
            project_has_downloads:          PersistentMap::new(CACHE_FILE_PROJECT_HAS_DOWNLOADS,          log.clone(),dir.clone()),
            project_has_wiki:               PersistentMap::new(CACHE_FILE_PROJECT_HAS_WIKI,               log.clone(),dir.clone()),
            project_has_pages:              PersistentMap::new(CACHE_FILE_PROJECT_HAS_PAGES,              log.clone(),dir.clone()),
            project_created:                PersistentMap::new(CACHE_FILE_PROJECT_CREATED,                log.clone(),dir.clone()),
            project_updated:                PersistentMap::new(CACHE_FILE_PROJECT_UPDATED,                log.clone(),dir.clone()),
            project_pushed:                 PersistentMap::new(CACHE_FILE_PROJECT_PUSHED,                 log.clone(),dir.clone()),
            project_default_branch:         PersistentMap::new(CACHE_FILE_PROJECT_DEFAULT_BRANCH,         log.clone(),dir.clone()),
            project_commit_contributions:   PersistentMap::new(CACHE_FILE_PROJECT_COMMIT_CONTRIBUTIONS,   log.clone(),dir.clone()),
            project_change_contributions:   PersistentMap::new(CACHE_FILE_PROJECT_CHANGE_CONTRIBUTIONS,   log.clone(),dir.clone()),
            project_cumulative_commit_contributions: PersistentMap::new(CACHE_FILE_PROJECT_CUMULATIVE_COMMIT_CONTRIBUTIONS, log.clone(),dir.clone()),
            project_cumulative_change_contributions: PersistentMap::new(CACHE_FILE_PROJECT_CUMULATIVE_CHANGE_CONTRIBUTIONS, log.clone(),dir.clone()),
            project_unique_files:           PersistentMap::new(CACHE_FILE_PROJECT_UNIQUE_FILES,           log.clone(),dir.clone()),
            project_original_files:         PersistentMap::new(CACHE_FILE_PROJECT_ORIGINAL_FILES,         log.clone(),dir.clone()),
            project_impact:                 PersistentMap::new(CACHE_FILE_PROJECT_IMPACT,                 log.clone(),dir.clone()),
            project_files:                  PersistentMap::new(CACHE_FILE_PROJECT_FILES,                  log.clone(), dir.clone()),
            project_languages:              PersistentMap::new(CACHE_FILE_PROJECT_LANGUAGES,              log.clone(), dir.clone()),
            project_languages_count:        PersistentMap::new(CACHE_FILE_PROJECT_LANGUAGES_COUNT,        log.clone(), dir.clone()),
            project_major_language:         PersistentMap::new(CACHE_FILE_PROJECT_MAJOR_LANGUAGE,         log.clone(), dir.clone()),
            project_major_language_ratio:   PersistentMap::new(CACHE_FILE_PROJECT_MAJOR_LANGUAGE_RATIO,   log.clone(), dir.clone()),
            project_major_language_changes: PersistentMap::new(CACHE_FILE_PROJECT_MAJOR_LANGUAGE_CHANGES, log.clone(), dir.clone()),
            project_all_forks:              PersistentMap::new(CACHE_FILE_PROJECT_ALL_FORKS,              log.clone(), dir.clone()),
            project_all_forks_count:        PersistentMap::new(CACHE_FILE_PROJECT_ALL_FORKS_COUNT,        log.clone(), dir.clone()),
            project_head_trees:             PersistentMap::new(CACHE_FILE_PROJECT_HEAD_TREES,             log.clone(), dir.clone()),
            project_head_trees_count:       PersistentMap::new(CACHE_FILE_PROJECT_HEAD_TREES_COUNT,       log.clone(), dir.clone()),
            users:                          PersistentMap::new(CACHE_FILE_USERS,                          log.clone(),dir.clone()).without_cache(),
            user_authored_commits:          PersistentMap::new(CACHE_FILE_USER_AUTHORED_COMMITS,          log.clone(),dir.clone()),
            user_committed_commits:         PersistentMap::new(CACHE_FILE_USER_COMMITTED_COMMITS,         log.clone(),dir.clone()),
            user_author_experience:         PersistentMap::new(CACHE_FILE_USER_AUTHOR_EXPERIENCE,         log.clone(),dir.clone()),
            user_committer_experience:      PersistentMap::new(CACHE_FILE_USER_COMMITTER_EXPERIENCE,      log.clone(),dir.clone()),
            user_experience:                PersistentMap::new(CACHE_FILE_USER_EXPERIENCE,                log.clone(),dir.clone()),
            user_authored_commit_count:     PersistentMap::new(CACHE_FILE_USER_AUTHORED_COMMIT_COUNT,     log.clone(),dir.clone()),
            user_committed_commit_count:    PersistentMap::new(CACHE_FILE_USER_COMMITTED_COMMIT_COUNT,    log.clone(),dir.clone()),
            developer_experience:           PersistentMap::new(CACHE_FILE_DEVELOPER_EXPERIENCE,           log.clone(),dir.clone()),
            paths:                          PersistentMap::new(CACHE_FILE_PATHS,                          log.clone(),dir.clone()).without_cache(),
            commits:                        PersistentMap::new(CACHE_FILE_COMMITS,                        log.clone(),dir.clone()),
            commit_hashes:                  PersistentMap::new(CACHE_FILE_COMMIT_HASHES,                  log.clone(),dir.clone()).without_cache(),
            commit_messages:                PersistentMap::new(CACHE_FILE_COMMIT_MESSAGES,                log.clone(),dir.clone()).without_cache(),
            commit_author_timestamps:       PersistentMap::new(CACHE_FILE_COMMIT_AUTHOR_TIMESTAMPS,       log.clone(),dir.clone()),
            commit_committer_timestamps:    PersistentMap::new(CACHE_FILE_COMMIT_COMMITTER_TIMESTAMPS,    log.clone(),dir.clone()),
            commit_changes:                 PersistentMap::new(CACHE_FILE_COMMIT_CHANGES,                 log.clone(),dir.clone()).without_cache(),
            commit_change_count:            PersistentMap::new(CACHE_FILE_COMMIT_CHANGE_COUNT,            log.clone(),dir.clone()),
            commit_projects:                PersistentMap::new(CACHE_FILE_COMMIT_PROJECTS,                log.clone(),dir.clone()),
            commit_projects_count:          PersistentMap::new(CACHE_FILE_COMMIT_PROJECTS_COUNT,          log.clone(),dir.clone()),
            commit_languages:               PersistentMap::new(CACHE_FILE_COMMIT_LANGUAGES,               log.clone(),dir.clone()),
            commit_languages_count:         PersistentMap::new(CACHE_FILE_COMMIT_LANGUAGES_COUNT,         log.clone(),dir.clone()),
            snapshot_projects:              PersistentMap::new(CACHE_FILE_SNAPSHOT_PROJECTS,              log.clone(),dir.clone()),
            project_max_commit_delta:       PersistentMap::new(CACHE_FILE_MAX_COMMIT_DELTA, log.clone(), dir.clone()),
            avg_commit_delta:               PersistentMap::new(CACHE_FILE_AVG_COMMIT_DELTA, log.clone(), dir.clone()),
            project_time_since_last_commit: PersistentMap::new(CACHE_FILE_TIME_SINCE_LAST_COMMIT, log.clone(), dir.clone()),
            project_time_since_first_commit:PersistentMap::new(CACHE_FILE_TIME_SINCE_FIRST_COMMIT, log.clone(), dir.clone()),
            is_abandoned:                   PersistentMap::new(CACHE_FILE_IS_ABANDONED, log.clone(), dir.clone()),
            snapshot_locs:                  PersistentMap::new(CACHE_FILE_SNAPSHOT_LOCS, log.clone(), dir.clone()),
            project_locs:                   PersistentMap::new(CACHE_FILE_PROJECT_LOCS, log.clone(), dir.clone()),
            duplicated_code:                PersistentMap::new(CACHE_FILE_DUPLICATED_CODE, log.clone(), dir.clone()),
            project_is_valid:               PersistentMap::new(CACHE_FILE_PROJECT_IS_VALID, log.clone(), dir.clone()),
            project_logs:                   PersistentMap::new(CACHE_FILE_PROJECT_LOGS, log.clone(), dir.clone()),
            project_max_experience:         PersistentMap::new(CACHE_FILE_PROJECT_MAX_EXPERIENCE, log.clone(), dir.clone()),
            project_experience:             PersistentMap::new(CACHE_FILE_PROJECT_EXPERIENCE, log.clone(), dir.clone()),
            commit_trees:                   LazyMap::new(CACHE_COMMIT_TREES, log.clone(), dir.clone()),  
        }
    }
}

impl Data { // Prequincunx, sort of
    pub fn all_project_ids(&mut self, source: &Source) -> Vec<ProjectId> {
        self.smart_load_project_urls(source).keys().collect::<Vec<&ProjectId>>().pirate()
    }
    pub fn all_user_ids(&mut self, source: &Source) -> Vec<UserId> {
        self.smart_load_users(source).keys().collect::<Vec<&UserId>>().pirate()
    }
    pub fn all_path_ids(&mut self, source: &Source) -> Vec<PathId> {
        self.smart_load_paths(source).keys().collect::<Vec<&PathId>>().pirate()
    }
    pub fn all_commit_ids(&mut self, source: &Source) -> Vec<CommitId> {
        self.smart_load_commits(source).keys().collect::<Vec<&CommitId>>().pirate()
    }
}

impl Data { // Quincunx, sort of
    #[allow(dead_code)] pub fn projects<'a>(&'a mut self, source: &Source) -> impl Iterator<Item=Project> + 'a {
        self.smart_load_project_urls(source).iter().map(|(id, url)| Project::new(id.clone(), url.clone()))
    }

    #[allow(dead_code)] pub fn users<'a>(&'a mut self, source: &Source) -> impl Iterator<Item=&'a User> + 'a {
        self.smart_load_users(source).iter().map(|(_, user)| user)
    }

    #[allow(dead_code)] pub fn paths<'a>(&'a mut self, source: &Source) -> impl Iterator<Item=&'a Path> + 'a {
        self.smart_load_paths(source).iter().map(|(_, path)| path)
    }

    #[allow(dead_code)] pub fn commits<'a>(&'a mut self, source: &Source) -> impl Iterator<Item=&'a Commit> + 'a {
        self.smart_load_commits(source).iter().map(|(_, commit)| commit)
    }
}

impl Data {
    pub fn project(&mut self, id: &ProjectId, source: &Source) -> Option<Project> {
        self.smart_load_project_urls(source).get(id)
            .map(|url| Project::new(id.clone(), url.clone()))
    }
    pub fn project_issues(&mut self, id: &ProjectId, source: &Source) -> Option<usize> {
        self.smart_load_project_issues(source).get(id).pirate()
    }
    pub fn project_buggy_issues(&mut self, id: &ProjectId, source: &Source) -> Option<usize> {
        self.smart_load_project_buggy_issues(source).get(id).pirate()
    }
    pub fn project_is_fork(&mut self, id: &ProjectId, source: &Source) -> Option<bool> {
        self.smart_load_project_is_fork(source).get(id).pirate()
    }
    pub fn project_is_archived(&mut self, id: &ProjectId, source: &Source) -> Option<bool> {
        self.smart_load_project_is_archived(source).get(id).pirate()
    }
    pub fn project_is_disabled(&mut self, id: &ProjectId, source: &Source) -> Option<bool> {
        self.smart_load_project_is_disabled(source).get(id).pirate()
    }
    pub fn project_star_gazer_count(&mut self, id: &ProjectId, source: &Source) -> Option<usize> {
        self.smart_load_project_star_gazer_count(source).get(id).pirate()
    }

    pub fn project_watcher_count(&mut self, id: &ProjectId, source: &Source) -> Option<usize> {
        self.smart_load_project_watcher_count(source).get(id).pirate()
    }
    pub fn project_size(&mut self, id: &ProjectId, source: &Source) -> Option<usize> {
        self.smart_load_project_size(source).get(id).pirate()
    }
    pub fn project_open_issue_count(&mut self, id: &ProjectId, source: &Source) -> Option<usize> {
        self.smart_load_project_open_issue_count(source).get(id).pirate()
    }
    pub fn project_fork_count(&mut self, id: &ProjectId, source: &Source) -> Option<usize> {
        self.smart_load_project_fork_count(source).get(id).pirate()
    }
    pub fn project_subscriber_count(&mut self, id: &ProjectId, source: &Source) -> Option<usize> {
        self.smart_load_project_subscriber_count(source).get(id).pirate()
    }
    pub fn project_license(&mut self, id: &ProjectId, source: &Source) -> Option<String> {
        self.smart_load_project_license(source).get(id).pirate()
    }
    pub fn project_language(&mut self, id: &ProjectId, source: &Source) -> Option<Language> {
        self.smart_load_project_language(source).get(id).pirate()
    }
    pub fn project_description(&mut self, id: &ProjectId, source: &Source) -> Option<String> {
        self.smart_load_project_description(source).get(id).pirate()
    }
    pub fn project_homepage(&mut self, id: &ProjectId, source: &Source) -> Option<String> {
        self.smart_load_project_homepage(source).get(id).pirate()
    }
    pub fn project_has_issues(&mut self, id: &ProjectId, source: &Source) -> Option<bool> {
        self.smart_load_project_has_issues(source).get(id).pirate()
    }
    pub fn project_has_downloads(&mut self, id: &ProjectId, source: &Source) -> Option<bool> {
        self.smart_load_project_has_downloads(source).get(id).pirate()
    }
    pub fn project_has_wiki(&mut self, id: &ProjectId, source: &Source) -> Option<bool> {
        self.smart_load_project_has_wiki(source).get(id).pirate()
    }
    pub fn project_has_pages(&mut self, id: &ProjectId, source: &Source) -> Option<bool> {
        self.smart_load_project_has_pages(source).get(id).pirate()
    }
    pub fn project_created(&mut self, id: &ProjectId, source: &Source) -> Option<Timestamp> {
        self.smart_load_project_created(source).get(id).pirate()        
    }
    pub fn project_updated(&mut self, id: &ProjectId, source: &Source) -> Option<Timestamp> {
        self.smart_load_project_updated(source).get(id).pirate()
    }
    pub fn project_pushed(&mut self, id: &ProjectId, source: &Source) -> Option<Timestamp> {
        self.smart_load_project_pushed(source).get(id).pirate()
    }
    pub fn project_default_branch(&mut self, id: &ProjectId, source: &Source) -> Option<String> {
        self.smart_load_project_default_branch(source).get(id).pirate()
    }
    pub fn project_commit_contribution_ids(&mut self, id: &ProjectId, source: &Source) -> Option<Vec<(UserId, usize)>> {
        self.smart_load_project_commit_contributions(source).get(id).pirate()
    }
    pub fn project_commit_contributions(&mut self, id: &ProjectId, source: &Source) -> Option<Vec<(User, usize)>> {
        self.smart_load_project_commit_contributions(source).get(id).pirate().map(|contributions| {
            contributions.iter().flat_map(|(user_id, n)| {
                self.user(user_id, source).map(|user| (user.clone(), *n))
            }).collect()
        })
    }
    pub fn project_cumulative_commit_contributions(&mut self, id: &ProjectId, source: &Source) -> Option<Vec<Percentage>> {
        self.smart_load_project_cumulative_commit_contributions(source).get(id).pirate()
    }
    pub fn project_change_contribution_ids(&mut self, id: &ProjectId, source: &Source) -> Option<Vec<(UserId, usize)>> {
        self.smart_load_project_change_contributions(source).get(id).pirate()
    }
    pub fn project_change_contributions(&mut self, id: &ProjectId, source: &Source) -> Option<Vec<(User, usize)>> {
        self.smart_load_project_change_contributions(source).get(id).pirate().map(|contributions| {
            contributions.iter().flat_map(|(user_id, n)| {
                self.user(user_id, source).map(|user| (user.clone(), *n))
            }).collect()
        })
    }
    pub fn project_cumulative_change_contributions(&mut self, id: &ProjectId, source: &Source) -> Option<Vec<Percentage>> {
        self.smart_load_project_cumulative_change_contributions(source).get(id).pirate()
    }
    // TODO make a mechanism for caching parameterized attributes
    fn calculate_contributing_authors_at_cutoff(contributions: Option<Vec<(UserId, usize)>>, percentage: Percentage) -> Option<Vec<UserId>>{
        if let Some(contributions) = contributions {
            let total_contributions: usize = contributions.iter().map(|(_, contributions)| contributions).sum();
            let target_contributions: usize = ((percentage as usize) * total_contributions) / 100;
            let mut contributing_author_ids: Vec<UserId> = Vec::new();
            let mut contributions_so_far: usize = 0usize;
            for (author_id, contribution) in contributions {
                contributions_so_far = contributions_so_far + contribution;
                contributing_author_ids.push(author_id);
                if contributions_so_far >= target_contributions {
                    break;
                }
            }
            return Some(contributing_author_ids)
        } else {
            None
        }  
    }
    pub fn project_author_ids_contributing_commits(&mut self, id: &ProjectId, percentage: Percentage, source: &Source) -> Option<Vec<UserId>> {        
        Self::calculate_contributing_authors_at_cutoff(self.project_commit_contribution_ids(id, source), percentage)
    }
    pub fn project_author_ids_contributing_changes(&mut self, id: &ProjectId, percentage: Percentage, source: &Source) -> Option<Vec<UserId>> {
        Self::calculate_contributing_authors_at_cutoff(self.project_change_contribution_ids(id, source), percentage)
    }
    pub fn project_authors_contributing_commits(&mut self, id: &ProjectId, percentage: Percentage, source: &Source) -> Option<Vec<User>> {
        self.project_author_ids_contributing_commits(id, percentage, source).map(|ids| {
            ids.iter().flat_map(|id| self.user(id, source)).collect()
        })
    }
    pub fn project_authors_contributing_changes(&mut self, id: &ProjectId, percentage: Percentage, source: &Source) -> Option<Vec<User>> {
        self.project_author_ids_contributing_changes(id, percentage, source).map(|ids| {
            ids.iter().flat_map(|id| self.user(id, source)).collect()
        })
    }
    pub fn project_authors_contributing_commits_count(&mut self, id: &ProjectId, percentage: Percentage, source: &Source) -> Option<usize> {
        self.project_author_ids_contributing_commits(id, percentage, source).map(|ids| ids.len())
    }
    pub fn project_authors_contributing_changes_count(&mut self, id: &ProjectId, percentage: Percentage, source: &Source) -> Option<usize> {
        self.project_author_ids_contributing_changes(id, percentage, source).map(|ids| ids.len())
    }
    pub fn project_url(&mut self, id: &ProjectId, source: &Source) -> Option<String> {
        self.smart_load_project_urls(source).get(id).pirate()
    }
    pub fn project_heads(&mut self, id: &ProjectId, source: &Source) -> Option<Vec<Head>> {
        self.smart_load_project_heads(source).get(id).pirate()
    }
    // pub fn project_heads(&mut self, source: &DataSource, id: &ProjectId) -> Option<Vec<(String, Commit)>> {
    //     self.smart_load_project_heads(source).get(id).pirate().map(|v| {
    //         v.into_iter().flat_map(|(name, commit_id)| {
    //             self.commit(source, &commit_id).map(|commit| {
    //                 Head::new(name, commit.clone())
    //             })
    //         }).collect()
    //     })
    // }
    pub fn project_commit_ids(&mut self, id: &ProjectId, source: &Source) -> Option<Vec<CommitId>> {
        self.smart_load_project_commits(source).get(id).pirate()
    }
    pub fn project_commits(&mut self, id: &ProjectId, source: &Source) -> Option<Vec<Commit>> {
        self.smart_load_project_commits(source).get(id).pirate().map(|ids| {
            ids.iter().flat_map(|id| self.commit(id, source)).collect()
            // FIXME issue warnings in situations like these (when self.commit(id) fails etc.)
        })
    }
    pub fn project_commit_count(&mut self, id: &ProjectId, source: &Source) -> Option<usize> {
        self.smart_load_project_commit_count(source).get(id).pirate()
    }
    pub fn project_path_ids(&mut self, id: &ProjectId, source: &Source) -> Option<Vec<PathId>> {
        self.smart_load_project_paths(source).get(id).pirate()
    }
    pub fn project_paths(&mut self, id: &ProjectId, source: &Source) -> Option<Vec<Path>> {
        self.smart_load_project_paths(source).get(id).pirate().map(|ids| {
            ids.iter().flat_map(|id| self.path(id, source)).collect()
        })
    }
    pub fn project_path_count(&mut self, id: &ProjectId, source: &Source) -> Option<usize> {
        self.smart_load_project_path_count(source).get(id).pirate()
    }
    pub fn project_snapshot_ids(&mut self, id: &ProjectId, source: &Source) -> Option<Vec<SnapshotId>> {
        self.smart_load_project_snapshots(source).get(id).pirate()
    }
    pub fn project_snapshot_count(&mut self, id: &ProjectId, source: &Source) -> Option<usize> {
        self.smart_load_project_snapshot_count(source).get(id).pirate()
    }
    pub fn project_author_ids(&mut self, id: &ProjectId, source: &Source) -> Option<Vec<UserId>> {
        self.smart_load_project_authors(source).get(id).pirate()
    }
    pub fn project_authors(&mut self, id: &ProjectId, source: &Source) -> Option<Vec<User>> {
        self.smart_load_project_authors(source).get(id).pirate().map(|ids| {
            ids.iter().flat_map(|id| self.user(id, source)).collect()
        })
    }
    pub fn project_author_count(&mut self, id: &ProjectId, source: &Source) -> Option<usize> {
        self.smart_load_project_author_count(source).get(id).pirate()
    }
    pub fn project_committer_ids(&mut self, id: &ProjectId, source: &Source) -> Option<Vec<UserId>> {
        self.smart_load_project_committers(source).get(id).pirate()
    }
    pub fn project_committers(&mut self, id: &ProjectId, source: &Source) -> Option<Vec<User>> {
        self.smart_load_project_committers(source).get(id).pirate().map(|ids| {
            ids.iter().flat_map(|id| self.user(id, source)).collect()
        })
    }
    pub fn project_committer_count(&mut self, id: &ProjectId, source: &Source) -> Option<usize> {
        self.smart_load_project_committer_count(source).get(id).pirate()
    }
    pub fn project_user_ids(&mut self, id: &ProjectId, source: &Source) -> Option<Vec<UserId>> {
        self.smart_load_project_users(source).get(id).pirate()
    }
    pub fn project_users(&mut self, id: &ProjectId, source: &Source) -> Option<Vec<User>> {
        self.smart_load_project_users(source).get(id).pirate().map(|ids| {
            ids.iter().flat_map(|id| self.user(id, source)).collect()
        })
    }
    pub fn project_user_count(&mut self, id: &ProjectId, source: &Source) -> Option<usize> {
        self.smart_load_project_user_count(source).get(id).pirate()
    }
    pub fn project_lifetime(&mut self, id: &ProjectId, source: &Source) -> Option<Duration> {
        self.smart_load_project_lifetimes(source).get(id)
            .pirate()
            .map(|seconds| Duration::from(seconds))
    }
    pub fn project_substore(&mut self, id: &ProjectId, source: &Source) -> Option<Store> {
        self.smart_load_project_substore(source).get(id)
            .pirate()
    }
    pub fn project_unique_files(& mut self, id: &ProjectId, source: &Source) -> Option<usize> {
        self.smart_load_project_unique_files(source).get(id)
            .pirate()
    }
    pub fn project_original_files(& mut self, id: &ProjectId, source: &Source) -> Option<usize> {
        self.smart_load_project_original_files(source).get(id)
            .pirate()
    }
    pub fn project_impact(& mut self, id: &ProjectId, source: &Source) -> Option<usize> {
        self.smart_load_project_impact(source).get(id)
            .pirate()
    }
    pub fn project_files(& mut self, id: &ProjectId, source: &Source) -> Option<usize> {
        self.smart_load_project_files(source).get(id)
            .pirate()
    }
    pub fn project_language_composition(& mut self, id: &ProjectId, source: &Source) -> Option<Vec<(Language,usize)>> {
        self.smart_load_project_languages(source).get(id)
            .pirate()
    }
    pub fn project_languages(& mut self, id: &ProjectId, source: &Source) -> Option<Vec<Language>> {
        self.smart_load_project_languages(source).get(id).map(|vector| {
            vector.iter().map(|e| e.0.clone()).collect::<Vec<Language>>()
        })
    }
    pub fn project_languages_count(& mut self, id: &ProjectId, source: &Source) -> Option<usize> {
        self.smart_load_project_languages_count(source).get(id)
            .pirate()
    }
    pub fn project_major_language(& mut self, id: &ProjectId, source: &Source) -> Option<Language> {
        self.smart_load_project_major_language(source).get(id)
            .pirate()
    }
    pub fn project_major_language_ratio(& mut self, id: &ProjectId, source: &Source) -> Option<f64> {
        self.smart_load_project_major_language_ratio(source).get(id)
            .pirate()
    }
    pub fn project_major_language_changes(& mut self, id: &ProjectId, source: &Source) -> Option<usize> {
        self.smart_load_project_major_language_changes(source).get(id)
            .pirate()
    }
    pub fn project_all_forks(& mut self, id: &ProjectId, source: &Source) -> Option<Vec<ProjectId>> {
        self.smart_load_project_all_forks(source).get(id)
            .pirate()
    }
    pub fn project_all_forks_count(& mut self, id: &ProjectId, source: &Source) -> Option<usize> {
        self.smart_load_project_all_forks_count(source).get(id)
            .pirate()
    }
    pub fn project_head_trees(& mut self, id: &ProjectId, source: &Source) -> Option<Vec<(String, Vec<(PathId, SnapshotId)>)>> {
        self.smart_load_project_head_trees(source).get(id)
            .pirate()
    }
    pub fn project_head_trees_count(& mut self, id: &ProjectId, source: &Source) -> Option<usize> {
        self.smart_load_project_head_trees_count(source).get(id)
            .pirate()
    }
    pub fn user(&mut self, id: &UserId, source: &Source) -> Option<User> {
        self.smart_load_users(source).get(id).pirate()
    }
    pub fn path(&mut self, id: &PathId, source: &Source) -> Option<Path> {
        self.smart_load_paths(source).get(id).pirate()
    }
    pub fn commit(&mut self, id: &CommitId, source: &Source) -> Option<Commit> {
        self.smart_load_commits(source).get(id).pirate()
    }
    pub fn commit_hash(&mut self, id: &CommitId, source: &Source) -> Option<String> {
        self.smart_load_commit_hashes(source).get(id).pirate()
    }
    pub fn commit_message(&mut self, id: &CommitId, source: &Source) -> Option<String> {
        self.smart_load_commit_messages(source).get(id).pirate()
    }
    pub fn commit_author_timestamp(&mut self, id: &CommitId, source: &Source) -> Option<Timestamp> {
        self.smart_load_commit_author_timestamps(source).get(id).pirate()
    }
    pub fn commit_committer_timestamp(&mut self, id: &CommitId, source: &Source) -> Option<Timestamp> {
        self.smart_load_commit_committer_timestamps(source).get(id).pirate()
    }
    pub fn commit_changes(&mut self, id: &CommitId, source: &Source) -> Option<Vec<Change>> {
        self.smart_load_commit_changes(source).get(id).map(|vector| {
            vector.iter().map(|(path_id, snapshot_id)| {
                Change::new(path_id.clone(), snapshot_id.clone())
            }).collect()
        })
    }
    pub fn commit_changed_paths(&mut self, id: &CommitId, source: &Source) -> Option<Vec<Path>> {
        self.smart_load_commit_changes(source).get(id).pirate().map(|ids| {
            ids.iter().flat_map(|change| self.path(&change.0/*path_id()*/, source)).collect()
        })
    }
    pub fn commit_change_count(&mut self, id: &CommitId, source: &Source) -> Option<usize> {
        self.smart_load_commit_change_count(source).get(id).pirate()
    }
    pub fn commit_changed_path_count(&mut self, id: &CommitId, source: &Source) -> Option<usize> {
        self.smart_load_commit_change_count(source).get(id).pirate()
    }
    pub fn commit_projects(&mut self, id: &CommitId, source: &Source) -> Option<Vec<Project>> {
        self.smart_load_commit_projects(source).get(id).pirate().map(|ids| {
            ids.iter().flat_map(|id| self.project(id, source)).collect()
        })   
    }
    pub fn commit_projects_count(&mut self, id: &CommitId, source: &Source) -> Option<usize> {
        self.smart_load_commit_projects_count(source).get(id).pirate()
    }
    pub fn commit_languages(&mut self, id: &CommitId, source: &Source) -> Option<Vec<Language>> {
        self.smart_load_commit_languages(source).get(id).pirate()   
    }
    pub fn commit_languages_count(&mut self, id: &CommitId, source: &Source) -> Option<usize> {
        self.smart_load_commit_languages_count(source).get(id).pirate()
    }
    pub fn user_committed_commit_ids(&mut self, id: &UserId, source: &Source) -> Option<Vec<CommitId>> {
        self.smart_load_user_committed_commits(source).get(id).pirate()
    }
    pub fn user_authored_commits(&mut self, id: &UserId, source: &Source) -> Option<Vec<Commit>> {
        self.smart_load_user_authored_commits(source).get(id).pirate().map(|ids| {
            ids.iter().flat_map(|id| self.commit(id, source)).collect()
        })
    }
    pub fn user_authored_commit_ids(&mut self, id: &UserId, source: &Source) -> Option<Vec<CommitId>> {
        self.smart_load_user_authored_commits(source).get(id).pirate()
    }
    pub fn user_committed_experience(&mut self, id: &UserId, source: &Source) -> Option<Duration> {
        self.smart_load_user_committer_experience(source)
            .get(id)
            .map(|seconds| Duration::from(*seconds))
    }
    pub fn user_author_experience(&mut self, id: &UserId, source: &Source) -> Option<Duration> {
        self.smart_load_user_author_experience(source)
            .get(id)
            .map(|seconds| Duration::from(*seconds))
    }
    pub fn user_experience(&mut self, id: &UserId, source: &Source) -> Option<Duration> {
        self.smart_load_user_experience(source)
            .get(id)
            .map(|seconds| Duration::from(*seconds))
    }
    pub fn user_committed_commit_count(&mut self, id: &UserId, source: &Source) -> Option<usize> {
        self.smart_load_user_committed_commit_count(source).get(id).pirate()
    }
    pub fn user_authored_commit_count(&mut self, id: &UserId, source: &Source) -> Option<usize> {
        self.smart_load_user_authored_commit_count(source).get(id).pirate()
    }
    pub fn user_committed_commits(&mut self, id: &UserId, source: &Source) -> Option<Vec<Commit>> {
        self.smart_load_user_committed_commits(source).get(id).pirate().map(|ids| {
            ids.iter().flat_map(|id| self.commit(id, source)).collect()
        })
    }
    pub fn developer_experience(&mut self, id: &UserId, source: &Source) -> Option<i32> {
        self.smart_load_developer_experience(source).get(id).pirate()
    }
    pub fn project_max_commit_delta(&mut self, id: &ProjectId, source: &Source) -> Option<i64> {
        self.smart_load_project_max_commit_delta(source).get(id).pirate()
    }
    pub fn project_max_experience(&mut self, id: &ProjectId, source: &Source) -> Option<i32> {
        self.smart_load_project_max_experience(source).get(id).pirate()
    }
    pub fn project_experience(&mut self, id: &ProjectId, source: &Source) -> Option<f64> {
        self.smart_load_project_experience(source).get(id).pirate()
    }
    pub fn project_avg_commit_delta(&mut self, id: &ProjectId, source: &Source) -> Option<i64> {
        self.smart_load_project_avg_commit_delta(source).get(id).pirate()
    }
    pub fn project_time_since_last_commit(&mut self, id: &ProjectId, source: &Source) -> Option<i64> {
        self.smart_load_project_time_since_last_commit(source).get(id).pirate()
    }
    pub fn project_time_since_first_commit(&mut self, id: &ProjectId, source: &Source) -> Option<i64> {
        self.smart_load_project_time_since_first_commit(source).get(id).pirate()
    }
    pub fn project_is_abandoned(&mut self, id: &ProjectId, source: &Source) -> Option<bool> {
        self.smart_load_project_is_abandoned(source).get(id).pirate()
    }
    pub fn snapshot_locs(&mut self, id: &SnapshotId, source: &Source) -> Option<usize> {
        self.smart_load_snapshot_locs(source).get(id).pirate()
    }
    pub fn project_locs(&mut self, id: &ProjectId, source: &Source) -> Option<usize> {
        self.smart_load_project_locs(source).get(id).pirate()
    }
    pub fn project_logs(&mut self, id: &ProjectId, source: &Source) -> Option<i64> {
        self.smart_load_project_logs(source).get(id).pirate()
    }
    pub fn project_duplicated_code(&mut self, id: &ProjectId, source: &Source) -> Option<f64> {
        self.smart_load_project_duplicated_code(source).get(id).pirate()
    }
    pub fn snapshot_unique_projects(&mut self, id : &SnapshotId, source: &Source) -> usize {
        // TODO I am sure rust frowns upon this, but how do I return ! attributes that are cached in the datastore? 
        self.smart_load_snapshot_projects(source).get(id).unwrap().0
    }
    pub fn snapshot_original_project(&mut self, id : &SnapshotId, source: &Source) -> ProjectId {
        // TODO I am sure rust frowns upon this, but how do I return ! attributes that are cached in the datastore? 
        self.smart_load_snapshot_projects(source).get(id).unwrap().1
    }
    pub fn project_is_valid(&mut self, id : &ProjectId, source: &Source) ->  Option<bool>{
        // TODO I am sure rust frowns upon this, but how do I return ! attributes that are cached in the datastore? 
        self.smart_load_project_is_valid(source).get(id).pirate()
    }
}

macro_rules! load_from_source {
    ($self:ident, $vector:ident, $source:expr)  => {{
        if !$self.$vector.is_loaded() {
            $self.$vector.load_from_source($source);
        }
        $self.$vector.grab_collection()
    }}
}

macro_rules! load_from_metadata {
    ($self:ident, $vector:ident, $source:expr)  => {{
        if !$self.$vector.is_loaded() {
            $self.$vector.load_from_metadata($source, &$self.project_metadata);
        }
        $self.$vector.grab_collection()
    }}
}

macro_rules! load_with_prerequisites {
    ($self:ident, $vector:ident, $source:expr, $n:ident, $($prereq:ident),*)  => {{
        mashup! {
            $( m["smart_load" $prereq] = smart_load_$prereq; )*
               m["load"] = load_from_$n;
        }
        if !$self.$vector.is_loaded() {
            m! { $(  $self."smart_load" $prereq($source); )*              }
            m! { $self.$vector."load"($source, $($self.$prereq.grab_collection()), *); }
        }
        $self.$vector.grab_collection()
    }}
}

impl Data {
    fn smart_load_project_substore(&mut self, source: &Source) -> &BTreeMap<ProjectId, Store> {
        load_from_source!(self, project_substores, source)
    }
    fn smart_load_project_urls(&mut self, source: &Source) -> &BTreeMap<ProjectId, String> {
        load_from_source!(self, project_urls, source)
    }
    fn smart_load_project_heads(&mut self, source: &Source) -> &BTreeMap<ProjectId, Vec<Head>> {
        load_from_source!(self, project_heads, source)
    }
    fn smart_load_project_users(&mut self, source: &Source) -> &BTreeMap<ProjectId, Vec<UserId>> {
        load_with_prerequisites!(self, project_users, source, two, project_authors, project_committers)
    }
    fn smart_load_project_authors(&mut self, source: &Source) -> &BTreeMap<ProjectId, Vec<UserId>> {
        load_with_prerequisites!(self, project_authors, source, two, project_commits, commits)
    }
    fn smart_load_project_committers(&mut self, source: &Source) -> &BTreeMap<ProjectId, Vec<UserId>> {
        load_with_prerequisites!(self, project_committers, source, two, project_commits, commits)
    }
    fn smart_load_project_commits(&mut self, source: &Source) -> &BTreeMap<ProjectId, Vec<CommitId>> {
        load_with_prerequisites!(self, project_commits, source, two, project_heads, commits)
    }
    fn smart_load_project_paths(&mut self, source: &Source) -> &BTreeMap<ProjectId, Vec<PathId>> {
        load_with_prerequisites!(self, project_paths, source, two, project_commits, commit_changes)
    }
    fn smart_load_project_snapshots(&mut self, source: &Source) -> &BTreeMap<ProjectId, Vec<SnapshotId>> {
        load_with_prerequisites!(self, project_snapshots, source, two, project_commits, commit_changes)
    }
    fn smart_load_project_user_count(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_user_count, source, one, project_users)
    }
    fn smart_load_project_author_count(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_author_count, source, one, project_authors)
    }
    fn smart_load_project_path_count(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_path_count, source, one, project_paths)
    }
    fn smart_load_project_snapshot_count(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_snapshot_count, source, one, project_snapshots)
    }
    fn smart_load_project_committer_count(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_committer_count, source, one, project_committers)
    }
    fn smart_load_project_commit_count(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_commit_count, source, one, project_commits)
    }
    fn smart_load_project_lifetimes(&mut self, source: &Source) -> &BTreeMap<ProjectId, u64> {
        load_with_prerequisites!(self, project_lifetimes, source, three, project_commits,
                                                                        commit_author_timestamps,
                                                                        commit_committer_timestamps)
    }
    fn smart_load_project_unique_files(& mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_unique_files, source, three, project_commits, commit_changes, snapshot_projects)
    }
    fn smart_load_project_original_files(& mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_original_files, source, three, project_commits, commit_changes, snapshot_projects)
    }
    fn smart_load_project_impact(& mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_impact, source, three, project_commits, commit_changes, snapshot_projects)
    }
    fn smart_load_project_files(& mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_files, source, two, project_commits, commit_changes)
    }
    fn smart_load_project_languages(& mut self, source: &Source) -> &BTreeMap<ProjectId, Vec<(Language,usize)>> {
        load_with_prerequisites!(self, project_languages, source, three, project_commits, commit_changes, paths)
    }
    fn smart_load_project_languages_count(& mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_languages_count, source, one, project_languages)
    }
    fn smart_load_project_major_language(& mut self, source: &Source) -> &BTreeMap<ProjectId, Language> {
        load_with_prerequisites!(self, project_major_language, source, one, project_languages)
    }
    fn smart_load_project_major_language_ratio(& mut self, source: &Source) -> &BTreeMap<ProjectId, f64> {
        load_with_prerequisites!(self, project_major_language_ratio, source, one, project_languages)
    }
    fn smart_load_project_major_language_changes(& mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_major_language_changes, source, one, project_languages)
    }
    fn smart_load_project_all_forks(& mut self, source: &Source) -> &BTreeMap<ProjectId, Vec<ProjectId>> {
        load_with_prerequisites!(self, project_all_forks, source, three, project_commits, commit_projects, project_created)
    }
    fn smart_load_project_all_forks_count(& mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_all_forks_count, source, one, project_all_forks)
    }
    fn smart_load_project_head_trees(& mut self, source: &Source) -> &BTreeMap<ProjectId, Vec<(String, Vec<(PathId, SnapshotId)>)>> {
        load_with_prerequisites!(self, project_head_trees, source, three, project_heads, commits, commit_changes)
    }
    fn smart_load_project_head_trees_count(& mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_head_trees_count, source, one, project_head_trees)
    }
    fn smart_load_users(&mut self, source: &Source) -> &BTreeMap<UserId, User> {
        load_from_source!(self, users, source)
    }
    fn smart_load_user_authored_commits(&mut self, source: &Source) -> &BTreeMap<UserId, Vec<CommitId>> {
        load_with_prerequisites!(self, user_authored_commits, source, one, commits)
    }
    fn smart_load_user_committed_commits(&mut self, source: &Source) -> &BTreeMap<UserId, Vec<CommitId>> {
        load_with_prerequisites!(self, user_committed_commits, source, one, commits)
    }
    fn smart_load_user_author_experience(&mut self, source: &Source) -> &BTreeMap<UserId, u64> {
        load_with_prerequisites!(self, user_author_experience, source, two, user_authored_commits,
                                                                           commit_author_timestamps)
    }
    fn smart_load_user_committer_experience(&mut self, source: &Source) -> &BTreeMap<UserId, u64> {
        load_with_prerequisites!(self, user_committer_experience, source, two, user_committed_commits,
                                                                              commit_committer_timestamps)
    }
    fn smart_load_user_experience(&mut self, source: &Source) -> &BTreeMap<UserId, u64> {
        load_with_prerequisites!(self, user_experience, source, three, user_committed_commits,
                                                                      commit_author_timestamps,
                                                                      commit_committer_timestamps)
    }
    fn smart_load_user_committed_commit_count(&mut self, source: &Source) -> &BTreeMap<UserId, usize> {
        load_with_prerequisites!(self, user_committed_commit_count, source, one, user_committed_commits)
    }
    fn smart_load_user_authored_commit_count(&mut self, source: &Source) -> &BTreeMap<UserId, usize> {
        load_with_prerequisites!(self, user_authored_commit_count, source, one, user_authored_commits)
    }
    fn smart_load_developer_experience(&mut self, source: &Source) -> &BTreeMap<UserId, i32> {
        load_with_prerequisites!(self, developer_experience, source, two, user_authored_commits, commit_author_timestamps)
    }
    fn smart_load_paths(&mut self, source: &Source) -> &BTreeMap<PathId, Path> {
        load_from_source!(self, paths, source)
    }
    // fn smart_load_snapshots(&mut self, source: &DataSource) -> &BTreeMap<SnapshotId, Snapshot> {
    //     load_from_source!(self, snapshots, source)
    // }
    fn smart_load_commits(&mut self, source: &Source) -> &BTreeMap<CommitId, Commit> {
        load_from_source!(self, commits, source)
    }
    fn smart_load_commit_hashes(&mut self, source: &Source) -> &BTreeMap<CommitId, String> {
        load_from_source!(self, commit_hashes, source)
    }
    fn smart_load_commit_messages(&mut self, source: &Source) -> &BTreeMap<CommitId, String> {
        load_from_source!(self, commit_messages, source)
    }
    fn smart_load_commit_committer_timestamps(&mut self, source: &Source) -> &BTreeMap<CommitId, Timestamp> {
        load_from_source!(self, commit_committer_timestamps, source)
    }
    fn smart_load_commit_author_timestamps(&mut self, source: &Source) -> &BTreeMap<CommitId, Timestamp> {
        load_from_source!(self, commit_author_timestamps, source)
    }
    fn smart_load_commit_changes(&mut self, source: &Source) -> &BTreeMap<CommitId, Vec<ChangeTuple>> {
        load_from_source!(self, commit_changes, source)
    }
    fn smart_load_commit_change_count(&mut self, source: &Source) -> &BTreeMap<CommitId, usize> {
        load_with_prerequisites!(self, commit_change_count, source, one, commit_changes)
    }
    fn smart_load_project_max_commit_delta(&mut self, source: &Source) -> &BTreeMap<ProjectId, i64> {
        load_with_prerequisites!(self, project_max_commit_delta, source, two, project_commits, commit_committer_timestamps)
    }
    fn smart_load_project_max_experience(&mut self, source: &Source) -> &BTreeMap<ProjectId, i32> {
        load_with_prerequisites!(self, project_max_experience, source, two, project_authors, developer_experience)
    }
    fn smart_load_project_experience(&mut self, source: &Source) -> &BTreeMap<ProjectId, f64> {
        load_with_prerequisites!(self, project_experience, source, three, developer_experience, project_commits, commits)
    }
    fn smart_load_project_avg_commit_delta(&mut self, source: &Source) -> &BTreeMap<ProjectId, i64> {
        load_with_prerequisites!(self, avg_commit_delta, source, two, project_commits, commit_committer_timestamps)
    }
    fn smart_load_project_time_since_last_commit(&mut self, source: &Source) -> &BTreeMap<ProjectId, i64> {
        load_with_prerequisites!(self, project_time_since_last_commit, source, three, project_commits, commit_committer_timestamps, project_logs)
    }
    fn smart_load_project_time_since_first_commit(&mut self, source: &Source) -> &BTreeMap<ProjectId, i64> {
        load_with_prerequisites!(self, project_time_since_first_commit, source, three, project_commits, commit_committer_timestamps, project_logs)
    }
    fn smart_load_project_is_abandoned(&mut self, source: &Source) -> &BTreeMap<ProjectId, bool> {
        load_with_prerequisites!(self, is_abandoned, source, two, project_max_commit_delta, project_time_since_last_commit)
    }
    fn smart_load_snapshot_locs(&mut self, source: &Source) -> &BTreeMap<SnapshotId, usize> {
        load_from_source!(self, snapshot_locs, source)
        //load_with_prerequisites!(self, is_abandoned, source, one, project_snapshots)
    }
    fn smart_load_project_locs(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_with_prerequisites!(self, project_locs, source, three, project_head_trees,  project_default_branch, snapshot_locs)
    }
    fn smart_load_project_duplicated_code(&mut self, source: &Source) -> &BTreeMap<ProjectId, f64> {
        load_with_prerequisites!(self, duplicated_code, source, three, project_commits,  commit_changes, snapshot_projects)
    }
    fn smart_load_project_logs(&mut self, source: &Source) -> &BTreeMap<ProjectId, i64> {
        load_with_prerequisites!(self, project_logs, source, one, project_is_valid)
    }
    fn smart_load_commit_languages(&mut self, source: &Source) -> &BTreeMap<CommitId, Vec<Language>> {
        load_with_prerequisites!(self, commit_languages, source, two, commit_changes, paths)
    }
    fn smart_load_commit_languages_count(&mut self, source: &Source) -> &BTreeMap<CommitId, usize> {
        load_with_prerequisites!(self, commit_languages_count, source, one, commit_languages)
    }
    fn smart_load_commit_projects(&mut self, source: &Source) -> &BTreeMap<CommitId, Vec<ProjectId>> {
        load_with_prerequisites!(self, commit_projects, source, one, project_commits)
    }
    fn smart_load_commit_projects_count(&mut self, source: &Source) -> &BTreeMap<CommitId, usize> {
        load_with_prerequisites!(self, commit_projects_count, source, one, commit_projects)
    }
    fn smart_load_snapshot_projects(& mut self, source: &Source) -> &BTreeMap<SnapshotId,(usize, ProjectId)> {
        load_with_prerequisites!(self, snapshot_projects, source, four, commit_changes, commit_projects, commit_author_timestamps, project_created)
    }
    pub fn smart_load_project_change_contributions(&mut self, source: &Source) -> &BTreeMap<ProjectId, Vec<(UserId, usize)>> {
        load_with_prerequisites!(self, project_change_contributions, source, three, project_commits, commits, commit_changes)
    }
    pub fn smart_load_project_commit_contributions(&mut self, source: &Source) -> &BTreeMap<ProjectId, Vec<(UserId, usize)>> {
        load_with_prerequisites!(self, project_commit_contributions, source, two, project_commits, commits)
    }
    pub fn smart_load_project_cumulative_change_contributions(&mut self, source: &Source) -> &BTreeMap<ProjectId, Vec<Percentage>> {
        load_with_prerequisites!(self, project_cumulative_change_contributions, source, one, project_change_contributions)
    }
    pub fn smart_load_project_cumulative_commit_contributions(&mut self, source: &Source) -> &BTreeMap<ProjectId, Vec<Percentage>> {
        load_with_prerequisites!(self, project_cumulative_commit_contributions, source, one, project_commit_contributions)
    }

    pub fn smart_load_project_issues(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_from_metadata!(self, project_issue_count, source)
    }
    pub fn smart_load_project_buggy_issues(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_from_metadata!(self, project_buggy_issue_count, source)
    }
    pub fn smart_load_project_is_fork(&mut self, source: &Source) -> &BTreeMap<ProjectId, bool> {
        load_from_metadata!(self, project_is_fork, source)
    }
    pub fn smart_load_project_is_archived(&mut self, source: &Source) -> &BTreeMap<ProjectId, bool> {
        load_from_metadata!(self, project_is_archived, source)
    }
    pub fn smart_load_project_is_disabled(&mut self, source: &Source) -> &BTreeMap<ProjectId, bool> {
        load_from_metadata!(self, project_is_disabled, source)
    }
    pub fn smart_load_project_star_gazer_count(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_from_metadata!(self, project_star_gazer_count, source)
    }
    pub fn smart_load_project_watcher_count(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_from_metadata!(self, project_watcher_count, source)
    }
    pub fn smart_load_project_size(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_from_metadata!(self, project_project_size, source)
    }
    pub fn smart_load_project_open_issue_count(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_from_metadata!(self, project_open_issue_count, source)
    }
    pub fn smart_load_project_fork_count(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_from_metadata!(self, project_fork_count, source)
    }
    pub fn smart_load_project_subscriber_count(&mut self, source: &Source) -> &BTreeMap<ProjectId, usize> {
        load_from_metadata!(self, project_subscriber_count, source)
    }
    pub fn smart_load_project_license(&mut self, source: &Source) -> &BTreeMap<ProjectId, String> {
        load_from_metadata!(self, project_license, source)
    }
    pub fn smart_load_project_language(&mut self, source: &Source) -> &BTreeMap<ProjectId, Language> {
        load_from_metadata!(self, project_language, source)
    }
    pub fn smart_load_project_description(&mut self, source: &Source) -> &BTreeMap<ProjectId, String> {
        load_from_metadata!(self, project_description, source)
    }
    pub fn smart_load_project_homepage(&mut self, source: &Source) -> &BTreeMap<ProjectId, String> {
        load_from_metadata!(self, project_homepage, source)
    }
    pub fn smart_load_project_has_issues(&mut self, source: &Source) -> &BTreeMap<ProjectId, bool> {
        load_from_metadata!(self, project_has_issues, source)
    }
    pub fn smart_load_project_has_downloads(&mut self, source: &Source) -> &BTreeMap<ProjectId, bool> {
        load_from_metadata!(self, project_has_downloads, source)
    }
    pub fn smart_load_project_has_wiki(&mut self, source: &Source) -> &BTreeMap<ProjectId, bool> {
        load_from_metadata!(self, project_has_wiki, source)
    }
    pub fn smart_load_project_has_pages(&mut self, source: &Source) -> &BTreeMap<ProjectId, bool> {
        load_from_metadata!(self, project_has_pages, source)
    }
    fn smart_load_project_created(&mut self, source: &Source) -> &BTreeMap<ProjectId, Timestamp> {
        load_from_metadata!(self, project_created, source)
    }
    pub fn smart_load_project_updated(&mut self, source: &Source) -> &BTreeMap<ProjectId, Timestamp> {
        load_from_metadata!(self, project_updated, source)
    }
    pub fn smart_load_project_pushed(&mut self, source: &Source) -> &BTreeMap<ProjectId, Timestamp> {
        load_from_metadata!(self, project_pushed, source)
    }
    pub fn smart_load_project_default_branch(&mut self, source: &Source) -> &BTreeMap<ProjectId, String> {
        load_from_metadata!(self, project_default_branch, source)
    }
    pub fn smart_load_project_is_valid(&mut self, source: &Source) -> &BTreeMap<ProjectId, bool> {
        load_from_source!(self, project_is_valid, source)
        
    }
}

impl Data {
    pub fn export_to_csv(&mut self, _dir: impl Into<String>, _: &Source) -> Result<(), std::io::Error> {
        // let dir = dir.into();
        // std::fs::create_dir_all(&dir)?;
        // macro_rules! path {
        //     ($filename:expr) => {
        //         format!("{}/{}.csv", dir, $filename)
        //     }
        // }

        // self.project_metadata.iter(source).into_csv(path!("project_metadata"))?;
        // FIXME add 
        todo!()

        // self.smart_load_project_urls(source).iter().into_csv(path!("project_urls"))?;
        // self.smart_load_project_heads(source).iter().into_csv(path!("project_heads"))?;
        // self.smart_load_users(source).iter().into_csv(path!("users"))?;
        // self.smart_load_paths(source).iter().into_csv(path!("paths"))?;
        // self.smart_load_commits(source).iter().into_csv(path!("commits"))?;
        // self.smart_load_commit_hashes(source).iter().into_csv(path!("commit_hashes"))?;
        // self.smart_load_commit_messages(source).iter().into_csv(path!("commit_messages"))?;
        // self.smart_load_commit_committer_timestamps(source).iter().into_csv(path!("commit_committer_timestamps"))?;
        // self.smart_load_commit_author_timestamps(source).iter().into_csv(path!("commit_author_timestamps"))?;
        // self.smart_load_commit_changes(source).iter().into_csv(path!("commit_changes"))?;

        // source.snapshot_bytes()
        //      .map(|(id, content)| {
        //          Snapshot::new(id, content)
        //      }).into_csv(path!("snapshots"))?;

        // Ok(())
    }
}