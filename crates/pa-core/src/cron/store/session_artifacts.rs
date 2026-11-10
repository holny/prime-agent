//! Session-artifact partitioning: registration of per-session scheduled-jobs
//! artifact files, recovery of a session's jobs from its artifact partition,
//! and the partitioned merge-write that keeps each artifact file holding only
//! its session's jobs and dispatches.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use super::state::{
    merge_fresh_jobs, read_jobs_state, recover_interrupted_in_state, with_state_locks,
    write_jobs_state,
};
use super::{
    AgentCronDispatchRecord, AgentCronJobStore, CronJobsState, SESSION_SCHEDULED_JOBS_FILENAME,
};
use crate::cron::AgentCronJob;

impl AgentCronJobStore {
    /// Register one session's artifact partition. Idempotent: re-registering
    /// the same directory is a no-op, so call sites can bind on every read.
    pub fn register_session_artifact(&self, session_id: &str, artifact_dir: &Path) -> bool {
        if !self.session_artifact_mode {
            return false;
        }
        let path = artifact_dir.join(SESSION_SCHEDULED_JOBS_FILENAME);
        let mut files = self
            .session_artifact_files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if files.get(session_id) == Some(&path) {
            return false;
        }
        files.insert(session_id.to_string(), path);
        // A fresh partition (a re-bind can carry existing jobs on disk)
        // joins the answer set: the next gate refresh must read it.
        self.mark_gate_index_stale();
        true
    }

    /// Drop one session's artifact registration — the #3864 fix for the
    /// register-only map. Safe when the session is gone for good (the
    /// deleted-session path calls it once the partition file went away
    /// with the session); a later `register_session_artifact` re-binds
    /// idempotently. Returns whether a registration existed.
    pub fn unregister_session_artifact(&self, session_id: &str) -> bool {
        if !self.session_artifact_mode {
            return false;
        }
        let Some(path) = self
            .session_artifact_files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id)
        else {
            return false;
        };
        self.state_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&path);
        self.mark_gate_index_stale();
        true
    }

    pub fn session_artifacts(&self) -> Vec<(String, PathBuf)> {
        let files = self
            .session_artifact_files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        files.clone().into_iter().collect()
    }

    /// # Errors
    ///
    /// Returns an error when the state lock or write fails.
    pub fn recover_session_artifact(
        &self,
        session_id: &str,
        now: u64,
    ) -> anyhow::Result<Vec<AgentCronJob>> {
        let Some(path) = self
            .session_artifacts()
            .into_iter()
            .find(|(id, _)| id == session_id)
            .map(|(_, path)| path)
        else {
            return Ok(Vec::new());
        };
        with_state_locks(
            std::slice::from_ref(&path),
            || -> anyhow::Result<Vec<AgentCronJob>> {
                let mut state = read_jobs_state(&path);
                let mut recovered = Vec::new();
                if !state.dispatches.is_empty() {
                    recover_interrupted_in_state(&mut state, now, &mut recovered, None);
                    write_jobs_state(&path, &state)?;
                    self.invalidate_cached_state(&path);
                }
                Ok(recovered)
            },
        )
        .ok_or_else(|| anyhow::anyhow!("cron jobs state lock not acquired; skipped"))?
    }
    pub(crate) fn write_jobs_session_artifacts(&self, jobs: &[AgentCronJob]) -> anyhow::Result<()> {
        let artifact_files = self
            .session_artifact_files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let registered: std::collections::HashSet<String> =
            artifact_files.keys().cloned().collect();
        for job in jobs {
            if !registered.contains(&job.session_id) {
                anyhow::bail!(
                    "cron job session artifact not registered: {}",
                    job.session_id
                );
            }
        }
        let paths: Vec<PathBuf> = artifact_files.values().cloned().collect();
        let with_locks = with_state_locks(&paths, || -> anyhow::Result<()> {
            let current_by_session_id: HashMap<String, CronJobsState> = artifact_files
                .iter()
                .map(|(session_id, path)| (session_id.clone(), read_jobs_state(path)))
                .collect();
            let incoming_by_id: HashMap<&str, &AgentCronJob> =
                jobs.iter().map(|job| (job.id.as_str(), job)).collect();
            let mut merged_by_session_id: HashMap<String, Vec<AgentCronJob>> = HashMap::new();
            for (session_id, current) in &current_by_session_id {
                let retained: Vec<AgentCronJob> = current
                    .jobs
                    .iter()
                    .filter(|job| {
                        incoming_by_id
                            .get(job.id.as_str())
                            .is_none_or(|incoming| incoming.session_id == *session_id)
                    })
                    .cloned()
                    .collect();
                let session_jobs: Vec<AgentCronJob> = jobs
                    .iter()
                    .filter(|job| job.session_id == *session_id)
                    .cloned()
                    .collect();
                merged_by_session_id
                    .insert(session_id.clone(), merge_fresh_jobs(retained, session_jobs));
            }
            let session_id_by_job_id: HashMap<String, String> = merged_by_session_id
                .iter()
                .flat_map(|(session_id, session_jobs)| {
                    session_jobs
                        .iter()
                        .map(move |job| (job.id.clone(), session_id.clone()))
                })
                .collect();
            let all_dispatches: Vec<AgentCronDispatchRecord> = current_by_session_id
                .values()
                .flat_map(|state| state.dispatches.clone())
                .collect();
            for (session_id, path) in &artifact_files {
                let current = current_by_session_id
                    .get(session_id)
                    .cloned()
                    .unwrap_or_default();
                let next_state = CronJobsState {
                    jobs: merged_by_session_id
                        .get(session_id)
                        .cloned()
                        .unwrap_or_default(),
                    dispatches: all_dispatches
                        .iter()
                        .filter(|dispatch| {
                            session_id_by_job_id.get(&dispatch.job_id) == Some(session_id)
                        })
                        .cloned()
                        .collect(),
                };
                if serde_json::to_string(&current).unwrap_or_default()
                    != serde_json::to_string(&next_state).unwrap_or_default()
                {
                    write_jobs_state(path, &next_state)?;
                    self.invalidate_cached_state(path);
                }
            }
            Ok(())
        });
        with_locks.ok_or_else(|| anyhow::anyhow!("cron jobs state lock not acquired; skipped"))?
    }
}

/// Read one scheduled-jobs artifact file's job rows. A missing or
/// unparseable file reads as no jobs.
#[must_use]
pub fn read_scheduled_jobs_artifact(path: &Path) -> Vec<AgentCronJob> {
    read_jobs_state(path).jobs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cron::store::{input, CancelJobsFilter, SessionBinding};

    /// The release gate's answer set (#3864): create populates it, a
    /// rebind moves it to the new active session id, cancel empties it.
    #[test]
    fn the_release_gate_index_tracks_create_rebind_and_cancel() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = AgentCronJobStore::for_session_artifacts();
        let artifacts = dir.path().join("a");
        std::fs::create_dir_all(&artifacts).unwrap();
        store.register_session_artifact("session-1", &artifacts);
        let now = 1_700_000_000_000;
        store.create(&input("gate job", "every 10m", now)).unwrap();
        // The input's active session is "live-1".
        assert!(store.has_active_or_paused_job_for("live-1"));
        assert!(!store.has_active_or_paused_job_for("session-1"));
        // A rebind moves the job to the new active session id even though
        // the partition stays keyed by the original session id.
        store
            .rebind_session_jobs(&SessionBinding {
                active_session_id: "live-2".to_string(),
                session_id: "session-1".to_string(),
                session_file: "/w/session.jsonl".to_string(),
                cwd: "/w".to_string(),
            })
            .unwrap();
        assert!(!store.has_active_or_paused_job_for("live-1"));
        assert!(store.has_active_or_paused_job_for("live-2"));
        // A cancelled job no longer blocks passivation.
        store
            .cancel_jobs_for_session(
                &CancelJobsFilter {
                    session_file: Some("/w/session.jsonl".to_string()),
                    ..Default::default()
                },
                now + 1,
            )
            .unwrap();
        assert!(!store.has_active_or_paused_job_for("live-2"));
    }

    /// The stat-keyed cache (#3864): a warm read populates it, and an
    /// external partition edit (mtime and len move) is still visible.
    #[test]
    fn external_partition_edits_invalidate_the_stat_cache() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = AgentCronJobStore::for_session_artifacts();
        let artifacts = dir.path().join("a");
        std::fs::create_dir_all(&artifacts).unwrap();
        store.register_session_artifact("session-1", &artifacts);
        let now = 1_700_000_000_000;
        store.create(&input("cache job", "every 10m", now)).unwrap();
        assert_eq!(store.list().len(), 1);
        let path = artifacts.join(SESSION_SCHEDULED_JOBS_FILENAME);
        {
            let cache = store
                .state_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(
                cache.contains_key(&path),
                "a read through the partition must warm the stat cache"
            );
        }
        std::fs::write(&path, "{\"jobs\":[],\"dispatches\":[]}").unwrap();
        assert!(
            store.list().is_empty(),
            "an external edit must invalidate the cached parse"
        );
        std::fs::remove_file(&path).unwrap();
        assert!(store.list().is_empty());
    }

    /// Unregistration (#3864): drops the partition and its cached parse,
    /// is idempotent, and a later bind re-registers and works.
    #[test]
    fn unregister_drops_the_partition_and_a_later_bind_rebinds() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = AgentCronJobStore::for_session_artifacts();
        let artifacts = dir.path().join("a");
        std::fs::create_dir_all(&artifacts).unwrap();
        store.register_session_artifact("session-1", &artifacts);
        assert!(store.unregister_session_artifact("session-1"));
        assert!(!store.unregister_session_artifact("session-1"));
        assert!(store.session_artifacts().is_empty());
        assert!(!store.has_active_or_paused_job_for("live-1"));
        assert!(store.register_session_artifact("session-1", &artifacts));
        let now = 1_700_000_000_000;
        store
            .create(&input("back again", "every 10m", now))
            .unwrap();
        assert!(store.has_active_or_paused_job_for("live-1"));
    }

    #[test]
    fn session_artifact_partitioning() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = AgentCronJobStore::for_session_artifacts();
        let artifacts_a = dir.path().join("a");
        let artifacts_b = dir.path().join("b");
        std::fs::create_dir_all(&artifacts_a).unwrap();
        std::fs::create_dir_all(&artifacts_b).unwrap();
        assert!(store.register_session_artifact("session-1", &artifacts_a));
        assert!(!store.register_session_artifact("session-1", &artifacts_a)); // idempotent
        assert!(store.register_session_artifact("session-2", &artifacts_b));
        let now = 1_700_000_000_000;
        store.create(&input("job a", "every 10m", now)).unwrap();
        let mut job_b = input("job b", "every 20m", now);
        job_b.session_id = "session-2".to_string();
        store.create(&job_b).unwrap();
        let jobs = store.list();
        assert_eq!(jobs.len(), 2);
        let state_a = read_jobs_state(&artifacts_a.join(SESSION_SCHEDULED_JOBS_FILENAME));
        assert_eq!(state_a.jobs.len(), 1);
        assert_eq!(state_a.jobs[0].prompt, "job a");
        let state_b = read_jobs_state(&artifacts_b.join(SESSION_SCHEDULED_JOBS_FILENAME));
        assert_eq!(state_b.jobs.len(), 1);
        let rebound = store
            .rebind_session_jobs(&SessionBinding {
                active_session_id: "live-2".to_string(),
                session_id: "session-1".to_string(),
                session_file: "/w/session.jsonl".to_string(),
                cwd: "/w".to_string(),
            })
            .unwrap();
        assert_eq!(rebound.len(), 2);
        assert!(rebound.iter().all(|job| job.active_session_id == "live-2"));
        let cancelled = store
            .cancel_jobs_for_session(
                &CancelJobsFilter {
                    session_file: Some("/w/session.jsonl".to_string()),
                    ..Default::default()
                },
                now + 1,
            )
            .unwrap();
        // Both jobs share the session file (the rebind moved both), so both cancel.
        assert_eq!(cancelled.len(), 2);
    }
}
