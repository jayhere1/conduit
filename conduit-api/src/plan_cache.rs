//! In-process cache for the compiled `ConduitPlan` plus the stitched
//! [`CrossTaskLineage`] for each DAG.
//!
//! ## Why
//!
//! Several API handlers — most notably the unified dataset view at
//! `GET /api/v1/lineage/datasets/:ns/:name/unified` — need both the
//! compiled plan and the cross-task stitched graph. Compiling and
//! stitching on every request is fine for a handful of DAGs but
//! degrades quickly as the catalog grows: tree-sitter parsing, YAML
//! parsing, SQL I/O inference, dependency resolution, and per-DAG
//! stitching all run.
//!
//! ## Cache semantics
//!
//! - **Signature-keyed.** The cache computes a signature from a walk
//!   of the DAGs directory: `(relative_path, mtime_nanos, size)` for
//!   every file, plus the directory's own mtime. Any add/remove/edit
//!   of a DAG source file changes the signature and forces a
//!   recompile on the next request.
//! - **Optional TTL ceiling.** A configurable `refresh_interval`
//!   forces a re-stat after that duration even when no edits happened,
//!   so a clock-skew or filesystem-metadata bug can't pin us to a
//!   stale plan forever. Setting it to `Duration::ZERO` (the default)
//!   means "always re-check the signature."
//! - **Double-checked locking on miss.** A read lock fast-paths the
//!   common case; on a miss we drop it, take a write lock, then
//!   re-check. Concurrent first-request bursts compile exactly once.
//!
//! The cache returns `Arc`s so handlers don't pay for clones.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};

use conduit_common::dag::DagId;
use conduit_compiler::ConduitPlan;
use conduit_lineage::cross_task::{self, CrossTaskLineage};
use tracing::{debug, info, warn};

/// Read-only view returned by [`PlanCache::get`]. Both fields are
/// shared via `Arc` so handlers can keep references across `await`
/// points without holding the cache lock.
pub struct CachedPlanView {
    pub plan: Arc<ConduitPlan>,
    /// Per-DAG stitched lineage. Entries are present for every DAG
    /// that stitched successfully; strict-mode failures or otherwise-
    /// unstitchable DAGs are simply absent (a warning was logged at
    /// build time).
    pub stitched: Arc<HashMap<DagId, Arc<CrossTaskLineage>>>,
}

impl CachedPlanView {
    pub fn stitched_for(&self, dag_id: &str) -> Option<Arc<CrossTaskLineage>> {
        self.stitched.get(dag_id).cloned()
    }
}

struct Entry {
    signature: u64,
    cached_at: Instant,
    plan: Arc<ConduitPlan>,
    stitched: Arc<HashMap<DagId, Arc<CrossTaskLineage>>>,
}

/// Stats for ops visibility — surfaced at
/// `GET /api/v1/lineage/cache/stats`.
#[derive(Debug, Clone, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlanCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub last_compile_ms: u64,
    pub last_compiled_at: Option<chrono::DateTime<chrono::Utc>>,
    pub cached_dag_count: usize,
    pub stitched_dag_count: usize,
    pub refresh_interval_secs: u64,
}

pub struct PlanCache {
    dags_path: PathBuf,
    refresh_interval: Duration,
    entry: RwLock<Option<Entry>>,
    hits: AtomicU64,
    misses: AtomicU64,
    last_compile_ms: AtomicU64,
    last_compiled_at: RwLock<Option<chrono::DateTime<chrono::Utc>>>,
}

impl PlanCache {
    /// Create a cache for the given DAGs directory. Defaults to
    /// `refresh_interval = 0` (always re-check signature). Use
    /// [`Self::with_refresh_interval`] to raise the floor.
    pub fn new(dags_path: impl Into<PathBuf>) -> Self {
        Self {
            dags_path: dags_path.into(),
            refresh_interval: Duration::ZERO,
            entry: RwLock::new(None),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            last_compile_ms: AtomicU64::new(0),
            last_compiled_at: RwLock::new(None),
        }
    }

    pub fn with_refresh_interval(mut self, interval: Duration) -> Self {
        self.refresh_interval = interval;
        self
    }

    /// Returns the cached view, recompiling if the DAG signature has
    /// changed or the TTL has expired. Errors propagate compile
    /// failures verbatim.
    pub fn get(&self) -> Result<CachedPlanView, String> {
        let current_sig = self.compute_signature();
        let now = Instant::now();

        // Fast path: read-only check.
        {
            let guard = self.entry.read().map_err(lock_err)?;
            if let Some(entry) = guard.as_ref() {
                if self.is_fresh(entry, current_sig, now) {
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    return Ok(CachedPlanView {
                        plan: Arc::clone(&entry.plan),
                        stitched: Arc::clone(&entry.stitched),
                    });
                }
            }
        }

        // Slow path: write lock + double check.
        let mut guard = self.entry.write().map_err(lock_err)?;
        if let Some(entry) = guard.as_ref() {
            // Another thread may have refreshed while we waited.
            if self.is_fresh(entry, current_sig, now) {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(CachedPlanView {
                    plan: Arc::clone(&entry.plan),
                    stitched: Arc::clone(&entry.stitched),
                });
            }
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let (plan, stitched) = self.compile_and_stitch()?;
        let elapsed_ms = started.elapsed().as_millis() as u64;
        self.last_compile_ms.store(elapsed_ms, Ordering::Relaxed);
        if let Ok(mut lc) = self.last_compiled_at.write() {
            *lc = Some(chrono::Utc::now());
        }

        let plan = Arc::new(plan);
        let stitched = Arc::new(stitched);
        let view = CachedPlanView {
            plan: Arc::clone(&plan),
            stitched: Arc::clone(&stitched),
        };
        *guard = Some(Entry {
            signature: current_sig,
            cached_at: now,
            plan,
            stitched,
        });
        info!(
            dags_path = %self.dags_path.display(),
            dags_count = view.plan.dags.len(),
            compile_ms = elapsed_ms,
            "PlanCache refreshed"
        );
        Ok(view)
    }

    /// Force the next call to [`Self::get`] to recompile, regardless
    /// of signature or TTL.
    pub fn invalidate(&self) {
        if let Ok(mut guard) = self.entry.write() {
            *guard = None;
        }
        debug!("PlanCache invalidated");
    }

    pub fn stats(&self) -> PlanCacheStats {
        let (cached_dag_count, stitched_dag_count) = match self.entry.read() {
            Ok(g) => g
                .as_ref()
                .map(|e| (e.plan.dags.len(), e.stitched.len()))
                .unwrap_or((0, 0)),
            Err(_) => (0, 0),
        };
        let last_compiled_at = self.last_compiled_at.read().ok().and_then(|g| *g);
        PlanCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            last_compile_ms: self.last_compile_ms.load(Ordering::Relaxed),
            last_compiled_at,
            cached_dag_count,
            stitched_dag_count,
            refresh_interval_secs: self.refresh_interval.as_secs(),
        }
    }

    fn is_fresh(&self, entry: &Entry, current_sig: u64, now: Instant) -> bool {
        if entry.signature != current_sig {
            return false;
        }
        if self.refresh_interval > Duration::ZERO
            && now.duration_since(entry.cached_at) > self.refresh_interval
        {
            return false;
        }
        true
    }

    fn compile_and_stitch(
        &self,
    ) -> Result<(ConduitPlan, HashMap<DagId, Arc<CrossTaskLineage>>), String> {
        let (plan, stats) = ConduitPlan::compile(&self.dags_path).map_err(|e| {
            format!(
                "plan cache: failed to compile DAGs at {}: {}",
                self.dags_path.display(),
                e
            )
        })?;
        if !stats.errors.is_empty() {
            return Err(format!(
                "plan cache: compile produced {} errors: {}",
                stats.errors.len(),
                stats
                    .errors
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join("; ")
            ));
        }

        let mut stitched = HashMap::with_capacity(plan.dags.len());
        for (dag_id, dag) in &plan.dags {
            match cross_task::stitch(dag) {
                Ok(s) => {
                    stitched.insert(dag_id.clone(), Arc::new(s));
                }
                Err(e) => {
                    // Strict-mode failures shouldn't poison the whole
                    // cache — just skip this DAG and log.
                    warn!(
                        dag_id = %e.dag_id,
                        unresolved = e.unresolved.len(),
                        "PlanCache: skipping DAG (strict-mode stitch failed)"
                    );
                }
            }
        }
        Ok((plan, stitched))
    }

    /// Hash of `(relative_path, mtime_nanos, size)` tuples for every
    /// real DAG file. Add/remove/edit on any real file changes the
    /// entry set; editor temp files are filtered out so noise from
    /// `.swp` / `~` files doesn't churn the signature. Walks at most
    /// a few hundred files for a real catalog — orders of magnitude
    /// cheaper than compiling.
    fn compute_signature(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();

        // Collect entries deterministically so the hash is stable.
        let mut entries: Vec<(String, u128, u64)> = Vec::new();
        for entry in walkdir::WalkDir::new(&self.dags_path)
            .follow_links(false)
            .into_iter()
            .filter_map(Result::ok)
        {
            if !entry.file_type().is_file() {
                continue;
            }
            // Ignore obvious editor / build noise so transient files
            // don't churn the signature.
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with('.') || name.ends_with('~') || name.ends_with(".swp") {
                    continue;
                }
            }
            let Ok(meta) = entry.metadata() else { continue };
            let relative = entry
                .path()
                .strip_prefix(&self.dags_path)
                .unwrap_or(entry.path())
                .to_string_lossy()
                .into_owned();
            entries.push((relative, mtime_nanos(meta.modified().ok()), meta.len()));
        }
        entries.sort();
        for e in &entries {
            e.hash(&mut hasher);
        }
        hasher.finish()
    }
}

fn lock_err<E: std::fmt::Display>(e: E) -> String {
    format!("plan cache lock poisoned: {}", e)
}

fn mtime_nanos(t: Option<SystemTime>) -> u128 {
    t.and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal valid Python DAG. Two tasks, no edges, no contracts —
    /// just enough that ConduitPlan::compile and cross_task::stitch
    /// succeed.
    const TRIVIAL_DAG: &str = r#"
from conduit_sdk import dag, task

@dag(schedule="@daily")
def trivial():
    @task()
    def extract():
        return None

    @task()
    def transform(data=extract):
        return None
"#;

    fn make_dag_dir(name: &str, contents: &str) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(name), contents).unwrap();
        dir
    }

    #[test]
    fn hit_when_dir_unchanged() {
        let dir = make_dag_dir("trivial.py", TRIVIAL_DAG);
        let cache = PlanCache::new(dir.path());
        let _ = cache.get().expect("first call compiles");
        let _ = cache.get().expect("second call is a hit");
        let stats = cache.stats();
        assert_eq!(stats.misses, 1, "exactly one miss for cold start");
        assert_eq!(stats.hits, 1, "second call hit the cache");
    }

    #[test]
    fn miss_when_file_mtime_advances() {
        let dir = make_dag_dir("trivial.py", TRIVIAL_DAG);
        let cache = PlanCache::new(dir.path());
        let _ = cache.get().unwrap();
        let initial_misses = cache.stats().misses;

        // Bump mtime by rewriting the file with the same contents (but
        // sleep first to ensure mtime granularity catches it).
        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::write(dir.path().join("trivial.py"), TRIVIAL_DAG).unwrap();

        let _ = cache.get().unwrap();
        assert!(
            cache.stats().misses > initial_misses,
            "expected a miss after mtime bump (misses: {} -> {})",
            initial_misses,
            cache.stats().misses
        );
    }

    #[test]
    fn miss_when_file_added() {
        let dir = make_dag_dir("a.py", TRIVIAL_DAG);
        let cache = PlanCache::new(dir.path());
        let _ = cache.get().unwrap();
        let initial_misses = cache.stats().misses;

        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::write(dir.path().join("b.py"), TRIVIAL_DAG).unwrap();

        let _ = cache.get().unwrap();
        assert!(
            cache.stats().misses > initial_misses,
            "new file should miss"
        );
    }

    #[test]
    fn miss_when_file_removed() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.py"), TRIVIAL_DAG).unwrap();
        std::fs::write(dir.path().join("b.py"), TRIVIAL_DAG).unwrap();
        let cache = PlanCache::new(dir.path());
        let _ = cache.get().unwrap();
        let initial_misses = cache.stats().misses;

        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::remove_file(dir.path().join("b.py")).unwrap();

        let _ = cache.get().unwrap();
        assert!(
            cache.stats().misses > initial_misses,
            "deletion should miss"
        );
    }

    #[test]
    fn invalidate_forces_recompile() {
        let dir = make_dag_dir("a.py", TRIVIAL_DAG);
        let cache = PlanCache::new(dir.path());
        let _ = cache.get().unwrap();
        let _ = cache.get().unwrap();
        let stats_before = cache.stats();

        cache.invalidate();
        let _ = cache.get().unwrap();
        let stats_after = cache.stats();

        assert!(
            stats_after.misses > stats_before.misses,
            "invalidate should force a miss"
        );
    }

    #[test]
    fn ignores_editor_temp_files() {
        let dir = make_dag_dir("a.py", TRIVIAL_DAG);
        let cache = PlanCache::new(dir.path());
        let _ = cache.get().unwrap();
        let initial_misses = cache.stats().misses;

        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::write(dir.path().join(".a.py.swp"), b"junk").unwrap();
        std::fs::write(dir.path().join("a.py~"), b"junk").unwrap();

        let _ = cache.get().unwrap();
        assert_eq!(
            cache.stats().misses,
            initial_misses,
            "editor noise should not invalidate"
        );
    }

    #[test]
    fn concurrent_first_calls_compile_once() {
        let dir = make_dag_dir("a.py", TRIVIAL_DAG);
        let cache = Arc::new(PlanCache::new(dir.path()));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let c = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || c.get().unwrap()));
        }
        for h in handles {
            let _ = h.join();
        }

        // Stampede protection: exactly one miss across all 8 threads.
        assert_eq!(
            cache.stats().misses,
            1,
            "double-checked locking should compile exactly once"
        );
        assert_eq!(cache.stats().hits, 7);
    }

    #[test]
    fn ttl_ceiling_forces_recheck() {
        let dir = make_dag_dir("a.py", TRIVIAL_DAG);
        let cache = PlanCache::new(dir.path()).with_refresh_interval(Duration::from_millis(1));

        let _ = cache.get().unwrap();
        std::thread::sleep(Duration::from_millis(10));
        // Even though the signature is unchanged, the TTL expired —
        // the next call recomputes. The signature still matches, so
        // the recompile produces the same plan but `misses` increments.
        let _ = cache.get().unwrap();
        let stats = cache.stats();
        assert!(stats.misses >= 2, "TTL expiry should miss: {:?}", stats);
    }

    #[test]
    fn stats_track_dag_counts() {
        let dir = make_dag_dir("a.py", TRIVIAL_DAG);
        let cache = PlanCache::new(dir.path());
        assert_eq!(cache.stats().cached_dag_count, 0);
        let _ = cache.get().unwrap();
        let stats = cache.stats();
        assert!(stats.cached_dag_count >= 1);
        assert!(stats.last_compile_ms > 0 || stats.cached_dag_count > 0);
        assert!(stats.last_compiled_at.is_some());
    }
}
