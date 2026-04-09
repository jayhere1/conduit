//! Worker pool management and health tracking.
//!
//! The coordinator maintains a registry of connected workers, tracks their
//! health via heartbeats, and selects workers for task assignment based on
//! capacity, pool affinity, and load.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::proto_types::*;

/// How long before a worker with no heartbeat is marked disconnected.
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(30);

/// How long after disconnection before a worker is declared dead.
const DEAD_TIMEOUT: Duration = Duration::from_secs(120);

/// Internal state for a registered worker.
#[derive(Debug, Clone)]
pub struct WorkerEntry {
    /// Registration info.
    pub worker_id: String,
    pub hostname: String,
    pub capacity: u32,
    pub pool_affinity: Vec<String>,
    pub labels: HashMap<String, String>,
    pub version: String,

    /// Runtime state.
    pub state: WorkerState,
    pub active_tasks: u32,
    pub registered_at: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,

    /// System metrics from latest heartbeat.
    pub cpu_percent: f64,
    pub memory_percent: f64,
    pub disk_percent: f64,

    /// Task assignment tracking.
    pub running_assignments: Vec<String>,

    /// Lifetime counters.
    pub tasks_completed: u64,
    pub tasks_failed: u64,
}

impl WorkerEntry {
    /// Create a new entry from a registration request.
    pub fn from_registration(req: &RegisterRequest) -> Self {
        let now = Utc::now();
        Self {
            worker_id: req.worker_id.clone(),
            hostname: req.hostname.clone(),
            capacity: req.capacity,
            pool_affinity: req.pool_affinity.clone(),
            labels: req.labels.clone(),
            version: req.version.clone(),
            state: WorkerState::Active,
            active_tasks: 0,
            registered_at: now,
            last_heartbeat: now,
            cpu_percent: 0.0,
            memory_percent: 0.0,
            disk_percent: 0.0,
            running_assignments: Vec::new(),
            tasks_completed: 0,
            tasks_failed: 0,
        }
    }

    /// Available capacity (slots not in use).
    pub fn available_slots(&self) -> u32 {
        self.capacity.saturating_sub(self.active_tasks)
    }

    /// Whether this worker can accept tasks from the given pool.
    pub fn can_handle_pool(&self, pool: &str) -> bool {
        if self.pool_affinity.is_empty() {
            // No affinity = accepts "default" pool only
            pool == "default" || pool.is_empty()
        } else {
            self.pool_affinity.iter().any(|p| p == pool || p == "*")
        }
    }

    /// Whether this worker is healthy and can accept work.
    pub fn is_available(&self) -> bool {
        self.state == WorkerState::Active && self.available_slots() > 0
    }

    /// Update from a heartbeat message.
    pub fn apply_heartbeat(&mut self, hb: &WorkerHeartbeat) {
        self.last_heartbeat = Utc::now();
        self.active_tasks = hb.active_tasks;
        self.cpu_percent = hb.cpu_percent;
        self.memory_percent = hb.memory_percent;
        self.disk_percent = hb.disk_percent;
        self.running_assignments = hb.running_assignments.clone();

        // If worker was disconnected, restore it.
        if self.state == WorkerState::Disconnected {
            info!(worker = %self.worker_id, "Worker reconnected");
            self.state = WorkerState::Active;
        }
    }

    /// Convert to the API-facing WorkerStatus type.
    pub fn to_status(&self) -> WorkerStatus {
        WorkerStatus {
            worker_id: self.worker_id.clone(),
            hostname: self.hostname.clone(),
            state: self.state,
            capacity: self.capacity,
            active_tasks: self.active_tasks,
            pool_affinity: self.pool_affinity.clone(),
            last_heartbeat_ms: self.last_heartbeat.timestamp_millis(),
            cpu_percent: self.cpu_percent,
            memory_percent: self.memory_percent,
            labels: self.labels.clone(),
            registered_at_ms: self.registered_at.timestamp_millis(),
            tasks_completed: self.tasks_completed,
            tasks_failed: self.tasks_failed,
        }
    }
}

/// Task routing strategy for selecting workers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RoutingStrategy {
    /// Assign to worker with most available slots (spread load).
    #[default]
    LeastLoaded,
    /// Assign to worker with fewest available slots (pack tightly, save resources).
    BinPack,
    /// Simple round-robin across available workers.
    RoundRobin,
}

/// Manages the set of connected workers and handles task routing.
pub struct WorkerPool {
    /// Worker entries keyed by worker_id.
    workers: DashMap<String, WorkerEntry>,

    /// Task routing strategy.
    strategy: RoutingStrategy,

    /// Round-robin counter (only used with RoundRobin strategy).
    round_robin_idx: Arc<RwLock<usize>>,

    /// Pending assignments: assignment_id → worker_id.
    /// Tracks which worker owns which assignment for result routing.
    assignment_map: DashMap<String, String>,
}

impl WorkerPool {
    /// Create a new empty worker pool.
    pub fn new(strategy: RoutingStrategy) -> Self {
        Self {
            workers: DashMap::new(),
            strategy,
            round_robin_idx: Arc::new(RwLock::new(0)),
            assignment_map: DashMap::new(),
        }
    }

    /// Register a new worker. Returns true if this is a new registration.
    pub fn register(&self, req: &RegisterRequest) -> bool {
        let is_new = !self.workers.contains_key(&req.worker_id);

        if is_new {
            info!(
                worker = %req.worker_id,
                hostname = %req.hostname,
                capacity = req.capacity,
                pools = ?req.pool_affinity,
                "Worker registered"
            );
            self.workers
                .insert(req.worker_id.clone(), WorkerEntry::from_registration(req));
        } else {
            // Re-registration: update entry, mark active.
            if let Some(mut entry) = self.workers.get_mut(&req.worker_id) {
                entry.state = WorkerState::Active;
                entry.capacity = req.capacity;
                entry.pool_affinity = req.pool_affinity.clone();
                entry.last_heartbeat = Utc::now();
                info!(worker = %req.worker_id, "Worker re-registered");
            }
        }

        is_new
    }

    /// Process a heartbeat from a worker.
    pub fn heartbeat(&self, hb: &WorkerHeartbeat) {
        if let Some(mut entry) = self.workers.get_mut(&hb.worker_id) {
            entry.apply_heartbeat(hb);
        } else {
            warn!(
                worker = %hb.worker_id,
                "Heartbeat from unknown worker (not registered)"
            );
        }
    }

    /// Select the best worker for a task in the given pool.
    ///
    /// Returns `None` if no worker is available.
    pub async fn select_worker(&self, pool: &str) -> Option<String> {
        let candidates: Vec<_> = self
            .workers
            .iter()
            .filter(|e| e.is_available() && e.can_handle_pool(pool))
            .map(|e| (e.worker_id.clone(), e.available_slots(), e.cpu_percent))
            .collect();

        if candidates.is_empty() {
            return None;
        }

        match self.strategy {
            RoutingStrategy::LeastLoaded => {
                // Pick worker with most available slots, breaking ties by CPU.
                candidates
                    .iter()
                    .max_by(|a, b| {
                        a.1.cmp(&b.1).then_with(|| {
                            a.2.partial_cmp(&b.2)
                                .unwrap_or(std::cmp::Ordering::Equal)
                                .reverse()
                        })
                    })
                    .map(|(id, _, _)| id.clone())
            }
            RoutingStrategy::BinPack => {
                // Pick worker with fewest available slots (but at least 1).
                candidates
                    .iter()
                    .min_by_key(|(_, slots, _)| *slots)
                    .map(|(id, _, _)| id.clone())
            }
            RoutingStrategy::RoundRobin => {
                let mut idx = self.round_robin_idx.write().await;
                let selected = &candidates[*idx % candidates.len()];
                *idx = (*idx + 1) % candidates.len();
                Some(selected.0.clone())
            }
        }
    }

    /// Record that an assignment was sent to a worker.
    pub fn assign_task(&self, assignment_id: &str, worker_id: &str) {
        self.assignment_map
            .insert(assignment_id.to_string(), worker_id.to_string());

        if let Some(mut entry) = self.workers.get_mut(worker_id) {
            entry.active_tasks += 1;
            entry.running_assignments.push(assignment_id.to_string());
        }
    }

    /// Record that an assignment completed (success or failure).
    pub fn complete_task(&self, assignment_id: &str, success: bool) {
        if let Some((_, worker_id)) = self.assignment_map.remove(assignment_id) {
            if let Some(mut entry) = self.workers.get_mut(&worker_id) {
                entry.active_tasks = entry.active_tasks.saturating_sub(1);
                entry.running_assignments.retain(|a| a != assignment_id);

                if success {
                    entry.tasks_completed += 1;
                } else {
                    entry.tasks_failed += 1;
                }
            }
        }
    }

    /// Look up which worker owns an assignment.
    pub fn get_assignment_worker(&self, assignment_id: &str) -> Option<String> {
        self.assignment_map
            .get(assignment_id)
            .map(|e| e.value().clone())
    }

    /// Mark a worker as draining (finish current tasks, accept no new ones).
    pub fn drain_worker(&self, worker_id: &str, reason: &str) {
        if let Some(mut entry) = self.workers.get_mut(worker_id) {
            info!(worker = %worker_id, reason = %reason, "Draining worker");
            entry.state = WorkerState::Draining;
        }
    }

    /// Remove a worker from the pool entirely.
    pub fn remove_worker(&self, worker_id: &str) {
        info!(worker = %worker_id, "Removing worker from pool");
        self.workers.remove(worker_id);

        // Clean up any assignments from this worker.
        self.assignment_map.retain(|_, v| v != worker_id);
    }

    /// Run periodic health checks. Call this on a timer (e.g., every 10s).
    ///
    /// Returns IDs of workers that transitioned to Dead (their tasks need reassignment).
    pub fn check_health(&self) -> Vec<String> {
        let now = Utc::now();
        let mut newly_dead = Vec::new();

        for mut entry in self.workers.iter_mut() {
            let since_heartbeat = now - entry.last_heartbeat;

            match entry.state {
                WorkerState::Active | WorkerState::Draining => {
                    if since_heartbeat
                        > chrono::Duration::from_std(DEAD_TIMEOUT).unwrap_or_default()
                    {
                        warn!(
                            worker = %entry.worker_id,
                            last_heartbeat = %entry.last_heartbeat,
                            "Worker declared dead (no heartbeat for {}s)",
                            since_heartbeat.num_seconds()
                        );
                        entry.state = WorkerState::Dead;
                        newly_dead.push(entry.worker_id.clone());
                    } else if since_heartbeat
                        > chrono::Duration::from_std(HEARTBEAT_TIMEOUT).unwrap_or_default()
                        && entry.state != WorkerState::Draining
                    {
                        warn!(
                            worker = %entry.worker_id,
                            "Worker disconnected (no heartbeat for {}s)",
                            since_heartbeat.num_seconds()
                        );
                        entry.state = WorkerState::Disconnected;
                    }
                }
                WorkerState::Disconnected => {
                    if since_heartbeat
                        > chrono::Duration::from_std(DEAD_TIMEOUT).unwrap_or_default()
                    {
                        warn!(worker = %entry.worker_id, "Disconnected worker declared dead");
                        entry.state = WorkerState::Dead;
                        newly_dead.push(entry.worker_id.clone());
                    }
                }
                _ => {}
            }
        }

        newly_dead
    }

    /// Get assignments for dead workers (these need to be reassigned).
    pub fn orphaned_assignments(&self, dead_worker_ids: &[String]) -> Vec<String> {
        let mut orphans = Vec::new();
        for entry in self.assignment_map.iter() {
            if dead_worker_ids.contains(entry.value()) {
                orphans.push(entry.key().clone());
            }
        }
        orphans
    }

    /// Number of registered workers (any state).
    pub fn total_workers(&self) -> usize {
        self.workers.len()
    }

    /// Number of workers that can accept tasks.
    pub fn active_workers(&self) -> usize {
        self.workers
            .iter()
            .filter(|e| e.state == WorkerState::Active)
            .count()
    }

    /// Total available slots across all active workers.
    pub fn total_available_slots(&self) -> u32 {
        self.workers
            .iter()
            .filter(|e| e.state == WorkerState::Active)
            .map(|e| e.available_slots())
            .sum()
    }

    /// Total running tasks across all workers.
    pub fn total_running_tasks(&self) -> u32 {
        self.workers.iter().map(|e| e.active_tasks).sum()
    }

    /// Get the full cluster status snapshot.
    pub fn cluster_status(&self, uptime_secs: u64) -> ClusterStatusResponse {
        let workers: Vec<WorkerStatus> = self.workers.iter().map(|e| e.to_status()).collect();

        let active = self.active_workers();
        let total = self.total_workers();

        let health = if total == 0 {
            ClusterHealth::Unhealthy
        } else if active == total {
            ClusterHealth::Healthy
        } else if active > 0 {
            ClusterHealth::Degraded
        } else {
            ClusterHealth::Unhealthy
        };

        ClusterStatusResponse {
            health,
            workers,
            active_runs: 0, // Set by coordinator.
            running_tasks: self.total_running_tasks(),
            queued_tasks: 0, // Set by coordinator.
            uptime_secs,
        }
    }

    /// List all registered workers as status entries.
    pub fn list_workers(&self) -> Vec<WorkerStatus> {
        self.workers.iter().map(|e| e.to_status()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_register_request(id: &str, capacity: u32, pools: Vec<&str>) -> RegisterRequest {
        RegisterRequest {
            worker_id: id.to_string(),
            hostname: format!("{}.local", id),
            capacity,
            pool_affinity: pools.into_iter().map(String::from).collect(),
            labels: HashMap::new(),
            version: "0.1.0".to_string(),
            health_port: 9090,
        }
    }

    #[allow(dead_code)]
    fn make_heartbeat(id: &str, active: u32) -> WorkerHeartbeat {
        WorkerHeartbeat {
            worker_id: id.to_string(),
            active_tasks: active,
            cpu_percent: 25.0,
            memory_percent: 50.0,
            disk_percent: 30.0,
            running_assignments: vec![],
            timestamp_ms: Utc::now().timestamp_millis(),
        }
    }

    #[test]
    fn test_worker_registration() {
        let pool = WorkerPool::new(RoutingStrategy::LeastLoaded);
        let req = make_register_request("w1", 4, vec!["default"]);
        assert!(pool.register(&req));
        assert_eq!(pool.total_workers(), 1);
        assert_eq!(pool.active_workers(), 1);
    }

    #[test]
    fn test_duplicate_registration() {
        let pool = WorkerPool::new(RoutingStrategy::LeastLoaded);
        let req = make_register_request("w1", 4, vec![]);
        assert!(pool.register(&req)); // first: new
        assert!(!pool.register(&req)); // second: existing
        assert_eq!(pool.total_workers(), 1);
    }

    #[tokio::test]
    async fn test_select_worker_least_loaded() {
        let pool = WorkerPool::new(RoutingStrategy::LeastLoaded);

        pool.register(&make_register_request("w1", 2, vec!["default"]));
        pool.register(&make_register_request("w2", 4, vec!["default"]));

        // w2 has more capacity
        let selected = pool.select_worker("default").await.unwrap();
        assert_eq!(selected, "w2");
    }

    #[tokio::test]
    async fn test_select_worker_bin_pack() {
        let pool = WorkerPool::new(RoutingStrategy::BinPack);

        pool.register(&make_register_request("w1", 2, vec!["default"]));
        pool.register(&make_register_request("w2", 4, vec!["default"]));

        // Bin pack prefers w1 (fewer slots to fill)
        let selected = pool.select_worker("default").await.unwrap();
        assert_eq!(selected, "w1");
    }

    #[tokio::test]
    async fn test_select_worker_pool_affinity() {
        let pool = WorkerPool::new(RoutingStrategy::LeastLoaded);

        pool.register(&make_register_request("w1", 4, vec!["gpu"]));
        pool.register(&make_register_request("w2", 4, vec!["default"]));

        // Only w1 handles "gpu" pool
        let selected = pool.select_worker("gpu").await.unwrap();
        assert_eq!(selected, "w1");

        // Only w2 handles "default" pool
        let selected = pool.select_worker("default").await.unwrap();
        assert_eq!(selected, "w2");
    }

    #[tokio::test]
    async fn test_select_worker_no_available() {
        let pool = WorkerPool::new(RoutingStrategy::LeastLoaded);

        pool.register(&make_register_request("w1", 1, vec!["default"]));
        pool.assign_task("a1", "w1");

        // w1 is now at capacity
        let selected = pool.select_worker("default").await;
        assert!(selected.is_none());
    }

    #[test]
    fn test_assign_and_complete_task() {
        let pool = WorkerPool::new(RoutingStrategy::LeastLoaded);
        pool.register(&make_register_request("w1", 4, vec![]));

        pool.assign_task("a1", "w1");
        assert_eq!(pool.total_running_tasks(), 1);
        assert_eq!(pool.get_assignment_worker("a1"), Some("w1".to_string()));

        pool.complete_task("a1", true);
        assert_eq!(pool.total_running_tasks(), 0);

        let workers = pool.list_workers();
        let w1 = workers.iter().find(|w| w.worker_id == "w1").unwrap();
        assert_eq!(w1.tasks_completed, 1);
        assert_eq!(w1.tasks_failed, 0);
    }

    #[test]
    fn test_failed_task_counter() {
        let pool = WorkerPool::new(RoutingStrategy::LeastLoaded);
        pool.register(&make_register_request("w1", 4, vec![]));

        pool.assign_task("a1", "w1");
        pool.complete_task("a1", false);

        let workers = pool.list_workers();
        let w1 = workers.iter().find(|w| w.worker_id == "w1").unwrap();
        assert_eq!(w1.tasks_completed, 0);
        assert_eq!(w1.tasks_failed, 1);
    }

    #[test]
    fn test_heartbeat_updates_metrics() {
        let pool = WorkerPool::new(RoutingStrategy::LeastLoaded);
        pool.register(&make_register_request("w1", 4, vec![]));

        pool.heartbeat(&WorkerHeartbeat {
            worker_id: "w1".to_string(),
            active_tasks: 2,
            cpu_percent: 75.0,
            memory_percent: 60.0,
            disk_percent: 40.0,
            running_assignments: vec!["a1".into(), "a2".into()],
            timestamp_ms: Utc::now().timestamp_millis(),
        });

        let workers = pool.list_workers();
        let w1 = workers.iter().find(|w| w.worker_id == "w1").unwrap();
        assert_eq!(w1.active_tasks, 2);
        assert!((w1.cpu_percent - 75.0).abs() < 0.01);
    }

    #[test]
    fn test_drain_worker() {
        let pool = WorkerPool::new(RoutingStrategy::LeastLoaded);
        pool.register(&make_register_request("w1", 4, vec![]));

        pool.drain_worker("w1", "maintenance");

        let workers = pool.list_workers();
        let w1 = workers.iter().find(|w| w.worker_id == "w1").unwrap();
        assert_eq!(w1.state, WorkerState::Draining);
    }

    #[tokio::test]
    async fn test_draining_worker_not_selected() {
        let pool = WorkerPool::new(RoutingStrategy::LeastLoaded);
        pool.register(&make_register_request("w1", 4, vec!["default"]));
        pool.drain_worker("w1", "maintenance");

        let selected = pool.select_worker("default").await;
        assert!(selected.is_none());
    }

    #[test]
    fn test_cluster_status_healthy() {
        let pool = WorkerPool::new(RoutingStrategy::LeastLoaded);
        pool.register(&make_register_request("w1", 4, vec![]));
        pool.register(&make_register_request("w2", 4, vec![]));

        let status = pool.cluster_status(3600);
        assert_eq!(status.health, ClusterHealth::Healthy);
        assert_eq!(status.workers.len(), 2);
        assert_eq!(status.uptime_secs, 3600);
    }

    #[test]
    fn test_cluster_status_empty() {
        let pool = WorkerPool::new(RoutingStrategy::LeastLoaded);
        let status = pool.cluster_status(0);
        assert_eq!(status.health, ClusterHealth::Unhealthy);
        assert_eq!(status.workers.len(), 0);
    }

    #[test]
    fn test_wildcard_pool_affinity() {
        let pool = WorkerPool::new(RoutingStrategy::LeastLoaded);
        pool.register(&make_register_request("w1", 4, vec!["*"]));

        // Wildcard worker accepts any pool
        let entry = pool.workers.get("w1").unwrap();
        assert!(entry.can_handle_pool("default"));
        assert!(entry.can_handle_pool("gpu"));
        assert!(entry.can_handle_pool("anything"));
    }

    #[test]
    fn test_remove_worker() {
        let pool = WorkerPool::new(RoutingStrategy::LeastLoaded);
        pool.register(&make_register_request("w1", 4, vec![]));
        pool.assign_task("a1", "w1");

        pool.remove_worker("w1");

        assert_eq!(pool.total_workers(), 0);
        assert!(pool.get_assignment_worker("a1").is_none());
    }

    #[test]
    fn test_total_available_slots() {
        let pool = WorkerPool::new(RoutingStrategy::LeastLoaded);
        pool.register(&make_register_request("w1", 4, vec![]));
        pool.register(&make_register_request("w2", 2, vec![]));

        assert_eq!(pool.total_available_slots(), 6);

        pool.assign_task("a1", "w1");
        assert_eq!(pool.total_available_slots(), 5);
    }
}
