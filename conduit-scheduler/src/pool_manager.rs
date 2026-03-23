//! Resource pool management for concurrency control.
//!
//! Pools are named collections of execution slots. Each task specifies which pool
//! it draws from. When all slots are in use, subsequent tasks wait.
//!
//! Example pools:
//! - "default": 128 slots (for general tasks)
//! - "gpu": 8 slots (for GPU-intensive tasks)
//! - "io": 256 slots (for I/O-bound tasks)
//!
//! This enables fine-grained concurrency control without global rate limiting.

use std::collections::{HashMap, HashSet};
use tracing::{debug, warn};

use conduit_common::dag::Pool;

/// Manages named resource pools with slot limits.
#[derive(Debug, Clone)]
pub struct PoolManager {
    pools: HashMap<String, PoolState>,
}

/// Internal state of a resource pool.
#[derive(Debug, Clone)]
struct PoolState {
    name: String,
    total_slots: u32,
    available_slots: u32,
    #[allow(dead_code)]
    description: Option<String>,
    occupants: HashSet<String>, // task IDs currently holding slots
}

impl Default for PoolManager {
    fn default() -> Self {
        Self::new(vec![Pool {
            name: "default".to_string(),
            slots: 128,
            description: Some("Default pool".to_string()),
        }])
    }
}

impl PoolManager {
    /// Create a new pool manager from a list of pool definitions.
    pub fn new(pools: Vec<Pool>) -> Self {
        let mut pool_states = HashMap::new();

        for pool in pools {
            pool_states.insert(
                pool.name.clone(),
                PoolState {
                    name: pool.name,
                    total_slots: pool.slots,
                    available_slots: pool.slots,
                    description: pool.description,
                    occupants: HashSet::new(),
                },
            );
        }

        Self {
            pools: pool_states,
        }
    }

    /// Try to acquire a slot in the named pool for the given task.
    ///
    /// Returns `true` if a slot was acquired, `false` if no slots available.
    pub fn acquire(&mut self, pool_name: &str, task_id: &str) -> bool {
        let pool = match self.pools.get_mut(pool_name) {
            Some(p) => p,
            None => {
                warn!(pool = %pool_name, task = %task_id, "Pool not found");
                return false;
            }
        };

        if pool.available_slots > 0 {
            pool.available_slots -= 1;
            pool.occupants.insert(task_id.to_string());

            debug!(
                pool = %pool_name,
                task = %task_id,
                available = %pool.available_slots,
                "Task acquired pool slot"
            );

            true
        } else {
            debug!(
                pool = %pool_name,
                task = %task_id,
                "No available slots in pool"
            );

            false
        }
    }

    /// Release a previously acquired slot.
    pub fn release(&mut self, pool_name: &str, task_id: &str) {
        let pool = match self.pools.get_mut(pool_name) {
            Some(p) => p,
            None => {
                warn!(pool = %pool_name, task = %task_id, "Pool not found");
                return;
            }
        };

        if pool.occupants.remove(task_id) {
            pool.available_slots += 1;

            debug!(
                pool = %pool_name,
                task = %task_id,
                available = %pool.available_slots,
                "Task released pool slot"
            );
        } else {
            warn!(
                pool = %pool_name,
                task = %task_id,
                "Task did not hold a slot in this pool"
            );
        }
    }

    /// Get the number of available slots in a pool.
    pub fn available(&self, pool_name: &str) -> u32 {
        self.pools
            .get(pool_name)
            .map(|p| p.available_slots)
            .unwrap_or(0)
    }

    /// Get the total number of slots in a pool.
    pub fn total(&self, pool_name: &str) -> u32 {
        self.pools
            .get(pool_name)
            .map(|p| p.total_slots)
            .unwrap_or(0)
    }

    /// Get the number of occupied slots in a pool.
    pub fn occupied(&self, pool_name: &str) -> u32 {
        self.pools
            .get(pool_name)
            .map(|p| p.occupants.len() as u32)
            .unwrap_or(0)
    }

    /// Check if a task currently holds a slot in a pool.
    pub fn is_occupant(&self, pool_name: &str, task_id: &str) -> bool {
        self.pools
            .get(pool_name)
            .map(|p| p.occupants.contains(task_id))
            .unwrap_or(false)
    }

    /// Get all pool names.
    pub fn pool_names(&self) -> Vec<String> {
        self.pools.keys().cloned().collect()
    }

    /// Get detailed stats for all pools.
    pub fn stats(&self) -> Vec<PoolStats> {
        self.pools
            .values()
            .map(|p| PoolStats {
                name: p.name.clone(),
                total_slots: p.total_slots,
                available_slots: p.available_slots,
                occupied_slots: p.occupants.len() as u32,
                utilization_pct: if p.total_slots > 0 {
                    ((p.total_slots - p.available_slots) as f64 / p.total_slots as f64) * 100.0
                } else {
                    0.0
                },
            })
            .collect()
    }
}

/// Statistics for a pool.
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub name: String,
    pub total_slots: u32,
    pub available_slots: u32,
    pub occupied_slots: u32,
    pub utilization_pct: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_pool_manager() {
        let pools = vec![
            Pool {
                name: "default".to_string(),
                slots: 128,
                description: None,
            },
            Pool {
                name: "gpu".to_string(),
                slots: 8,
                description: Some("GPU tasks".to_string()),
            },
        ];

        let mgr = PoolManager::new(pools);

        assert_eq!(mgr.available("default"), 128);
        assert_eq!(mgr.available("gpu"), 8);
        assert_eq!(mgr.total("default"), 128);
        assert_eq!(mgr.total("gpu"), 8);
    }

    #[test]
    fn test_acquire_release() {
        let pools = vec![Pool {
            name: "test".to_string(),
            slots: 2,
            description: None,
        }];

        let mut mgr = PoolManager::new(pools);

        // Acquire first slot
        assert!(mgr.acquire("test", "task_1"));
        assert_eq!(mgr.available("test"), 1);
        assert_eq!(mgr.occupied("test"), 1);

        // Acquire second slot
        assert!(mgr.acquire("test", "task_2"));
        assert_eq!(mgr.available("test"), 0);
        assert_eq!(mgr.occupied("test"), 2);

        // Pool is full
        assert!(!mgr.acquire("test", "task_3"));

        // Release a slot
        mgr.release("test", "task_1");
        assert_eq!(mgr.available("test"), 1);
        assert_eq!(mgr.occupied("test"), 1);

        // Now we can acquire again
        assert!(mgr.acquire("test", "task_3"));
        assert_eq!(mgr.available("test"), 0);
    }

    #[test]
    fn test_is_occupant() {
        let pools = vec![Pool {
            name: "test".to_string(),
            slots: 10,
            description: None,
        }];

        let mut mgr = PoolManager::new(pools);

        assert!(!mgr.is_occupant("test", "task_1"));
        mgr.acquire("test", "task_1");
        assert!(mgr.is_occupant("test", "task_1"));

        mgr.release("test", "task_1");
        assert!(!mgr.is_occupant("test", "task_1"));
    }

    #[test]
    fn test_nonexistent_pool() {
        let pools = vec![];
        let mut mgr = PoolManager::new(pools);

        assert!(!mgr.acquire("nonexistent", "task_1"));
        assert_eq!(mgr.available("nonexistent"), 0);
        assert_eq!(mgr.total("nonexistent"), 0);

        mgr.release("nonexistent", "task_1");
    }

    #[test]
    fn test_pool_stats() {
        let pools = vec![Pool {
            name: "test".to_string(),
            slots: 100,
            description: None,
        }];

        let mut mgr = PoolManager::new(pools);

        let stats = mgr.stats();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].name, "test");
        assert_eq!(stats[0].total_slots, 100);
        assert_eq!(stats[0].available_slots, 100);
        assert_eq!(stats[0].utilization_pct, 0.0);

        mgr.acquire("test", "task_1");
        mgr.acquire("test", "task_2");

        let stats = mgr.stats();
        assert_eq!(stats[0].occupied_slots, 2);
        assert_eq!(stats[0].available_slots, 98);
        assert!(stats[0].utilization_pct >= 1.9 && stats[0].utilization_pct <= 2.1);
    }

    #[test]
    fn test_pool_names() {
        let pools = vec![
            Pool {
                name: "default".to_string(),
                slots: 128,
                description: None,
            },
            Pool {
                name: "gpu".to_string(),
                slots: 8,
                description: None,
            },
        ];

        let mgr = PoolManager::new(pools);
        let mut names = mgr.pool_names();
        names.sort();

        assert_eq!(names, vec!["default", "gpu"]);
    }
}
