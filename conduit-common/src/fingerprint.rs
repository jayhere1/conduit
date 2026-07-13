//! Content-addressable fingerprinting for snapshot reuse.
//!
//! A fingerprint uniquely identifies the "identity" of a task execution:
//! if the task code, configuration, and all upstream fingerprints are identical,
//! the fingerprint will be the same — and execution can be skipped.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

/// A content-addressable fingerprint (64-bit SipHash of content, config,
/// and upstream fingerprints, rendered as a hex string).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Fingerprint(pub String);

impl Fingerprint {
    /// Compute a fingerprint from task content and upstream fingerprints.
    ///
    /// The fingerprint is derived from:
    /// 1. The task's source code / query / command
    /// 2. The task's configuration (retries, timeout, pool, etc.)
    /// 3. The fingerprints of all upstream task snapshots (sorted for determinism)
    pub fn compute(
        task_content: &str,
        task_config: &str,
        upstream_fingerprints: &BTreeMap<String, Fingerprint>,
    ) -> Self {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        task_content.hash(&mut hasher);
        task_config.hash(&mut hasher);

        // BTreeMap ensures deterministic ordering
        for (task_id, fp) in upstream_fingerprints {
            task_id.hash(&mut hasher);
            fp.0.hash(&mut hasher);
        }

        let hash = hasher.finish();
        Fingerprint(format!("{:016x}", hash))
    }

    /// Create a fingerprint from a raw hex string.
    pub fn from_hex(hex: &str) -> Self {
        Fingerprint(hex.to_string())
    }
}

impl fmt::Display for Fingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.0[..12]) // Short display: first 12 chars
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_inputs_produce_same_fingerprint() {
        let upstream = BTreeMap::new();
        let fp1 = Fingerprint::compute("SELECT * FROM orders", "{}", &upstream);
        let fp2 = Fingerprint::compute("SELECT * FROM orders", "{}", &upstream);
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn different_content_produces_different_fingerprint() {
        let upstream = BTreeMap::new();
        let fp1 = Fingerprint::compute("SELECT * FROM orders", "{}", &upstream);
        let fp2 = Fingerprint::compute("SELECT * FROM customers", "{}", &upstream);
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn upstream_changes_propagate() {
        let mut upstream1 = BTreeMap::new();
        upstream1.insert("extract".to_string(), Fingerprint::from_hex("aaa"));

        let mut upstream2 = BTreeMap::new();
        upstream2.insert("extract".to_string(), Fingerprint::from_hex("bbb"));

        let fp1 = Fingerprint::compute("transform", "{}", &upstream1);
        let fp2 = Fingerprint::compute("transform", "{}", &upstream2);
        assert_ne!(fp1, fp2);
    }
}
