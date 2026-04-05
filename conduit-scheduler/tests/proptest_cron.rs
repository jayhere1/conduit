//! Property-based tests for the cron parser.
//!
//! Ensures that `CronSchedule::parse` never panics on arbitrary input
//! and that well-formed step expressions always parse successfully.

use conduit_scheduler::CronSchedule;
use proptest::prelude::*;

// ─── Fuzz: random cron-like strings never cause a panic ──────────────────────

proptest! {
    #[test]
    fn test_cron_parse_never_panics(
        input in "[0-9 *\\-/,]{0,50}"
    ) {
        // Any combination of digits, spaces, *, -, /, and commas
        // must produce Ok or Err — never panic.
        let _ = CronSchedule::parse(&input);
    }
}

// ─── Valid step expressions always succeed ───────────────────────────────────

proptest! {
    #[test]
    fn test_cron_valid_step_expressions(step in 1u32..=59) {
        let expr = format!("*/{step} * * * *");
        let result = CronSchedule::parse(&expr);
        prop_assert!(
            result.is_ok(),
            "Step expression '*/{step} * * * *' should parse, got: {:?}",
            result
        );
    }
}
