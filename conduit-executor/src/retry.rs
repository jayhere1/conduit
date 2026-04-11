//! Retry policy and backoff strategy management.
//!
//! This module provides utilities for parsing retry delays and computing
//! exponential backoff for task retries.

use conduit_common::{ConduitError, ConduitResult};
use std::time::Duration;
use tracing::trace;

/// Retry policy configuration
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub delay: Duration,
    pub backoff_factor: f64,
}

impl RetryPolicy {
    /// Create a new retry policy
    pub fn new(max_retries: u32, delay: Duration, backoff_factor: f64) -> Self {
        Self {
            max_retries,
            delay,
            backoff_factor,
        }
    }

    /// Create a retry policy with no retries
    pub fn no_retries() -> Self {
        Self {
            max_retries: 0,
            delay: Duration::from_secs(0),
            backoff_factor: 1.0,
        }
    }

    /// Create a retry policy with fixed delay
    pub fn fixed(max_retries: u32, delay: Duration) -> Self {
        Self {
            max_retries,
            delay,
            backoff_factor: 1.0,
        }
    }

    /// Create a retry policy with exponential backoff
    pub fn exponential(max_retries: u32, initial_delay: Duration, backoff_factor: f64) -> Self {
        Self {
            max_retries,
            delay: initial_delay,
            backoff_factor,
        }
    }

    /// Calculate the delay for a given attempt number
    ///
    /// With exponential backoff: delay * backoff_factor^attempt
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        if attempt == 0 || self.backoff_factor == 1.0 {
            return self.delay;
        }

        let multiplier = self.backoff_factor.powi(attempt as i32);
        let millis = (self.delay.as_millis() as f64 * multiplier) as u64;

        Duration::from_millis(millis)
    }

    /// Calculate the next retry time
    ///
    /// Returns the duration to wait before retrying after the given attempt
    pub fn next_retry_at(&self, attempt: u32) -> Option<Duration> {
        if attempt >= self.max_retries {
            return None;
        }

        Some(self.delay_for_attempt(attempt))
    }

    /// Check if retries are exhausted
    pub fn is_exhausted(&self, attempt: u32) -> bool {
        attempt >= self.max_retries
    }
}

/// Parse a duration string to Duration
///
/// Supports common formats:
/// - "30s" -> 30 seconds
/// - "5m" -> 5 minutes
/// - "1h" -> 1 hour
/// - "2d" -> 2 days
/// - "500ms" -> 500 milliseconds
/// - "90" -> 90 seconds (default if no suffix)
///
/// # Examples
/// ```
/// # use conduit_executor::parse_duration;
/// assert_eq!(parse_duration("30s").unwrap(), std::time::Duration::from_secs(30));
/// assert_eq!(parse_duration("5m").unwrap(), std::time::Duration::from_secs(300));
/// assert_eq!(parse_duration("1h").unwrap(), std::time::Duration::from_secs(3600));
/// ```
pub fn parse_duration(input: &str) -> ConduitResult<Duration> {
    let input = input.trim();

    if input.is_empty() {
        return Err(ConduitError::ConfigError(
            "Duration string cannot be empty".to_string(),
        ));
    }

    // Try to parse as pure number (seconds)
    if let Ok(secs) = input.parse::<u64>() {
        trace!(input = %input, seconds = secs, "Parsed duration as seconds");
        return Ok(Duration::from_secs(secs));
    }

    // Check multi-char suffixes first
    if let Some(num_str) = input.strip_suffix("ms") {
        let value = num_str
            .trim()
            .parse::<f64>()
            .map_err(|_| ConduitError::ConfigError(format!("Invalid duration value: {}", input)))?;
        let duration = Duration::from_millis(value as u64);
        trace!(input = %input, duration_ms = duration.as_millis(), "Parsed duration");
        return Ok(duration);
    }

    // Parse single-char suffix
    let (num_str, suffix) = input.split_at(input.len() - 1);

    let value = num_str
        .trim()
        .parse::<f64>()
        .map_err(|_| ConduitError::ConfigError(format!("Invalid duration value: {}", input)))?;

    let duration = match suffix.to_lowercase().as_str() {
        "s" => Duration::from_secs_f64(value),
        "m" => Duration::from_secs_f64(value * 60.0),
        "h" => Duration::from_secs_f64(value * 3600.0),
        "d" => Duration::from_secs_f64(value * 86400.0),
        _ => {
            return Err(ConduitError::ConfigError(format!(
                "Unknown duration suffix: {}",
                suffix
            )));
        }
    };

    trace!(input = %input, duration_ms = duration.as_millis(), "Parsed duration");
    Ok(duration)
}

/// Format a Duration as a human-readable string
pub fn format_duration(duration: Duration) -> String {
    let total_secs = duration.as_secs();

    if total_secs == 0 {
        return format!("{}ms", duration.subsec_millis());
    }

    let days = total_secs / 86400;
    let remainder = total_secs % 86400;
    let hours = remainder / 3600;
    let remainder = remainder % 3600;
    let minutes = remainder / 60;
    let seconds = remainder % 60;

    if days > 0 {
        format!("{}d {}h {}m", days, hours, minutes)
    } else if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, seconds)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, seconds)
    } else {
        format!("{}s", seconds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration_seconds() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("0s").unwrap(), Duration::from_secs(0));
        assert_eq!(parse_duration("60s").unwrap(), Duration::from_secs(60));
    }

    #[test]
    fn test_parse_duration_minutes() {
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("1m").unwrap(), Duration::from_secs(60));
        assert_eq!(parse_duration("60m").unwrap(), Duration::from_secs(3600));
    }

    #[test]
    fn test_parse_duration_hours() {
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
        assert_eq!(parse_duration("24h").unwrap(), Duration::from_secs(86400));
    }

    #[test]
    fn test_parse_duration_days() {
        assert_eq!(parse_duration("1d").unwrap(), Duration::from_secs(86400));
        assert_eq!(parse_duration("7d").unwrap(), Duration::from_secs(604800));
    }

    #[test]
    fn test_parse_duration_milliseconds() {
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("1000ms").unwrap(), Duration::from_secs(1));
    }

    #[test]
    fn test_parse_duration_plain_number() {
        assert_eq!(parse_duration("30").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("300").unwrap(), Duration::from_secs(300));
    }

    #[test]
    fn test_parse_duration_decimal() {
        assert_eq!(parse_duration("1.5s").unwrap(), Duration::from_millis(1500));
        assert_eq!(parse_duration("2.5m").unwrap(), Duration::from_secs(150));
        assert_eq!(parse_duration("0.5h").unwrap(), Duration::from_secs(1800));
    }

    #[test]
    fn test_parse_duration_whitespace() {
        assert_eq!(parse_duration("  30s  ").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("5 m").unwrap(), Duration::from_secs(300));
    }

    #[test]
    fn test_parse_duration_errors() {
        assert!(parse_duration("").is_err());
        assert!(parse_duration("invalid").is_err());
        assert!(parse_duration("30x").is_err());
        assert!(parse_duration("abc").is_err());
    }

    #[test]
    fn test_retry_policy_creation() {
        let policy = RetryPolicy::new(3, Duration::from_secs(30), 2.0);
        assert_eq!(policy.max_retries, 3);
        assert_eq!(policy.delay, Duration::from_secs(30));
        assert_eq!(policy.backoff_factor, 2.0);
    }

    #[test]
    fn test_retry_policy_no_retries() {
        let policy = RetryPolicy::no_retries();
        assert_eq!(policy.max_retries, 0);
        assert!(policy.next_retry_at(0).is_none());
        assert!(policy.is_exhausted(0));
    }

    #[test]
    fn test_retry_policy_fixed_delay() {
        let policy = RetryPolicy::fixed(3, Duration::from_secs(60));
        assert_eq!(policy.max_retries, 3);
        assert_eq!(policy.backoff_factor, 1.0);

        // All attempts should have same delay
        assert_eq!(policy.delay_for_attempt(0), Duration::from_secs(60));
        assert_eq!(policy.delay_for_attempt(1), Duration::from_secs(60));
        assert_eq!(policy.delay_for_attempt(2), Duration::from_secs(60));
    }

    #[test]
    fn test_retry_policy_exponential_backoff() {
        let policy = RetryPolicy::exponential(5, Duration::from_secs(10), 2.0);

        assert_eq!(policy.delay_for_attempt(0), Duration::from_secs(10));
        assert_eq!(policy.delay_for_attempt(1), Duration::from_secs(20));
        assert_eq!(policy.delay_for_attempt(2), Duration::from_secs(40));
        assert_eq!(policy.delay_for_attempt(3), Duration::from_secs(80));
        assert_eq!(policy.delay_for_attempt(4), Duration::from_secs(160));
    }

    #[test]
    fn test_retry_policy_next_retry_at() {
        let policy = RetryPolicy::fixed(3, Duration::from_secs(30));

        assert!(policy.next_retry_at(0).is_some());
        assert!(policy.next_retry_at(1).is_some());
        assert!(policy.next_retry_at(2).is_some());
        assert!(policy.next_retry_at(3).is_none()); // Exhausted
        assert!(policy.next_retry_at(4).is_none());
    }

    #[test]
    fn test_retry_policy_is_exhausted() {
        let policy = RetryPolicy::fixed(3, Duration::from_secs(30));

        assert!(!policy.is_exhausted(0));
        assert!(!policy.is_exhausted(1));
        assert!(!policy.is_exhausted(2));
        assert!(policy.is_exhausted(3));
        assert!(policy.is_exhausted(4));
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(Duration::from_secs(45)), "45s");
        assert_eq!(format_duration(Duration::from_secs(90)), "1m 30s");
        assert_eq!(format_duration(Duration::from_secs(3661)), "1h 1m 1s");
        assert_eq!(format_duration(Duration::from_secs(86400)), "1d 0h 0m");
    }

    #[test]
    fn test_parse_and_format_roundtrip() {
        let inputs = vec!["30s", "5m", "1h", "2d"];
        for input in inputs {
            let parsed = parse_duration(input).unwrap();
            let formatted = format_duration(parsed);
            assert!(!formatted.is_empty(), "Format failed for {}", input);
        }
    }
}
