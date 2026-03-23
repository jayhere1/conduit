//! Cron expression parsing and scheduling.
//!
//! Supports standard 5-field cron expressions:
//! ```text
//! minute hour day-of-month month day-of-week
//! (0-59) (0-23) (1-31) (1-12) (0-6, 0=Sunday)
//! ```
//!
//! Features:
//! - Wildcards: `*`
//! - Ranges: `1-5`
//! - Steps: `*/15` (every 15 units)
//! - Lists: `1,3,5`
//!
//! Examples:
//! - `0 6 * * *` - Every day at 6:00 AM
//! - `30 2 * * *` - Every day at 2:30 AM
//! - `0 */4 * * *` - Every 4 hours
//! - `0 0 1 * *` - First day of every month at midnight
//! - `0 0 * * 1` - Every Monday at midnight

use chrono::{DateTime, Datelike, Timelike, Utc};
use std::collections::BTreeSet;

#[derive(Debug, Clone)]
pub struct CronSchedule {
    minutes: BTreeSet<u32>,
    hours: BTreeSet<u32>,
    days: BTreeSet<u32>,
    months: BTreeSet<u32>,
    weekdays: BTreeSet<u32>,
    days_is_wildcard: bool,
    weekdays_is_wildcard: bool,
}

impl CronSchedule {
    /// Parse a 5-field cron expression.
    ///
    /// # Errors
    /// Returns an error if the expression is malformed or contains invalid values.
    pub fn parse(expr: &str) -> Result<Self, String> {
        let parts: Vec<&str> = expr.trim().split_whitespace().collect();
        if parts.len() != 5 {
            return Err(format!(
                "Expected 5 fields, got {}",
                parts.len()
            ));
        }

        Ok(CronSchedule {
            minutes: parse_field(parts[0], 0, 59)?,
            hours: parse_field(parts[1], 0, 23)?,
            days: parse_field(parts[2], 1, 31)?,
            months: parse_field(parts[3], 1, 12)?,
            weekdays: parse_field(parts[4], 0, 6)?,
            days_is_wildcard: parts[2] == "*",
            weekdays_is_wildcard: parts[4] == "*",
        })
    }

    /// Check if this cron schedule is due at the given timestamp.
    pub fn is_due(&self, dt: DateTime<Utc>) -> bool {
        let minute = dt.minute() as u32;
        let hour = dt.hour() as u32;
        let day = dt.day() as u32;
        let month = dt.month() as u32;
        let weekday = dt.weekday().number_from_sunday() as u32 - 1; // 0=Sunday

        let day_match = match (self.days_is_wildcard, self.weekdays_is_wildcard) {
            (true, true) => true,                          // both wildcard: any day
            (false, true) => self.days.contains(&day),     // only day-of-month specified
            (true, false) => self.weekdays.contains(&weekday), // only weekday specified
            (false, false) => self.days.contains(&day) || self.weekdays.contains(&weekday), // both: OR
        };

        self.minutes.contains(&minute)
            && self.hours.contains(&hour)
            && self.months.contains(&month)
            && day_match
    }

    /// Calculate the next scheduled time after the given timestamp.
    pub fn next_from(&self, dt: DateTime<Utc>) -> Option<DateTime<Utc>> {
        // Start from the next minute
        let mut current = dt + chrono::Duration::minutes(1);
        current = current.with_second(0).unwrap_or(current);
        current = current.with_nanosecond(0).unwrap_or(current);

        // Search up to 4 years ahead to find next occurrence
        for _ in 0..4 * 365 * 24 * 60 {
            if self.is_due(current) {
                return Some(current);
            }
            current = current + chrono::Duration::minutes(1);
        }
        None
    }
}

/// Parse a single cron field.
fn parse_field(field: &str, min: u32, max: u32) -> Result<BTreeSet<u32>, String> {
    if field == "*" {
        return Ok((min..=max).collect());
    }

    let mut result = BTreeSet::new();

    // Handle step expressions like "*/15"
    if field.starts_with("*/") {
        let step_str = field.strip_prefix("*/").unwrap();
        let step: u32 = step_str.parse().map_err(|_| {
            format!("Invalid step value: {}", step_str)
        })?;

        if step == 0 {
            return Err("Step must be > 0".to_string());
        }

        let mut val = min;
        while val <= max {
            result.insert(val);
            val += step;
        }
        return Ok(result);
    }

    // Handle comma-separated values
    for part in field.split(',') {
        if let Some(dash_pos) = part.find('-') {
            // Range like "1-5"
            let start_str = &part[..dash_pos];
            let end_str = &part[dash_pos + 1..];

            let start: u32 = start_str.parse().map_err(|_| {
                format!("Invalid range start: {}", start_str)
            })?;
            let end: u32 = end_str.parse().map_err(|_| {
                format!("Invalid range end: {}", end_str)
            })?;

            if start > end {
                return Err(format!(
                    "Invalid range: {}-{} (start > end)",
                    start, end
                ));
            }

            for val in start..=end {
                if val < min || val > max {
                    return Err(format!(
                        "Value {} out of range [{}, {}]",
                        val, min, max
                    ));
                }
                result.insert(val);
            }
        } else {
            // Single value
            let val: u32 = part.parse().map_err(|_| {
                format!("Invalid value: {}", part)
            })?;

            if val < min || val > max {
                return Err(format!(
                    "Value {} out of range [{}, {}]",
                    val, min, max
                ));
            }
            result.insert(val);
        }
    }

    if result.is_empty() {
        return Err("Field must contain at least one value".to_string());
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_parse_wildcard() {
        let cron = CronSchedule::parse("* * * * *").unwrap();
        let dt = Utc.with_ymd_and_hms(2026, 3, 22, 10, 30, 0).unwrap();
        assert!(cron.is_due(dt));
    }

    #[test]
    fn test_parse_specific_time() {
        let cron = CronSchedule::parse("30 10 * * *").unwrap();
        let dt = Utc.with_ymd_and_hms(2026, 3, 22, 10, 30, 0).unwrap();
        assert!(cron.is_due(dt));

        let dt2 = Utc.with_ymd_and_hms(2026, 3, 22, 10, 31, 0).unwrap();
        assert!(!cron.is_due(dt2));
    }

    #[test]
    fn test_parse_range() {
        let cron = CronSchedule::parse("0 6-8 * * *").unwrap();
        let dt6 = Utc.with_ymd_and_hms(2026, 3, 22, 6, 0, 0).unwrap();
        let dt7 = Utc.with_ymd_and_hms(2026, 3, 22, 7, 0, 0).unwrap();
        let dt8 = Utc.with_ymd_and_hms(2026, 3, 22, 8, 0, 0).unwrap();
        let dt5 = Utc.with_ymd_and_hms(2026, 3, 22, 5, 0, 0).unwrap();
        let dt9 = Utc.with_ymd_and_hms(2026, 3, 22, 9, 0, 0).unwrap();

        assert!(cron.is_due(dt6));
        assert!(cron.is_due(dt7));
        assert!(cron.is_due(dt8));
        assert!(!cron.is_due(dt5));
        assert!(!cron.is_due(dt9));
    }

    #[test]
    fn test_parse_step() {
        let cron = CronSchedule::parse("0 */4 * * *").unwrap();
        let dt0 = Utc.with_ymd_and_hms(2026, 3, 22, 0, 0, 0).unwrap();
        let dt4 = Utc.with_ymd_and_hms(2026, 3, 22, 4, 0, 0).unwrap();
        let dt8 = Utc.with_ymd_and_hms(2026, 3, 22, 8, 0, 0).unwrap();
        let dt1 = Utc.with_ymd_and_hms(2026, 3, 22, 1, 0, 0).unwrap();

        assert!(cron.is_due(dt0));
        assert!(cron.is_due(dt4));
        assert!(cron.is_due(dt8));
        assert!(!cron.is_due(dt1));
    }

    #[test]
    fn test_parse_list() {
        let cron = CronSchedule::parse("0 6,12,18 * * *").unwrap();
        let dt6 = Utc.with_ymd_and_hms(2026, 3, 22, 6, 0, 0).unwrap();
        let dt12 = Utc.with_ymd_and_hms(2026, 3, 22, 12, 0, 0).unwrap();
        let dt18 = Utc.with_ymd_and_hms(2026, 3, 22, 18, 0, 0).unwrap();
        let dt9 = Utc.with_ymd_and_hms(2026, 3, 22, 9, 0, 0).unwrap();

        assert!(cron.is_due(dt6));
        assert!(cron.is_due(dt12));
        assert!(cron.is_due(dt18));
        assert!(!cron.is_due(dt9));
    }

    #[test]
    fn test_parse_daily() {
        let cron = CronSchedule::parse("0 6 * * *").unwrap();
        let dt_mon = Utc.with_ymd_and_hms(2026, 3, 22, 6, 0, 0).unwrap(); // Sunday
        let dt_tue = Utc.with_ymd_and_hms(2026, 3, 23, 6, 0, 0).unwrap(); // Monday

        assert!(cron.is_due(dt_mon));
        assert!(cron.is_due(dt_tue));
    }

    #[test]
    fn test_parse_monthly() {
        let cron = CronSchedule::parse("0 0 1 * *").unwrap();
        let dt_1st = Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0).unwrap();
        let dt_2nd = Utc.with_ymd_and_hms(2026, 3, 2, 0, 0, 0).unwrap();

        assert!(cron.is_due(dt_1st));
        assert!(!cron.is_due(dt_2nd));
    }

    #[test]
    fn test_next_from() {
        let cron = CronSchedule::parse("0 6 * * *").unwrap();
        let dt = Utc.with_ymd_and_hms(2026, 3, 22, 5, 0, 0).unwrap();
        let next = cron.next_from(dt).unwrap();
        let expected = Utc.with_ymd_and_hms(2026, 3, 22, 6, 0, 0).unwrap();
        assert_eq!(next, expected);
    }

    #[test]
    fn test_invalid_field_count() {
        assert!(CronSchedule::parse("0 6 * *").is_err());
        assert!(CronSchedule::parse("0 6 * * * *").is_err());
    }

    #[test]
    fn test_invalid_range() {
        assert!(CronSchedule::parse("0 8-6 * * *").is_err());
    }

    #[test]
    fn test_out_of_bounds() {
        assert!(CronSchedule::parse("60 * * * *").is_err());
        assert!(CronSchedule::parse("0 24 * * *").is_err());
    }
}
