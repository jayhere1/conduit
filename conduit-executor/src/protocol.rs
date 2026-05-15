//! Task communication protocol via stdout.
//!
//! Tasks communicate with Conduit via structured stdout lines:
//! - CONDUIT::XCOM::{json} - XCom (cross-communication) data
//! - CONDUIT::LOG::LEVEL::message - Structured logs
//! - CONDUIT::PROGRESS::percent - Task progress (0-100)
//! - CONDUIT::METRIC::name::value - Custom metrics
//!
//! Exit codes:
//! - 0 = success
//! - 1 = failure
//! - 2 = retry (task requests explicit retry)
//! - 3 = skip (task requests to be skipped)

use conduit_common::{ConduitError, ConduitResult};
use serde_json::json;
use std::fmt;
use tracing::trace;

/// Log level for structured logs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogLevel::Debug => write!(f, "DEBUG"),
            LogLevel::Info => write!(f, "INFO"),
            LogLevel::Warn => write!(f, "WARN"),
            LogLevel::Error => write!(f, "ERROR"),
        }
    }
}

impl std::str::FromStr for LogLevel {
    type Err = ConduitError;

    fn from_str(s: &str) -> ConduitResult<Self> {
        match s.to_uppercase().as_str() {
            "DEBUG" => Ok(LogLevel::Debug),
            "INFO" => Ok(LogLevel::Info),
            "WARN" | "WARNING" => Ok(LogLevel::Warn),
            "ERROR" => Ok(LogLevel::Error),
            other => Err(ConduitError::ProtocolError(format!(
                "Unknown log level: {}",
                other
            ))),
        }
    }
}

/// Structured protocol message from task stdout
#[derive(Debug, Clone, PartialEq)]
pub enum ProtocolMessage {
    /// Cross-communication data (XCom)
    XCom {
        key: String,
        value: serde_json::Value,
    },
    /// Structured log message
    Log { level: LogLevel, message: String },
    /// Task progress (0-100%)
    Progress { percent: u8 },
    /// Custom metric
    Metric { name: String, value: f64 },
}

impl fmt::Display for ProtocolMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolMessage::XCom { key, value } => {
                write!(f, "CONDUIT::XCOM::{}", json!({key: value}))
            }
            ProtocolMessage::Log { level, message } => {
                write!(f, "CONDUIT::LOG::{}::{}", level, message)
            }
            ProtocolMessage::Progress { percent } => {
                write!(f, "CONDUIT::PROGRESS::{}", percent)
            }
            ProtocolMessage::Metric { name, value } => {
                write!(f, "CONDUIT::METRIC::{}::{}", name, value)
            }
        }
    }
}

/// Parse a stdout line for protocol messages
///
/// Returns Some(ProtocolMessage) if the line matches the protocol format,
/// None otherwise (indicating regular log output).
///
/// # Examples
/// ```text
/// CONDUIT::XCOM::{"result": 42}
/// CONDUIT::LOG::INFO::Processing complete
/// CONDUIT::PROGRESS::75
/// CONDUIT::METRIC::rows_processed::1000
/// ```
pub fn parse_stdout_line(line: &str) -> Option<ProtocolMessage> {
    let line = line.trim();

    if !line.starts_with("CONDUIT::") {
        return None;
    }

    trace!(line = %line, "Parsing protocol message");

    if let Some(rest) = line.strip_prefix("CONDUIT::XCOM::") {
        parse_xcom(rest)
    } else if let Some(rest) = line.strip_prefix("CONDUIT::LOG::") {
        parse_log(rest)
    } else if let Some(rest) = line.strip_prefix("CONDUIT::PROGRESS::") {
        parse_progress(rest)
    } else if let Some(rest) = line.strip_prefix("CONDUIT::METRIC::") {
        parse_metric(rest)
    } else {
        trace!(line = %line, "Unknown protocol prefix");
        None
    }
}

/// Parse XCOM message: CONDUIT::XCOM::{json}
fn parse_xcom(rest: &str) -> Option<ProtocolMessage> {
    match serde_json::from_str::<serde_json::Value>(rest) {
        Ok(value) => {
            // Extract first key-value pair (assuming single key object)
            if let Some(obj) = value.as_object() {
                if let Some((key, val)) = obj.iter().next() {
                    trace!(key = %key, "Parsed XCom message");
                    return Some(ProtocolMessage::XCom {
                        key: key.clone(),
                        value: val.clone(),
                    });
                }
            }
            None
        }
        Err(e) => {
            trace!(error = %e, "Failed to parse XCOM JSON");
            None
        }
    }
}

/// Parse LOG message: CONDUIT::LOG::LEVEL::message
fn parse_log(rest: &str) -> Option<ProtocolMessage> {
    let mut parts = rest.splitn(2, "::");
    let level_str = parts.next()?;
    let message = parts.next().unwrap_or("");

    match level_str.parse::<LogLevel>() {
        Ok(level) => {
            trace!(level = ?level, message = %message, "Parsed log message");
            Some(ProtocolMessage::Log {
                level,
                message: message.to_string(),
            })
        }
        Err(_) => {
            trace!(level_str = %level_str, "Unknown log level");
            None
        }
    }
}

/// Parse PROGRESS message: CONDUIT::PROGRESS::percent
fn parse_progress(rest: &str) -> Option<ProtocolMessage> {
    match rest.parse::<u8>() {
        Ok(percent) if percent <= 100 => {
            trace!(percent = percent, "Parsed progress message");
            Some(ProtocolMessage::Progress { percent })
        }
        Ok(percent) => {
            trace!(percent = percent, "Progress out of range");
            None
        }
        Err(e) => {
            trace!(error = %e, "Failed to parse progress value");
            None
        }
    }
}

/// Parse METRIC message: CONDUIT::METRIC::name::value
fn parse_metric(rest: &str) -> Option<ProtocolMessage> {
    let (name, value_str) = rest.split_once("::")?;

    match value_str.parse::<f64>() {
        Ok(value) => {
            trace!(name = %name, value = value, "Parsed metric message");
            Some(ProtocolMessage::Metric {
                name: name.to_string(),
                value,
            })
        }
        Err(e) => {
            trace!(error = %e, "Failed to parse metric value");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_level_display() {
        assert_eq!(LogLevel::Debug.to_string(), "DEBUG");
        assert_eq!(LogLevel::Info.to_string(), "INFO");
        assert_eq!(LogLevel::Warn.to_string(), "WARN");
        assert_eq!(LogLevel::Error.to_string(), "ERROR");
    }

    #[test]
    fn test_log_level_parse() {
        assert_eq!("DEBUG".parse::<LogLevel>().unwrap(), LogLevel::Debug);
        assert_eq!("INFO".parse::<LogLevel>().unwrap(), LogLevel::Info);
        assert_eq!("WARN".parse::<LogLevel>().unwrap(), LogLevel::Warn);
        assert_eq!("WARNING".parse::<LogLevel>().unwrap(), LogLevel::Warn);
        assert_eq!("ERROR".parse::<LogLevel>().unwrap(), LogLevel::Error);
        assert!("INVALID".parse::<LogLevel>().is_err());
    }

    #[test]
    fn test_parse_xcom_message() {
        let msg = parse_stdout_line(r#"CONDUIT::XCOM::{"result": 42}"#);

        if let Some(ProtocolMessage::XCom { key, value }) = msg {
            assert_eq!(key, "result");
            assert_eq!(value.as_i64(), Some(42));
        } else {
            panic!("Expected XCom message, got: {:?}", msg);
        }
    }

    #[test]
    fn test_parse_xcom_complex() {
        let msg = parse_stdout_line(r#"CONDUIT::XCOM::{"data": {"nested": "value"}}"#);

        if let Some(ProtocolMessage::XCom { key, value }) = msg {
            assert_eq!(key, "data");
            assert!(value.is_object());
        } else {
            panic!("Expected XCom message, got: {:?}", msg);
        }
    }

    #[test]
    fn test_parse_log_message() {
        let msg = parse_stdout_line("CONDUIT::LOG::INFO::Processing complete");

        if let Some(ProtocolMessage::Log { level, message }) = msg {
            assert_eq!(level, LogLevel::Info);
            assert_eq!(message, "Processing complete");
        } else {
            panic!("Expected Log message, got: {:?}", msg);
        }
    }

    #[test]
    fn test_parse_log_with_colons() {
        let msg = parse_stdout_line("CONDUIT::LOG::WARN::Time: 12:30:45");

        if let Some(ProtocolMessage::Log { level, message }) = msg {
            assert_eq!(level, LogLevel::Warn);
            assert_eq!(message, "Time: 12:30:45");
        } else {
            panic!("Expected Log message, got: {:?}", msg);
        }
    }

    #[test]
    fn test_parse_progress_message() {
        let msg = parse_stdout_line("CONDUIT::PROGRESS::75");

        if let Some(ProtocolMessage::Progress { percent }) = msg {
            assert_eq!(percent, 75);
        } else {
            panic!("Expected Progress message, got: {:?}", msg);
        }
    }

    #[test]
    fn test_parse_progress_boundary() {
        assert!(parse_stdout_line("CONDUIT::PROGRESS::0").is_some());
        assert!(parse_stdout_line("CONDUIT::PROGRESS::100").is_some());
        assert!(parse_stdout_line("CONDUIT::PROGRESS::101").is_none());
    }

    #[test]
    fn test_parse_metric_message() {
        let msg = parse_stdout_line("CONDUIT::METRIC::rows_processed::1000");

        if let Some(ProtocolMessage::Metric { name, value }) = msg {
            assert_eq!(name, "rows_processed");
            assert_eq!(value, 1000.0);
        } else {
            panic!("Expected Metric message, got: {:?}", msg);
        }
    }

    #[test]
    fn test_parse_metric_float() {
        let msg = parse_stdout_line("CONDUIT::METRIC::latency_ms::45.5");

        if let Some(ProtocolMessage::Metric { name, value }) = msg {
            assert_eq!(name, "latency_ms");
            assert_eq!(value, 45.5);
        } else {
            panic!("Expected Metric message, got: {:?}", msg);
        }
    }

    #[test]
    fn test_ignore_regular_output() {
        assert!(parse_stdout_line("Regular log output").is_none());
        assert!(parse_stdout_line("Some other message").is_none());
        assert!(parse_stdout_line("").is_none());
    }

    #[test]
    fn test_protocol_message_display() {
        let xcom = ProtocolMessage::XCom {
            key: "result".to_string(),
            value: json!(42),
        };
        let display = xcom.to_string();
        assert!(display.contains("CONDUIT::XCOM::"));

        let log = ProtocolMessage::Log {
            level: LogLevel::Info,
            message: "Test".to_string(),
        };
        assert_eq!(log.to_string(), "CONDUIT::LOG::INFO::Test");

        let progress = ProtocolMessage::Progress { percent: 50 };
        assert_eq!(progress.to_string(), "CONDUIT::PROGRESS::50");

        let metric = ProtocolMessage::Metric {
            name: "count".to_string(),
            value: 100.0,
        };
        assert_eq!(metric.to_string(), "CONDUIT::METRIC::count::100");
    }
}
