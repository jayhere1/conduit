//! Property-based tests for the YAML DAG parser.
//!
//! Verifies that arbitrary input never causes a panic — the parser must always
//! return `Ok` or `Err`.

use std::path::Path;

use conduit_compiler::YamlDagParser;
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_yaml_parse_never_panics(input in "\\PC{0,200}") {
        // Feeding arbitrary strings into the parser must not panic.
        // Ok or Err are both acceptable outcomes.
        let _ = YamlDagParser::parse_string(&input, Path::new("fuzz.yaml"));
    }
}
