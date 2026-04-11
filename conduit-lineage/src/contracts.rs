//! Schema contracts — validate that task outputs match declared expectations.
//!
//! Contracts are the "tests" for schemas. A task declares what it produces
//! (columns, types, non-null guarantees), and the contract validator checks
//! actual output against the declaration.
//!
//! This catches schema drift before it reaches production:
//! - "extract_orders always produces an 'id' column of type INTEGER NOT NULL"
//! - "transform never produces NULL in the 'customer_name' column"
//! - "The schema has at most 20 columns"

use serde::{Deserialize, Serialize};

use crate::schema::{ColumnType, Schema};

/// A contract rule that a schema must satisfy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ContractRule {
    /// Column must exist with a specific type.
    RequiredColumn {
        name: String,
        expected_type: Option<ColumnType>,
        must_be_not_null: bool,
    },

    /// Column must NOT exist (e.g., PII was supposed to be removed).
    ForbiddenColumn { name: String, reason: String },

    /// Schema must have at most N columns.
    MaxColumns(usize),

    /// Schema must have at least N columns.
    MinColumns(usize),

    /// All columns must have descriptions (documentation quality).
    AllColumnsDocumented,

    /// No column should have type Unknown (all types must be resolved).
    NoUnknownTypes,

    /// Specific columns must have a tag (e.g., PII columns tagged as "pii").
    ColumnMustHaveTag { column_name: String, tag: String },

    /// Custom predicate with a description.
    Custom {
        description: String,
        /// Serialized predicate — in practice, a closure or expression.
        /// We store the description for display and the validator handles logic.
        check_name: String,
    },
}

/// A schema contract — a set of rules for a task's output schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaContract {
    /// The task this contract applies to.
    pub task_id: String,
    /// Optional DAG scope.
    pub dag_id: Option<String>,
    /// The rules to validate.
    pub rules: Vec<ContractRule>,
    /// Human-readable description.
    pub description: Option<String>,
}

impl SchemaContract {
    /// Create a new contract for a task.
    pub fn new(task_id: impl Into<String>) -> Self {
        Self {
            task_id: task_id.into(),
            dag_id: None,
            rules: Vec::new(),
            description: None,
        }
    }

    /// Builder: add a required column.
    pub fn require_column(
        mut self,
        name: impl Into<String>,
        column_type: Option<ColumnType>,
        not_null: bool,
    ) -> Self {
        self.rules.push(ContractRule::RequiredColumn {
            name: name.into(),
            expected_type: column_type,
            must_be_not_null: not_null,
        });
        self
    }

    /// Builder: forbid a column.
    pub fn forbid_column(mut self, name: impl Into<String>, reason: impl Into<String>) -> Self {
        self.rules.push(ContractRule::ForbiddenColumn {
            name: name.into(),
            reason: reason.into(),
        });
        self
    }

    /// Builder: set max columns.
    pub fn max_columns(mut self, max: usize) -> Self {
        self.rules.push(ContractRule::MaxColumns(max));
        self
    }

    /// Builder: require all columns documented.
    pub fn require_docs(mut self) -> Self {
        self.rules.push(ContractRule::AllColumnsDocumented);
        self
    }

    /// Builder: require no unknown types.
    pub fn no_unknown_types(mut self) -> Self {
        self.rules.push(ContractRule::NoUnknownTypes);
        self
    }

    /// Builder: add a description.
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }
}

/// A contract violation — one rule that failed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractViolation {
    /// The rule that was violated.
    pub rule: ContractRule,
    /// Human-readable violation message.
    pub message: String,
    /// Severity: error (blocks deployment) or warning (informational).
    pub severity: ViolationSeverity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ViolationSeverity {
    Error,
    Warning,
}

/// Result of contract validation.
#[derive(Debug)]
pub struct ContractResult {
    pub task_id: String,
    pub passed: bool,
    pub violations: Vec<ContractViolation>,
    pub rules_checked: usize,
    pub rules_passed: usize,
}

/// Validates schemas against contracts.
pub struct ContractValidator;

impl ContractValidator {
    /// Validate a schema against a contract.
    pub fn validate(schema: &Schema, contract: &SchemaContract) -> ContractResult {
        let mut violations = Vec::new();
        let rules_checked = contract.rules.len();

        for rule in &contract.rules {
            if let Some(violation) = Self::check_rule(schema, rule) {
                violations.push(violation);
            }
        }

        let rules_passed = rules_checked - violations.len();
        let passed = violations
            .iter()
            .all(|v| v.severity == ViolationSeverity::Warning);

        ContractResult {
            task_id: contract.task_id.clone(),
            passed,
            violations,
            rules_checked,
            rules_passed,
        }
    }

    /// Check a single rule against a schema.
    fn check_rule(schema: &Schema, rule: &ContractRule) -> Option<ContractViolation> {
        match rule {
            ContractRule::RequiredColumn {
                name,
                expected_type,
                must_be_not_null,
            } => match schema.get_column(name) {
                None => Some(ContractViolation {
                    rule: rule.clone(),
                    message: format!("Required column '{}' is missing", name),
                    severity: ViolationSeverity::Error,
                }),
                Some(col) => {
                    if let Some(expected) = expected_type {
                        if &col.column_type != expected {
                            return Some(ContractViolation {
                                rule: rule.clone(),
                                message: format!(
                                    "Column '{}' has type {}, expected {}",
                                    name, col.column_type, expected
                                ),
                                severity: ViolationSeverity::Error,
                            });
                        }
                    }
                    if *must_be_not_null && col.nullable {
                        return Some(ContractViolation {
                            rule: rule.clone(),
                            message: format!("Column '{}' must be NOT NULL but is nullable", name),
                            severity: ViolationSeverity::Error,
                        });
                    }
                    None
                }
            },

            ContractRule::ForbiddenColumn { name, reason } => {
                if schema.has_column(name) {
                    Some(ContractViolation {
                        rule: rule.clone(),
                        message: format!("Forbidden column '{}' exists: {}", name, reason),
                        severity: ViolationSeverity::Error,
                    })
                } else {
                    None
                }
            }

            ContractRule::MaxColumns(max) => {
                if schema.columns.len() > *max {
                    Some(ContractViolation {
                        rule: rule.clone(),
                        message: format!(
                            "Schema has {} columns, maximum is {}",
                            schema.columns.len(),
                            max
                        ),
                        severity: ViolationSeverity::Warning,
                    })
                } else {
                    None
                }
            }

            ContractRule::MinColumns(min) => {
                if schema.columns.len() < *min {
                    Some(ContractViolation {
                        rule: rule.clone(),
                        message: format!(
                            "Schema has {} columns, minimum is {}",
                            schema.columns.len(),
                            min
                        ),
                        severity: ViolationSeverity::Error,
                    })
                } else {
                    None
                }
            }

            ContractRule::AllColumnsDocumented => {
                let undocumented: Vec<&str> = schema
                    .columns
                    .iter()
                    .filter(|c| c.description.is_none())
                    .map(|c| c.name.as_str())
                    .collect();

                if undocumented.is_empty() {
                    None
                } else {
                    Some(ContractViolation {
                        rule: rule.clone(),
                        message: format!("Undocumented columns: {}", undocumented.join(", ")),
                        severity: ViolationSeverity::Warning,
                    })
                }
            }

            ContractRule::NoUnknownTypes => {
                let unknown: Vec<&str> = schema
                    .columns
                    .iter()
                    .filter(|c| c.column_type == ColumnType::Unknown)
                    .map(|c| c.name.as_str())
                    .collect();

                if unknown.is_empty() {
                    None
                } else {
                    Some(ContractViolation {
                        rule: rule.clone(),
                        message: format!("Columns with unknown type: {}", unknown.join(", ")),
                        severity: ViolationSeverity::Error,
                    })
                }
            }

            ContractRule::ColumnMustHaveTag { column_name, tag } => {
                match schema.get_column(column_name) {
                    None => None, // Don't double-report if column is missing
                    Some(col) => {
                        if col.tags.iter().any(|t| t == tag) {
                            None
                        } else {
                            Some(ContractViolation {
                                rule: rule.clone(),
                                message: format!(
                                    "Column '{}' must have tag '{}' but doesn't",
                                    column_name, tag
                                ),
                                severity: ViolationSeverity::Warning,
                            })
                        }
                    }
                }
            }

            ContractRule::Custom { description, .. } => {
                // Custom rules need external evaluation
                // For now, always pass
                tracing::debug!(
                    "Custom contract rule '{}' — skipping (external evaluation needed)",
                    description
                );
                None
            }
        }
    }

    /// Validate multiple contracts against a schema registry.
    pub fn validate_all(
        schemas: &crate::schema::SchemaRegistry,
        contracts: &[SchemaContract],
    ) -> Vec<ContractResult> {
        contracts
            .iter()
            .filter_map(|contract| {
                let dag_id = contract.dag_id.as_deref().unwrap_or("default");
                schemas
                    .get(dag_id, &contract.task_id)
                    .map(|schema| Self::validate(schema, contract))
            })
            .collect()
    }
}

impl std::fmt::Display for ContractResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = if self.passed { "PASSED" } else { "FAILED" };
        writeln!(
            f,
            "Contract for '{}': {} ({}/{} rules passed)",
            self.task_id, status, self.rules_passed, self.rules_checked
        )?;

        for violation in &self.violations {
            let severity = match violation.severity {
                ViolationSeverity::Error => "ERROR",
                ViolationSeverity::Warning => "WARN ",
            };
            writeln!(f, "  [{}] {}", severity, violation.message)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Column, ColumnType, Schema};

    fn sample_schema() -> Schema {
        Schema::new(
            "extract_orders",
            vec![
                Column::new("id", ColumnType::Integer)
                    .not_null()
                    .with_description("Primary key"),
                Column::new("customer_id", ColumnType::Integer).not_null(),
                Column::new(
                    "total",
                    ColumnType::Decimal {
                        precision: 10,
                        scale: 2,
                    },
                )
                .with_description("Order total"),
                Column::new("status", ColumnType::String),
            ],
        )
    }

    #[test]
    fn valid_contract_passes() {
        let schema = sample_schema();
        let contract = SchemaContract::new("extract_orders")
            .require_column("id", Some(ColumnType::Integer), true)
            .require_column("total", None, false);

        let result = ContractValidator::validate(&schema, &contract);
        assert!(result.passed);
        assert!(result.violations.is_empty());
    }

    #[test]
    fn missing_required_column_fails() {
        let schema = sample_schema();
        let contract =
            SchemaContract::new("extract_orders").require_column("nonexistent", None, false);

        let result = ContractValidator::validate(&schema, &contract);
        assert!(!result.passed);
        assert_eq!(result.violations.len(), 1);
        assert!(result.violations[0].message.contains("missing"));
    }

    #[test]
    fn wrong_type_fails() {
        let schema = sample_schema();
        let contract = SchemaContract::new("extract_orders").require_column(
            "id",
            Some(ColumnType::String),
            false,
        );

        let result = ContractValidator::validate(&schema, &contract);
        assert!(!result.passed);
        assert!(result.violations[0].message.contains("type"));
    }

    #[test]
    fn forbidden_column_detected() {
        let schema = Schema::new(
            "anonymized",
            vec![
                Column::new("id", ColumnType::Integer),
                Column::new("ssn", ColumnType::String), // PII that should have been removed
            ],
        );

        let contract = SchemaContract::new("anonymized")
            .forbid_column("ssn", "PII must be removed before loading");

        let result = ContractValidator::validate(&schema, &contract);
        assert!(!result.passed);
        assert!(result.violations[0].message.contains("ssn"));
    }

    #[test]
    fn max_columns_warning() {
        let schema = sample_schema();
        let contract = SchemaContract::new("extract_orders").max_columns(2);

        let result = ContractValidator::validate(&schema, &contract);
        // MaxColumns is a warning, not an error — so it still "passes"
        assert!(result.passed);
        assert_eq!(result.violations.len(), 1);
        assert_eq!(result.violations[0].severity, ViolationSeverity::Warning);
    }

    #[test]
    fn undocumented_columns_detected() {
        let schema = sample_schema();
        let contract = SchemaContract::new("extract_orders").require_docs();

        let result = ContractValidator::validate(&schema, &contract);
        // "customer_id" and "status" lack descriptions
        assert_eq!(result.violations.len(), 1);
        assert!(result.violations[0].message.contains("customer_id"));
    }

    #[test]
    fn unknown_types_detected() {
        let schema = Schema::new(
            "task",
            vec![
                Column::new("id", ColumnType::Integer),
                Column::new("mystery", ColumnType::Unknown),
            ],
        );

        let contract = SchemaContract::new("task").no_unknown_types();
        let result = ContractValidator::validate(&schema, &contract);

        assert!(!result.passed);
        assert!(result.violations[0].message.contains("mystery"));
    }

    #[test]
    fn nullable_violation() {
        let schema = Schema::new(
            "task",
            vec![
                Column::new("id", ColumnType::Integer), // nullable by default
            ],
        );

        let contract =
            SchemaContract::new("task").require_column("id", Some(ColumnType::Integer), true); // require NOT NULL

        let result = ContractValidator::validate(&schema, &contract);
        assert!(!result.passed);
        assert!(result.violations[0].message.contains("NOT NULL"));
    }

    #[test]
    fn display_format() {
        let schema = sample_schema();
        let contract = SchemaContract::new("extract_orders")
            .require_column("missing_col", None, false)
            .max_columns(100);

        let result = ContractValidator::validate(&schema, &contract);
        let output = format!("{}", result);
        assert!(output.contains("FAILED"));
        assert!(output.contains("missing_col"));
    }
}
