//! Python DAG file parser using tree-sitter.
//!
//! Extracts DAG and task definitions from Python source files by parsing
//! the AST — without executing any Python code. This enables:
//! - Parallel parsing across all CPU cores
//! - Compile-time cycle detection
//! - IDE integration (errors appear as red squiggles, not runtime crashes)

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

use conduit_common::contracts::TaskContracts;
use conduit_common::dag::*;
use conduit_common::error::{ConduitError, ConduitResult};

/// The DAG parser — extracts DAG definitions from Python source files.
pub struct DagParser {
    parser: tree_sitter::Parser,
}

/// Raw parsed data before dependency resolution.
#[derive(Debug)]
pub struct ParsedDag {
    pub id: String,
    pub description: Option<String>,
    pub schedule: Option<String>,
    pub tags: Vec<String>,
    pub max_active_runs: u32,
    pub on_failure: Option<String>,
    pub tasks: Vec<ParsedTask>,
    pub source_file: String,
    /// Opt-in: when true, cross-task lineage stitching escalates unresolved
    /// column references from warnings to compile errors.
    pub lineage_strict: bool,
}

/// A task as extracted from the AST (before dependency resolution).
#[derive(Debug)]
pub struct ParsedTask {
    pub id: String,
    pub task_type: TaskType,
    pub retries: u32,
    pub retry_delay: Option<String>,
    pub pool: Option<String>,
    pub timeout: Option<String>,
    pub priority: i32,
    /// Raw dependency references (task function names used as arguments).
    pub raw_dependencies: Vec<String>,
    /// Data quality contracts (from YAML or Python decorator).
    pub contracts: Option<TaskContracts>,
    /// Verbatim text of the function's parameter list, e.g. `(data=greet)`.
    /// Used by `extract_default_arg_deps` to discover deps expressed via the
    /// SDK-documented `def fn(param=other_task)` pattern.
    pub parameters_text: String,
    /// Datasets read by this task, as declared via `@task(inputs=[…])`.
    pub inputs: Vec<Dataset>,
    /// Datasets written by this task, as declared via `@task(outputs=[…])`.
    pub outputs: Vec<Dataset>,
}

impl DagParser {
    /// Create a new DAG parser with the Python tree-sitter grammar.
    pub fn new() -> ConduitResult<Self> {
        let mut parser = tree_sitter::Parser::new();
        let language = tree_sitter_python::LANGUAGE;
        parser
            .set_language(&language.into())
            .map_err(|e| ConduitError::ParseError {
                file: "<init>".to_string(),
                message: format!("Failed to set Python language: {}", e),
            })?;

        Ok(Self { parser })
    }

    /// Parse a single Python file and extract all DAG definitions.
    pub fn parse_file(&mut self, path: &Path) -> ConduitResult<Vec<ParsedDag>> {
        let source = std::fs::read_to_string(path).map_err(|e| ConduitError::ParseError {
            file: path.display().to_string(),
            message: format!("Failed to read file: {}", e),
        })?;

        self.parse_source(&source, &path.display().to_string())
    }

    /// Parse Python source code and extract DAG definitions.
    pub fn parse_source(&mut self, source: &str, file_name: &str) -> ConduitResult<Vec<ParsedDag>> {
        let tree = self
            .parser
            .parse(source, None)
            .ok_or_else(|| ConduitError::ParseError {
                file: file_name.to_string(),
                message: "tree-sitter failed to parse file".to_string(),
            })?;

        let root = tree.root_node();
        let source_bytes = source.as_bytes();

        let mut dags = Vec::new();

        // Walk top-level decorated function definitions looking for @dag decorator
        let mut cursor = root.walk();
        for child in root.children(&mut cursor) {
            if child.kind() == "decorated_definition" {
                if let Some(dag) = self.try_parse_dag(&child, source_bytes, file_name)? {
                    debug!(dag_id = %dag.id, tasks = dag.tasks.len(), "Parsed DAG");
                    dags.push(dag);
                }
            }
        }

        if dags.is_empty() {
            debug!(file = file_name, "No DAG definitions found");
        }

        Ok(dags)
    }

    /// Try to parse a decorated_definition as a @dag-decorated function.
    fn try_parse_dag(
        &self,
        node: &tree_sitter::Node,
        source: &[u8],
        file_name: &str,
    ) -> ConduitResult<Option<ParsedDag>> {
        // Find the @dag decorator
        let mut has_dag_decorator = false;
        let mut dag_args = HashMap::new();

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            if child.kind() == "decorator" {
                let decorator_text = self.node_text(&child, source);
                if decorator_text.contains("@dag") {
                    has_dag_decorator = true;
                    dag_args = self.extract_decorator_args(&child, source);
                }
            }
        }

        if !has_dag_decorator {
            return Ok(None);
        }

        // Find the function definition
        let func_def = node
            .children(&mut node.walk())
            .find(|c| c.kind() == "function_definition")
            .ok_or_else(|| ConduitError::ParseError {
                file: file_name.to_string(),
                message: "Decorated definition has no function".to_string(),
            })?;

        // Extract function name as DAG ID
        let dag_id = func_def
            .child_by_field_name("name")
            .map(|n| self.node_text(&n, source))
            .unwrap_or_else(|| "unknown".to_string());

        // Extract docstring as description
        let description = self.extract_docstring(&func_def, source);

        // Parse tasks inside the function body
        let tasks = self.extract_tasks(&func_def, source, file_name)?;

        Ok(Some(ParsedDag {
            id: dag_id,
            description,
            schedule: dag_args.get("schedule").cloned(),
            tags: dag_args
                .get("tags")
                .map(|t| {
                    t.trim_matches(|c| c == '[' || c == ']')
                        .split(',')
                        .map(|s| s.trim().trim_matches('"').to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
            max_active_runs: dag_args
                .get("max_active_runs")
                .and_then(|s| s.parse().ok())
                .unwrap_or(1),
            on_failure: dag_args.get("on_failure").cloned(),
            tasks,
            source_file: file_name.to_string(),
            lineage_strict: dag_args
                .get("lineage_strict")
                .map(|v| v.trim().eq_ignore_ascii_case("true"))
                .unwrap_or(false),
        }))
    }

    /// Extract tasks (nested @task-decorated functions) from a DAG function body.
    fn extract_tasks(
        &self,
        func_def: &tree_sitter::Node,
        source: &[u8],
        file_name: &str,
    ) -> ConduitResult<Vec<ParsedTask>> {
        let mut tasks = Vec::new();

        let body = match func_def.child_by_field_name("body") {
            Some(b) => b,
            None => return Ok(tasks),
        };

        // Walk direct children of the body block
        let mut cursor = body.walk();
        for child in body.children(&mut cursor) {
            if child.kind() == "decorated_definition" {
                if let Some(task) = self.try_parse_task(&child, source, file_name)? {
                    tasks.push(task);
                }
            }
        }

        // Fallback: if no tasks found via direct children, do a recursive walk
        // (some tree-sitter-python versions nest decorated_definitions differently)
        if tasks.is_empty() {
            self.find_tasks_recursive(&body, source, file_name, &mut tasks)?;
        }

        // Also parse the function call chain at the bottom of the DAG function
        // to extract data-flow dependencies (e.g., `cleaned = transform(raw)`)
        self.extract_call_chain_deps(&body, source, &mut tasks);

        // Extract data-flow deps expressed as parameter defaults,
        // the SDK's documented pattern: `def fn(data=upstream_task)`.
        Self::extract_default_arg_deps(&mut tasks);

        Ok(tasks)
    }

    /// Discover dependencies expressed via parameter defaults referencing
    /// another task by name, e.g. `def farewell(data=greet)`. This is the
    /// pattern documented in `conduit_sdk.__init__` and used by
    /// `conduit init`'s scaffolded DAG; without this pass, those DAGs run
    /// in the wrong order.
    fn extract_default_arg_deps(tasks: &mut [ParsedTask]) {
        let task_names: Vec<String> = tasks.iter().map(|t| t.id.clone()).collect();
        for task in tasks.iter_mut() {
            // Strip surrounding parens so we can scan parameter clauses uniformly.
            let params = task
                .parameters_text
                .trim()
                .trim_start_matches('(')
                .trim_end_matches(')');
            for clause in params.split(',') {
                let Some(eq_pos) = clause.find('=') else {
                    continue;
                };
                let default = clause[eq_pos + 1..].trim();
                // Strip optional type annotation in the LHS of `=` (we only
                // care about the default value identifier on the RHS).
                let default = default.trim_end_matches(|c: char| c == ',' || c.is_whitespace());
                for name in &task_names {
                    if default == name && name != &task.id && !task.raw_dependencies.contains(name)
                    {
                        task.raw_dependencies.push(name.clone());
                    }
                }
            }
        }
    }

    /// Recursively walk a node tree to find @task-decorated functions.
    fn find_tasks_recursive(
        &self,
        node: &tree_sitter::Node,
        source: &[u8],
        file_name: &str,
        tasks: &mut Vec<ParsedTask>,
    ) -> ConduitResult<()> {
        let mut cursor = node.walk();
        if cursor.goto_first_child() {
            loop {
                let child = cursor.node();
                if child.kind() == "decorated_definition" {
                    if let Some(task) = self.try_parse_task(&child, source, file_name)? {
                        tasks.push(task);
                    }
                } else if child.kind() != "function_definition" {
                    // Recurse into non-function children (avoid descending into nested
                    // function bodies which would be separate DAGs)
                    self.find_tasks_recursive(&child, source, file_name, tasks)?;
                }
                if !cursor.goto_next_sibling() {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Try to parse a decorated function as a @task.
    fn try_parse_task(
        &self,
        node: &tree_sitter::Node,
        source: &[u8],
        file_name: &str,
    ) -> ConduitResult<Option<ParsedTask>> {
        let mut has_task_decorator = false;
        let mut task_args = HashMap::new();

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            if child.kind() == "decorator" {
                let text = self.node_text(&child, source);
                if text.contains("@task") {
                    has_task_decorator = true;
                    task_args = self.extract_decorator_args(&child, source);
                } else if text.contains("@sensor") {
                    has_task_decorator = true;
                    task_args = self.extract_decorator_args(&child, source);
                    task_args.insert("_is_sensor".to_string(), "true".to_string());
                }
            }
        }

        if !has_task_decorator {
            return Ok(None);
        }

        let func_def = node
            .children(&mut node.walk())
            .find(|c| c.kind() == "function_definition")
            .ok_or_else(|| ConduitError::ParseError {
                file: file_name.to_string(),
                message: "Task decorator has no function".to_string(),
            })?;

        let task_id = func_def
            .child_by_field_name("name")
            .map(|n| self.node_text(&n, source))
            .unwrap_or_else(|| "unknown".to_string());

        let task_type = if task_args.contains_key("_is_sensor") {
            TaskType::Sensor {
                sensor_type: "python".to_string(),
                poke_interval: task_args.get("poke_interval").cloned(),
            }
        } else {
            TaskType::Python {
                module: String::new(), // Resolved later from file path
                function: task_id.clone(),
            }
        };

        let parameters_text = func_def
            .child_by_field_name("parameters")
            .map(|n| self.node_text(&n, source))
            .unwrap_or_default();

        let inputs = match task_args.get("inputs") {
            Some(text) => parse_dataset_list(text).map_err(|e| ConduitError::ParseError {
                file: file_name.to_string(),
                message: format!("@task '{}': invalid inputs=…: {}", task_id, e),
            })?,
            None => Vec::new(),
        };
        let outputs = match task_args.get("outputs") {
            Some(text) => parse_dataset_list(text).map_err(|e| ConduitError::ParseError {
                file: file_name.to_string(),
                message: format!("@task '{}': invalid outputs=…: {}", task_id, e),
            })?,
            None => Vec::new(),
        };

        Ok(Some(ParsedTask {
            id: task_id,
            task_type,
            retries: task_args
                .get("retries")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
            retry_delay: task_args.get("retry_delay").cloned(),
            pool: task_args.get("pool").cloned(),
            timeout: task_args.get("timeout").cloned(),
            priority: task_args
                .get("priority")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
            raw_dependencies: Vec::new(),
            contracts: None, // Python contracts are extracted via decorator analysis (future)
            parameters_text,
            inputs,
            outputs,
        }))
    }

    /// Extract data-flow dependencies from the call chain at the bottom of a DAG function.
    /// e.g., `raw = extract(date)` then `cleaned = transform(raw)` means transform depends on extract.
    fn extract_call_chain_deps(
        &self,
        body: &tree_sitter::Node,
        source: &[u8],
        tasks: &mut [ParsedTask],
    ) {
        // Map variable names to task function names
        let mut var_to_task: HashMap<String, String> = HashMap::new();
        let task_names: Vec<String> = tasks.iter().map(|t| t.id.clone()).collect();

        let mut cursor = body.walk();
        for child in body.children(&mut cursor) {
            // Look for assignment statements: `var = task_func(args)`
            if child.kind() == "expression_statement" {
                let text = self.node_text(&child, source);
                // Simple heuristic: look for `var = func_name(...)` patterns
                if let Some((var, func)) = self.parse_assignment(&text, &task_names) {
                    var_to_task.insert(var, func);
                }
            } else if child.kind() == "assignment" {
                let text = self.node_text(&child, source);
                if let Some((var, func)) = self.parse_assignment(&text, &task_names) {
                    var_to_task.insert(var, func);
                }
            }
        }

        // Now resolve: if a task call uses a variable that maps to another task,
        // that's a data-flow dependency.
        // Second pass: extract function call arguments
        let mut cursor2 = body.walk();
        for child in body.children(&mut cursor2) {
            let text = self.node_text(&child, source);
            for task in tasks.iter_mut() {
                // Check if this line calls this task and passes variables
                if text.contains(&format!("{}(", task.id)) {
                    for (var, source_task) in &var_to_task {
                        if text.contains(var)
                            && source_task != &task.id
                            && !task.raw_dependencies.contains(source_task)
                        {
                            task.raw_dependencies.push(source_task.clone());
                        }
                    }
                }
            }
        }
    }

    /// Parse an assignment like `raw = extract_orders(date)` and return (var_name, task_name).
    fn parse_assignment(&self, text: &str, task_names: &[String]) -> Option<(String, String)> {
        let parts: Vec<&str> = text.splitn(2, '=').collect();
        if parts.len() != 2 {
            return None;
        }

        let var = parts[0].trim().to_string();
        let rhs = parts[1].trim();

        for name in task_names {
            if rhs.starts_with(&format!("{}(", name)) || rhs.starts_with(&format!(" {}(", name)) {
                return Some((var, name.clone()));
            }
        }
        None
    }

    /// Extract decorator keyword arguments.
    fn extract_decorator_args(
        &self,
        decorator: &tree_sitter::Node,
        source: &[u8],
    ) -> HashMap<String, String> {
        let mut args = HashMap::new();
        let text = self.node_text(decorator, source);

        // Extract content between outer parentheses
        if let Some(start) = text.find('(') {
            if let Some(end) = text.rfind(')') {
                let inner = &text[start + 1..end];
                // Parse keyword arguments: key=value, key="value", key=[value]
                for part in Self::split_args(inner) {
                    let kv: Vec<&str> = part.splitn(2, '=').collect();
                    if kv.len() == 2 {
                        let key = kv[0].trim().to_string();
                        let val = kv[1].trim().trim_matches('"').to_string();
                        args.insert(key, val);
                    }
                }
            }
        }

        args
    }

    /// Split decorator arguments respecting brackets and quotes.
    fn split_args(s: &str) -> Vec<String> {
        let mut parts = Vec::new();
        let mut current = String::new();
        let mut depth = 0;
        let mut in_string = false;
        let mut string_char = '"';

        for ch in s.chars() {
            match ch {
                '"' | '\'' if !in_string => {
                    in_string = true;
                    string_char = ch;
                    current.push(ch);
                }
                c if in_string && c == string_char => {
                    in_string = false;
                    current.push(ch);
                }
                '[' | '(' if !in_string => {
                    depth += 1;
                    current.push(ch);
                }
                ']' | ')' if !in_string => {
                    depth -= 1;
                    current.push(ch);
                }
                ',' if depth == 0 && !in_string => {
                    let trimmed = current.trim().to_string();
                    if !trimmed.is_empty() {
                        parts.push(trimmed);
                    }
                    current.clear();
                }
                _ => current.push(ch),
            }
        }

        let trimmed = current.trim().to_string();
        if !trimmed.is_empty() {
            parts.push(trimmed);
        }

        parts
    }

    /// Extract the docstring from a function definition.
    fn extract_docstring(&self, func_def: &tree_sitter::Node, source: &[u8]) -> Option<String> {
        let body = func_def.child_by_field_name("body")?;
        let first_stmt = body.child(0)?;

        if first_stmt.kind() == "expression_statement" {
            let expr = first_stmt.child(0)?;
            if expr.kind() == "string" {
                let text = self.node_text(&expr, source);
                return Some(text.trim_matches('"').trim_matches('\'').trim().to_string());
            }
        }
        None
    }

    /// Get the text content of a tree-sitter node.
    fn node_text(&self, node: &tree_sitter::Node, source: &[u8]) -> String {
        node.utf8_text(source).unwrap_or("").to_string()
    }

    /// Parse all .py files in a directory (recursively).
    pub fn parse_directory(&mut self, dir: &Path) -> ConduitResult<Vec<ParsedDag>> {
        let mut all_dags = Vec::new();
        let entries = Self::find_python_files(dir)?;

        info!(count = entries.len(), dir = %dir.display(), "Found Python files");

        for path in entries {
            match self.parse_file(&path) {
                Ok(dags) => all_dags.extend(dags),
                Err(e) => {
                    warn!(file = %path.display(), error = %e, "Failed to parse file");
                }
            }
        }

        info!(dags = all_dags.len(), "Total DAGs parsed");
        Ok(all_dags)
    }

    /// Recursively find all .py files in a directory.
    fn find_python_files(dir: &Path) -> ConduitResult<Vec<PathBuf>> {
        let mut files = Vec::new();

        if !dir.exists() {
            return Err(ConduitError::FileNotFound(dir.display().to_string()));
        }

        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                files.extend(Self::find_python_files(&path)?);
            } else if path.extension().is_some_and(|ext| ext == "py") {
                files.push(path);
            }
        }

        Ok(files)
    }
}

impl Default for DagParser {
    fn default() -> Self {
        Self::new().expect("Failed to initialize DAG parser")
    }
}

// ── Dataset list parser for @task(inputs=…, outputs=…) ────────────────
//
// Tree-sitter doesn't give us a typed AST per Python expression — the
// existing decorator extractor only captures keyword-argument values as
// verbatim text. We re-parse that text to produce strongly-typed
// `Dataset` literals, rejecting anything that isn't a static
// `Dataset(...)` / `ColumnSpec(...)` call.

/// Parse the text following `outputs=` or `inputs=`. Must be a list literal
/// of `Dataset(...)` calls. Returns an error pointing at the first
/// non-statically-resolvable construct.
fn parse_dataset_list(text: &str) -> Result<Vec<Dataset>, String> {
    let inner = strip_list_brackets(text)?;
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }

    let mut out = Vec::new();
    for raw_item in DagParser::split_args(inner) {
        let item = raw_item.trim();
        let (ctor, args_inner) = parse_call(item)?;
        if ctor != "Dataset" {
            return Err(format!(
                "expected `Dataset(...)`, got `{}(…)` (lineage decorators may only use Dataset/ColumnSpec literals)",
                ctor
            ));
        }
        out.push(parse_dataset_args(args_inner)?);
    }
    Ok(out)
}

fn parse_dataset_args(text: &str) -> Result<Dataset, String> {
    let parts = DagParser::split_args(text);
    if parts.is_empty() {
        return Err("Dataset() requires a name argument".to_string());
    }

    let mut name: Option<String> = None;
    let mut columns: Vec<ColumnSpec> = Vec::new();

    for (idx, raw) in parts.iter().enumerate() {
        let part = raw.trim();
        if let Some(eq) = top_level_eq_pos(part) {
            let key = part[..eq].trim();
            let val = part[eq + 1..].trim();
            match key {
                "name" => name = Some(parse_string_literal(val)?),
                "columns" => columns = parse_column_list(val)?,
                other => return Err(format!("Dataset has no field `{}`", other)),
            }
        } else if idx == 0 {
            name = Some(parse_string_literal(part)?);
        } else {
            return Err(format!("unexpected positional Dataset arg: `{}`", part));
        }
    }

    let name = name.ok_or_else(|| "Dataset(...) missing name".to_string())?;
    Ok(Dataset::new(name, columns))
}

fn parse_column_list(text: &str) -> Result<Vec<ColumnSpec>, String> {
    let inner = strip_list_brackets(text)?;
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for raw_item in DagParser::split_args(inner) {
        let item = raw_item.trim();
        let (ctor, args_inner) = parse_call(item)?;
        if ctor != "ColumnSpec" {
            return Err(format!(
                "expected `ColumnSpec(...)` in columns=…, got `{}(…)`",
                ctor
            ));
        }
        out.push(parse_columnspec_args(args_inner)?);
    }
    Ok(out)
}

fn parse_columnspec_args(text: &str) -> Result<ColumnSpec, String> {
    let parts = DagParser::split_args(text);
    if parts.is_empty() {
        return Err("ColumnSpec() requires a name argument".to_string());
    }

    let mut name: Option<String> = None;
    let mut dtype: Option<String> = None;

    for (idx, raw) in parts.iter().enumerate() {
        let part = raw.trim();
        if let Some(eq) = top_level_eq_pos(part) {
            let key = part[..eq].trim();
            let val = part[eq + 1..].trim();
            match key {
                "name" => name = Some(parse_string_literal(val)?),
                "dtype" => dtype = Some(parse_string_literal(val)?),
                other => return Err(format!("ColumnSpec has no field `{}`", other)),
            }
        } else if idx == 0 {
            name = Some(parse_string_literal(part)?);
        } else {
            return Err(format!("unexpected positional ColumnSpec arg: `{}`", part));
        }
    }

    let name = name.ok_or_else(|| "ColumnSpec(...) missing name".to_string())?;
    Ok(ColumnSpec { name, dtype })
}

/// Split `Constructor(arg, arg, …)` into `("Constructor", "arg, arg, …")`.
fn parse_call(text: &str) -> Result<(&str, &str), String> {
    let open = text.find('(').ok_or_else(|| {
        format!(
            "expected a constructor call like `Dataset(...)`, got `{}`",
            text
        )
    })?;
    if !text.ends_with(')') {
        return Err(format!("unterminated call expression: `{}`", text));
    }
    let ctor = text[..open].trim();
    let inner = &text[open + 1..text.len() - 1];
    Ok((ctor, inner))
}

/// Strip `[ … ]` from a list literal. Returns the inner text.
fn strip_list_brackets(text: &str) -> Result<&str, String> {
    let trimmed = text.trim();
    let stripped = trimmed
        .strip_prefix('[')
        .ok_or_else(|| format!("expected `[`, got `{}`", trimmed))?;
    let stripped = stripped
        .strip_suffix(']')
        .ok_or_else(|| format!("expected `]`, got `{}`", trimmed))?;
    Ok(stripped)
}

/// Find the position of `=` at bracket-depth 0 (skipping `==`).
fn top_level_eq_pos(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut depth: i32 = 0;
    let mut in_string = false;
    let mut quote = b'"';
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if in_string {
            if c == quote {
                in_string = false;
            }
        } else {
            match c {
                b'"' | b'\'' => {
                    in_string = true;
                    quote = c;
                }
                b'(' | b'[' | b'{' => depth += 1,
                b')' | b']' | b'}' => depth -= 1,
                b'=' if depth == 0 => {
                    let next = bytes.get(i + 1).copied();
                    if next != Some(b'=') {
                        return Some(i);
                    } else {
                        i += 1; // skip ==
                    }
                }
                _ => {}
            }
        }
        i += 1;
    }
    None
}

/// Parse a Python string literal — must be a static `"..."` or `'...'`.
/// Rejects f-strings, concatenations, identifiers, etc.
fn parse_string_literal(text: &str) -> Result<String, String> {
    let s = text.trim();
    if s.len() < 2 {
        return Err(format!("expected string literal, got `{}`", s));
    }
    let first = s.as_bytes()[0];
    let last = s.as_bytes()[s.len() - 1];
    if (first != b'"' && first != b'\'') || first != last {
        return Err(format!(
            "expected static string literal, got `{}` (no f-strings, concatenations, or names)",
            s
        ));
    }
    // Reject f-strings / raw-strings by ensuring the literal isn't prefixed.
    // (We see the literal in isolation here, so any prefix would already have
    // been consumed by the leading quote check above — but if a future tree
    // reshape changes that, this is the place to harden.)
    Ok(s[1..s.len() - 1].to_string())
}

#[cfg(test)]
mod parser_lineage_tests {
    use super::*;

    #[test]
    fn empty_list_ok() {
        let v = parse_dataset_list("[]").unwrap();
        assert!(v.is_empty());
    }

    #[test]
    fn single_dataset_no_columns() {
        let v = parse_dataset_list(r#"[Dataset("staging.orders")]"#).unwrap();
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].name, "staging.orders");
        assert!(v[0].columns.is_empty());
    }

    #[test]
    fn dataset_with_columns_and_dtype() {
        let v = parse_dataset_list(
            r#"[Dataset("staging.orders", columns=[ColumnSpec("id"), ColumnSpec("amount", dtype="DECIMAL")])]"#,
        )
        .unwrap();
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].columns.len(), 2);
        assert_eq!(v[0].columns[1].dtype.as_deref(), Some("DECIMAL"));
    }

    #[test]
    fn rejects_non_dataset_call() {
        let err = parse_dataset_list(r#"[NotADataset("x")]"#).unwrap_err();
        assert!(err.contains("Dataset"));
    }

    #[test]
    fn rejects_identifier_as_name() {
        // `some_var` is not a static string literal.
        let err = parse_dataset_list(r#"[Dataset(some_var)]"#).unwrap_err();
        assert!(err.contains("string literal"));
    }

    #[test]
    fn rejects_unknown_kwarg() {
        let err = parse_dataset_list(r#"[Dataset("x", schema="weird")]"#).unwrap_err();
        assert!(err.contains("Dataset has no field"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_DAG: &str = r#"
from conduit import dag, task, Param

@dag(
    schedule="0 6 * * *",
    tags=["etl", "warehouse"],
    max_active_runs=3,
)
def daily_warehouse_refresh(date: Param[str] = "{{ ds }}"):
    """Refresh the warehouse daily."""

    @task(retries=3, retry_delay="5m", pool="snowflake")
    def extract_orders(date: str):
        """Pull orders from source."""
        pass

    @task(pool="snowflake", timeout="30m")
    def transform_orders(raw):
        """Clean and transform."""
        pass

    @task(pool="snowflake")
    def load_to_warehouse(data):
        """Load into warehouse."""
        pass

    raw = extract_orders(date)
    cleaned = transform_orders(raw)
    load_to_warehouse(cleaned)
"#;

    #[test]
    fn parse_dag_from_source() {
        let mut parser = DagParser::new().unwrap();
        let dags = parser.parse_source(SAMPLE_DAG, "test.py").unwrap();

        assert_eq!(dags.len(), 1);
        let dag = &dags[0];
        assert_eq!(dag.id, "daily_warehouse_refresh");
        assert_eq!(dag.schedule, Some("0 6 * * *".to_string()));
        assert_eq!(dag.tags, vec!["etl", "warehouse"]);
        assert_eq!(dag.max_active_runs, 3);
        assert_eq!(dag.tasks.len(), 3);
    }

    #[test]
    fn parse_task_attributes() {
        let mut parser = DagParser::new().unwrap();
        let dags = parser.parse_source(SAMPLE_DAG, "test.py").unwrap();
        let dag = &dags[0];

        let extract = dag.tasks.iter().find(|t| t.id == "extract_orders").unwrap();
        assert_eq!(extract.retries, 3);
        assert_eq!(extract.retry_delay, Some("5m".to_string()));
        assert_eq!(extract.pool, Some("snowflake".to_string()));

        let transform = dag
            .tasks
            .iter()
            .find(|t| t.id == "transform_orders")
            .unwrap();
        assert_eq!(transform.timeout, Some("30m".to_string()));
    }

    #[test]
    fn parse_data_flow_dependencies() {
        let mut parser = DagParser::new().unwrap();
        let dags = parser.parse_source(SAMPLE_DAG, "test.py").unwrap();
        let dag = &dags[0];

        let transform = dag
            .tasks
            .iter()
            .find(|t| t.id == "transform_orders")
            .unwrap();
        assert!(transform
            .raw_dependencies
            .contains(&"extract_orders".to_string()));

        let load = dag
            .tasks
            .iter()
            .find(|t| t.id == "load_to_warehouse")
            .unwrap();
        assert!(load
            .raw_dependencies
            .contains(&"transform_orders".to_string()));
    }

    #[test]
    fn split_args_respects_brackets() {
        let args =
            DagParser::split_args(r#"schedule="0 6 * * *", tags=["etl", "warehouse"], max=3"#);
        assert_eq!(args.len(), 3);
        assert!(args[1].contains("["));
    }

    const LINEAGE_DAG: &str = r#"
from conduit_sdk import dag, task, Dataset, ColumnSpec

@dag(schedule="@daily", lineage_strict=True)
def cross_task_demo():
    @task(outputs=[Dataset("staging.orders", columns=[ColumnSpec("id"), ColumnSpec("amount", dtype="DECIMAL")])])
    def extract_orders():
        pass

    @task(inputs=[Dataset("analytics.daily_revenue", columns=[ColumnSpec("customer_id"), ColumnSpec("total")])])
    def push_to_warehouse(data=extract_orders):
        pass
"#;

    #[test]
    fn parses_lineage_strict_and_dataset_decorators() {
        let mut parser = DagParser::new().unwrap();
        let dags = parser.parse_source(LINEAGE_DAG, "lineage.py").unwrap();
        assert_eq!(dags.len(), 1);
        let dag = &dags[0];
        assert!(dag.lineage_strict);

        let extract = dag.tasks.iter().find(|t| t.id == "extract_orders").unwrap();
        assert_eq!(extract.outputs.len(), 1);
        assert_eq!(extract.outputs[0].name, "staging.orders");
        assert_eq!(extract.outputs[0].columns.len(), 2);
        assert_eq!(
            extract.outputs[0].columns[1].dtype.as_deref(),
            Some("DECIMAL")
        );
        assert!(extract.inputs.is_empty());

        let push = dag
            .tasks
            .iter()
            .find(|t| t.id == "push_to_warehouse")
            .unwrap();
        assert_eq!(push.inputs.len(), 1);
        assert_eq!(push.inputs[0].name, "analytics.daily_revenue");
        let cols: Vec<&str> = push.inputs[0]
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(cols, vec!["customer_id", "total"]);
    }

    #[test]
    fn parser_rejects_non_static_dataset_arg() {
        let src = r#"
from conduit_sdk import dag, task, Dataset

@dag()
def bad():
    @task(outputs=[Dataset(some_var)])
    def t():
        pass
"#;
        let mut parser = DagParser::new().unwrap();
        let err = parser.parse_source(src, "bad.py").unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("string literal") || msg.contains("invalid outputs"),
            "unexpected error: {}",
            msg
        );
    }
}
