//! JSON configuration file parser
//!
//! Supports JSON as an alternative to .jibs DSL files for programmatic config generation.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use serde::Deserialize;

use jibs_protocol::{
    AnonymizeRule, AnonymizeTarget, Assignment, ExecutionPlan, PreserveRule, Relation,
    ResolvedAggregate, SetRule, SortDirection, Value,
};

use crate::error::{ClientError, Result};

/// Parse a JSON config file into an ExecutionPlan
pub fn parse_json_config(
    config_path: &Path,
    cli_vars: &HashMap<String, String>,
) -> Result<ExecutionPlan> {
    let content = std::fs::read_to_string(config_path).map_err(|e| ClientError::Io {
        operation: format!("read config '{}'", config_path.display()),
        message: e.to_string(),
    })?;

    let config: JsonConfig = serde_json::from_str(&content)
        .map_err(|e| ClientError::Parse(format!("Invalid JSON config: {}", e)))?;

    let mut resolver = JsonResolver::new(config_path, cli_vars.clone());
    resolver.resolve(config)
}

/// JSON configuration file structure
#[derive(Debug, Deserialize)]
struct JsonConfig {
    /// Variable declarations
    #[serde(default)]
    variable: Vec<JsonVariable>,

    /// File imports
    #[serde(default)]
    import: Vec<JsonImport>,

    /// Faker pools (array format with optional when)
    #[serde(default)]
    faker: JsonFakers,

    /// Relation definitions
    #[serde(default)]
    relation: Vec<JsonRelation>,

    /// Tables to exclude (no data, structure preserved)
    #[serde(default)]
    exclude: Vec<JsonTableRef>,

    /// Tables to ignore entirely
    #[serde(default)]
    ignore: Vec<JsonTableRef>,

    /// Relations to ignore (filter out from auto-discovered FKs)
    #[serde(default)]
    ignore_relation: Vec<JsonRelation>,

    /// Anonymization rules per table (array format with optional when)
    #[serde(default)]
    anonymize: JsonAnonymizeBlocks,

    /// Aggregate definitions
    #[serde(default)]
    aggregate: Vec<JsonAggregate>,

    /// Include statements (additional aggregate where clauses)
    #[serde(default)]
    include: Vec<JsonInclude>,

    /// Preserve rules
    #[serde(default)]
    preserve: Vec<JsonPreserve>,

    /// Set (upsert) blocks
    #[serde(default)]
    set: Vec<JsonSet>,

    /// After statements (SQL to run post-import)
    #[serde(default)]
    after: Vec<JsonAfter>,
}

#[derive(Debug, Deserialize)]
struct JsonVariable {
    name: String,
    #[serde(rename = "type")]
    var_type: String,
    #[serde(default)]
    default: Option<serde_json::Value>,
}

/// Faker pools - supports both HashMap format and array format with `when`
#[derive(Debug, Default, Deserialize)]
#[serde(untagged)]
enum JsonFakers {
    /// Simple HashMap format: {"emails": ["a@b.c"]}
    Map(HashMap<String, Vec<String>>),
    /// Array format with when: [{"name": "emails", "values": ["a@b.c"], "when": "$cond"}]
    Array(Vec<JsonFaker>),
    #[default]
    Empty,
}

#[derive(Debug, Deserialize)]
struct JsonFaker {
    name: String,
    values: Vec<String>,
    #[serde(default)]
    when: Option<String>,
}

/// Anonymize blocks - supports both HashMap format and array format with `when`
#[derive(Debug, Default, Deserialize)]
#[serde(untagged)]
enum JsonAnonymizeBlocks {
    /// Simple HashMap format: {"users": [...]}
    Map(HashMap<String, Vec<JsonAnonymizeRule>>),
    /// Array format with when: [{"table": "users", "rules": [...], "when": "$cond"}]
    Array(Vec<JsonAnonymizeBlock>),
    #[default]
    Empty,
}

#[derive(Debug, Deserialize)]
struct JsonAnonymizeBlock {
    table: String,
    #[serde(default)]
    rules: Vec<JsonAnonymizeRule>,
    #[serde(default)]
    when: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JsonImport {
    path: String,
    #[serde(default)]
    when: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JsonRelation {
    from: String,
    to: String,
    #[serde(default)]
    when: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JsonTableRef {
    table: String,
    #[serde(default)]
    when: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JsonAnonymizeRule {
    column: String,
    #[serde(default)]
    faker: Option<String>,
    #[serde(default)]
    null: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct JsonAggregate {
    name: String,
    root: String,
    #[serde(default, rename = "where")]
    where_clause: Option<String>,
    #[serde(default)]
    order_by: Option<JsonOrderBy>,
    #[serde(default)]
    limit: Option<JsonLimit>,
    #[serde(default)]
    when: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum JsonOrderBy {
    Simple(String),
    Full {
        column: String,
        #[serde(default)]
        direction: Option<String>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum JsonLimit {
    Number(i64),
    Variable(String),
}

#[derive(Debug, Deserialize)]
struct JsonInclude {
    aggregate: String,
    #[serde(default, rename = "where")]
    where_clause: Option<String>,
    #[serde(default)]
    when: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JsonPreserve {
    table: String,
    #[serde(rename = "where")]
    where_clause: String,
    #[serde(default)]
    when: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JsonSet {
    table: String,
    #[serde(rename = "match")]
    match_clause: HashMap<String, serde_json::Value>,
    #[serde(default)]
    value: HashMap<String, serde_json::Value>,
    #[serde(default)]
    when: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JsonAfter {
    sql: String,
    #[serde(default)]
    when: Option<String>,
}

/// State for JSON config resolution
struct JsonResolver {
    base_path: std::path::PathBuf,
    imported_files: HashSet<std::path::PathBuf>,
    variables: HashMap<String, Value>,
    pending_vars: HashMap<String, (String, Option<serde_json::Value>)>,
    plan: ExecutionPlan,
}

impl JsonResolver {
    fn new(base_path: &Path, cli_vars: HashMap<String, String>) -> Self {
        let mut variables = HashMap::new();
        for (k, v) in cli_vars {
            variables.insert(k, Value::String(v));
        }

        let mut imported_files = HashSet::new();
        if let Ok(canonical) = base_path.canonicalize() {
            imported_files.insert(canonical);
        }

        Self {
            base_path: base_path.to_path_buf(),
            imported_files,
            variables,
            pending_vars: HashMap::new(),
            plan: ExecutionPlan::new(),
        }
    }

    fn resolve(&mut self, config: JsonConfig) -> Result<ExecutionPlan> {
        // First pass: process imports
        for import in &config.import {
            if let Some(when) = &import.when {
                if !self.evaluate_condition_string(when)? {
                    continue;
                }
            }
            self.process_import(&import.path)?;
        }

        // Second pass: collect variable declarations
        for var in &config.variable {
            self.pending_vars.insert(
                var.name.clone(),
                (var.var_type.clone(), var.default.clone()),
            );
        }

        // Finalize variables
        self.finalize_variables()?;

        // Third pass: process all statements
        self.process_fakers(&config.faker)?;
        self.process_relations(&config.relation)?;
        self.process_excludes(&config.exclude)?;
        self.process_ignores(&config.ignore)?;
        self.process_ignore_relations(&config.ignore_relation)?;
        self.process_anonymize(&config.anonymize)?;
        self.process_aggregates(&config.aggregate)?;
        self.process_includes(&config.include)?;
        self.process_preserves(&config.preserve)?;
        self.process_sets(&config.set)?;
        self.process_after(&config.after)?;

        Ok(std::mem::take(&mut self.plan))
    }

    fn process_import(&mut self, import_path: &str) -> Result<()> {
        let base_dir = self.base_path.parent().unwrap_or(Path::new("."));
        let import_file = base_dir.join(import_path);

        let canonical_path = import_file.canonicalize().map_err(|e| ClientError::Io {
            operation: format!("resolve import path '{}'", import_path),
            message: e.to_string(),
        })?;

        if self.imported_files.contains(&canonical_path) {
            return Ok(());
        }
        self.imported_files.insert(canonical_path.clone());

        let content = std::fs::read_to_string(&canonical_path).map_err(|e| ClientError::Io {
            operation: format!("read import '{}'", import_path),
            message: e.to_string(),
        })?;

        // Determine if JSON or jibs based on extension
        let extension = canonical_path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("");

        if extension == "json" {
            let config: JsonConfig = serde_json::from_str(&content).map_err(|e| {
                ClientError::Parse(format!("Parse error in '{}': {}", import_path, e))
            })?;

            let old_base_path = std::mem::replace(&mut self.base_path, canonical_path);

            // Process nested imports
            for import in &config.import {
                if let Some(when) = &import.when {
                    if !self.evaluate_condition_string(when)? {
                        continue;
                    }
                }
                self.process_import(&import.path)?;
            }

            // Collect variables
            for var in &config.variable {
                self.pending_vars.insert(
                    var.name.clone(),
                    (var.var_type.clone(), var.default.clone()),
                );
            }

            // Process statements
            self.process_fakers(&config.faker)?;
            self.process_relations(&config.relation)?;
            self.process_excludes(&config.exclude)?;
            self.process_ignores(&config.ignore)?;
            self.process_ignore_relations(&config.ignore_relation)?;
            self.process_anonymize(&config.anonymize)?;
            self.process_aggregates(&config.aggregate)?;
            self.process_includes(&config.include)?;
            self.process_preserves(&config.preserve)?;
            self.process_sets(&config.set)?;
            self.process_after(&config.after)?;

            self.base_path = old_base_path;
        } else {
            // Parse as .jibs file using the DSL parser
            let program = jibs_parser::parse(&content).map_err(|errors| {
                ClientError::Parse(format!(
                    "Parse error in '{}': {}",
                    import_path,
                    errors
                        .iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            })?;

            // Convert CLI vars to the format expected by the DSL resolver
            let cli_vars: HashMap<String, String> = self
                .variables
                .iter()
                .map(|(k, v)| (k.clone(), v.as_string()))
                .collect();

            // Use the DSL resolver for .jibs files
            let imported_config = crate::resolver::resolve(&canonical_path, &program, &cli_vars)
                .map_err(|e| ClientError::Parse(format!("Resolution error in '{}': {}", import_path, e)))?;
            let imported_plan = imported_config.plan;

            // Merge the imported plan into our plan
            self.merge_plan(imported_plan);
        }

        Ok(())
    }

    fn merge_plan(&mut self, other: ExecutionPlan) {
        // Merge variables (don't override existing)
        for (k, v) in other.variables {
            self.plan.variables.entry(k).or_insert(v);
        }

        // Append collections
        self.plan.relations.extend(other.relations);
        self.plan.ignored_relations.extend(other.ignored_relations);
        self.plan.aggregates.extend(other.aggregates);
        self.plan.excluded_tables.extend(other.excluded_tables);
        self.plan.ignored_tables.extend(other.ignored_tables);

        for (table, rules) in other.anonymization {
            self.plan
                .anonymization
                .entry(table)
                .or_default()
                .extend(rules);
        }

        for (name, values) in other.fakers {
            self.plan.fakers.entry(name).or_insert(values);
        }

        self.plan.preserves.extend(other.preserves);
        self.plan.sets.extend(other.sets);
        self.plan.after_statements.extend(other.after_statements);
    }

    fn finalize_variables(&mut self) -> Result<()> {
        for (name, (var_type, default)) in &self.pending_vars {
            if let Some(value) = self.variables.get(name) {
                let typed_value = self.coerce_value(value.clone(), var_type)?;
                self.variables.insert(name.clone(), typed_value.clone());
                self.plan.variables.insert(name.clone(), typed_value);
            } else if let Some(default) = default {
                let value = self.json_to_value(default, var_type)?;
                self.variables.insert(name.clone(), value.clone());
                self.plan.variables.insert(name.clone(), value);
            } else {
                return Err(ClientError::UndefinedVariable(name.clone()));
            }
        }
        Ok(())
    }

    fn coerce_value(&self, value: Value, target_type: &str) -> Result<Value> {
        match (&value, target_type) {
            (Value::String(s), "int") => s
                .parse::<i64>()
                .map(Value::Int)
                .map_err(|_| ClientError::TypeError(format!("Cannot convert '{}' to int", s))),
            (Value::String(s), "float") => s
                .parse::<f64>()
                .map(Value::Float)
                .map_err(|_| ClientError::TypeError(format!("Cannot convert '{}' to float", s))),
            (Value::String(s), "bool") => match s.to_lowercase().as_str() {
                "true" | "1" | "yes" => Ok(Value::Bool(true)),
                "false" | "0" | "no" => Ok(Value::Bool(false)),
                _ => Err(ClientError::TypeError(format!(
                    "Cannot convert '{}' to bool",
                    s
                ))),
            },
            (Value::String(_), "string") => Ok(value),
            (Value::StringArray(_), "string[]") => Ok(value),
            (Value::Int(_), "int") => Ok(value),
            (Value::IntArray(_), "int[]") => Ok(value),
            (Value::Float(_), "float") => Ok(value),
            (Value::FloatArray(_), "float[]") => Ok(value),
            (Value::Bool(_), "bool") => Ok(value),
            (Value::BoolArray(_), "bool[]") => Ok(value),
            _ => Ok(value),
        }
    }

    fn json_to_value(&self, json: &serde_json::Value, var_type: &str) -> Result<Value> {
        match (json, var_type) {
            (serde_json::Value::Number(n), "int") => n
                .as_i64()
                .map(Value::Int)
                .ok_or_else(|| ClientError::TypeError("Expected integer".to_string())),
            (serde_json::Value::Number(n), "float") => n
                .as_f64()
                .map(Value::Float)
                .ok_or_else(|| ClientError::TypeError("Expected float".to_string())),
            (serde_json::Value::Bool(b), "bool") => Ok(Value::Bool(*b)),
            (serde_json::Value::String(s), "string") => {
                Ok(Value::String(self.interpolate_string(s)?))
            }
            (serde_json::Value::String(s), "int") => s
                .parse::<i64>()
                .map(Value::Int)
                .map_err(|_| ClientError::TypeError(format!("Cannot convert '{}' to int", s))),
            (serde_json::Value::String(s), "float") => s
                .parse::<f64>()
                .map(Value::Float)
                .map_err(|_| ClientError::TypeError(format!("Cannot convert '{}' to float", s))),
            (serde_json::Value::Array(arr), "string[]") => {
                let strings: Result<Vec<String>> = arr
                    .iter()
                    .map(|v| match v {
                        serde_json::Value::String(s) => self.interpolate_string(s),
                        _ => Err(ClientError::TypeError(
                            "Expected string in string array".to_string(),
                        )),
                    })
                    .collect();
                Ok(Value::StringArray(strings?))
            }
            (serde_json::Value::Array(arr), "int[]") => {
                let ints: Result<Vec<i64>> = arr
                    .iter()
                    .map(|v| match v {
                        serde_json::Value::Number(n) => n.as_i64().ok_or_else(|| {
                            ClientError::TypeError("Expected integer in int array".to_string())
                        }),
                        _ => Err(ClientError::TypeError(
                            "Expected integer in int array".to_string(),
                        )),
                    })
                    .collect();
                Ok(Value::IntArray(ints?))
            }
            (serde_json::Value::Array(arr), "float[]") => {
                let floats: Result<Vec<f64>> = arr
                    .iter()
                    .map(|v| match v {
                        serde_json::Value::Number(n) => n.as_f64().ok_or_else(|| {
                            ClientError::TypeError("Expected float in float array".to_string())
                        }),
                        _ => Err(ClientError::TypeError(
                            "Expected float in float array".to_string(),
                        )),
                    })
                    .collect();
                Ok(Value::FloatArray(floats?))
            }
            (serde_json::Value::Array(arr), "bool[]") => {
                let bools: Result<Vec<bool>> = arr
                    .iter()
                    .map(|v| match v {
                        serde_json::Value::Bool(b) => Ok(*b),
                        _ => Err(ClientError::TypeError(
                            "Expected boolean in bool array".to_string(),
                        )),
                    })
                    .collect();
                Ok(Value::BoolArray(bools?))
            }
            (serde_json::Value::Null, _) => Ok(Value::Null),
            _ => Err(ClientError::TypeError(format!(
                "Type mismatch: expected {}",
                var_type
            ))),
        }
    }

    fn interpolate_string(&self, s: &str) -> Result<String> {
        let mut result = String::new();
        let mut chars = s.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '{' && chars.peek() == Some(&'$') {
                // Skip the {$
                chars.next();

                // Read variable name until }
                let mut var_name = String::new();
                while let Some(c) = chars.next() {
                    if c == '}' {
                        break;
                    }
                    var_name.push(c);
                }

                // Look up variable
                if let Some(value) = self.variables.get(&var_name) {
                    result.push_str(&value.as_string());
                } else {
                    return Err(ClientError::UndefinedVariable(var_name));
                }
            } else if c == '$' {
                // Simple variable reference $name
                let mut var_name = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_alphanumeric() || c == '_' {
                        var_name.push(c);
                        chars.next();
                    } else {
                        break;
                    }
                }

                if !var_name.is_empty() {
                    if let Some(value) = self.variables.get(&var_name) {
                        result.push_str(&value.as_string());
                    } else {
                        // Check if it's a condition reference like "$production"
                        // Leave it as-is for condition evaluation
                        result.push('$');
                        result.push_str(&var_name);
                    }
                } else {
                    result.push('$');
                }
            } else {
                result.push(c);
            }
        }

        Ok(result)
    }

    fn evaluate_condition_string(&self, condition: &str) -> Result<bool> {
        // Simple condition evaluation for variable references
        // Supports: $var, !$var
        let condition = condition.trim();

        if let Some(var_name) = condition.strip_prefix("!$") {
            match self.variables.get(var_name) {
                Some(Value::Bool(b)) => Ok(!b),
                Some(Value::Null) => Ok(true),
                Some(_) => Err(ClientError::TypeError(
                    "Condition must evaluate to bool".to_string(),
                )),
                None => Err(ClientError::UndefinedVariable(var_name.to_string())),
            }
        } else if let Some(var_name) = condition.strip_prefix('$') {
            match self.variables.get(var_name) {
                Some(Value::Bool(b)) => Ok(*b),
                Some(Value::Null) => Ok(false),
                Some(_) => Err(ClientError::TypeError(
                    "Condition must evaluate to bool".to_string(),
                )),
                None => Err(ClientError::UndefinedVariable(var_name.to_string())),
            }
        } else {
            // Literal true/false
            match condition.to_lowercase().as_str() {
                "true" => Ok(true),
                "false" => Ok(false),
                _ => Err(ClientError::TypeError(format!(
                    "Invalid condition: {}",
                    condition
                ))),
            }
        }
    }

    fn process_fakers(&mut self, fakers: &JsonFakers) -> Result<()> {
        match fakers {
            JsonFakers::Map(map) => {
                for (name, values) in map {
                    let resolved: Vec<String> = values
                        .iter()
                        .map(|v| self.interpolate_string(v))
                        .collect::<Result<Vec<_>>>()?;
                    self.plan.fakers.insert(name.clone(), resolved);
                }
            }
            JsonFakers::Array(arr) => {
                for faker in arr {
                    if let Some(when) = &faker.when {
                        if !self.evaluate_condition_string(when)? {
                            continue;
                        }
                    }
                    let resolved: Vec<String> = faker
                        .values
                        .iter()
                        .map(|v| self.interpolate_string(v))
                        .collect::<Result<Vec<_>>>()?;
                    self.plan.fakers.insert(faker.name.clone(), resolved);
                }
            }
            JsonFakers::Empty => {}
        }
        Ok(())
    }

    fn process_relations(&mut self, relations: &[JsonRelation]) -> Result<()> {
        for rel in relations {
            if let Some(when) = &rel.when {
                if !self.evaluate_condition_string(when)? {
                    continue;
                }
            }
            let (from_table, from_column) = parse_column_ref(&rel.from)?;
            let (to_table, to_column) = parse_column_ref(&rel.to)?;

            self.plan.relations.push(Relation {
                from_table,
                from_column,
                to_table,
                to_column,
            });
        }
        Ok(())
    }

    fn process_excludes(&mut self, excludes: &[JsonTableRef]) -> Result<()> {
        for exclude in excludes {
            if let Some(when) = &exclude.when {
                if !self.evaluate_condition_string(when)? {
                    continue;
                }
            }
            self.plan.excluded_tables.insert(exclude.table.clone());
        }
        Ok(())
    }

    fn process_ignores(&mut self, ignores: &[JsonTableRef]) -> Result<()> {
        for ignore in ignores {
            if let Some(when) = &ignore.when {
                if !self.evaluate_condition_string(when)? {
                    continue;
                }
            }
            self.plan.ignored_tables.insert(ignore.table.clone());
        }
        Ok(())
    }

    fn process_ignore_relations(&mut self, relations: &[JsonRelation]) -> Result<()> {
        for rel in relations {
            if let Some(when) = &rel.when {
                if !self.evaluate_condition_string(when)? {
                    continue;
                }
            }
            let (from_table, from_column) = parse_column_ref(&rel.from)?;
            let (to_table, to_column) = parse_column_ref(&rel.to)?;

            self.plan.ignored_relations.push(Relation {
                from_table,
                from_column,
                to_table,
                to_column,
            });
        }
        Ok(())
    }

    fn process_anonymize(&mut self, anonymize: &JsonAnonymizeBlocks) -> Result<()> {
        match anonymize {
            JsonAnonymizeBlocks::Map(map) => {
                for (table, rules) in map {
                    self.add_anonymize_rules(table, rules)?;
                }
            }
            JsonAnonymizeBlocks::Array(arr) => {
                for block in arr {
                    if let Some(when) = &block.when {
                        if !self.evaluate_condition_string(when)? {
                            continue;
                        }
                    }
                    self.add_anonymize_rules(&block.table, &block.rules)?;
                }
            }
            JsonAnonymizeBlocks::Empty => {}
        }
        Ok(())
    }

    fn add_anonymize_rules(&mut self, table: &str, rules: &[JsonAnonymizeRule]) -> Result<()> {
        let mut resolved_rules = Vec::new();
        for rule in rules {
            let target = if rule.null == Some(true) {
                AnonymizeTarget::Null
            } else if let Some(faker) = &rule.faker {
                AnonymizeTarget::Faker(faker.clone())
            } else {
                return Err(ClientError::Parse(format!(
                    "Anonymize rule for {}.{} must specify 'faker' or 'null: true'",
                    table, rule.column
                )));
            };

            resolved_rules.push(AnonymizeRule {
                column: rule.column.clone(),
                target,
            });
        }
        self.plan
            .anonymization
            .entry(table.to_string())
            .or_default()
            .extend(resolved_rules);
        Ok(())
    }

    fn process_aggregates(&mut self, aggregates: &[JsonAggregate]) -> Result<()> {
        for agg in aggregates {
            if let Some(when) = &agg.when {
                if !self.evaluate_condition_string(when)? {
                    continue;
                }
            }

            let where_clause = agg
                .where_clause
                .as_ref()
                .map(|w| self.interpolate_string(w))
                .transpose()?;

            let (order_by, order_direction) = match &agg.order_by {
                Some(JsonOrderBy::Simple(col)) => (Some(col.clone()), None),
                Some(JsonOrderBy::Full { column, direction }) => {
                    let dir = direction.as_ref().map(|d| match d.to_lowercase().as_str() {
                        "asc" => SortDirection::Asc,
                        "desc" => SortDirection::Desc,
                        _ => SortDirection::Asc,
                    });
                    (Some(column.clone()), dir)
                }
                None => (None, None),
            };

            let limit = match &agg.limit {
                Some(JsonLimit::Number(n)) => Some(*n),
                Some(JsonLimit::Variable(var)) => {
                    let var_name = var.strip_prefix('$').unwrap_or(var);
                    let value = self
                        .variables
                        .get(var_name)
                        .ok_or_else(|| ClientError::UndefinedVariable(var_name.to_string()))?;
                    value.as_int()
                }
                None => None,
            };

            self.plan.aggregates.push(ResolvedAggregate {
                name: agg.name.clone(),
                root_table: agg.root.clone(),
                where_clause,
                order_by,
                order_direction,
                limit,
                exclude_tables: Vec::new(),
                exclude_patterns: Vec::new(),
                root_only: false,
            });
        }
        Ok(())
    }

    fn process_includes(&mut self, includes: &[JsonInclude]) -> Result<()> {
        for include in includes {
            if let Some(when) = &include.when {
                if !self.evaluate_condition_string(when)? {
                    continue;
                }
            }

            if let Some(existing) = self
                .plan
                .aggregates
                .iter()
                .find(|a| a.name == include.aggregate)
            {
                let mut new_agg = existing.clone();
                new_agg.name = format!(
                    "{}_include_{}",
                    include.aggregate,
                    self.plan.aggregates.len()
                );
                if let Some(where_clause) = &include.where_clause {
                    new_agg.where_clause = Some(self.interpolate_string(where_clause)?);
                }
                self.plan.aggregates.push(new_agg);
            }
        }
        Ok(())
    }

    fn process_preserves(&mut self, preserves: &[JsonPreserve]) -> Result<()> {
        for preserve in preserves {
            if let Some(when) = &preserve.when {
                if !self.evaluate_condition_string(when)? {
                    continue;
                }
            }
            self.plan.preserves.push(PreserveRule {
                table: preserve.table.clone(),
                where_clause: self.interpolate_string(&preserve.where_clause)?,
            });
        }
        Ok(())
    }

    fn process_sets(&mut self, sets: &[JsonSet]) -> Result<()> {
        for set in sets {
            if let Some(when) = &set.when {
                if !self.evaluate_condition_string(when)? {
                    continue;
                }
            }

            let match_clause: Vec<Assignment> = set
                .match_clause
                .iter()
                .map(|(col, val)| {
                    Ok(Assignment {
                        column: col.clone(),
                        value: self.json_value_to_protocol_value(val)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            let assignments: Vec<Assignment> = set
                .value
                .iter()
                .map(|(col, val)| {
                    Ok(Assignment {
                        column: col.clone(),
                        value: self.json_value_to_protocol_value(val)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            self.plan.sets.push(SetRule {
                table: set.table.clone(),
                match_clause,
                assignments,
            });
        }
        Ok(())
    }

    fn process_after(&mut self, after: &[JsonAfter]) -> Result<()> {
        for stmt in after {
            if let Some(when) = &stmt.when {
                if !self.evaluate_condition_string(when)? {
                    continue;
                }
            }
            self.plan
                .after_statements
                .push(self.interpolate_string(&stmt.sql)?);
        }
        Ok(())
    }

    fn json_value_to_protocol_value(&self, json: &serde_json::Value) -> Result<Value> {
        match json {
            serde_json::Value::String(s) => Ok(Value::String(self.interpolate_string(s)?)),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(Value::Int(i))
                } else if let Some(f) = n.as_f64() {
                    Ok(Value::Float(f))
                } else {
                    Err(ClientError::TypeError("Invalid number".to_string()))
                }
            }
            serde_json::Value::Bool(b) => Ok(Value::Bool(*b)),
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::Array(arr) => {
                // Try to infer array type from first element
                if arr.is_empty() {
                    // Default to string array for empty arrays
                    Ok(Value::StringArray(Vec::new()))
                } else {
                    match &arr[0] {
                        serde_json::Value::String(_) => {
                            let strings: Result<Vec<String>> = arr
                                .iter()
                                .map(|v| match v {
                                    serde_json::Value::String(s) => self.interpolate_string(s),
                                    _ => Err(ClientError::TypeError(
                                        "Mixed types in array".to_string(),
                                    )),
                                })
                                .collect();
                            Ok(Value::StringArray(strings?))
                        }
                        serde_json::Value::Number(n) if n.is_i64() => {
                            let ints: Result<Vec<i64>> = arr
                                .iter()
                                .map(|v| match v {
                                    serde_json::Value::Number(n) => {
                                        n.as_i64().ok_or_else(|| {
                                            ClientError::TypeError(
                                                "Expected integer in array".to_string(),
                                            )
                                        })
                                    }
                                    _ => Err(ClientError::TypeError(
                                        "Mixed types in array".to_string(),
                                    )),
                                })
                                .collect();
                            Ok(Value::IntArray(ints?))
                        }
                        serde_json::Value::Number(_) => {
                            let floats: Result<Vec<f64>> = arr
                                .iter()
                                .map(|v| match v {
                                    serde_json::Value::Number(n) => {
                                        n.as_f64().ok_or_else(|| {
                                            ClientError::TypeError(
                                                "Expected number in array".to_string(),
                                            )
                                        })
                                    }
                                    _ => Err(ClientError::TypeError(
                                        "Mixed types in array".to_string(),
                                    )),
                                })
                                .collect();
                            Ok(Value::FloatArray(floats?))
                        }
                        serde_json::Value::Bool(_) => {
                            let bools: Result<Vec<bool>> = arr
                                .iter()
                                .map(|v| match v {
                                    serde_json::Value::Bool(b) => Ok(*b),
                                    _ => Err(ClientError::TypeError(
                                        "Mixed types in array".to_string(),
                                    )),
                                })
                                .collect();
                            Ok(Value::BoolArray(bools?))
                        }
                        _ => Err(ClientError::TypeError(
                            "Unsupported array element type".to_string(),
                        )),
                    }
                }
            }
            _ => Err(ClientError::TypeError(
                "Unsupported JSON value type".to_string(),
            )),
        }
    }
}

/// Parse a column reference like "table.column" into (table, column)
fn parse_column_ref(s: &str) -> Result<(String, String)> {
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() != 2 {
        return Err(ClientError::Parse(format!(
            "Invalid column reference '{}', expected 'table.column'",
            s
        )));
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_column_ref() {
        let (table, col) = parse_column_ref("orders.user_id").unwrap();
        assert_eq!(table, "orders");
        assert_eq!(col, "user_id");
    }

    #[test]
    fn test_parse_column_ref_invalid() {
        assert!(parse_column_ref("invalid").is_err());
        assert!(parse_column_ref("a.b.c").is_err());
    }
}
