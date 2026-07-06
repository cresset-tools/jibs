//! DSL resolution - evaluating conditions and interpolating variables

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use jibs_parser::ast::{
    AggregateBlock, AnonymizeBlock, Expr, FakerDecl, FakerSource, FakerValue, GetFunctionDef,
    LimitValue, Literal, PreserveStmt, Program, RelationDecl, SetBlock,
    SortDirection as AstSortDirection, Statement, StatementKind, StringLiteral, StringPart,
    TablePattern, VarDecl, VarType,
};
use jibs_protocol::{
    AnonymizeRule, AnonymizeTarget, Assignment, ExecutionPlan, PreserveRule, Relation,
    ResolvedAggregate, SetRule, SortDirection, Value,
};

use crate::error::{ClientError, Result};

/// Result of resolving a DSL program
#[derive(Debug)]
pub struct ResolvedConfig {
    pub plan: ExecutionPlan,
    pub get_functions: Vec<ResolvedGetFunction>,
    /// Declared variables that had neither a value nor a default (only
    /// populated by [`resolve_lenient`]; strict resolution errors instead)
    pub missing_vars: Vec<(String, VarType)>,
}

/// A resolved get function definition (ready for CLI invocation)
#[derive(Debug, Clone)]
pub struct ResolvedGetFunction {
    pub name: String,
    pub params: Vec<ResolvedParam>,
    pub aggregate_name: String,
    pub where_template: Option<String>,
    pub order_by: Option<String>,
    pub order_direction: Option<SortDirection>,
    pub limit: Option<LimitOverride>,
    pub exclude_tables: Vec<String>,
    pub exclude_patterns: Vec<String>,
    pub root_only: Option<bool>,
}

/// A resolved parameter for a get function
#[derive(Debug, Clone)]
pub struct ResolvedParam {
    pub name: String,
    pub param_type: VarType,
    pub default: Option<Value>,
}

/// How the limit is determined in a get function
#[derive(Debug, Clone)]
pub enum LimitOverride {
    /// A concrete limit value
    Concrete(i64),
    /// References a function parameter by name
    Param(String),
}

/// Resolve a parsed program into an execution plan and get function definitions
///
/// `base_path` is the path to the .jibs file being resolved, used for resolving
/// relative import paths.
pub fn resolve(
    base_path: &Path,
    program: &Program<'_>,
    cli_vars: &HashMap<String, String>,
) -> Result<ResolvedConfig> {
    let mut resolver = Resolver::new(base_path, cli_vars.clone());
    let config = resolver.resolve_program(program)?;
    validate_plan(&config.plan, &config.get_functions)?;
    Ok(config)
}

/// Resolve for validation (`jibs check`): declared variables without a value
/// get a type-appropriate placeholder instead of failing, and are reported in
/// [`ResolvedConfig::missing_vars`]. Everything else is validated exactly
/// like a real resolution.
///
/// Caveat: a missing bool variable placeholders to `false`, so statements
/// gated on `#[when($flag)]` are skipped during validation.
pub fn resolve_lenient(
    base_path: &Path,
    program: &Program<'_>,
    cli_vars: &HashMap<String, String>,
) -> Result<ResolvedConfig> {
    let mut resolver = Resolver::new(base_path, cli_vars.clone());
    resolver.lenient_missing_vars = true;
    let config = resolver.resolve_program(program)?;
    validate_plan(&config.plan, &config.get_functions)?;
    Ok(config)
}

/// Cross-reference checks on a fully resolved plan. Runs for both import and
/// check, so configuration mistakes fail before any SSH connection:
/// - anonymize rules must reference defined fakers (a typo here would
///   otherwise silently NULL the column at import time)
/// - regex table patterns must compile (the server would reject them, but
///   only after connecting)
pub(crate) fn validate_plan(
    plan: &ExecutionPlan,
    get_functions: &[ResolvedGetFunction],
) -> Result<()> {
    for (table, rules) in &plan.anonymization {
        for rule in rules {
            if let AnonymizeTarget::Faker(faker) = &rule.target {
                if !plan.fakers.contains_key(faker) {
                    return Err(ClientError::Resolution(format!(
                        "anonymize rule for {}.{} references undefined faker '{}'",
                        table, rule.column, faker
                    )));
                }
            }
        }
    }

    // (get function -> aggregate references are validated during resolution
    // itself, with a message that lists the available aggregates)

    let pattern_sources = plan
        .excluded_patterns
        .iter()
        .map(|p| ("exclude_data", p))
        .chain(plan.ignored_patterns.iter().map(|p| ("ignore_table", p)))
        .chain(plan.full_patterns.iter().map(|p| ("full", p)))
        .chain(
            plan
                .aggregates
                .iter()
                .flat_map(|a| a.exclude_patterns.iter().map(|p| ("aggregate exclude", p))),
        )
        .chain(
            get_functions
                .iter()
                .flat_map(|f| f.exclude_patterns.iter().map(|p| ("get exclude", p))),
        );
    for (context, pattern) in pattern_sources {
        if let Err(e) = regex::Regex::new(pattern) {
            return Err(ClientError::Resolution(format!(
                "invalid regex /{}/ in {}: {}",
                pattern, context, e
            )));
        }
    }

    Ok(())
}

/// State for the resolver
struct Resolver {
    /// Base path for resolving relative imports
    base_path: PathBuf,
    /// Files that have been imported (to detect circular imports)
    imported_files: HashSet<PathBuf>,
    /// Variable values (from CLI, files, or defaults)
    variables: HashMap<String, Value>,
    /// Pending variable declarations (name -> (type, default))
    pending_vars: HashMap<String, (VarType, Option<Value>)>,
    /// The execution plan being built
    plan: ExecutionPlan,
    /// Resolved get function definitions
    get_functions: Vec<ResolvedGetFunction>,
    /// Check mode: placeholder missing variables instead of erroring
    lenient_missing_vars: bool,
    /// Variables that were placeholdered in lenient mode
    missing_vars: Vec<(String, VarType)>,
}

impl Resolver {
    fn new(base_path: &Path, cli_vars: HashMap<String, String>) -> Self {
        // Convert CLI string vars to Values (we'll validate types later)
        let mut variables = HashMap::new();
        for (k, v) in cli_vars {
            variables.insert(k, Value::String(v));
        }

        // Track the initial file as imported
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
            get_functions: Vec::new(),
            lenient_missing_vars: false,
            missing_vars: Vec::new(),
        }
    }

    fn resolve_program(&mut self, program: &Program<'_>) -> Result<ResolvedConfig> {
        // First pass: process imports (they may define variables, fakers, etc.)
        for (stmt, _span) in &program.statements {
            if let StatementKind::Import((path, _path_span)) = &stmt.kind {
                // Check #[when] condition if present
                if let Some((condition, _span)) = &stmt.attribute {
                    if !self.evaluate_condition(condition)? {
                        continue; // Skip this import
                    }
                }
                self.process_import(path)?;
            }
        }

        // Second pass: collect all variable declarations
        for (stmt, _span) in &program.statements {
            if let StatementKind::Var(var_decl) = &stmt.kind {
                self.collect_var_decl(var_decl)?;
            }
        }

        // Validate and finalize variable values
        self.finalize_variables()?;

        // Third pass: process all statements (evaluating #[when] conditions)
        for (stmt, _span) in &program.statements {
            self.process_statement(stmt)?;
        }

        Ok(ResolvedConfig {
            plan: std::mem::take(&mut self.plan),
            get_functions: std::mem::take(&mut self.get_functions),
            missing_vars: std::mem::take(&mut self.missing_vars),
        })
    }

    /// Process an import statement by loading and resolving the imported file.
    ///
    /// Import processing order (depth-first):
    /// 1. Recursively process nested imports in the imported file
    /// 2. Collect variable declarations from the imported file
    /// 3. Process all other statements (relations, aggregates, after blocks, etc.)
    ///
    /// This means that for `after` blocks, the order is:
    /// - Nested imports' after blocks run first (depth-first)
    /// - Then the imported file's own after blocks
    /// - Finally, the importing file's after blocks
    fn process_import(&mut self, import_path: &str) -> Result<()> {
        // Resolve the import path relative to the current file's directory
        let base_dir = self.base_path.parent().unwrap_or(Path::new("."));
        let import_file = base_dir.join(import_path);

        // Canonicalize to detect circular imports
        let canonical_path = import_file.canonicalize().map_err(|e| ClientError::Io {
            operation: format!("resolve import path '{}'", import_path),
            message: e.to_string(),
        })?;

        // Check for circular imports
        if self.imported_files.contains(&canonical_path) {
            // Already imported, skip (this is not an error, allows diamond imports)
            return Ok(());
        }
        self.imported_files.insert(canonical_path.clone());

        // Read and parse the imported file
        let source = std::fs::read_to_string(&canonical_path).map_err(|e| ClientError::Io {
            operation: format!("read import '{}'", import_path),
            message: e.to_string(),
        })?;

        let program = jibs_parser::parse(&source).map_err(|errors| {
            ClientError::Parse(format!(
                "in imported file '{}':\n{}",
                import_path,
                jibs_parser::render_errors(import_path, &source, &errors, false)
            ))
        })?;

        // Save and update base path for nested imports
        let old_base_path = std::mem::replace(&mut self.base_path, canonical_path);

        // Recursively process the imported program's statements
        // First: handle nested imports
        for (stmt, _span) in &program.statements {
            if let StatementKind::Import((path, _path_span)) = &stmt.kind {
                if let Some((condition, _span)) = &stmt.attribute {
                    if !self.evaluate_condition(condition)? {
                        continue;
                    }
                }
                self.process_import(path)?;
            }
        }

        // Second: collect variable declarations from import
        for (stmt, _span) in &program.statements {
            if let StatementKind::Var(var_decl) = &stmt.kind {
                self.collect_var_decl(var_decl)?;
            }
        }

        // Finalize any newly collected variables so they're available
        // when processing statements (e.g. aggregate limits using $var)
        self.finalize_variables()?;

        // Third: process other statements from import
        for (stmt, _span) in &program.statements {
            self.process_statement(stmt)?;
        }

        // Restore base path
        self.base_path = old_base_path;

        Ok(())
    }

    fn collect_var_decl(&mut self, var_decl: &VarDecl<'_>) -> Result<()> {
        let name = var_decl.name.0.to_string();
        let var_type = var_decl.var_type.0;

        let default = if let Some((lit, _span)) = &var_decl.default {
            Some(self.literal_to_value(lit, var_type)?)
        } else {
            None
        };

        self.pending_vars.insert(name, (var_type, default));
        Ok(())
    }

    fn finalize_variables(&mut self) -> Result<()> {
        for (name, (var_type, default)) in &self.pending_vars {
            if let Some(value) = self.variables.get(name) {
                // Variable was provided via CLI - convert to correct type
                let typed_value = self.coerce_value(value.clone(), *var_type)?;
                self.variables.insert(name.clone(), typed_value.clone());
                self.plan.variables.insert(name.clone(), typed_value);
            } else if let Some(default) = default {
                // Use default value
                self.variables.insert(name.clone(), default.clone());
                self.plan.variables.insert(name.clone(), default.clone());
            } else if self.lenient_missing_vars {
                // Check mode: substitute a type-appropriate placeholder so
                // the rest of the config still resolves, and report it
                let placeholder = placeholder_value(*var_type);
                self.variables.insert(name.clone(), placeholder.clone());
                self.plan.variables.insert(name.clone(), placeholder);
                self.missing_vars.push((name.clone(), *var_type));
            } else {
                return Err(ClientError::UndefinedVariable(name.clone()));
            }
        }
        Ok(())
    }

    fn coerce_value(&self, value: Value, target_type: VarType) -> Result<Value> {
        match (&value, target_type) {
            (Value::String(s), VarType::Int) => s
                .parse::<i64>()
                .map(Value::Int)
                .map_err(|_| ClientError::TypeError(format!("Cannot convert '{}' to int", s))),
            (Value::String(s), VarType::Float) => s
                .parse::<f64>()
                .map(Value::Float)
                .map_err(|_| ClientError::TypeError(format!("Cannot convert '{}' to float", s))),
            (Value::String(s), VarType::Bool) => match s.to_lowercase().as_str() {
                "true" | "1" | "yes" => Ok(Value::Bool(true)),
                "false" | "0" | "no" => Ok(Value::Bool(false)),
                _ => Err(ClientError::TypeError(format!(
                    "Cannot convert '{}' to bool",
                    s
                ))),
            },
            (Value::String(_), VarType::String) => Ok(value),
            (Value::StringArray(_), VarType::StringArray) => Ok(value),
            (Value::Int(_), VarType::Int) => Ok(value),
            (Value::IntArray(_), VarType::IntArray) => Ok(value),
            (Value::Float(_), VarType::Float) => Ok(value),
            (Value::FloatArray(_), VarType::FloatArray) => Ok(value),
            (Value::Bool(_), VarType::Bool) => Ok(value),
            (Value::BoolArray(_), VarType::BoolArray) => Ok(value),
            _ => Ok(value), // Allow other conversions for now
        }
    }

    fn literal_to_value(&self, lit: &Literal<'_>, var_type: VarType) -> Result<Value> {
        match (lit, var_type) {
            (Literal::Int(i), VarType::Int) => Ok(Value::Int(*i)),
            (Literal::IntArray(arr), VarType::IntArray) => Ok(Value::IntArray(arr.clone())),
            (Literal::Float(f), VarType::Float) => Ok(Value::Float(*f)),
            (Literal::FloatArray(arr), VarType::FloatArray) => Ok(Value::FloatArray(arr.clone())),
            (Literal::Bool(b), VarType::Bool) => Ok(Value::Bool(*b)),
            (Literal::BoolArray(arr), VarType::BoolArray) => Ok(Value::BoolArray(arr.clone())),
            (Literal::String(s), VarType::String) => {
                let resolved = self.resolve_string_literal(s)?;
                Ok(Value::String(resolved))
            }
            (Literal::StringArray(arr), VarType::StringArray) => {
                let resolved: Result<Vec<String>> = arr
                    .iter()
                    .map(|(s, _span)| self.resolve_string_literal(s))
                    .collect();
                Ok(Value::StringArray(resolved?))
            }
            (Literal::Null, _) => Ok(Value::Null),
            (Literal::Int(i), VarType::Float) => Ok(Value::Float(*i as f64)),
            _ => Err(ClientError::TypeError(format!(
                "Type mismatch in literal"
            ))),
        }
    }

    fn process_statement(&mut self, stmt: &Statement<'_>) -> Result<()> {
        // Check #[when] condition if present
        if let Some((condition, _span)) = &stmt.attribute {
            if !self.evaluate_condition(condition)? {
                return Ok(()); // Skip this statement
            }
        }

        match &stmt.kind {
            StatementKind::Import(_) => {
                // Imports are processed in the first pass
                Ok(())
            }
            StatementKind::Var(_) => {
                // Already processed in first pass
                Ok(())
            }
            StatementKind::Faker(faker_decl) => self.process_faker(faker_decl),
            StatementKind::Relation(relation_decl) => self.process_relation(relation_decl),
            StatementKind::IgnoreRelation(relation_decl) => self.process_ignore_relation(relation_decl),
            StatementKind::Anonymize(anon_block) => self.process_anonymize(anon_block),
            StatementKind::Exclude(pattern) => {
                match pattern {
                    TablePattern::Exact((table, _span)) => {
                        self.plan.excluded_tables.insert(table.to_string());
                    }
                    TablePattern::Regex((pattern, _span)) => {
                        self.plan.excluded_patterns.push(pattern.to_string());
                    }
                }
                Ok(())
            }
            StatementKind::Ignore(pattern) => {
                match pattern {
                    TablePattern::Exact((table, _span)) => {
                        self.plan.ignored_tables.insert(table.to_string());
                    }
                    TablePattern::Regex((pattern, _span)) => {
                        self.plan.ignored_patterns.push(pattern.to_string());
                    }
                }
                Ok(())
            }
            StatementKind::Full(patterns) => {
                for pattern in patterns {
                    match pattern {
                        TablePattern::Exact((table, _span)) => {
                            self.plan.full_tables.insert(table.to_string());
                        }
                        TablePattern::Regex((pattern, _span)) => {
                            self.plan.full_patterns.push(pattern.to_string());
                        }
                    }
                }
                Ok(())
            }
            StatementKind::Aggregate(agg_block) => self.process_aggregate(agg_block),
            StatementKind::Get(get_func) => self.process_get_function(get_func),
            StatementKind::Preserve(preserve_stmt) => self.process_preserve(preserve_stmt),
            StatementKind::Set(set_block) => self.process_set(set_block),
            StatementKind::After(after_block) => {
                for (sql, _span) in &after_block.statements {
                    self.plan.after_statements.push(sql.to_string());
                }
                Ok(())
            }
        }
    }

    fn evaluate_condition(&self, expr: &Expr<'_>) -> Result<bool> {
        let value = self.evaluate_expr(expr)?;
        match value {
            Value::Bool(b) => Ok(b),
            Value::Null => Ok(false),
            _ => Err(ClientError::TypeError(
                "Condition must evaluate to bool".to_string(),
            )),
        }
    }

    fn evaluate_expr(&self, expr: &Expr<'_>) -> Result<Value> {
        match expr {
            Expr::Literal(lit) => self.eval_literal(lit),
            Expr::Variable(name) => self
                .variables
                .get(*name)
                .cloned()
                .ok_or_else(|| ClientError::UndefinedVariable(name.to_string())),
            Expr::Binary(left, op, right) => {
                let left_val = self.evaluate_expr(&left.0)?;
                let right_val = self.evaluate_expr(&right.0)?;
                self.eval_binary_op(&left_val, *op, &right_val)
            }
            Expr::Unary(op, operand) => {
                let val = self.evaluate_expr(&operand.0)?;
                self.eval_unary_op(*op, &val)
            }
            Expr::Unique => Err(ClientError::Parse(
                "unique() can only be used inside faker string interpolation".to_string(),
            )),
        }
    }

    fn eval_literal(&self, lit: &Literal<'_>) -> Result<Value> {
        match lit {
            Literal::Int(i) => Ok(Value::Int(*i)),
            Literal::IntArray(arr) => Ok(Value::IntArray(arr.clone())),
            Literal::Float(f) => Ok(Value::Float(*f)),
            Literal::FloatArray(arr) => Ok(Value::FloatArray(arr.clone())),
            Literal::Bool(b) => Ok(Value::Bool(*b)),
            Literal::BoolArray(arr) => Ok(Value::BoolArray(arr.clone())),
            Literal::Null => Ok(Value::Null),
            Literal::String(s) => {
                let resolved = self.resolve_string_literal(s)?;
                Ok(Value::String(resolved))
            }
            Literal::StringArray(arr) => {
                let resolved: Result<Vec<String>> = arr
                    .iter()
                    .map(|(s, _span)| self.resolve_string_literal(s))
                    .collect();
                Ok(Value::StringArray(resolved?))
            }
        }
    }

    fn eval_binary_op(
        &self,
        left: &Value,
        op: jibs_parser::ast::BinaryOp,
        right: &Value,
    ) -> Result<Value> {
        use jibs_parser::ast::BinaryOp::*;

        match op {
            Eq => Ok(Value::Bool(left == right)),
            NotEq => Ok(Value::Bool(left != right)),
            Lt => self.compare_values(left, right, |a, b| a < b),
            Gt => self.compare_values(left, right, |a, b| a > b),
            LtEq => self.compare_values(left, right, |a, b| a <= b),
            GtEq => self.compare_values(left, right, |a, b| a >= b),
            And => {
                let l = self.value_to_bool(left)?;
                let r = self.value_to_bool(right)?;
                Ok(Value::Bool(l && r))
            }
            Or => {
                let l = self.value_to_bool(left)?;
                let r = self.value_to_bool(right)?;
                Ok(Value::Bool(l || r))
            }
            Add => self.numeric_op(left, right, |a, b| a + b, |a, b| a + b),
            Sub => self.numeric_op(left, right, |a, b| a - b, |a, b| a - b),
            Mul => self.numeric_op(left, right, |a, b| a * b, |a, b| a * b),
            Div => self.numeric_op(left, right, |a, b| a / b, |a, b| a / b),
            Mod => self.numeric_op(left, right, |a, b| a % b, |a, b| a % b),
        }
    }

    fn compare_values<F>(&self, left: &Value, right: &Value, f: F) -> Result<Value>
    where
        F: Fn(f64, f64) -> bool,
    {
        let l = self.value_to_f64(left)?;
        let r = self.value_to_f64(right)?;
        Ok(Value::Bool(f(l, r)))
    }

    fn numeric_op<FI, FF>(
        &self,
        left: &Value,
        right: &Value,
        int_op: FI,
        float_op: FF,
    ) -> Result<Value>
    where
        FI: Fn(i64, i64) -> i64,
        FF: Fn(f64, f64) -> f64,
    {
        match (left, right) {
            (Value::Int(l), Value::Int(r)) => Ok(Value::Int(int_op(*l, *r))),
            (Value::Float(l), Value::Float(r)) => Ok(Value::Float(float_op(*l, *r))),
            (Value::Int(l), Value::Float(r)) => Ok(Value::Float(float_op(*l as f64, *r))),
            (Value::Float(l), Value::Int(r)) => Ok(Value::Float(float_op(*l, *r as f64))),
            _ => Err(ClientError::TypeError(
                "Numeric operation requires numeric operands".to_string(),
            )),
        }
    }

    fn value_to_bool(&self, value: &Value) -> Result<bool> {
        match value {
            Value::Bool(b) => Ok(*b),
            Value::Null => Ok(false),
            _ => Err(ClientError::TypeError(
                "Expected boolean value".to_string(),
            )),
        }
    }

    fn value_to_f64(&self, value: &Value) -> Result<f64> {
        match value {
            Value::Int(i) => Ok(*i as f64),
            Value::Float(f) => Ok(*f),
            _ => Err(ClientError::TypeError(
                "Expected numeric value".to_string(),
            )),
        }
    }

    fn eval_unary_op(&self, op: jibs_parser::ast::UnaryOp, val: &Value) -> Result<Value> {
        use jibs_parser::ast::UnaryOp::*;

        match op {
            Not => {
                let b = self.value_to_bool(val)?;
                Ok(Value::Bool(!b))
            }
            Neg => match val {
                Value::Int(i) => Ok(Value::Int(-i)),
                Value::Float(f) => Ok(Value::Float(-f)),
                _ => Err(ClientError::TypeError(
                    "Negation requires numeric value".to_string(),
                )),
            },
        }
    }

    fn resolve_string_literal(&self, lit: &StringLiteral<'_>) -> Result<String> {
        let mut result = String::new();
        for part in &lit.parts {
            match part {
                StringPart::Text(text) => result.push_str(text),
                StringPart::Interpolation((Expr::Unique, _span)) => {
                    // Pass through as sentinel for server-side replacement
                    result.push_str("{unique()}");
                }
                StringPart::Interpolation((expr, _span)) => {
                    let value = self.evaluate_expr(expr)?;
                    result.push_str(&value.as_string());
                }
            }
        }
        Ok(result)
    }

    fn process_faker(&mut self, faker_decl: &FakerDecl<'_>) -> Result<()> {
        let name = faker_decl.name.0.to_string();
        let values = match &faker_decl.source {
            FakerSource::Array(arr) => {
                let mut values = Vec::new();
                for (faker_value, _span) in arr {
                    match faker_value {
                        FakerValue::Literal(lit) => {
                            let resolved = self.resolve_string_literal(lit)?;
                            values.push(resolved);
                        }
                        FakerValue::Spread(var_name) => {
                            self.spread_string_array_into(&mut values, var_name)?;
                        }
                    }
                }
                values
            }
            FakerSource::Variable(var_name) => {
                // Direct variable reference - must be a string array
                let var_value = self
                    .variables
                    .get(*var_name)
                    .ok_or_else(|| ClientError::UndefinedVariable(var_name.to_string()))?;

                match var_value.as_string_array() {
                    Some(arr) => arr.to_vec(),
                    None => {
                        return Err(ClientError::TypeError(format!(
                            "Variable '{}' must be string[] for faker, got {}",
                            var_name,
                            self.value_type_name(var_value)
                        )));
                    }
                }
            }
        };

        self.plan.fakers.insert(name, values);
        Ok(())
    }

    fn spread_string_array_into(&self, values: &mut Vec<String>, var_name: &str) -> Result<()> {
        let var_value = self
            .variables
            .get(var_name)
            .ok_or_else(|| ClientError::UndefinedVariable(var_name.to_string()))?;

        match var_value.as_string_array() {
            Some(arr) => {
                values.extend(arr.iter().cloned());
                Ok(())
            }
            None => Err(ClientError::TypeError(format!(
                "Cannot spread variable '{}': expected string[], got {}",
                var_name,
                self.value_type_name(var_value)
            ))),
        }
    }

    fn value_type_name(&self, value: &Value) -> &'static str {
        match value {
            Value::String(_) => "string",
            Value::StringArray(_) => "string[]",
            Value::Int(_) => "int",
            Value::IntArray(_) => "int[]",
            Value::Float(_) => "float",
            Value::FloatArray(_) => "float[]",
            Value::Bool(_) => "bool",
            Value::BoolArray(_) => "bool[]",
            Value::Null => "null",
        }
    }

    fn process_relation(&mut self, relation_decl: &RelationDecl<'_>) -> Result<()> {
        let from = &relation_decl.from.0;
        let to = &relation_decl.to.0;

        self.plan.relations.push(Relation {
            from_table: from.table.to_string(),
            from_column: from.column.to_string(),
            to_table: to.table.to_string(),
            to_column: to.column.to_string(),
        });
        Ok(())
    }

    fn process_ignore_relation(&mut self, relation_decl: &RelationDecl<'_>) -> Result<()> {
        let from = &relation_decl.from.0;
        let to = &relation_decl.to.0;

        self.plan.ignored_relations.push(Relation {
            from_table: from.table.to_string(),
            from_column: from.column.to_string(),
            to_table: to.table.to_string(),
            to_column: to.column.to_string(),
        });
        Ok(())
    }

    fn process_anonymize(&mut self, anon_block: &AnonymizeBlock<'_>) -> Result<()> {
        let table = anon_block.table.0.to_string();
        let mut rules = Vec::new();

        for (rule, _span) in &anon_block.rules {
            let column = rule.column.0.to_string();
            let target = match &rule.target {
                jibs_parser::ast::AnonymizeTarget::Faker((name, _span)) => {
                    AnonymizeTarget::Faker(name.to_string())
                }
                jibs_parser::ast::AnonymizeTarget::Null => AnonymizeTarget::Null,
            };
            rules.push(AnonymizeRule { column, target });
        }

        self.plan.anonymization.insert(table, rules);
        Ok(())
    }

    fn process_aggregate(&mut self, agg_block: &AggregateBlock<'_>) -> Result<()> {
        let name = agg_block.name.0.to_string();
        let root_table = agg_block.root.0.to_string();

        let where_clause = if let Some((lit, _span)) = &agg_block.where_clause {
            Some(self.resolve_string_literal(lit)?)
        } else {
            None
        };

        let order_by = agg_block.order_by.as_ref().map(|o| o.column.0.to_string());
        let order_direction = agg_block.order_by.as_ref().and_then(|o| {
            o.direction.map(|d| match d {
                AstSortDirection::Asc => SortDirection::Asc,
                AstSortDirection::Desc => SortDirection::Desc,
            })
        });

        let limit = if let Some((limit_val, _span)) = &agg_block.limit {
            match limit_val {
                LimitValue::Literal(n) => Some(*n),
                LimitValue::Variable(name) => {
                    let value = self
                        .variables
                        .get(*name)
                        .ok_or_else(|| ClientError::UndefinedVariable(name.to_string()))?;
                    value.as_int()
                }
            }
        } else {
            None
        };

        let mut exclude_tables = Vec::new();
        let mut exclude_patterns = Vec::new();
        for pattern in &agg_block.exclude_tables {
            match pattern {
                TablePattern::Exact((name, _span)) => {
                    exclude_tables.push(name.to_string());
                }
                TablePattern::Regex((pat, _span)) => {
                    exclude_patterns.push(pat.to_string());
                }
            }
        }

        self.plan.aggregates.push(ResolvedAggregate {
            name,
            root_table,
            where_clause,
            order_by,
            order_direction,
            limit,
            exclude_tables,
            exclude_patterns,
            root_only: agg_block.root_only,
        });
        Ok(())
    }

    fn process_get_function(&mut self, func: &GetFunctionDef<'_>) -> Result<()> {
        let name = func.name.0.to_string();
        let aggregate_name = func.aggregate.0.to_string();

        // Validate the referenced aggregate exists
        if !self.plan.aggregates.iter().any(|a| a.name == aggregate_name) {
            return Err(ClientError::Parse(format!(
                "Get function '{}' references unknown aggregate '{}'. Available aggregates: {}",
                name,
                aggregate_name,
                self.plan
                    .aggregates
                    .iter()
                    .map(|a| a.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }

        // Resolve parameters (names + defaults)
        let param_names: Vec<&str> = func.params.iter().map(|(decl, _)| decl.name.0).collect();
        let mut resolved_params = Vec::new();
        for (decl, _span) in &func.params {
            let param_name = decl.name.0.to_string();
            let param_type = decl.var_type.0;
            let default = if let Some((lit, _span)) = &decl.default {
                Some(self.literal_to_value(lit, param_type)?)
            } else {
                None
            };
            resolved_params.push(ResolvedParam {
                name: param_name,
                param_type,
                default,
            });
        }

        // Resolve WHERE template (parameters left as {param_name} placeholders)
        let where_template = if let Some((lit, _span)) = &func.where_clause {
            Some(self.resolve_string_literal_with_params(lit, &param_names)?)
        } else {
            None
        };

        // Resolve order_by (concrete, no param references)
        let order_by = func.order_by.as_ref().map(|o| o.column.0.to_string());
        let order_direction = func.order_by.as_ref().and_then(|o| {
            o.direction.map(|d| match d {
                AstSortDirection::Asc => SortDirection::Asc,
                AstSortDirection::Desc => SortDirection::Desc,
            })
        });

        // Resolve limit (may reference a param)
        let limit = if let Some((limit_val, _span)) = &func.limit {
            match limit_val {
                LimitValue::Literal(n) => Some(LimitOverride::Concrete(*n)),
                LimitValue::Variable(var_name) => {
                    if param_names.contains(var_name) {
                        Some(LimitOverride::Param(var_name.to_string()))
                    } else {
                        // Resolve from global variables
                        let value = self.variables.get(*var_name).ok_or_else(|| {
                            ClientError::UndefinedVariable(var_name.to_string())
                        })?;
                        Some(LimitOverride::Concrete(value.as_int().ok_or_else(
                            || {
                                ClientError::TypeError(format!(
                                    "Variable '{}' must be an integer for limit",
                                    var_name
                                ))
                            },
                        )?))
                    }
                }
            }
        } else {
            None
        };

        // Resolve excludes (concrete)
        let mut exclude_tables = Vec::new();
        let mut exclude_patterns = Vec::new();
        for pattern in &func.exclude_tables {
            match pattern {
                TablePattern::Exact((name, _span)) => {
                    exclude_tables.push(name.to_string());
                }
                TablePattern::Regex((pat, _span)) => {
                    exclude_patterns.push(pat.to_string());
                }
            }
        }

        let root_only = if func.root_only { Some(true) } else { None };

        self.get_functions.push(ResolvedGetFunction {
            name,
            params: resolved_params,
            aggregate_name,
            where_template,
            order_by,
            order_direction,
            limit,
            exclude_tables,
            exclude_patterns,
            root_only,
        });

        Ok(())
    }

    /// Resolve a string literal but leave function parameters as {param_name} placeholders
    fn resolve_string_literal_with_params(
        &self,
        lit: &StringLiteral<'_>,
        param_names: &[&str],
    ) -> Result<String> {
        let mut result = String::new();
        for part in &lit.parts {
            match part {
                StringPart::Text(text) => result.push_str(text),
                StringPart::Interpolation((Expr::Variable(name), _span))
                    if param_names.contains(name) =>
                {
                    // Leave as placeholder for runtime substitution
                    result.push('{');
                    result.push_str(name);
                    result.push('}');
                }
                StringPart::Interpolation((Expr::Unique, _span)) => {
                    result.push_str("{unique()}");
                }
                StringPart::Interpolation((expr, _span)) => {
                    let value = self.evaluate_expr(expr)?;
                    result.push_str(&value.as_string());
                }
            }
        }
        Ok(result)
    }

    fn process_preserve(&mut self, preserve_stmt: &PreserveStmt<'_>) -> Result<()> {
        let table = preserve_stmt.table.0.to_string();
        let where_clause = self.resolve_string_literal(&preserve_stmt.where_clause.0)?;

        self.plan.preserves.push(PreserveRule {
            table,
            where_clause,
        });
        Ok(())
    }

    fn process_set(&mut self, set_block: &SetBlock<'_>) -> Result<()> {
        let table = set_block.table.0.to_string();

        let match_clause: Vec<Assignment> = set_block
            .match_clause
            .iter()
            .map(|(assign, _span)| self.resolve_assignment(assign))
            .collect::<Result<Vec<_>>>()?;

        let assignments: Vec<Assignment> = set_block
            .assignments
            .iter()
            .map(|(assign, _span)| self.resolve_assignment(assign))
            .collect::<Result<Vec<_>>>()?;

        self.plan.sets.push(SetRule {
            table,
            match_clause,
            assignments,
        });
        Ok(())
    }

    fn resolve_assignment(
        &self,
        assign: &jibs_parser::ast::Assignment<'_>,
    ) -> Result<Assignment> {
        let column = assign.column.0.to_string();
        let value = match &assign.value.0 {
            jibs_parser::ast::Value::Literal(lit) => self.eval_literal(lit)?,
            jibs_parser::ast::Value::Variable(name) => self
                .variables
                .get(*name)
                .cloned()
                .ok_or_else(|| ClientError::UndefinedVariable(name.to_string()))?,
            jibs_parser::ast::Value::Expr(expr) => self.evaluate_expr(expr)?,
        };

        Ok(Assignment { column, value })
    }
}

/// Type-appropriate placeholder for a missing variable in check mode
fn placeholder_value(var_type: VarType) -> Value {
    match var_type {
        VarType::String => Value::String(String::new()),
        VarType::Int => Value::Int(0),
        VarType::Float => Value::Float(0.0),
        VarType::Bool => Value::Bool(false),
        VarType::StringArray => Value::StringArray(Vec::new()),
        VarType::IntArray => Value::IntArray(Vec::new()),
        VarType::FloatArray => Value::FloatArray(Vec::new()),
        VarType::BoolArray => Value::BoolArray(Vec::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolve_str(source: &str) -> Result<ResolvedConfig> {
        let program = jibs_parser::parse(source).expect("test source must parse");
        resolve(Path::new("test.jibs"), &program, &HashMap::new())
    }

    fn resolve_lenient_str(source: &str) -> Result<ResolvedConfig> {
        let program = jibs_parser::parse(source).expect("test source must parse");
        resolve_lenient(Path::new("test.jibs"), &program, &HashMap::new())
    }

    #[test]
    fn undefined_faker_is_a_resolution_error() {
        // A typo'd faker name would silently NULL the column at import time
        let err = resolve_str(
            r#"
            anonymize users {
                email -> emals
            }
            "#,
        )
        .expect_err("undefined faker must fail");
        assert!(
            err.to_string().contains("undefined faker 'emals'"),
            "got: {}",
            err
        );
    }

    #[test]
    fn defined_faker_passes() {
        let config = resolve_str(
            r#"
            faker emails ["a@test", "b@test"]
            anonymize users {
                email -> emails
            }
            "#,
        )
        .expect("valid config");
        assert_eq!(config.plan.anonymization.len(), 1);
    }

    #[test]
    fn get_function_with_unknown_aggregate_is_an_error() {
        let err = resolve_str(
            r#"
            get order_by_id (id: int) {
                orders where "id = {$id}"
            }
            "#,
        )
        .expect_err("unknown aggregate must fail");
        assert!(
            err.to_string().contains("unknown aggregate 'orders'"),
            "got: {}",
            err
        );
    }

    #[test]
    fn invalid_regex_pattern_is_an_error() {
        let err = resolve_str("ignore_table /[unclosed/")
            .expect_err("invalid regex must fail");
        assert!(err.to_string().contains("invalid regex"), "got: {}", err);
    }

    #[test]
    fn strict_resolve_errors_on_missing_variable() {
        let err = resolve_str(
            r#"
            var base_url: string
            "#,
        )
        .expect_err("missing variable must fail strict resolution");
        assert!(matches!(err, ClientError::UndefinedVariable(_)));
    }

    #[test]
    fn lenient_resolve_placeholders_and_reports_missing_variables() {
        let config = resolve_lenient_str(
            r#"
            var base_url: string
            var order_limit: int

            aggregate orders {
                root sales_order
                where "url = '{$base_url}'"
                limit $order_limit
            }
            "#,
        )
        .expect("lenient resolution must succeed");

        let mut names: Vec<&str> = config.missing_vars.iter().map(|(n, _)| n.as_str()).collect();
        names.sort();
        assert_eq!(names, vec!["base_url", "order_limit"]);

        // The aggregate still resolved, with placeholder values substituted
        assert_eq!(config.plan.aggregates.len(), 1);
        assert_eq!(
            config.plan.aggregates[0].where_clause.as_deref(),
            Some("url = ''")
        );
    }

    #[test]
    fn lenient_resolve_still_errors_on_undeclared_variable() {
        // Only declared-but-unset variables are placeholdered; a reference
        // to a variable that is never declared is a real config bug
        let err = resolve_lenient_str(
            r#"
            aggregate orders {
                root sales_order
                where "id = {$never_declared}"
            }
            "#,
        )
        .expect_err("undeclared variable must fail even in lenient mode");
        assert!(matches!(err, ClientError::UndefinedVariable(_)));
    }
}
