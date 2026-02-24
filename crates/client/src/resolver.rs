//! DSL resolution - evaluating conditions and interpolating variables

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use jibs_parser::ast::{
    AggregateBlock, AnonymizeBlock, Expr, FakerDecl, FakerSource, FakerValue, IncludeStmt,
    LimitValue, Literal, PreserveStmt, Program, RelationDecl, SetBlock,
    SortDirection as AstSortDirection, Statement, StatementKind, StringLiteral, StringPart,
    TablePattern, VarDecl, VarType,
};
use jibs_protocol::{
    AnonymizeRule, AnonymizeTarget, Assignment, ExecutionPlan, PreserveRule, Relation,
    ResolvedAggregate, SetRule, SortDirection, Value,
};

use crate::error::{ClientError, Result};

/// Resolve a parsed program into an execution plan
///
/// `base_path` is the path to the .jibs file being resolved, used for resolving
/// relative import paths.
pub fn resolve(
    base_path: &Path,
    program: &Program<'_>,
    cli_vars: &HashMap<String, String>,
) -> Result<ExecutionPlan> {
    let mut resolver = Resolver::new(base_path, cli_vars.clone());
    resolver.resolve_program(program)
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
        }
    }

    fn resolve_program(&mut self, program: &Program<'_>) -> Result<ExecutionPlan> {
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

        Ok(std::mem::take(&mut self.plan))
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
                "Parse error in '{}': {}",
                import_path,
                errors
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
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
            StatementKind::Include(include_stmt) => self.process_include(include_stmt),
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

    fn process_include(&mut self, include_stmt: &IncludeStmt<'_>) -> Result<()> {
        let aggregate_name = include_stmt.aggregate.0.to_string();

        if let Some(existing) = self.plan.aggregates.iter().find(|a| a.name == aggregate_name) {
            let mut new_agg = existing.clone();
            new_agg.name = format!("{}_include_{}", aggregate_name, self.plan.aggregates.len());

            match &include_stmt.where_clause {
                Some(wc) => {
                    // Override with the specified where clause
                    new_agg.where_clause = Some(self.resolve_string_literal(&wc.0)?);
                }
                None => {
                    // No where clause: import the full root table (+ relations)
                    new_agg.where_clause = None;
                    new_agg.order_by = None;
                    new_agg.order_direction = None;
                    new_agg.limit = None;
                }
            }

            self.plan.aggregates.push(new_agg);
        }
        Ok(())
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
