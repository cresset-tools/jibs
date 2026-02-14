//! DSL resolution - evaluating conditions and interpolating variables

use std::collections::HashMap;

use jibs_parser::ast::{
    AggregateBlock, AnonymizeBlock, Expr, FakerDecl, IncludeStmt, LimitValue, Literal,
    PreserveStmt, Program, RelationDecl, SetBlock, SortDirection as AstSortDirection, Statement,
    StatementKind, StringLiteral, StringPart, VarDecl, VarType,
};
use jibs_protocol::{
    AnonymizeRule, AnonymizeTarget, Assignment, ExecutionPlan, PreserveRule, Relation,
    ResolvedAggregate, SetRule, SortDirection, Value,
};

use crate::error::{ClientError, Result};

/// Resolve a parsed program into an execution plan
pub fn resolve(
    _source: &str,
    program: &Program<'_>,
    cli_vars: &HashMap<String, String>,
) -> Result<ExecutionPlan> {
    let mut resolver = Resolver::new(cli_vars.clone());
    resolver.resolve_program(program)
}

/// State for the resolver
struct Resolver {
    /// Variable values (from CLI, files, or defaults)
    variables: HashMap<String, Value>,
    /// Pending variable declarations (name -> (type, default))
    pending_vars: HashMap<String, (VarType, Option<Value>)>,
    /// The execution plan being built
    plan: ExecutionPlan,
}

impl Resolver {
    fn new(cli_vars: HashMap<String, String>) -> Self {
        // Convert CLI string vars to Values (we'll validate types later)
        let mut variables = HashMap::new();
        for (k, v) in cli_vars {
            variables.insert(k, Value::String(v));
        }

        Self {
            variables,
            pending_vars: HashMap::new(),
            plan: ExecutionPlan::new(),
        }
    }

    fn resolve_program(&mut self, program: &Program<'_>) -> Result<ExecutionPlan> {
        // First pass: collect all variable declarations
        for (stmt, _span) in &program.statements {
            if let StatementKind::Var(var_decl) = &stmt.kind {
                self.collect_var_decl(var_decl)?;
            }
        }

        // Validate and finalize variable values
        self.finalize_variables()?;

        // Second pass: process all statements (evaluating #[when] conditions)
        for (stmt, _span) in &program.statements {
            self.process_statement(stmt)?;
        }

        Ok(std::mem::take(&mut self.plan))
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
            (Value::Int(_), VarType::Int) => Ok(value),
            (Value::Float(_), VarType::Float) => Ok(value),
            (Value::Bool(_), VarType::Bool) => Ok(value),
            _ => Ok(value), // Allow other conversions for now
        }
    }

    fn literal_to_value(&self, lit: &Literal<'_>, var_type: VarType) -> Result<Value> {
        match (lit, var_type) {
            (Literal::Int(i), VarType::Int) => Ok(Value::Int(*i)),
            (Literal::Float(f), VarType::Float) => Ok(Value::Float(*f)),
            (Literal::Bool(b), VarType::Bool) => Ok(Value::Bool(*b)),
            (Literal::String(s), VarType::String) => {
                let resolved = self.resolve_string_literal(s)?;
                Ok(Value::String(resolved))
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
                // TODO: Handle imports
                Ok(())
            }
            StatementKind::Var(_) => {
                // Already processed in first pass
                Ok(())
            }
            StatementKind::Faker(faker_decl) => self.process_faker(faker_decl),
            StatementKind::Relation(relation_decl) => self.process_relation(relation_decl),
            StatementKind::Anonymize(anon_block) => self.process_anonymize(anon_block),
            StatementKind::Exclude((table, _span)) => {
                self.plan.excluded_tables.insert(table.to_string());
                Ok(())
            }
            StatementKind::Ignore((table, _span)) => {
                self.plan.ignored_tables.insert(table.to_string());
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
            Literal::Float(f) => Ok(Value::Float(*f)),
            Literal::Bool(b) => Ok(Value::Bool(*b)),
            Literal::Null => Ok(Value::Null),
            Literal::String(s) => {
                let resolved = self.resolve_string_literal(s)?;
                Ok(Value::String(resolved))
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
        let mut values = Vec::new();
        for (lit, _span) in &faker_decl.values {
            let resolved = self.resolve_string_literal(lit)?;
            values.push(resolved);
        }
        self.plan.fakers.insert(name, values);
        Ok(())
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

        self.plan.aggregates.push(ResolvedAggregate {
            name,
            root_table,
            where_clause,
            order_by,
            order_direction,
            limit,
        });
        Ok(())
    }

    fn process_include(&mut self, include_stmt: &IncludeStmt<'_>) -> Result<()> {
        // Find the aggregate and add another where clause
        let aggregate_name = include_stmt.aggregate.0.to_string();
        let where_clause = self.resolve_string_literal(&include_stmt.where_clause.0)?;

        // For simplicity, we treat include as creating a new aggregate entry
        // with the same root table but different where clause
        if let Some(existing) = self.plan.aggregates.iter().find(|a| a.name == aggregate_name) {
            let mut new_agg = existing.clone();
            new_agg.name = format!("{}_include_{}", aggregate_name, self.plan.aggregates.len());
            new_agg.where_clause = Some(where_clause);
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
