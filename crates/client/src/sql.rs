//! Local SQL generation and execution: table DDL from remote schemas and
//! set-block (upsert) handling

use anyhow::Result;
use mysql::prelude::*;
use mysql::Conn;
use tracing::{debug, info};

use jibs_protocol::{ColumnDef, SetRule, Value};

pub(crate) fn create_table(
    conn: &mut Conn,
    table: &str,
    columns: &[ColumnDef],
    anon_rules: Option<&Vec<jibs_protocol::AnonymizeRule>>,
) -> Result<()> {
    use jibs_protocol::AnonymizeTarget;

    // Drop existing table
    conn.query_drop(format!("DROP TABLE IF EXISTS `{}`", table))?;

    let mut column_defs = Vec::new();

    for col in columns {
        // Use full_type which includes the complete type definition
        // (e.g., "enum('a','b')", "varchar(255)", "int unsigned")
        let mut def = format!("`{}` {}", col.name, col.full_type);

        // Check if this column is being anonymized to NULL
        let is_anonymized_to_null = anon_rules
            .map(|rules| {
                rules
                    .iter()
                    .any(|r| r.column == col.name && matches!(r.target, AnonymizeTarget::Null))
            })
            .unwrap_or(false);

        // Make column nullable if not already or if being anonymized to NULL
        if !col.nullable && !is_anonymized_to_null {
            def.push_str(" NOT NULL");
        }

        if col.flags.auto_increment {
            def.push_str(" AUTO_INCREMENT");
        }

        column_defs.push(def);
    }

    // Add primary key
    let pk_cols: Vec<&str> = columns
        .iter()
        .filter(|c| c.is_primary_key)
        .map(|c| c.name.as_str())
        .collect();

    if !pk_cols.is_empty() {
        column_defs.push(format!(
            "PRIMARY KEY ({})",
            pk_cols
                .iter()
                .map(|c| format!("`{}`", c))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    let create_sql = format!(
        "CREATE TABLE `{}` (\n  {}\n)",
        table,
        column_defs.join(",\n  ")
    );

    debug!("Creating table: {}", create_sql);
    conn.query_drop(&create_sql)?;
    Ok(())
}

/// Load TSV data into a table using LOAD DATA LOCAL INFILE

/// Execute a set (upsert) block
///
/// Logic:
/// 1. Check if a row matching the match_clause exists
/// 2. If found: UPDATE with the assignments
pub(crate) fn execute_set_block(conn: &mut Conn, set_rule: &SetRule) -> Result<()> {
    // Build WHERE clause from match conditions
    let where_parts: Vec<String> = set_rule
        .match_clause
        .iter()
        .map(|a| format!("`{}` = {}", a.column, value_to_sql(&a.value)))
        .collect();
    let where_clause = where_parts.join(" AND ");

    // Check if row exists
    let select_sql = format!(
        "SELECT 1 FROM `{}` WHERE {} LIMIT 1",
        set_rule.table, where_clause
    );
    debug!("Set block check: {}", select_sql);

    let exists: Option<u8> = conn.query_first(&select_sql)?;

    if exists.is_some() {
        // Row exists - UPDATE
        if !set_rule.assignments.is_empty() {
            let set_parts: Vec<String> = set_rule
                .assignments
                .iter()
                .map(|a| format!("`{}` = {}", a.column, value_to_sql(&a.value)))
                .collect();

            let update_sql = format!(
                "UPDATE `{}` SET {} WHERE {}",
                set_rule.table,
                set_parts.join(", "),
                where_clause
            );
            debug!("Set block update: {}", update_sql);
            conn.query_drop(&update_sql)?;
            info!(
                "Updated row in {} where {}",
                set_rule.table, where_clause
            );
        }
    } else {
        // Row doesn't exist - INSERT
        let mut all_assignments: Vec<_> = set_rule.match_clause.iter().collect();
        all_assignments.extend(set_rule.assignments.iter());

        let columns: Vec<String> = all_assignments
            .iter()
            .map(|a| format!("`{}`", a.column))
            .collect();
        let values: Vec<String> = all_assignments
            .iter()
            .map(|a| value_to_sql(&a.value))
            .collect();

        let insert_sql = format!(
            "INSERT INTO `{}` ({}) VALUES ({})",
            set_rule.table,
            columns.join(", "),
            values.join(", ")
        );
        debug!("Set block insert: {}", insert_sql);
        conn.query_drop(&insert_sql)?;
        info!(
            "Inserted row into {} with {}",
            set_rule.table, where_clause
        );
    }

    Ok(())
}

/// Convert a Value to SQL literal
fn value_to_sql(value: &Value) -> String {
    match value {
        Value::String(s) => {
            // Escape single quotes
            let escaped = s.replace('\'', "''");
            format!("'{}'", escaped)
        }
        Value::StringArray(arr) => {
            // Convert array to comma-separated quoted strings
            arr.iter()
                .map(|s| {
                    let escaped = s.replace('\'', "''");
                    format!("'{}'", escaped)
                })
                .collect::<Vec<_>>()
                .join(", ")
        }
        Value::Int(i) => i.to_string(),
        Value::IntArray(arr) => arr
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(", "),
        Value::Float(f) => f.to_string(),
        Value::FloatArray(arr) => arr
            .iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>()
            .join(", "),
        Value::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        Value::BoolArray(arr) => arr
            .iter()
            .map(|b| if *b { "1" } else { "0" })
            .collect::<Vec<_>>()
            .join(", "),
        Value::Null => "NULL".to_string(),
    }
}

