//! Local SQL generation and execution: table DDL from remote schemas and
//! set-block (upsert) handling

use anyhow::Result;
use mysql::prelude::*;
use mysql::Conn;
use tracing::{debug, info};

use jibs_protocol::{ColumnDef, IndexDef, IndexKind, SetRule, TableOptions, Value};

/// Create a table in local MySQL from the remote schema.
///
/// When `upsert` is true (the `get` command), an existing table is kept as-is so
/// its rows survive and incoming rows are merged in; only missing tables are
/// created. Otherwise the table is dropped and recreated.
pub(crate) fn create_table(
    conn: &mut Conn,
    table: &str,
    columns: &[ColumnDef],
    indexes: &[IndexDef],
    options: &TableOptions,
    anon_rules: Option<&Vec<jibs_protocol::AnonymizeRule>>,
    upsert: bool,
) -> Result<()> {
    use jibs_protocol::AnonymizeTarget;

    if upsert {
        if table_exists(conn, table)? {
            debug!("Table {} already exists, keeping it for upsert", table);
            return Ok(());
        }
    } else {
        conn.query_drop(format!("DROP TABLE IF EXISTS `{}`", table))?;
    }

    let mut column_defs = Vec::new();

    for col in columns {
        // Use full_type which includes the complete type definition
        // (e.g., "enum('a','b')", "varchar(255)", "int unsigned")
        let mut def = format!("`{}` {}", col.name, col.full_type);

        // Per-column collation, emitted only when it differs from the table
        // default. COLUMN_TYPE (full_type) doesn't carry COLLATE, so without this
        // the column silently takes the table default and case/sort semantics can
        // change.
        if let Some(col_collation) = &col.collation {
            if options.collation.as_deref() != Some(col_collation.as_str()) {
                def.push_str(&format!(" COLLATE {}", col_collation));
            }
        }

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

    // Secondary indexes (unique + non-unique). PRIMARY is already emitted above;
    // the server never sends it in `indexes`.
    for idx in indexes {
        column_defs.push(index_ddl(idx));
    }

    let mut create_sql = format!(
        "CREATE TABLE `{}` (\n  {}\n)",
        table,
        column_defs.join(",\n  ")
    );

    create_sql.push_str(&table_options_ddl(options));

    debug!("Creating table: {}", create_sql);
    conn.query_drop(&create_sql)?;
    Ok(())
}

/// Render the table options that follow `CREATE TABLE (...)`: engine, default
/// charset/collation and row format so the copy doesn't silently inherit the
/// server/database defaults, and the source's AUTO_INCREMENT counter. InnoDB
/// keeps the higher of that counter and max(id)+1 as rows are loaded.
fn table_options_ddl(options: &TableOptions) -> String {
    let mut ddl = String::new();
    if let Some(engine) = &options.engine {
        ddl.push_str(&format!(" ENGINE={}", engine));
    }
    if let Some(charset) = &options.charset {
        ddl.push_str(&format!(" DEFAULT CHARSET={}", charset));
    }
    if let Some(collation) = &options.collation {
        ddl.push_str(&format!(" COLLATE={}", collation));
    }
    if let Some(row_format) = &options.row_format {
        ddl.push_str(&format!(" ROW_FORMAT={}", row_format));
    }
    if let Some(auto_increment) = options.auto_increment {
        ddl.push_str(&format!(" AUTO_INCREMENT={}", auto_increment));
    }
    ddl
}

/// Render one secondary index as a `CREATE TABLE` clause.
fn index_ddl(idx: &IndexDef) -> String {
    let keyword = if idx.unique {
        "UNIQUE KEY"
    } else {
        match idx.kind {
            IndexKind::Fulltext => "FULLTEXT KEY",
            IndexKind::Spatial => "SPATIAL KEY",
            _ => "KEY",
        }
    };

    let cols = idx
        .columns
        .iter()
        .map(|c| {
            if let Some(expr) = &c.expression {
                // Functional key part: MySQL wants it wrapped in parentheses.
                format!("({})", expr)
            } else {
                let mut part = format!("`{}`", c.name);
                if let Some(len) = c.prefix_len {
                    part.push_str(&format!("({})", len));
                }
                if c.descending {
                    part.push_str(" DESC");
                }
                part
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    // BTREE is the default; HASH needs to be stated (MEMORY tables). FULLTEXT /
    // SPATIAL are conveyed by the keyword above, not USING.
    let using = if matches!(idx.kind, IndexKind::Hash) {
        " USING HASH"
    } else {
        ""
    };

    format!("{} `{}` ({}){}", keyword, idx.name, cols, using)
}

/// Load TSV data into a table using LOAD DATA LOCAL INFILE

/// Whether `table` exists in the current database.
pub(crate) fn table_exists(conn: &mut Conn, table: &str) -> Result<bool> {
    let found: Option<String> = conn.exec_first(
        "SELECT TABLE_NAME FROM information_schema.TABLES \
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
        (table,),
    )?;
    Ok(found.is_some())
}

/// `--keep-tables`: an existing table is loaded into as it is, so it has to
/// have the columns the source sends, with the same types. A table that does
/// not exist yet is fine (it is created from the source schema).
///
/// Loading into a table that drifted would fail with a LOAD DATA error, or put
/// a value in a column of another type; refuse when the table's schema
/// arrives, before any of its rows are loaded, and say which column.
pub(crate) fn ensure_table_matches(conn: &mut Conn, table: &str, columns: &[ColumnDef]) -> Result<()> {
    let local: Vec<(String, String)> = conn.exec(
        "SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
        (table,),
    )?;
    if local.is_empty() {
        return Ok(());
    }
    if let Some(difference) = column_difference(&local, columns) {
        anyhow::bail!(
            "--keep-tables: local table `{table}` does not match the source ({difference}). \
             Recreate it from the source (an import without --keep-tables does), then retry."
        );
    }
    Ok(())
}

/// The first way `local` (name, type) differs from the source's columns, if any.
fn column_difference(local: &[(String, String)], source: &[ColumnDef]) -> Option<String> {
    for col in source {
        match local.iter().find(|(name, _)| name == &col.name) {
            None => return Some(format!("column `{}` is missing locally", col.name)),
            Some((_, local_type)) if !local_type.eq_ignore_ascii_case(&col.full_type) => {
                return Some(format!(
                    "column `{}` is {} locally and {} on the source",
                    col.name, local_type, col.full_type
                ));
            }
            Some(_) => {}
        }
    }
    local
        .iter()
        .find(|(name, _)| !source.iter().any(|c| &c.name == name))
        .map(|(name, _)| format!("column `{name}` exists only locally"))
}

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

#[cfg(test)]
mod tests {
    use super::{column_difference, index_ddl, table_options_ddl};
    use jibs_protocol::{ColumnDef, IndexColumn, IndexDef, IndexKind, TableOptions};

    fn source_col(name: &str, full_type: &str) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            type_name: String::new(),
            full_type: full_type.into(),
            max_length: None,
            nullable: true,
            is_primary_key: false,
            charset: None,
            collation: None,
            flags: Default::default(),
        }
    }

    #[test]
    fn a_kept_table_must_have_the_source_columns() {
        let source = [source_col("id", "int(10) unsigned"), source_col("email", "varchar(255)")];
        let local = |cols: &[(&str, &str)]| -> Vec<(String, String)> {
            cols.iter().map(|(n, t)| (n.to_string(), t.to_string())).collect()
        };
        // Same columns, whatever their order or the type's case.
        assert_eq!(
            column_difference(&local(&[("email", "VARCHAR(255)"), ("id", "int(10) unsigned")]), &source),
            None
        );
        assert_eq!(
            column_difference(&local(&[("id", "int(10) unsigned")]), &source).unwrap(),
            "column `email` is missing locally"
        );
        assert_eq!(
            column_difference(&local(&[("id", "int(10) unsigned"), ("email", "text")]), &source).unwrap(),
            "column `email` is text locally and varchar(255) on the source"
        );
        assert_eq!(
            column_difference(
                &local(&[("id", "int(10) unsigned"), ("email", "varchar(255)"), ("note", "text")]),
                &source
            )
            .unwrap(),
            "column `note` exists only locally"
        );
    }

    #[test]
    fn table_options_include_auto_increment() {
        let options = TableOptions {
            engine: Some("InnoDB".to_string()),
            charset: Some("utf8mb4".to_string()),
            collation: Some("utf8mb4_general_ci".to_string()),
            row_format: None,
            auto_increment: Some(1042),
        };
        assert_eq!(
            table_options_ddl(&options),
            " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci AUTO_INCREMENT=1042"
        );
    }

    #[test]
    fn table_options_omit_missing_auto_increment() {
        assert_eq!(table_options_ddl(&TableOptions::default()), "");
    }

    fn part(name: &str) -> IndexColumn {
        IndexColumn {
            name: name.to_string(),
            prefix_len: None,
            descending: false,
            expression: None,
        }
    }

    fn idx(name: &str, columns: Vec<IndexColumn>, unique: bool, kind: IndexKind) -> IndexDef {
        IndexDef {
            name: name.to_string(),
            columns,
            unique,
            kind,
        }
    }

    #[test]
    fn unique_single_column() {
        let d = idx("EMAIL_WEBSITE", vec![part("email"), part("website_id")], true, IndexKind::BTree);
        assert_eq!(index_ddl(&d), "UNIQUE KEY `EMAIL_WEBSITE` (`email`, `website_id`)");
    }

    #[test]
    fn non_unique_secondary() {
        let d = idx("STORE_ID", vec![part("store_id")], false, IndexKind::BTree);
        assert_eq!(index_ddl(&d), "KEY `STORE_ID` (`store_id`)");
    }

    #[test]
    fn prefix_length() {
        let mut p = part("body");
        p.prefix_len = Some(255);
        let d = idx("BODY", vec![p], false, IndexKind::BTree);
        assert_eq!(index_ddl(&d), "KEY `BODY` (`body`(255))");
    }

    #[test]
    fn fulltext() {
        let d = idx("FTS", vec![part("content")], false, IndexKind::Fulltext);
        assert_eq!(index_ddl(&d), "FULLTEXT KEY `FTS` (`content`)");
    }

    #[test]
    fn descending_part() {
        let mut p = part("created_at");
        p.descending = true;
        let d = idx("RECENT", vec![p], false, IndexKind::BTree);
        assert_eq!(index_ddl(&d), "KEY `RECENT` (`created_at` DESC)");
    }

    #[test]
    fn expression_part_double_parens() {
        let mut p = part("");
        p.expression = Some("json_value(`d`,'$.x')".to_string());
        let d = idx("FUNC", vec![p], false, IndexKind::BTree);
        assert_eq!(index_ddl(&d), "KEY `FUNC` ((json_value(`d`,'$.x')))");
    }

    #[test]
    fn hash_uses_clause() {
        let d = idx("H", vec![part("k")], false, IndexKind::Hash);
        assert_eq!(index_ddl(&d), "KEY `H` (`k`) USING HASH");
    }
}

