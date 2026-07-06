//! MySQL connection and query handling

use std::collections::HashMap;

use mysql::prelude::*;
use mysql::{Conn, Opts, Row, Value as MySqlValue};

use jibs_protocol::{ColumnDef, ColumnFlags, ExecutionPlan, Relation, TableInfo};

use crate::error::{Result, ServerError};

/// Compile a list of regex pattern strings into Regex objects
fn compile_patterns(patterns: &[String]) -> Result<Vec<regex::Regex>> {
    patterns
        .iter()
        .map(|p| {
            regex::Regex::new(p)
                .map_err(|e| ServerError::Config(format!("Invalid regex pattern '{}': {}", p, e)))
        })
        .collect()
}

/// MySQL connection wrapper
pub struct MySqlConnection {
    conn: Conn,
    /// Cached table schemas
    schemas: HashMap<String, Vec<ColumnDef>>,
    /// Cached primary keys
    primary_keys: HashMap<String, Vec<String>>,
}

impl MySqlConnection {
    /// Connect to MySQL using a connection URL
    pub fn connect(url: &str) -> Result<Self> {
        let opts = Opts::from_url(url)
            .map_err(|e| ServerError::Config(format!("Invalid MySQL URL: {}", e)))?;

        let conn = Conn::new(opts)?;

        Ok(Self {
            conn,
            schemas: HashMap::new(),
            primary_keys: HashMap::new(),
        })
    }

    /// Get all table names in the database
    pub fn get_all_table_names(&mut self) -> Result<Vec<String>> {
        let table_names: Vec<String> = self.conn.query("SHOW TABLES")?;
        Ok(table_names)
    }

    /// Discover tables and their metadata.
    /// Regex patterns in the plan are expanded into the exact table name sets.
    pub fn discover_tables(&mut self, plan: &mut ExecutionPlan) -> Result<Vec<TableInfo>> {
        let mut tables = Vec::new();

        // Get all tables from the database
        let table_names = self.get_all_table_names()?;

        // Expand regex patterns into the exact table name sets
        let ignored_regexes = compile_patterns(&plan.ignored_patterns)?;
        let excluded_regexes = compile_patterns(&plan.excluded_patterns)?;
        let full_regexes = compile_patterns(&plan.full_patterns)?;
        for table_name in &table_names {
            if ignored_regexes.iter().any(|re| re.is_match(table_name)) {
                plan.ignored_tables.insert(table_name.clone());
            }
            if excluded_regexes.iter().any(|re| re.is_match(table_name)) {
                plan.excluded_tables.insert(table_name.clone());
            }
            if full_regexes.iter().any(|re| re.is_match(table_name)) {
                plan.full_tables.insert(table_name.clone());
            }
        }

        let mut next_id: u16 = 0;
        for table_name in &table_names {
            // Skip ignored tables
            if plan.ignored_tables.contains(table_name) {
                continue;
            }

            // Get estimated row count
            let estimated_rows = self.get_estimated_row_count(table_name)?;

            // Get primary key columns
            let primary_key = self.get_primary_key(table_name)?;

            // Cache the schema
            let schema = self.get_column_defs(table_name)?;
            self.schemas.insert(table_name.clone(), schema);
            self.primary_keys
                .insert(table_name.clone(), primary_key.clone());

            tables.push(TableInfo {
                name: table_name.clone(),
                table_id: next_id,
                estimated_rows,
                primary_key,
            });
            next_id += 1;
        }

        Ok(tables)
    }

    /// Get estimated row count for a table
    fn get_estimated_row_count(&mut self, table: &str) -> Result<u64> {
        // Use SHOW TABLE STATUS for a quick estimate
        // Note: SHOW commands don't support prepared statements, so we escape manually
        let escaped_table = escape_identifier(table);
        let query = format!("SHOW TABLE STATUS LIKE '{}'", escaped_table);
        let result: Option<Row> = self.conn.query_first(&query)?;

        if let Some(row) = result {
            // The 'Rows' column contains the estimate (may be NULL for some engines)
            // Use get_opt to handle NULL values without panicking
            match row.get_opt::<u64, _>("Rows") {
                Some(Ok(n)) => Ok(n),
                _ => Ok(0),
            }
        } else {
            Ok(0)
        }
    }

    /// Get primary key columns for a table
    fn get_primary_key(&mut self, table: &str) -> Result<Vec<String>> {
        if let Some(pk) = self.primary_keys.get(table) {
            return Ok(pk.clone());
        }

        // Note: SHOW commands don't support prepared statements
        let escaped_table = escape_identifier(table);
        let query = format!("SHOW KEYS FROM `{}` WHERE Key_name = 'PRIMARY'", escaped_table);
        let rows: Vec<Row> = self.conn.query(&query)?;

        let mut pk_columns: Vec<(u32, String)> = Vec::new();
        for row in rows {
            let seq: u32 = row.get_opt("Seq_in_index").and_then(|r| r.ok()).unwrap_or(0);
            let column: String = row.get_opt("Column_name").and_then(|r| r.ok()).unwrap_or_default();
            pk_columns.push((seq, column));
        }

        // Sort by sequence
        pk_columns.sort_by_key(|(seq, _)| *seq);
        let pk: Vec<String> = pk_columns.into_iter().map(|(_, col)| col).collect();

        self.primary_keys.insert(table.to_string(), pk.clone());
        Ok(pk)
    }

    /// Get column definitions for a table
    pub fn get_column_defs(&mut self, table: &str) -> Result<Vec<ColumnDef>> {
        if let Some(schema) = self.schemas.get(table) {
            return Ok(schema.clone());
        }

        let pk = self.get_primary_key(table)?;

        let rows: Vec<Row> = self.conn.exec(
            r#"
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                IS_NULLABLE,
                CHARACTER_SET_NAME,
                COLLATION_NAME,
                COLUMN_TYPE,
                EXTRA
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
            "#,
            (table,),
        )?;

        let mut columns = Vec::new();
        for row in rows {
            let name: String = row.get_opt("COLUMN_NAME").and_then(|r| r.ok()).unwrap_or_default();
            let type_name: String = row.get_opt("DATA_TYPE").and_then(|r| r.ok()).unwrap_or_default();
            let max_length: Option<u64> = row.get_opt("CHARACTER_MAXIMUM_LENGTH").and_then(|r| r.ok());
            let nullable: String = row.get_opt("IS_NULLABLE").and_then(|r| r.ok()).unwrap_or_default();
            let charset: Option<String> = row.get_opt("CHARACTER_SET_NAME").and_then(|r| r.ok());
            let collation: Option<String> = row.get_opt("COLLATION_NAME").and_then(|r| r.ok());
            let column_type: String = row.get_opt("COLUMN_TYPE").and_then(|r| r.ok()).unwrap_or_default();
            let extra: String = row.get_opt("EXTRA").and_then(|r| r.ok()).unwrap_or_default();

            let is_primary_key = pk.contains(&name);

            let flags = ColumnFlags {
                unsigned: column_type.contains("unsigned"),
                zerofill: column_type.contains("zerofill"),
                binary: column_type.contains("binary"),
                auto_increment: extra.contains("auto_increment"),
            };

            columns.push(ColumnDef {
                name,
                type_name: type_name.to_uppercase(),
                full_type: column_type.clone(),
                max_length,
                nullable: nullable == "YES",
                is_primary_key,
                charset,
                collation,
                flags,
            });
        }

        self.schemas.insert(table.to_string(), columns.clone());
        Ok(columns)
    }

    /// Discover foreign key constraints from MySQL's INFORMATION_SCHEMA.
    /// Returns single-column FK relations (composite FKs are skipped).
    pub fn discover_foreign_keys(&mut self) -> Result<Vec<Relation>> {
        let rows: Vec<Row> = self.conn.query(
            r#"
            SELECT
                TABLE_NAME, COLUMN_NAME,
                REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME,
                CONSTRAINT_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = DATABASE()
              AND REFERENCED_TABLE_NAME IS NOT NULL
            "#,
        )?;

        // Group by constraint name to detect composite FKs
        let mut by_constraint: HashMap<String, Vec<(String, String, String, String)>> =
            HashMap::new();
        for row in &rows {
            let constraint: String = row
                .get_opt("CONSTRAINT_NAME")
                .and_then(|r| r.ok())
                .unwrap_or_default();
            let from_table: String = row
                .get_opt("TABLE_NAME")
                .and_then(|r| r.ok())
                .unwrap_or_default();
            let from_column: String = row
                .get_opt("COLUMN_NAME")
                .and_then(|r| r.ok())
                .unwrap_or_default();
            let to_table: String = row
                .get_opt("REFERENCED_TABLE_NAME")
                .and_then(|r| r.ok())
                .unwrap_or_default();
            let to_column: String = row
                .get_opt("REFERENCED_COLUMN_NAME")
                .and_then(|r| r.ok())
                .unwrap_or_default();
            by_constraint
                .entry(constraint)
                .or_default()
                .push((from_table, from_column, to_table, to_column));
        }

        // Only keep single-column constraints
        let mut relations = Vec::new();
        for (_constraint, columns) in by_constraint {
            if columns.len() == 1 {
                let (from_table, from_column, to_table, to_column) = columns.into_iter().next().unwrap();
                relations.push(Relation {
                    from_table,
                    from_column,
                    to_table,
                    to_column,
                });
            }
        }

        Ok(relations)
    }

    /// Get cached primary key for a table
    pub fn get_cached_primary_key(&self, table: &str) -> Option<&Vec<String>> {
        self.primary_keys.get(table)
    }

    /// Count rows in a table matching an optional WHERE clause (dry run)
    pub fn count_rows(&mut self, table: &str, where_clause: Option<&str>) -> Result<u64> {
        let mut query = format!("SELECT COUNT(*) FROM `{}`", escape_identifier(table));
        if let Some(clause) = where_clause {
            query.push_str(" WHERE ");
            query.push_str(clause);
        }
        let count: Option<u64> = self.conn.query_first(&query)?;
        Ok(count.unwrap_or(0))
    }

    /// Execute a query and return a streaming iterator over rows.
    /// This avoids loading all rows into memory at once.
    pub fn query_iter(
        &mut self,
        query: &str,
    ) -> Result<mysql::QueryResult<'_, '_, '_, mysql::Text>> {
        Ok(self.conn.query_iter(query)?)
    }

    /// Execute a parameterized query and return a streaming iterator over rows.
    /// This avoids loading all rows into memory at once.
    pub fn exec_iter<P: Into<mysql::Params>>(
        &mut self,
        query: &str,
        params: P,
    ) -> Result<mysql::QueryResult<'_, '_, '_, mysql::Binary>> {
        Ok(self.conn.exec_iter(query, params)?)
    }
}

/// Escape a MySQL identifier (table name, column name) for use in queries
/// This escapes backticks by doubling them
fn escape_identifier(s: &str) -> String {
    s.replace('`', "``")
}

/// Write a MySQL Value directly to a buffer in TSV format (zero-allocation hot path)
pub fn write_tsv_value(buf: &mut Vec<u8>, value: &MySqlValue) {
    match value {
        MySqlValue::NULL => buf.extend_from_slice(b"\\N"),
        // TSV escaping is byte-oriented, so arbitrary (non-UTF-8) bytes pass
        // through unchanged. Columns with binary types are hex-encoded by
        // TsvWriter before reaching this function.
        MySqlValue::Bytes(b) => escape_tsv_bytes(buf, b),
        MySqlValue::Int(i) => {
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(*i).as_bytes());
        }
        MySqlValue::UInt(u) => {
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(*u).as_bytes());
        }
        MySqlValue::Float(f) => {
            let mut ryu_buf = ryu::Buffer::new();
            buf.extend_from_slice(ryu_buf.format(*f).as_bytes());
        }
        MySqlValue::Double(d) => {
            let mut ryu_buf = ryu::Buffer::new();
            buf.extend_from_slice(ryu_buf.format(*d).as_bytes());
        }
        MySqlValue::Date(y, m, d, h, mi, s, us) => {
            use std::io::Write;
            if *h == 0 && *mi == 0 && *s == 0 && *us == 0 {
                let _ = write!(buf, "{:04}-{:02}-{:02}", y, m, d);
            } else if *us == 0 {
                let _ = write!(buf, "{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, m, d, h, mi, s);
            } else {
                let _ = write!(
                    buf,
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
                    y, m, d, h, mi, s, us
                );
            }
        }
        MySqlValue::Time(neg, d, h, m, s, us) => {
            use std::io::Write;
            let total_hours = (*d as u32) * 24 + (*h as u32);
            if *neg {
                buf.push(b'-');
            }
            if *us == 0 {
                let _ = write!(buf, "{:02}:{:02}:{:02}", total_hours, m, s);
            } else {
                let _ = write!(buf, "{:02}:{:02}:{:02}.{:06}", total_hours, m, s, us);
            }
        }
    }
}

/// Escape bytes for TSV format, writing directly to output buffer (zero-allocation)
///
/// Only ASCII special characters need escaping: \t, \n, \r, \\, \0
/// We scan for clean segments and copy them in bulk.
pub fn escape_tsv_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    let mut start = 0;
    for (i, &b) in bytes.iter().enumerate() {
        let replacement = match b {
            b'\\' => &b"\\\\"[..],
            b'\t' => &b"\\t"[..],
            b'\n' => &b"\\n"[..],
            b'\r' => &b"\\r"[..],
            b'\0' => &b"\\0"[..],
            _ => continue,
        };
        // Flush the clean segment before this special byte
        if start < i {
            buf.extend_from_slice(&bytes[start..i]);
        }
        buf.extend_from_slice(replacement);
        start = i + 1;
    }
    // Flush remaining clean segment
    if start < bytes.len() {
        buf.extend_from_slice(&bytes[start..]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn escape_to_string(input: &[u8]) -> String {
        let mut buf = Vec::new();
        escape_tsv_bytes(&mut buf, input);
        String::from_utf8(buf).unwrap()
    }

    fn value_to_string(value: &MySqlValue) -> String {
        let mut buf = Vec::new();
        write_tsv_value(&mut buf, value);
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn test_escape_tsv_bytes() {
        assert_eq!(escape_to_string(b"hello"), "hello");
        assert_eq!(escape_to_string(b"hello\tworld"), "hello\\tworld");
        assert_eq!(escape_to_string(b"line1\nline2"), "line1\\nline2");
        assert_eq!(escape_to_string(b"back\\slash"), "back\\\\slash");
        assert_eq!(escape_to_string(b"a\tb\nc\\d"), "a\\tb\\nc\\\\d");
    }

    #[test]
    fn test_write_tsv_value() {
        assert_eq!(value_to_string(&MySqlValue::NULL), "\\N");
        assert_eq!(value_to_string(&MySqlValue::Int(42)), "42");
        assert_eq!(value_to_string(&MySqlValue::UInt(100)), "100");
        assert_eq!(
            value_to_string(&MySqlValue::Bytes(b"hello".to_vec())),
            "hello"
        );
        assert_eq!(
            value_to_string(&MySqlValue::Bytes(b"hello\tworld".to_vec())),
            "hello\\tworld"
        );
    }
}
