//! MySQL connection and query handling

use std::collections::HashMap;

use mysql::prelude::*;
use mysql::{Conn, Opts, Row, Value as MySqlValue};

use jibs_protocol::{ColumnDef, ColumnFlags, ExecutionPlan, TableInfo};

use crate::error::{Result, ServerError};

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

    /// Discover tables and their metadata
    pub fn discover_tables(&mut self, plan: &ExecutionPlan) -> Result<Vec<TableInfo>> {
        let mut tables = Vec::new();

        // Get all tables from the database
        let table_names = self.get_all_table_names()?;

        for table_name in table_names {
            // Skip ignored tables
            if plan.ignored_tables.contains(&table_name) {
                continue;
            }

            // Get estimated row count
            let estimated_rows = self.get_estimated_row_count(&table_name)?;

            // Get primary key columns
            let primary_key = self.get_primary_key(&table_name)?;

            // Cache the schema
            let schema = self.get_column_defs(&table_name)?;
            self.schemas.insert(table_name.clone(), schema);
            self.primary_keys
                .insert(table_name.clone(), primary_key.clone());

            tables.push(TableInfo {
                name: table_name,
                estimated_rows,
                primary_key,
            });
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

    /// Get cached primary key for a table
    pub fn get_cached_primary_key(&self, table: &str) -> Option<&Vec<String>> {
        self.primary_keys.get(table)
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

/// Convert a MySQL Value to a TSV-safe string
pub fn mysql_value_to_tsv(value: &MySqlValue) -> String {
    match value {
        MySqlValue::NULL => "\\N".to_string(),
        MySqlValue::Bytes(b) => {
            // Check if it's valid UTF-8
            if let Ok(s) = std::str::from_utf8(b) {
                escape_tsv_string(s)
            } else {
                // Binary data: hex encode
                format!("0x{}", hex::encode(b))
            }
        }
        MySqlValue::Int(i) => i.to_string(),
        MySqlValue::UInt(u) => u.to_string(),
        MySqlValue::Float(f) => f.to_string(),
        MySqlValue::Double(d) => d.to_string(),
        MySqlValue::Date(y, m, d, h, mi, s, us) => {
            if *h == 0 && *mi == 0 && *s == 0 && *us == 0 {
                format!("{:04}-{:02}-{:02}", y, m, d)
            } else if *us == 0 {
                format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, m, d, h, mi, s)
            } else {
                format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
                    y, m, d, h, mi, s, us
                )
            }
        }
        MySqlValue::Time(neg, d, h, m, s, us) => {
            let sign = if *neg { "-" } else { "" };
            let total_hours = (*d as u32) * 24 + (*h as u32);
            if *us == 0 {
                format!("{}{:02}:{:02}:{:02}", sign, total_hours, m, s)
            } else {
                format!("{}{:02}:{:02}:{:02}.{:06}", sign, total_hours, m, s, us)
            }
        }
    }
}

/// Escape a string for TSV format
pub fn escape_tsv_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => result.push_str("\\\\"),
            '\t' => result.push_str("\\t"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\0' => result.push_str("\\0"),
            _ => result.push(c),
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_tsv_string() {
        assert_eq!(escape_tsv_string("hello"), "hello");
        assert_eq!(escape_tsv_string("hello\tworld"), "hello\\tworld");
        assert_eq!(escape_tsv_string("line1\nline2"), "line1\\nline2");
        assert_eq!(escape_tsv_string("back\\slash"), "back\\\\slash");
        assert_eq!(escape_tsv_string("a\tb\nc\\d"), "a\\tb\\nc\\\\d");
    }

    #[test]
    fn test_mysql_value_to_tsv() {
        assert_eq!(mysql_value_to_tsv(&MySqlValue::NULL), "\\N");
        assert_eq!(mysql_value_to_tsv(&MySqlValue::Int(42)), "42");
        assert_eq!(mysql_value_to_tsv(&MySqlValue::UInt(100)), "100");
        assert_eq!(
            mysql_value_to_tsv(&MySqlValue::Bytes(b"hello".to_vec())),
            "hello"
        );
        assert_eq!(
            mysql_value_to_tsv(&MySqlValue::Bytes(b"hello\tworld".to_vec())),
            "hello\\tworld"
        );
    }
}
