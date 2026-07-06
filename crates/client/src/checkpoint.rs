//! Local import state: the resume checkpoint table and preserve-rule
//! backup tables

use anyhow::Result;
use mysql::prelude::*;
use mysql::Conn;
use tracing::{debug, info};

use jibs_protocol::PreserveRule;

pub(crate) const BACKUP_TABLE_PREFIX: &str = "_jibs_preserve_";

/// Name of the checkpoint table
const CHECKPOINT_TABLE: &str = "_jibs_checkpoint";

/// Name of the backup table used to preserve rows
pub(crate) fn preserve_backup_table(table: &str) -> String {
    format!("{}{}", BACKUP_TABLE_PREFIX, table)
}

/// Find all existing backup tables from a previous import
pub(crate) fn find_backup_tables(conn: &mut Conn) -> Result<Vec<String>> {
    // Filter by prefix in Rust rather than LIKE: `_` is a single-character
    // wildcard in LIKE patterns, so 'LIKE '_jibs_preserve_%'' would also match
    // (and under --clean, drop) unrelated tables like 'xjibsXpreserveXfoo'.
    let tables: Vec<String> = conn.query_map(
        "SELECT TABLE_NAME FROM information_schema.TABLES \
         WHERE TABLE_SCHEMA = DATABASE()",
        |table_name: String| table_name,
    )?;
    Ok(tables
        .into_iter()
        .filter(|t| t.starts_with(BACKUP_TABLE_PREFIX))
        .collect())
}

// ============================================================================
// Checkpoint - tracks import progress for resume functionality
// ============================================================================

/// Checkpoint manager for tracking import progress
pub(crate) struct Checkpoint;

impl Checkpoint {
    /// Check if checkpoint table exists
    pub(crate) fn exists(conn: &mut Conn) -> Result<bool> {
        let exists: Option<String> = conn.query_first(format!(
            "SELECT TABLE_NAME FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{}'",
            CHECKPOINT_TABLE
        ))?;
        Ok(exists.is_some())
    }

    /// Create the checkpoint table
    pub(crate) fn create(conn: &mut Conn) -> Result<()> {
        conn.query_drop(format!(
            "CREATE TABLE IF NOT EXISTS `{}` (
                table_name VARCHAR(255) PRIMARY KEY,
                row_count BIGINT UNSIGNED NOT NULL,
                completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            CHECKPOINT_TABLE
        ))?;
        Ok(())
    }

    /// Get set of completed tables from checkpoint
    pub(crate) fn get_completed(conn: &mut Conn) -> Result<std::collections::HashSet<String>> {
        if !Self::exists(conn)? {
            return Ok(std::collections::HashSet::new());
        }
        let tables: Vec<String> = conn.query_map(
            format!("SELECT table_name FROM `{}`", CHECKPOINT_TABLE),
            |name: String| name,
        )?;
        Ok(tables.into_iter().collect())
    }

    /// Mark a table as complete in the checkpoint
    pub(crate) fn mark_complete(conn: &mut Conn, table: &str, row_count: u64) -> Result<()> {
        conn.query_drop(format!(
            "INSERT INTO `{}` (table_name, row_count) VALUES ('{}', {})",
            CHECKPOINT_TABLE, table, row_count
        ))?;
        Ok(())
    }

    /// Clean up the checkpoint table
    pub(crate) fn cleanup(conn: &mut Conn) -> Result<()> {
        conn.query_drop(format!("DROP TABLE IF EXISTS `{}`", CHECKPOINT_TABLE))?;
        Ok(())
    }
}

/// Backup rows matching preserve rules to a backup table before the main table is dropped.
/// Returns true if a backup exists (either created now or from a previous run).
///
/// On resume: uses existing backup table if present.
pub(crate) fn backup_preserved_rows(
    conn: &mut Conn,
    table: &str,
    preserve_rules: &[&PreserveRule],
) -> Result<bool> {
    let backup_table = preserve_backup_table(table);

    // Check if backup table already exists (resume scenario)
    let backup_exists: Option<String> = conn.query_first(format!(
        "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{}'",
        backup_table
    ))?;

    if backup_exists.is_some() {
        let count: Option<u64> = conn.query_first(format!("SELECT COUNT(*) FROM `{}`", backup_table))?;
        debug!(
            "Using existing backup {} ({} rows)",
            backup_table,
            count.unwrap_or(0)
        );
        return Ok(true);
    }

    // Check if the source table exists
    let table_exists: Option<String> = conn.query_first(format!(
        "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{}'",
        table
    ))?;

    if table_exists.is_none() {
        debug!("Table {} doesn't exist locally, nothing to preserve", table);
        return Ok(false);
    }

    // Build combined WHERE clause from all preserve rules for this table
    let where_clauses: Vec<String> = preserve_rules
        .iter()
        .map(|p| format!("({})", p.where_clause))
        .collect();
    let combined_where = where_clauses.join(" OR ");

    // Create backup table with same structure and copy matching rows
    let create_backup_sql = format!(
        "CREATE TABLE `{}` AS SELECT * FROM `{}` WHERE {}",
        backup_table, table, combined_where
    );
    debug!("Backup preserve: {}", create_backup_sql);
    conn.query_drop(&create_backup_sql)?;

    // Check how many rows were backed up
    let count: Option<u64> = conn.query_first(format!("SELECT COUNT(*) FROM `{}`", backup_table))?;
    let row_count = count.unwrap_or(0);

    if row_count == 0 {
        // No rows matched, drop the empty backup table
        conn.query_drop(format!("DROP TABLE `{}`", backup_table))?;
        debug!("No rows to preserve in {}", table);
        return Ok(false);
    }

    info!("Backed up {} preserved rows from {} to {}", row_count, table, backup_table);
    Ok(true)
}


pub(crate) fn restore_preserved_rows(conn: &mut Conn, table: &str) -> Result<()> {
    let backup_table = preserve_backup_table(table);

    // Check if backup table exists
    let backup_exists: Option<String> = conn.query_first(format!(
        "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{}'",
        backup_table
    ))?;

    if backup_exists.is_none() {
        debug!("No backup table {} found", backup_table);
        return Ok(());
    }

    // Get column names from backup table
    let columns: Vec<String> = conn.query_map(
        format!("SHOW COLUMNS FROM `{}`", backup_table),
        |row: mysql::Row| {
            let field: String = row.get(0).unwrap();
            field
        },
    )?;

    if columns.is_empty() {
        conn.query_drop(format!("DROP TABLE `{}`", backup_table))?;
        return Ok(());
    }

    let col_list = columns
        .iter()
        .map(|c| format!("`{}`", c))
        .collect::<Vec<_>>()
        .join(", ");

    // Use REPLACE INTO to restore rows (handles both insert and update)
    let restore_sql = format!(
        "REPLACE INTO `{}` ({}) SELECT {} FROM `{}`",
        table, col_list, col_list, backup_table
    );
    debug!("Restore preserve: {}", restore_sql);
    conn.query_drop(&restore_sql)?;

    // Get count of restored rows
    let count: Option<u64> = conn.query_first(format!("SELECT COUNT(*) FROM `{}`", backup_table))?;
    let row_count = count.unwrap_or(0);

    // Drop backup table
    conn.query_drop(format!("DROP TABLE `{}`", backup_table))?;

    info!("Restored {} preserved rows to {}", row_count, table);
    Ok(())
}

