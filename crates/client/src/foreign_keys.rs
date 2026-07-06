//! Foreign key preservation: captures FK constraints before an import
//! drops them, and restores them once the import completes

use std::collections::HashMap;

use anyhow::Result;
use mysql::prelude::*;
use mysql::Conn;
use tracing::{info, warn};

// ============================================================================
// Foreign key preservation - captures FK constraints before the import drops
// them, and restores them once the import completes
// ============================================================================

/// Name of the table that persists captured FK definitions across runs
const FK_STORE_TABLE: &str = "_jibs_foreign_keys";

/// A foreign key constraint definition read from information_schema
struct ForeignKeyDef {
    table: String,
    constraint: String,
    columns: Vec<String>,
    ref_schema: String,
    ref_table: String,
    ref_columns: Vec<String>,
    update_rule: String,
    delete_rule: String,
}

/// Escape an identifier for use inside backticks
fn escape_ident(name: &str) -> String {
    name.replace('`', "``")
}

/// Build the ALTER TABLE statement that recreates a foreign key
fn build_fk_ddl(fk: &ForeignKeyDef) -> String {
    let quote_list = |cols: &[String]| {
        cols.iter()
            .map(|c| format!("`{}`", escape_ident(c)))
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        "ALTER TABLE `{}` ADD CONSTRAINT `{}` FOREIGN KEY ({}) \
         REFERENCES `{}`.`{}` ({}) ON UPDATE {} ON DELETE {}",
        escape_ident(&fk.table),
        escape_ident(&fk.constraint),
        quote_list(&fk.columns),
        escape_ident(&fk.ref_schema),
        escape_ident(&fk.ref_table),
        quote_list(&fk.ref_columns),
        fk.update_rule,
        fk.delete_rule
    )
}

/// Read all FK constraints in the current schema
fn capture_foreign_keys(conn: &mut Conn) -> Result<Vec<ForeignKeyDef>> {
    type FkRow = (String, String, String, String, String, String, String, String);
    let rows: Vec<FkRow> = conn.query(
        "SELECT kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME, \
                kcu.REFERENCED_TABLE_SCHEMA, kcu.REFERENCED_TABLE_NAME, kcu.REFERENCED_COLUMN_NAME, \
                rc.UPDATE_RULE, rc.DELETE_RULE \
         FROM information_schema.KEY_COLUMN_USAGE kcu \
         JOIN information_schema.REFERENTIAL_CONSTRAINTS rc \
           ON rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA \
          AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME \
          AND rc.TABLE_NAME = kcu.TABLE_NAME \
         WHERE kcu.TABLE_SCHEMA = DATABASE() AND kcu.REFERENCED_TABLE_NAME IS NOT NULL \
         ORDER BY kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION",
    )?;

    let mut defs: Vec<ForeignKeyDef> = Vec::new();
    for (table, constraint, column, ref_schema, ref_table, ref_column, update_rule, delete_rule) in
        rows
    {
        match defs.last_mut() {
            Some(last) if last.table == table && last.constraint == constraint => {
                last.columns.push(column);
                last.ref_columns.push(ref_column);
            }
            _ => defs.push(ForeignKeyDef {
                table,
                constraint,
                columns: vec![column],
                ref_schema,
                ref_table,
                ref_columns: vec![ref_column],
                update_rule,
                delete_rule,
            }),
        }
    }
    Ok(defs)
}

/// Persistent store for captured FK definitions, keyed by (table, constraint)
struct ForeignKeyStore;

impl ForeignKeyStore {
    fn load(conn: &mut Conn) -> Result<HashMap<(String, String), String>> {
        let exists: Option<String> = conn.query_first(format!(
            "SELECT TABLE_NAME FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{}'",
            FK_STORE_TABLE
        ))?;
        if exists.is_none() {
            return Ok(HashMap::new());
        }
        let rows: Vec<(String, String, String)> = conn.query(format!(
            "SELECT table_name, constraint_name, ddl FROM `{}`",
            FK_STORE_TABLE
        ))?;
        Ok(rows
            .into_iter()
            .map(|(t, c, ddl)| ((t, c), ddl))
            .collect())
    }

    fn save(conn: &mut Conn, store: &HashMap<(String, String), String>) -> Result<()> {
        conn.query_drop(format!(
            "CREATE TABLE IF NOT EXISTS `{}` (
                table_name VARCHAR(255) NOT NULL,
                constraint_name VARCHAR(255) NOT NULL,
                ddl TEXT NOT NULL,
                PRIMARY KEY (table_name, constraint_name)
            )",
            FK_STORE_TABLE
        ))?;
        conn.exec_batch(
            format!(
                "REPLACE INTO `{}` (table_name, constraint_name, ddl) VALUES (?, ?, ?)",
                FK_STORE_TABLE
            ),
            store
                .iter()
                .map(|((t, c), ddl)| (t.as_str(), c.as_str(), ddl.as_str())),
        )?;
        Ok(())
    }

    fn cleanup(conn: &mut Conn) -> Result<()> {
        conn.query_drop(format!("DROP TABLE IF EXISTS `{}`", FK_STORE_TABLE))?;
        Ok(())
    }
}

/// Capture all FK constraints, persist their definitions, then drop them.
///
/// Dropping prevents MySQL ERROR 1822 when tables are recreated in parallel:
/// when we DROP + CREATE a table that was previously referenced by FK
/// constraints from other tables, MySQL re-validates those orphaned FK
/// constraints against the new table. Since we only create a PRIMARY KEY
/// (no secondary indexes), the required index for the FK may be missing,
/// causing the error.
///
/// The definitions are persisted to a table so that an interrupted import can
/// still restore them on a later successful run.
pub(crate) fn preserve_and_drop_foreign_keys(conn: &mut Conn) -> Result<()> {
    let captured = capture_foreign_keys(conn)?;

    // Merge with FKs recorded by a previous interrupted run (already dropped
    // from the schema, so not in `captured`). Current definitions win.
    let mut store = ForeignKeyStore::load(conn)?;
    for fk in &captured {
        store.insert((fk.table.clone(), fk.constraint.clone()), build_fk_ddl(fk));
    }
    if !store.is_empty() {
        ForeignKeyStore::save(conn, &store)?;
    }

    for fk in &captured {
        conn.query_drop(format!(
            "ALTER TABLE `{}` DROP FOREIGN KEY `{}`",
            escape_ident(&fk.table),
            escape_ident(&fk.constraint)
        ))?;
    }
    if !captured.is_empty() {
        info!(
            "Dropped {} foreign key constraints (they are restored after the import)",
            captured.len()
        );
    }
    Ok(())
}

/// Restore FK constraints recorded before this (or a previous interrupted)
/// import. Must run while FOREIGN_KEY_CHECKS=0 so existing rows are not
/// validated — aggregate imports intentionally load partial data.
pub(crate) fn restore_foreign_keys(conn: &mut Conn) -> Result<()> {
    let store = ForeignKeyStore::load(conn)?;
    if store.is_empty() {
        ForeignKeyStore::cleanup(conn)?;
        return Ok(());
    }

    let mut restored = 0usize;
    let mut failed = 0usize;
    for ((table, constraint), ddl) in &store {
        match conn.query_drop(ddl) {
            Ok(()) => restored += 1,
            Err(e) => {
                failed += 1;
                warn!(
                    "Could not restore foreign key `{}` on `{}`: {}\n  To restore it manually: {}",
                    constraint, table, e, ddl
                );
            }
        }
    }
    info!("Restored {} foreign key constraints", restored);
    if failed > 0 {
        warn!(
            "{} foreign key constraints could not be restored (statements printed above); \
             they will not be retried on future runs",
            failed
        );
    }
    ForeignKeyStore::cleanup(conn)?;
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fk_ddl_single_column() {
        let fk = ForeignKeyDef {
            table: "orders".to_string(),
            constraint: "fk_orders_user".to_string(),
            columns: vec!["user_id".to_string()],
            ref_schema: "imported".to_string(),
            ref_table: "users".to_string(),
            ref_columns: vec!["id".to_string()],
            update_rule: "CASCADE".to_string(),
            delete_rule: "SET NULL".to_string(),
        };
        assert_eq!(
            build_fk_ddl(&fk),
            "ALTER TABLE `orders` ADD CONSTRAINT `fk_orders_user` FOREIGN KEY (`user_id`) \
             REFERENCES `imported`.`users` (`id`) ON UPDATE CASCADE ON DELETE SET NULL"
        );
    }
    #[test]
    fn fk_ddl_multi_column_and_backticks() {
        let fk = ForeignKeyDef {
            table: "weird`table".to_string(),
            constraint: "fk_multi".to_string(),
            columns: vec!["a".to_string(), "b".to_string()],
            ref_schema: "db".to_string(),
            ref_table: "parent".to_string(),
            ref_columns: vec!["x".to_string(), "y".to_string()],
            update_rule: "NO ACTION".to_string(),
            delete_rule: "RESTRICT".to_string(),
        };
        assert_eq!(
            build_fk_ddl(&fk),
            "ALTER TABLE `weird``table` ADD CONSTRAINT `fk_multi` FOREIGN KEY (`a`, `b`) \
             REFERENCES `db`.`parent` (`x`, `y`) ON UPDATE NO ACTION ON DELETE RESTRICT"
        );
    }
}
