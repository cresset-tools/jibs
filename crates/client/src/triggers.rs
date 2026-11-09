//! Recreate source-schema triggers on the local database.
//!
//! `DROP TABLE` drops a table's triggers with it, so every table an import
//! rebuilds loses them. They are recreated from the source's definitions once
//! all data is loaded, so they don't fire during the load itself.

use anyhow::Result;
use mysql::prelude::*;
use mysql::Conn;
use tracing::{info, warn};

use jibs_protocol::TriggerDef;

use crate::sql::table_exists;

/// `sql_mode` the import connection runs under, restored after each trigger.
const LOAD_SQL_MODE: &str = "NO_AUTO_VALUE_ON_ZERO";

/// The `CREATE TRIGGER` statement for a source trigger. No `DEFINER` clause,
/// so the trigger is owned by the importing user.
fn build_trigger_ddl(trigger: &TriggerDef) -> String {
    format!(
        "CREATE TRIGGER `{}` {} {} ON `{}` FOR EACH ROW {}",
        trigger.name.replace('`', "``"),
        trigger.timing,
        trigger.event,
        trigger.table.replace('`', "``"),
        trigger.statement
    )
}

/// Recreate `triggers` (in the given order, which is their firing order) on
/// tables that exist locally. A same-named local trigger is replaced. Failures
/// are warned about with the statement to run by hand, like foreign keys.
pub(crate) fn apply_triggers(conn: &mut Conn, triggers: &[TriggerDef]) -> Result<()> {
    if triggers.is_empty() {
        return Ok(());
    }
    let mut applied = 0usize;
    let mut failed = 0usize;
    for trigger in triggers {
        // Not every non-ignored table is created (e.g. aggregates-only imports).
        if !table_exists(conn, &trigger.table)? {
            continue;
        }
        let ddl = build_trigger_ddl(trigger);
        // The body is parsed under the session's sql_mode and keeps it, so
        // create it under the mode it had at the source.
        if let Err(e) = conn.exec_drop("SET SESSION sql_mode = ?", (&trigger.sql_mode,)) {
            warn!(
                "Could not apply sql_mode '{}' for trigger `{}` ({}); creating it under the current mode",
                trigger.sql_mode, trigger.name, e
            );
        }
        let result = conn
            .query_drop(format!(
                "DROP TRIGGER IF EXISTS `{}`",
                trigger.name.replace('`', "``")
            ))
            .and_then(|()| conn.query_drop(&ddl));
        conn.query_drop(format!("SET SESSION sql_mode = '{}'", LOAD_SQL_MODE))?;
        match result {
            Ok(()) => applied += 1,
            Err(e) => {
                failed += 1;
                warn!(
                    "Could not create trigger `{}` on `{}`: {}\n  To create it manually: {}",
                    trigger.name, trigger.table, e, ddl
                );
            }
        }
    }
    info!("Recreated {} triggers from the source schema", applied);
    if failed > 0 {
        warn!(
            "{} triggers could not be created (statements printed above)",
            failed
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ddl_has_no_definer() {
        let trigger = TriggerDef {
            name: "trg_product_after_insert".to_string(),
            table: "product".to_string(),
            timing: "AFTER".to_string(),
            event: "INSERT".to_string(),
            statement: "BEGIN\nINSERT IGNORE INTO `product_cl` (`id`) VALUES (NEW.`id`);\nEND"
                .to_string(),
            sql_mode: String::new(),
        };
        assert_eq!(
            build_trigger_ddl(&trigger),
            "CREATE TRIGGER `trg_product_after_insert` AFTER INSERT ON `product` FOR EACH ROW BEGIN\nINSERT IGNORE INTO `product_cl` (`id`) VALUES (NEW.`id`);\nEND"
        );
    }
}
