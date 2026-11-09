//! Trigger checks done before any data moves.
//!
//! A trigger on an imported table that writes into a table the import skips
//! would be recreated fine (MySQL doesn't check the tables a trigger body
//! names), but every later write to its table would then fail. Magento's mview
//! triggers are the common case: they insert into `*_cl` changelog tables,
//! which should be imported empty with `exclude_data`, not ignored.

use std::collections::HashSet;
use std::sync::OnceLock;

use jibs_protocol::TriggerDef;
use regex::Regex;

/// Tables a trigger body writes into: the targets of INSERT / REPLACE /
/// UPDATE / DELETE FROM. A best-effort scan, not a parser — only names right
/// after a write keyword count, so a column that shares a table's name isn't
/// mistaken for one. A name inside a string literal can still match.
pub fn written_tables(statement: &str) -> Vec<String> {
    static WRITE_TARGET: OnceLock<Regex> = OnceLock::new();
    static ON_DUPLICATE: OnceLock<Regex> = OnceLock::new();
    let write_target = WRITE_TARGET.get_or_init(|| {
        Regex::new(
            r#"(?ix)
            \b(?:
                (?:INSERT|REPLACE)(?:\s+(?:LOW_PRIORITY|DELAYED|HIGH_PRIORITY|IGNORE))*(?:\s+INTO)?
              | UPDATE(?:\s+(?:LOW_PRIORITY|IGNORE))*
              | DELETE(?:\s+(?:LOW_PRIORITY|QUICK|IGNORE))*\s+FROM
            )
            \s+
            (?:`[^`]+`\.|"[^"]+"\.|[A-Za-z0-9_$]+\.)?   # optional schema qualifier
            (?:`([^`]+)`|"([^"]+)"|([A-Za-z0-9_$]+))   # "..." under ANSI_QUOTES
            "#,
        )
        .expect("valid regex")
    });
    // `ON DUPLICATE KEY UPDATE col = ...` names a column, not a table.
    let on_duplicate = ON_DUPLICATE
        .get_or_init(|| Regex::new(r"(?i)\bON\s+DUPLICATE\s+KEY\s+UPDATE\b").expect("valid regex"));
    let statement = on_duplicate.replace_all(statement, " ");

    let mut tables: Vec<String> = Vec::new();
    for caps in write_target.captures_iter(&statement) {
        let name = caps
            .get(1)
            .or_else(|| caps.get(2))
            .or_else(|| caps.get(3))
            .map(|m| m.as_str().to_string())
            .unwrap_or_default();
        if !name.is_empty() && !tables.contains(&name) {
            tables.push(name);
        }
    }
    tables
}

/// Fail when a trigger on a table this import creates writes into a table
/// the import ignores. Triggers on ignored tables are not checked: jibs
/// leaves those tables (and their triggers) alone.
pub fn check_trigger_targets(
    triggers: &[TriggerDef],
    ignored_tables: &HashSet<String>,
) -> Result<(), String> {
    let mut problems: Vec<String> = Vec::new();
    for trigger in triggers {
        if ignored_tables.contains(&trigger.table) {
            continue;
        }
        for target in written_tables(&trigger.statement) {
            if ignored_tables.contains(&target) {
                problems.push(format!(
                    "trigger `{}` on `{}` writes to `{}`, which this import ignores; \
                     add `exclude_data {}` to import it empty instead",
                    trigger.name, trigger.table, target, target
                ));
            }
        }
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{} trigger write target(s) would be missing locally, so every write to \
             the trigger's table would fail:\n  {}",
            problems.len(),
            problems.join("\n  ")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trigger(table: &str, statement: &str) -> TriggerDef {
        TriggerDef {
            name: format!("trg_{}", table),
            table: table.to_string(),
            timing: "AFTER".to_string(),
            event: "INSERT".to_string(),
            statement: statement.to_string(),
            sql_mode: String::new(),
        }
    }

    #[test]
    fn finds_magento_mview_target() {
        let body = "BEGIN\nINSERT IGNORE INTO `catalog_product_flat_cl` (`entity_id`) VALUES (NEW.`entity_id`);\nEND";
        assert_eq!(written_tables(body), vec!["catalog_product_flat_cl"]);
    }

    #[test]
    fn finds_every_write_kind() {
        let body = "BEGIN
            INSERT INTO a (x) VALUES (1);
            REPLACE LOW_PRIORITY INTO `b` VALUES (2);
            UPDATE c SET x = NEW.x WHERE id = NEW.id;
            DELETE QUICK FROM shop.d WHERE id = OLD.id;
            INSERT `e` SET x = 1;
        END";
        assert_eq!(written_tables(body), vec!["a", "b", "c", "d", "e"]);
    }

    #[test]
    fn finds_ansi_quoted_target() {
        let body = r#"INSERT INTO "products_cl" ("entity_id") VALUES (OLD."id")"#;
        assert_eq!(written_tables(body), vec!["products_cl"]);
    }

    #[test]
    fn ignores_on_duplicate_key_update_column() {
        let body = "INSERT INTO stats (id, store) VALUES (NEW.id, 1) ON DUPLICATE KEY UPDATE store = store + 1";
        assert_eq!(written_tables(body), vec!["stats"]);
    }

    #[test]
    fn ignores_reads_and_columns() {
        let body = "SET NEW.store = (SELECT code FROM store WHERE id = NEW.store_id)";
        assert!(written_tables(body).is_empty());
    }

    #[test]
    fn errors_when_target_is_ignored() {
        let ignored: HashSet<String> = ["product_cl".to_string()].into();
        let triggers = vec![trigger(
            "product",
            "INSERT IGNORE INTO `product_cl` (`id`) VALUES (NEW.`id`)",
        )];
        let err = check_trigger_targets(&triggers, &ignored).unwrap_err();
        assert!(err.contains("exclude_data product_cl"), "got: {err}");
    }

    #[test]
    fn skips_triggers_on_ignored_tables() {
        let ignored: HashSet<String> = ["product".to_string(), "product_cl".to_string()].into();
        let triggers = vec![trigger(
            "product",
            "INSERT INTO product_cl (id) VALUES (NEW.id)",
        )];
        assert!(check_trigger_targets(&triggers, &ignored).is_ok());
    }

    #[test]
    fn passes_when_target_is_imported() {
        let triggers = vec![trigger(
            "product",
            "INSERT INTO product_cl (id) VALUES (NEW.id)",
        )];
        assert!(check_trigger_targets(&triggers, &HashSet::new()).is_ok());
    }
}
