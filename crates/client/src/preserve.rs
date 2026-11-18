//! How `preserve` restores match preserved rows against imported rows.
//!
//! Without a `match` clause, preserved rows go back with `REPLACE INTO` and all
//! of their columns. With an auto-increment primary key that is wrong: the
//! preserved row keeps its *local* id, and if the imported data uses that id for
//! an unrelated row, `REPLACE` deletes that row. `match unique` / `match key
//! (...)` instead restore by a unique key with
//! `INSERT ... ON DUPLICATE KEY UPDATE`, leaving the auto-increment id out.

use anyhow::{bail, Result};
use mysql::prelude::*;
use mysql::Conn;

use jibs_protocol::{ColumnDef, IndexDef, PreserveMatch};

/// The keys of a table, as far as matching preserved rows needs them.
#[derive(Debug, Clone, Default)]
pub(crate) struct TableKeys {
    /// Primary key columns, in key order.
    pub primary: Vec<String>,
    /// The single-column auto-increment column, if any.
    pub auto_increment: Option<String>,
    /// Unique secondary indexes: (name, columns). Indexes with expression key
    /// parts are left out, because they cannot be named in `match key`.
    pub unique: Vec<(String, Vec<String>)>,
}

/// How to restore the preserved rows of one table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RestoreMode {
    /// `REPLACE INTO` with every column (no `match` clause).
    Replace,
    /// `INSERT ... ON DUPLICATE KEY UPDATE` on `key`, without `omit`.
    Upsert {
        key: Vec<String>,
        /// Auto-increment column left out of the insert, so the imported row
        /// keeps its id and new rows get a fresh one.
        omit: Option<String>,
    },
}

impl TableKeys {
    /// Keys from the schema the server sends for the table being imported.
    pub(crate) fn from_schema(columns: &[ColumnDef], indexes: &[IndexDef]) -> Self {
        Self {
            primary: columns
                .iter()
                .filter(|c| c.is_primary_key)
                .map(|c| c.name.clone())
                .collect(),
            auto_increment: columns
                .iter()
                .find(|c| c.flags.auto_increment)
                .map(|c| c.name.clone()),
            unique: indexes
                .iter()
                .filter(|i| i.unique && i.columns.iter().all(|c| c.expression.is_none()))
                .map(|i| (i.name.clone(), i.columns.iter().map(|c| c.name.clone()).collect()))
                .collect(),
        }
    }

    /// Keys of a table in the connected database.
    pub(crate) fn from_db(conn: &mut Conn, table: &str) -> Result<Self> {
        let auto_increment: Option<String> = conn.exec_first(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND EXTRA LIKE '%auto_increment%'",
            (table,),
        )?;

        // An expression key part has no COLUMN_NAME (MySQL 8.0.13+).
        let parts: Vec<(String, i64, Option<String>)> = conn.exec(
            "SELECT INDEX_NAME, NON_UNIQUE, COLUMN_NAME FROM information_schema.STATISTICS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? \
             ORDER BY INDEX_NAME, SEQ_IN_INDEX",
            (table,),
        )?;

        let mut keys = TableKeys {
            auto_increment,
            ..Default::default()
        };
        let mut current: Option<(String, Option<Vec<String>>)> = None;
        let finish = |keys: &mut TableKeys, index: Option<(String, Option<Vec<String>>)>| {
            if let Some((name, Some(columns))) = index {
                if name == "PRIMARY" {
                    keys.primary = columns;
                } else {
                    keys.unique.push((name, columns));
                }
            }
        };
        for (name, non_unique, column) in parts {
            if non_unique != 0 {
                continue;
            }
            match &mut current {
                Some((current_name, columns)) if *current_name == name => {
                    match (columns.as_mut(), column) {
                        (Some(cols), Some(column)) => cols.push(column),
                        _ => *columns = None,
                    }
                }
                _ => {
                    finish(&mut keys, current.take());
                    current = Some((name, column.map(|c| vec![c])));
                }
            }
        }
        finish(&mut keys, current);
        Ok(keys)
    }
}

/// Work out how to restore preserved rows of `table` for a `match` clause.
pub(crate) fn restore_mode(table: &str, match_key: &PreserveMatch, keys: &TableKeys) -> Result<RestoreMode> {
    let key = match match_key {
        PreserveMatch::Replace => return Ok(RestoreMode::Replace),
        PreserveMatch::Unique => match keys.unique.as_slice() {
            [(_, columns)] => columns.clone(),
            [] => bail!(
                "preserve {table} match unique: the table has no unique key besides the \
                 primary key; drop the match clause or use match key (...)"
            ),
            several => bail!(
                "preserve {table} match unique: the table has {} unique keys ({}); \
                 pick one with match key (...)",
                several.len(),
                describe(several.iter().map(|(_, c)| c.as_slice()))
            ),
        },
        PreserveMatch::Key(wanted) => {
            let same = |columns: &[String]| {
                columns.len() == wanted.len() && wanted.iter().all(|w| columns.contains(w))
            };
            let primary = (!keys.primary.is_empty()).then_some(keys.primary.as_slice());
            match primary
                .into_iter()
                .chain(keys.unique.iter().map(|(_, c)| c.as_slice()))
                .find(|c| same(c))
            {
                Some(columns) => columns.to_vec(),
                None => {
                    let candidates: Vec<&[String]> = primary
                        .into_iter()
                        .chain(keys.unique.iter().map(|(_, c)| c.as_slice()))
                        .collect();
                    bail!(
                        "preserve {table} match key ({}): no primary or unique key has exactly \
                         these columns; the table's keys are: {}",
                        wanted.join(", "),
                        if candidates.is_empty() {
                            "none".to_string()
                        } else {
                            describe(candidates.into_iter())
                        }
                    )
                }
            }
        }
    };

    let omit = keys
        .auto_increment
        .clone()
        .filter(|ai| keys.primary == [ai.clone()] && !key.contains(ai));
    Ok(RestoreMode::Upsert { key, omit })
}

fn describe<'a>(keys: impl Iterator<Item = &'a [String]>) -> String {
    keys.map(|c| format!("({})", c.join(", ")))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Build the `INSERT ... ON DUPLICATE KEY UPDATE` that restores preserved rows
/// from `backup` into `table`. `columns` are the columns both tables have (and
/// that can be written, so no generated columns).
pub(crate) fn upsert_sql(
    table: &str,
    backup: &str,
    columns: &[String],
    key: &[String],
    omit: Option<&str>,
) -> Result<String> {
    let insert: Vec<&String> = columns.iter().filter(|c| Some(c.as_str()) != omit).collect();
    if let Some(missing) = key.iter().find(|k| !insert.contains(k)) {
        bail!(
            "cannot restore preserved rows of {table}: key column '{missing}' is missing from \
             the preserved rows or the imported table"
        );
    }
    let quoted = |c: &String| format!("`{}`", c.replace('`', "``"));
    let list = insert.iter().map(|c| quoted(c)).collect::<Vec<_>>().join(", ");

    // A no-op assignment when every inserted column is part of the key: the
    // matching row already holds exactly these values.
    let updates: Vec<String> = insert
        .iter()
        .filter(|c| !key.contains(c))
        .map(|c| format!("{0} = `kept`.{0}", quoted(c)))
        .collect();
    let updates = if updates.is_empty() {
        format!("{0} = {0}", quoted(&key[0]))
    } else {
        updates.join(", ")
    };

    Ok(format!(
        "INSERT INTO `{table}` ({list}) SELECT * FROM (SELECT {list} FROM `{backup}`) AS `kept` \
         ON DUPLICATE KEY UPDATE {updates}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|c| c.to_string()).collect()
    }

    /// Magento's core_config_data: auto-increment config_id plus one unique key.
    fn core_config_data() -> TableKeys {
        TableKeys {
            primary: s(&["config_id"]),
            auto_increment: Some("config_id".into()),
            unique: vec![(
                "CORE_CONFIG_DATA_SCOPE_SCOPE_ID_PATH".into(),
                s(&["scope", "scope_id", "path"]),
            )],
        }
    }

    #[test]
    fn no_match_clause_replaces() {
        let mode = restore_mode("t", &PreserveMatch::Replace, &core_config_data()).unwrap();
        assert_eq!(mode, RestoreMode::Replace);
    }

    #[test]
    fn match_unique_uses_the_only_unique_key_and_omits_auto_increment() {
        let mode = restore_mode("t", &PreserveMatch::Unique, &core_config_data()).unwrap();
        assert_eq!(
            mode,
            RestoreMode::Upsert {
                key: s(&["scope", "scope_id", "path"]),
                omit: Some("config_id".into()),
            }
        );
    }

    #[test]
    fn match_unique_rejects_zero_or_several_unique_keys() {
        let mut keys = core_config_data();
        keys.unique.clear();
        let err = restore_mode("t", &PreserveMatch::Unique, &keys).unwrap_err();
        assert!(err.to_string().contains("no unique key"), "{err}");

        keys.unique = vec![("a".into(), s(&["email"])), ("b".into(), s(&["username"]))];
        let err = restore_mode("t", &PreserveMatch::Unique, &keys).unwrap_err();
        assert!(err.to_string().contains("(email), (username)"), "{err}");
    }

    #[test]
    fn match_key_accepts_any_column_order() {
        let key = PreserveMatch::Key(s(&["path", "scope", "scope_id"]));
        let mode = restore_mode("t", &key, &core_config_data()).unwrap();
        assert_eq!(
            mode,
            RestoreMode::Upsert {
                key: s(&["scope", "scope_id", "path"]),
                omit: Some("config_id".into()),
            }
        );
    }

    #[test]
    fn match_key_must_be_a_whole_key() {
        for wanted in [&["scope", "path"][..], &["scope", "scope_id", "path", "value"], &["value"]] {
            let err = restore_mode("t", &PreserveMatch::Key(s(wanted)), &core_config_data())
                .unwrap_err();
            assert!(err.to_string().contains("(config_id), (scope, scope_id, path)"), "{err}");
        }
    }

    #[test]
    fn match_key_on_the_primary_key_keeps_it() {
        let mode =
            restore_mode("t", &PreserveMatch::Key(s(&["config_id"])), &core_config_data()).unwrap();
        assert_eq!(mode, RestoreMode::Upsert { key: s(&["config_id"]), omit: None });

        // A natural primary key without auto-increment
        let keys = TableKeys { primary: s(&["code"]), ..Default::default() };
        let mode = restore_mode("t", &PreserveMatch::Key(s(&["code"])), &keys).unwrap();
        assert_eq!(mode, RestoreMode::Upsert { key: s(&["code"]), omit: None });
    }

    #[test]
    fn auto_increment_outside_the_primary_key_is_kept() {
        let keys = TableKeys {
            primary: s(&["code", "lang"]),
            auto_increment: Some("seq".into()),
            unique: vec![("u".into(), s(&["slug"]))],
        };
        let mode = restore_mode("t", &PreserveMatch::Unique, &keys).unwrap();
        assert_eq!(mode, RestoreMode::Upsert { key: s(&["slug"]), omit: None });
    }

    #[test]
    fn upsert_sql_leaves_out_the_auto_increment_column() {
        let sql = upsert_sql(
            "core_config_data",
            "_jibs_preserve_core_config_data",
            &s(&["config_id", "scope", "scope_id", "path", "value", "updated_at"]),
            &s(&["scope", "scope_id", "path"]),
            Some("config_id"),
        )
        .unwrap();
        assert_eq!(
            sql,
            "INSERT INTO `core_config_data` (`scope`, `scope_id`, `path`, `value`, `updated_at`) \
             SELECT * FROM (SELECT `scope`, `scope_id`, `path`, `value`, `updated_at` \
             FROM `_jibs_preserve_core_config_data`) AS `kept` \
             ON DUPLICATE KEY UPDATE `value` = `kept`.`value`, `updated_at` = `kept`.`updated_at`"
        );
    }

    #[test]
    fn upsert_sql_with_only_key_columns_is_a_no_op_update() {
        let sql = upsert_sql("t", "b", &s(&["id", "code"]), &s(&["code"]), Some("id")).unwrap();
        assert!(sql.ends_with("ON DUPLICATE KEY UPDATE `code` = `code`"), "{sql}");
    }

    #[test]
    fn upsert_sql_needs_every_key_column() {
        let err = upsert_sql("t", "b", &s(&["id", "value"]), &s(&["path"]), Some("id")).unwrap_err();
        assert!(err.to_string().contains("'path'"), "{err}");
    }
}
