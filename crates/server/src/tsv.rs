//! TSV (Tab-Separated Values) formatting for MySQL LOAD DATA INFILE

use mysql::{Row, Value as MySqlValue};

use crate::error::{Result, ServerError};
use crate::mysql::{escape_tsv_bytes, write_tsv_value};
use jibs_protocol::{
    AnonymizeRule, AnonymizeTarget, ColumnDef, CompressionMode, RAW_CHUNK_FLAG,
    RAW_CHUNK_HEADER_LEN,
};

/// Sentinel string emitted by the resolver for `{unique()}` interpolation.
const UNIQUE_SENTINEL: &str = "{unique()}";

/// Pre-computed anonymization action for a column
#[derive(Clone, Copy)]
enum AnonAction {
    /// Write \\N
    Null,
    /// Pick a random value from this faker pool (index into fakers vec)
    Faker(usize),
    /// Pool contains {unique()} — replace with counter at write time
    FakerUnique(usize),
}

/// One anonymization rule for a column, resolved to an action
struct ColumnRule {
    /// Index into [`TsvWriter::conditions`]; `None` applies to every row
    condition: Option<usize>,
    action: AnonAction,
}

/// TSV writer for streaming table data
pub struct TsvWriter {
    /// Buffer for building rows
    buffer: Vec<u8>,
    /// Number of columns
    num_columns: usize,
    /// Pre-computed anonymization lookup: the rules for each column, in the
    /// order they are tried (conditional rules first, then the unconditional
    /// one). Empty = no anonymization.
    anon_by_column: Vec<Vec<ColumnRule>>,
    /// Distinct row conditions of the rules. Each is evaluated by the SELECT
    /// that reads the rows (see [`TsvWriter::select_list`]) and arrives as an
    /// extra column after the table's own, at `num_columns + index`.
    conditions: Vec<String>,
    /// Per-column flag: binary-typed columns are hex-encoded in the TSV stream
    /// (decoded with UNHEX() by the client's LOAD DATA)
    binary_by_column: Vec<bool>,
    /// Faker pools stored as pre-escaped byte slices for zero-copy writes
    faker_pools: Vec<Vec<Vec<u8>>>,
    /// Raw (unescaped) faker pools for unique mode — need raw strings to insert counter
    faker_pools_raw: Vec<Vec<String>>,
    /// RNG for faker selection
    rng: rand::rngs::ThreadRng,
    /// Counter for unique faker values (monotonically increasing per writer)
    unique_counter: u64,
}

impl TsvWriter {
    /// Create a new TSV writer
    pub fn new(
        column_defs: &[ColumnDef],
        anonymization: Vec<AnonymizeRule>,
        fakers: std::collections::HashMap<String, Vec<String>>,
    ) -> Self {
        let columns: Vec<String> = column_defs.iter().map(|c| c.name.clone()).collect();
        let binary_by_column: Vec<bool> =
            column_defs.iter().map(|c| c.is_binary_type()).collect();
        // Pre-escape all faker pool values into byte buffers
        let mut faker_pool_names: Vec<String> = Vec::new();
        let mut faker_pools: Vec<Vec<Vec<u8>>> = Vec::new();
        let mut faker_pools_raw: Vec<Vec<String>> = Vec::new();

        // Helper: find or create a faker pool index
        let get_or_create_pool = |faker_name: &str,
                                       faker_pool_names: &mut Vec<String>,
                                       faker_pools: &mut Vec<Vec<Vec<u8>>>,
                                       faker_pools_raw: &mut Vec<Vec<String>>|
         -> usize {
            if let Some(idx) = faker_pool_names.iter().position(|n| n == faker_name) {
                idx
            } else {
                let idx = faker_pool_names.len();
                faker_pool_names.push(faker_name.to_string());
                let raw_pool: Vec<String> = fakers
                    .get(faker_name)
                    .cloned()
                    .unwrap_or_default();
                // Pre-escape all values in the pool
                let escaped_pool: Vec<Vec<u8>> = raw_pool
                    .iter()
                    .map(|s| {
                        let mut buf = Vec::with_capacity(s.len());
                        escape_tsv_bytes(&mut buf, s.as_bytes());
                        buf
                    })
                    .collect();
                faker_pools.push(escaped_pool);
                faker_pools_raw.push(raw_pool);
                idx
            }
        };

        // Distinct conditions, in first-seen order
        let mut conditions: Vec<String> = Vec::new();
        for rule in &anonymization {
            if let Some(cond) = &rule.condition {
                if !conditions.contains(cond) {
                    conditions.push(cond.clone());
                }
            }
        }

        // Conditional rules are tried first (in declaration order) so they can
        // carve exceptions out of an unconditional rule for the same column
        let ordered: Vec<&AnonymizeRule> = anonymization
            .iter()
            .filter(|r| r.condition.is_some())
            .chain(anonymization.iter().filter(|r| r.condition.is_none()))
            .collect();

        // Build pre-computed anonymization lookup by column index
        let anon_by_column: Vec<Vec<ColumnRule>> = columns
            .iter()
            .map(|col_name| {
                ordered
                    .iter()
                    .filter(|r| &r.column == col_name)
                    .map(|rule| {
                        let action = match &rule.target {
                            AnonymizeTarget::Null => AnonAction::Null,
                            AnonymizeTarget::Faker(faker_name) => {
                                let pool_idx = get_or_create_pool(
                                    faker_name,
                                    &mut faker_pool_names,
                                    &mut faker_pools,
                                    &mut faker_pools_raw,
                                );
                                // Auto-detect: if any pool value contains {unique()}, use unique mode
                                let has_unique = faker_pools_raw[pool_idx]
                                    .iter()
                                    .any(|v| v.contains(UNIQUE_SENTINEL));
                                if has_unique {
                                    AnonAction::FakerUnique(pool_idx)
                                } else {
                                    AnonAction::Faker(pool_idx)
                                }
                            }
                        };
                        ColumnRule {
                            condition: rule
                                .condition
                                .as_ref()
                                .and_then(|c| conditions.iter().position(|x| x == c)),
                            action,
                        }
                    })
                    .collect()
            })
            .collect();

        let num_columns = columns.len();

        let mut buffer = Vec::with_capacity(64 * 1024); // 64KB buffer
        buffer.resize(RAW_CHUNK_HEADER_LEN, 0); // preallocate frame header

        Self {
            buffer,
            num_columns,
            anon_by_column,
            conditions,
            binary_by_column,
            faker_pools,
            faker_pools_raw,
            rng: rand::thread_rng(),
            unique_counter: 0,
        }
    }

    /// The select list for reading this table's rows: `*`, followed by one
    /// `(<condition>) IS TRUE` flag per row condition. `IS TRUE` makes a NULL
    /// result count as false, like a WHERE clause would.
    pub fn select_list(&self) -> String {
        select_list(&self.conditions)
    }

    /// Write a row to the internal buffer
    pub fn write_row(&mut self, row: &Row) -> Result<()> {
        use rand::seq::SliceRandom;

        for i in 0..self.num_columns {
            if i > 0 {
                self.buffer.push(b'\t');
            }

            let is_binary = self.binary_by_column[i];
            let num_columns = self.num_columns;
            let action = pick_action(&self.anon_by_column[i], |cond| {
                flag_is_true(row.as_ref(num_columns + cond))
            });

            match action {
                Some(AnonAction::Null) => {
                    self.buffer.extend_from_slice(b"\\N");
                }
                Some(AnonAction::Faker(pool_idx)) => {
                    if is_binary {
                        // Binary columns carry hex on the wire, so hex-encode
                        // the raw faker value instead of the escaped one
                        let pool = &self.faker_pools_raw[pool_idx];
                        if let Some(value) = pool.choose(&mut self.rng) {
                            self.buffer
                                .extend_from_slice(hex::encode(value.as_bytes()).as_bytes());
                        } else {
                            self.buffer.extend_from_slice(b"\\N");
                        }
                    } else {
                        let pool = &self.faker_pools[pool_idx];
                        if let Some(value) = pool.choose(&mut self.rng) {
                            self.buffer.extend_from_slice(value);
                        } else {
                            self.buffer.extend_from_slice(b"\\N");
                        }
                    }
                }
                Some(AnonAction::FakerUnique(pool_idx)) => {
                    let pool = &self.faker_pools_raw[pool_idx];
                    if pool.is_empty() {
                        self.buffer.extend_from_slice(b"\\N");
                    } else {
                        let base = &pool[self.unique_counter as usize % pool.len()];
                        let counter_str = self.unique_counter.to_string();
                        let unique_value = base.replace(UNIQUE_SENTINEL, &counter_str);
                        if is_binary {
                            self.buffer
                                .extend_from_slice(hex::encode(unique_value.as_bytes()).as_bytes());
                        } else {
                            escape_tsv_bytes(&mut self.buffer, unique_value.as_bytes());
                        }
                        self.unique_counter += 1;
                    }
                }
                None => {
                    // Direct index access — no column name lookup, no cloning
                    match row.as_ref(i) {
                        Some(MySqlValue::Bytes(b)) if is_binary => {
                            // Hex-encode so arbitrary bytes survive the TSV
                            // stream; the client loads with UNHEX()
                            self.buffer.extend_from_slice(hex::encode(b).as_bytes());
                        }
                        Some(val) => write_tsv_value(&mut self.buffer, val),
                        None => self.buffer.extend_from_slice(b"\\N"),
                    }
                }
            }
        }

        self.buffer.push(b'\n');
        Ok(())
    }

    /// Finalize the buffer as a wire-ready data chunk frame.
    ///
    /// Fills in the preallocated header (`u32 flagged_len || u16 table_id || u16 row_count`),
    /// optionally compresses the TSV payload, and returns the complete frame.
    ///
    /// For uncompressed data this is zero-copy — the header is written in-place.
    /// For zstd the TSV portion is compressed into a new buffer.
    pub fn take_encoded_chunk(
        &mut self,
        table_id: u16,
        row_count: u16,
        compression: CompressionMode,
    ) -> Result<Vec<u8>> {
        let tsv_start = RAW_CHUNK_HEADER_LEN;
        let tsv_len = self.buffer.len() - tsv_start;

        let frame = match compression {
            CompressionMode::None | CompressionMode::Auto => {
                // Fill in the preallocated header in-place — zero copy
                let payload_len = 2 + 2 + tsv_len;
                let flagged_len = (payload_len as u32) | RAW_CHUNK_FLAG;
                self.buffer[0..4].copy_from_slice(&flagged_len.to_le_bytes());
                self.buffer[4..6].copy_from_slice(&table_id.to_le_bytes());
                self.buffer[6..8].copy_from_slice(&row_count.to_le_bytes());

                std::mem::replace(&mut self.buffer, Vec::with_capacity(64 * 1024))
            }
            CompressionMode::Zstd => {
                let tsv_data = &self.buffer[tsv_start..];
                // Compression was negotiated stream-wide; there is no per-chunk
                // flag, so falling back to an uncompressed chunk here would
                // silently corrupt the stream. Fail instead.
                let compressed = zstd::encode_all(tsv_data, 3).map_err(|e| {
                    ServerError::Protocol(format!("zstd compression failed: {}", e))
                })?;

                // Build: header + u32 original_len + compressed data
                let compressed_payload_len = 4 + compressed.len();
                let payload_len = 2 + 2 + compressed_payload_len;
                let flagged_len = (payload_len as u32) | RAW_CHUNK_FLAG;

                let mut frame = Vec::with_capacity(4 + payload_len);
                frame.extend_from_slice(&flagged_len.to_le_bytes());
                frame.extend_from_slice(&table_id.to_le_bytes());
                frame.extend_from_slice(&row_count.to_le_bytes());
                frame.extend_from_slice(&(tsv_len as u32).to_le_bytes());
                frame.extend(compressed);

                // Reuse the existing allocation
                self.buffer.clear();
                frame
            }
        };

        self.buffer.resize(RAW_CHUNK_HEADER_LEN, 0);
        Ok(frame)
    }

    /// Get the current TSV data size (excluding the preallocated frame header).
    pub fn buffer_size(&self) -> usize {
        self.buffer.len() - RAW_CHUNK_HEADER_LEN
    }

    /// Check if no TSV data has been written (header-only).
    pub fn is_empty(&self) -> bool {
        self.buffer.len() <= RAW_CHUNK_HEADER_LEN
    }
}

/// `*` plus one `(<condition>) IS TRUE` flag column per condition.
fn select_list(conditions: &[String]) -> String {
    let mut list = String::from("*");
    for cond in conditions {
        list.push_str(", (");
        list.push_str(cond);
        list.push_str(") IS TRUE");
    }
    list
}

/// The first rule whose condition holds (an unconditional rule always does).
fn pick_action(rules: &[ColumnRule], flag: impl Fn(usize) -> bool) -> Option<AnonAction> {
    rules
        .iter()
        .find(|r| r.condition.is_none_or(&flag))
        .map(|r| r.action)
}

/// Read a condition flag column. The text protocol (`query_iter`) returns it
/// as bytes, the binary protocol (`exec_iter`) as an integer.
fn flag_is_true(value: Option<&MySqlValue>) -> bool {
    match value {
        Some(MySqlValue::Int(i)) => *i != 0,
        Some(MySqlValue::UInt(u)) => *u != 0,
        Some(MySqlValue::Bytes(b)) => !b.is_empty() && b.as_slice() != b"0",
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(condition: Option<usize>, action: AnonAction) -> ColumnRule {
        ColumnRule { condition, action }
    }

    #[test]
    fn select_list_appends_one_flag_per_condition() {
        assert_eq!(select_list(&[]), "*");
        assert_eq!(
            select_list(&["path LIKE 'payment/%'".to_string(), "id = 1".to_string()]),
            "*, (path LIKE 'payment/%') IS TRUE, (id = 1) IS TRUE"
        );
    }

    #[test]
    fn conditional_rule_applies_only_when_its_flag_is_set() {
        let rules = [rule(Some(0), AnonAction::Null)];
        assert!(matches!(
            pick_action(&rules, |_| true),
            Some(AnonAction::Null)
        ));
        assert!(pick_action(&rules, |_| false).is_none());
    }

    #[test]
    fn conditional_rule_overrides_unconditional_fallback() {
        // Ordered as TsvWriter::new orders them: conditional first
        let rules = [
            rule(Some(0), AnonAction::Null),
            rule(None, AnonAction::Faker(3)),
        ];
        assert!(matches!(
            pick_action(&rules, |_| true),
            Some(AnonAction::Null)
        ));
        assert!(matches!(
            pick_action(&rules, |_| false),
            Some(AnonAction::Faker(3))
        ));
    }

    #[test]
    fn flags_from_both_protocols() {
        assert!(flag_is_true(Some(&MySqlValue::Int(1))));
        assert!(flag_is_true(Some(&MySqlValue::Bytes(b"1".to_vec()))));
        assert!(!flag_is_true(Some(&MySqlValue::Int(0))));
        assert!(!flag_is_true(Some(&MySqlValue::Bytes(b"0".to_vec()))));
        assert!(!flag_is_true(Some(&MySqlValue::NULL)));
        assert!(!flag_is_true(None));
    }

    #[test]
    fn writer_orders_conditional_rules_first_and_dedupes_conditions() {
        let cols = vec![ColumnDef {
            name: "value".into(),
            type_name: "TEXT".into(),
            full_type: "text".into(),
            max_length: None,
            nullable: true,
            is_primary_key: false,
            charset: None,
            collation: None,
            flags: Default::default(),
        }];
        let rules = vec![
            AnonymizeRule {
                column: "value".into(),
                target: AnonymizeTarget::Faker("x".into()),
                condition: None,
            },
            AnonymizeRule {
                column: "value".into(),
                target: AnonymizeTarget::Null,
                condition: Some("a = 1".into()),
            },
            AnonymizeRule {
                column: "other".into(),
                target: AnonymizeTarget::Null,
                condition: Some("a = 1".into()),
            },
        ];
        let w = TsvWriter::new(&cols, rules, Default::default());
        assert_eq!(w.select_list(), "*, (a = 1) IS TRUE");
        let value_rules = &w.anon_by_column[0];
        assert_eq!(value_rules.len(), 2);
        assert_eq!(value_rules[0].condition, Some(0));
        assert!(matches!(value_rules[0].action, AnonAction::Null));
        assert_eq!(value_rules[1].condition, None);
    }
}
