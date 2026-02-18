//! TSV (Tab-Separated Values) formatting for MySQL LOAD DATA INFILE

use mysql::Row;

use crate::error::Result;
use crate::mysql::{escape_tsv_bytes, write_tsv_value};
use jibs_protocol::{AnonymizeRule, AnonymizeTarget};

/// Pre-computed anonymization action for a column
enum AnonAction {
    /// Write \\N
    Null,
    /// Pick a random value from this faker pool (index into fakers vec)
    Faker(usize),
}

/// TSV writer for streaming table data
pub struct TsvWriter {
    /// Buffer for building rows
    buffer: Vec<u8>,
    /// Number of columns
    num_columns: usize,
    /// Pre-computed anonymization lookup: one entry per column, None = no anonymization
    anon_by_column: Vec<Option<AnonAction>>,
    /// Faker pools stored as pre-escaped byte slices for zero-copy writes
    faker_pools: Vec<Vec<Vec<u8>>>,
    /// RNG for faker selection
    rng: rand::rngs::ThreadRng,
}

impl TsvWriter {
    /// Create a new TSV writer
    pub fn new(
        columns: Vec<String>,
        anonymization: Vec<AnonymizeRule>,
        fakers: std::collections::HashMap<String, Vec<String>>,
    ) -> Self {
        // Pre-escape all faker pool values into byte buffers
        let mut faker_pool_names: Vec<String> = Vec::new();
        let mut faker_pools: Vec<Vec<Vec<u8>>> = Vec::new();

        // Build pre-computed anonymization lookup by column index
        let anon_by_column: Vec<Option<AnonAction>> = columns
            .iter()
            .map(|col_name| {
                anonymization
                    .iter()
                    .find(|r| &r.column == col_name)
                    .map(|rule| match &rule.target {
                        AnonymizeTarget::Null => AnonAction::Null,
                        AnonymizeTarget::Faker(faker_name) => {
                            // Find or create the faker pool index
                            let pool_idx =
                                if let Some(idx) = faker_pool_names.iter().position(|n| n == faker_name) {
                                    idx
                                } else {
                                    let idx = faker_pool_names.len();
                                    faker_pool_names.push(faker_name.clone());
                                    // Pre-escape all values in the pool
                                    let escaped_pool: Vec<Vec<u8>> = fakers
                                        .get(faker_name)
                                        .map(|pool| {
                                            pool.iter()
                                                .map(|s| {
                                                    let mut buf = Vec::with_capacity(s.len());
                                                    escape_tsv_bytes(&mut buf, s.as_bytes());
                                                    buf
                                                })
                                                .collect()
                                        })
                                        .unwrap_or_default();
                                    faker_pools.push(escaped_pool);
                                    idx
                                };
                            AnonAction::Faker(pool_idx)
                        }
                    })
            })
            .collect();

        let num_columns = columns.len();

        Self {
            buffer: Vec::with_capacity(64 * 1024), // 64KB buffer
            num_columns,
            anon_by_column,
            faker_pools,
            rng: rand::thread_rng(),
        }
    }

    /// Write a row to the internal buffer
    pub fn write_row(&mut self, row: &Row) -> Result<()> {
        use rand::seq::SliceRandom;

        for i in 0..self.num_columns {
            if i > 0 {
                self.buffer.push(b'\t');
            }

            match &self.anon_by_column[i] {
                Some(AnonAction::Null) => {
                    self.buffer.extend_from_slice(b"\\N");
                }
                Some(AnonAction::Faker(pool_idx)) => {
                    let pool = &self.faker_pools[*pool_idx];
                    if let Some(value) = pool.choose(&mut self.rng) {
                        self.buffer.extend_from_slice(value);
                    } else {
                        self.buffer.extend_from_slice(b"\\N");
                    }
                }
                None => {
                    // Direct index access — no column name lookup, no cloning
                    match row.as_ref(i) {
                        Some(val) => write_tsv_value(&mut self.buffer, val),
                        None => self.buffer.extend_from_slice(b"\\N"),
                    }
                }
            }
        }

        self.buffer.push(b'\n');
        Ok(())
    }

    /// Get the current buffer contents and clear it
    pub fn take_buffer(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buffer)
    }

    /// Get the current buffer size
    pub fn buffer_size(&self) -> usize {
        self.buffer.len()
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}
