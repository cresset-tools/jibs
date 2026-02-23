//! TSV (Tab-Separated Values) formatting for MySQL LOAD DATA INFILE

use mysql::Row;

use crate::error::Result;
use crate::mysql::{escape_tsv_bytes, write_tsv_value};
use jibs_protocol::{AnonymizeRule, AnonymizeTarget, CompressionMode, RAW_CHUNK_FLAG, RAW_CHUNK_HEADER_LEN};

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

        let mut buffer = Vec::with_capacity(64 * 1024); // 64KB buffer
        buffer.resize(RAW_CHUNK_HEADER_LEN, 0); // preallocate frame header

        Self {
            buffer,
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
    ) -> Vec<u8> {
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
                let compressed = match zstd::encode_all(tsv_data, 3) {
                    Ok(c) => c,
                    Err(_) => {
                        // Fallback: send uncompressed
                        let payload_len = 2 + 2 + tsv_len;
                        let flagged_len = (payload_len as u32) | RAW_CHUNK_FLAG;
                        self.buffer[0..4].copy_from_slice(&flagged_len.to_le_bytes());
                        self.buffer[4..6].copy_from_slice(&table_id.to_le_bytes());
                        self.buffer[6..8].copy_from_slice(&row_count.to_le_bytes());
                        let frame =
                            std::mem::replace(&mut self.buffer, Vec::with_capacity(64 * 1024));
                        self.buffer.resize(RAW_CHUNK_HEADER_LEN, 0);
                        return frame;
                    }
                };

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
        frame
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
