//! TSV (Tab-Separated Values) formatting for MySQL LOAD DATA INFILE

use mysql::{Row, Value as MySqlValue};

use crate::error::Result;
use crate::mysql::{escape_tsv_string, mysql_value_to_tsv};
use jibs_protocol::{AnonymizeRule, AnonymizeTarget};

/// TSV writer for streaming table data
pub struct TsvWriter {
    /// Buffer for building rows
    buffer: Vec<u8>,
    /// Column names in order
    columns: Vec<String>,
    /// Anonymization rules for this table
    anonymization: Vec<AnonymizeRule>,
    /// Faker pools for anonymization
    fakers: std::collections::HashMap<String, Vec<String>>,
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
        Self {
            buffer: Vec::with_capacity(64 * 1024), // 64KB buffer
            columns,
            anonymization,
            fakers,
            rng: rand::thread_rng(),
        }
    }

    /// Write a row to the internal buffer
    pub fn write_row(&mut self, row: &Row) -> Result<()> {
        use rand::seq::SliceRandom;

        for (i, col_name) in self.columns.iter().enumerate() {
            if i > 0 {
                self.buffer.push(b'\t');
            }

            // Check if this column should be anonymized
            let anonymized_value = self
                .anonymization
                .iter()
                .find(|r| &r.column == col_name)
                .map(|rule| match &rule.target {
                    AnonymizeTarget::Null => "\\N".to_string(),
                    AnonymizeTarget::Faker(faker_name) => {
                        if let Some(pool) = self.fakers.get(faker_name) {
                            pool.choose(&mut self.rng)
                                .map(|s| escape_tsv_string(s))
                                .unwrap_or_else(|| "\\N".to_string())
                        } else {
                            "\\N".to_string()
                        }
                    }
                });

            let value = if let Some(anon) = anonymized_value {
                anon
            } else {
                // Get the actual value from the row
                let mysql_value: Option<MySqlValue> = row.get_opt(col_name as &str).and_then(|r| r.ok());
                match mysql_value {
                    Some(v) => mysql_value_to_tsv(&v),
                    None => "\\N".to_string(),
                }
            };

            self.buffer.extend_from_slice(value.as_bytes());
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

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests would require mock MySQL rows
    // which is complex to set up without a real connection
}
