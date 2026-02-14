//! Checkpoint types for resumable transfers

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A checkpoint for resuming an interrupted transfer
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct Checkpoint {
    /// Name of the aggregate being transferred
    pub aggregate: String,
    /// For each table: last processed primary key value(s)
    /// Key: table name
    /// Value: serialized PK value(s) for resume position
    pub table_positions: HashMap<String, Vec<u8>>,
    /// Tables that have been fully transferred
    pub completed_tables: Vec<String>,
    /// Total rows transferred so far
    pub rows_transferred: u64,
    /// Total bytes transferred so far
    pub bytes_transferred: u64,
}

impl Checkpoint {
    /// Create a new checkpoint for an aggregate
    pub fn new(aggregate: impl Into<String>) -> Self {
        Self {
            aggregate: aggregate.into(),
            table_positions: HashMap::new(),
            completed_tables: Vec::new(),
            rows_transferred: 0,
            bytes_transferred: 0,
        }
    }

    /// Mark a table as complete
    pub fn complete_table(&mut self, table: impl Into<String>) {
        let table = table.into();
        self.table_positions.remove(&table);
        if !self.completed_tables.contains(&table) {
            self.completed_tables.push(table);
        }
    }

    /// Update the position for a table
    pub fn update_position(&mut self, table: impl Into<String>, pk_bytes: Vec<u8>) {
        self.table_positions.insert(table.into(), pk_bytes);
    }

    /// Check if a table has been completed
    pub fn is_table_complete(&self, table: &str) -> bool {
        self.completed_tables.contains(&table.to_string())
    }

    /// Get the resume position for a table, if any
    pub fn get_position(&self, table: &str) -> Option<&[u8]> {
        self.table_positions.get(table).map(|v| v.as_slice())
    }
}

/// Import session metadata for checkpoint persistence
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ImportSession {
    /// Unique import ID (hash of host + plan + timestamp)
    pub import_id: String,
    /// Remote host
    pub remote_host: String,
    /// Hash of the execution plan
    pub plan_hash: String,
    /// When the import started
    pub started_at: u64,
    /// Current checkpoint
    pub checkpoint: Checkpoint,
}
