//! Checkpoint types for resumable transfers

use bincode::{Decode, Encode};
use std::collections::HashSet;

/// A checkpoint for resuming an interrupted transfer
///
/// Simple model: tracks completed tables only.
/// If interrupted mid-table, that table is re-imported on resume.
#[derive(Debug, Clone, Default, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Checkpoint {
    /// Tables that have been fully transferred
    pub completed_tables: HashSet<String>,
    /// Table currently being transferred (if any)
    pub current_table: Option<String>,
    /// Total rows transferred so far
    pub rows_transferred: u64,
    /// Total bytes transferred so far
    pub bytes_transferred: u64,
}

impl Checkpoint {
    /// Create a new empty checkpoint
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark a table as complete
    pub fn complete_table(&mut self, table: impl Into<String>) {
        let table = table.into();
        if self.current_table.as_ref() == Some(&table) {
            self.current_table = None;
        }
        self.completed_tables.insert(table);
    }

    /// Mark a table as in progress
    pub fn start_table(&mut self, table: impl Into<String>) {
        self.current_table = Some(table.into());
    }

    /// Check if a table has been completed
    pub fn is_table_complete(&self, table: &str) -> bool {
        self.completed_tables.contains(table)
    }

    /// Check if a table should be skipped on resume
    pub fn should_skip_table(&self, table: &str) -> bool {
        self.completed_tables.contains(table)
    }
}

/// Import session metadata for checkpoint persistence
#[derive(Debug, Clone, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ImportSession {
    /// Unique import ID (hash of host + plan + timestamp)
    pub import_id: String,
    /// Remote host
    pub remote_host: String,
    /// Hash of the execution plan (to detect plan changes)
    pub plan_hash: String,
    /// When the import started (unix timestamp)
    pub started_at: u64,
    /// Current checkpoint
    pub checkpoint: Checkpoint,
}

impl ImportSession {
    /// Create a new import session
    pub fn new(
        import_id: impl Into<String>,
        remote_host: impl Into<String>,
        plan_hash: impl Into<String>,
    ) -> Self {
        Self {
            import_id: import_id.into(),
            remote_host: remote_host.into(),
            plan_hash: plan_hash.into(),
            started_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            checkpoint: Checkpoint::new(),
        }
    }
}
