//! Protocol message types for client-server communication

use bincode::{Decode, Encode};

use crate::checkpoint::Checkpoint;
use crate::plan::{ColumnDef, CompressionMode, ExecutionPlan, TableInfo};

/// Server-side performance metrics collected during import
#[derive(Debug, Clone, Default, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ServerMetrics {
    /// Time spent executing MySQL queries (ms)
    pub query_time_ms: u64,
    /// Time spent iterating over result rows (ms)
    pub iterate_time_ms: u64,
    /// Time spent serializing rows to TSV (ms)
    pub serialize_time_ms: u64,
    /// Time spent writing to stdout (ms) - high values indicate client backpressure
    pub write_time_ms: u64,
    /// Total rows sent
    pub rows_sent: u64,
    /// Total bytes sent (before compression)
    pub bytes_sent: u64,
}

/// Messages sent from client to server
#[derive(Debug, Clone, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ClientMessage {
    /// Credentials message - sent before Init to provide MySQL connection URL
    /// This is sent separately to avoid exposing credentials in process listings
    Credentials {
        /// MySQL connection URL for the remote database
        mysql_url: String,
    },
    /// Initial handshake with full execution plan
    Init {
        plan: ExecutionPlan,
        compression: CompressionMode,
        /// Server-side parallelism for full table streaming (0 or 1 = sequential)
        parallel: u32,
        /// Whether to collect detailed timing metrics
        collect_metrics: bool,
    },
    /// Start streaming data (optionally resume from checkpoint)
    Start {
        resume_from: Option<Checkpoint>,
    },
    /// Acknowledge receipt of chunk (for flow control)
    Ack { checkpoint: Checkpoint },
    /// Graceful shutdown
    Shutdown,
}

/// Messages sent from server to client
#[derive(Debug, Clone, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ServerMessage {
    /// Server ready, reports discovered tables and estimated row counts
    Ready {
        tables: Vec<TableInfo>,
        compression: CompressionMode,
    },
    /// Schema for a table (sent before first data chunk)
    Schema {
        table: String,
        columns: Vec<ColumnDef>,
    },
    /// Data chunk with rows in TSV format
    Data {
        table: String,
        row_count: u32,
        tsv_data: Vec<u8>,
        checkpoint: Checkpoint,
    },
    /// Table fully transferred
    TableDone { table: String, row_count: u64 },
    /// All data transferred
    Done {
        /// Tables that were imported via aggregate BFS (partial imports)
        aggregate_tables: Vec<String>,
        /// Performance metrics (only populated if --metrics flag was used)
        metrics: Option<ServerMetrics>,
    },
    /// Error occurred
    Error { message: String, recoverable: bool },
}
