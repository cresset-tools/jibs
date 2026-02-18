//! Protocol message types for client-server communication

use bincode::{Decode, Encode};

use crate::checkpoint::Checkpoint;
use crate::plan::{ColumnDef, CompressionMode, ExecutionPlan, TableInfo};

/// Timing for a single query executed during aggregate BFS traversal
#[derive(Debug, Clone, Default, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct QueryTiming {
    /// Table queried
    pub table: String,
    /// Column used in WHERE/IN clause (empty for root queries)
    pub column: String,
    /// Number of values in IN clause (0 for root queries)
    pub num_values: u32,
    /// Time spent executing the query (ms)
    pub query_ms: u64,
    /// Time spent iterating over result rows (ms)
    pub iterate_ms: u64,
    /// Number of rows returned
    pub rows: u64,
}

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
    /// Time spent on dedup and FK extraction during aggregate BFS (ms)
    pub dedup_time_ms: u64,
    /// Wall-clock time for aggregate BFS traversal (Phase 1) in ms
    pub aggregate_wall_ms: u64,
    /// Wall-clock time for full table streaming (Phase 2) in ms
    pub full_tables_wall_ms: u64,
    /// Serialize time during aggregate phase only (ms)
    pub aggregate_serialize_ms: u64,
    /// Write time during aggregate phase only (ms)
    pub aggregate_write_ms: u64,
    /// Time spent on zstd compression (ms)
    pub compress_time_ms: u64,
    /// Compression time during aggregate phase only (ms)
    pub aggregate_compress_ms: u64,
    /// Time spent pre-caching table schemas (ms)
    pub schema_cache_time_ms: u64,
    /// Per-query timing for aggregate BFS queries
    pub query_timings: Vec<QueryTiming>,
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
