//! Protocol message types for client-server communication

use bincode::{Decode, Encode};

use crate::checkpoint::Checkpoint;
use crate::plan::{ColumnDef, CompressionMode, ExecutionPlan, TableInfo};

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
    },
    /// Error occurred
    Error { message: String, recoverable: bool },
}
